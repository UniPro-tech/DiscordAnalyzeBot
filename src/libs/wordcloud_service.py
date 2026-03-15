from calendar import monthrange
from datetime import datetime, timedelta, timezone
import io
from typing import Optional
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pymongo import UpdateOne
from zoneinfo import ZoneInfo

from libs.visualization_common import resolve_font_path

from libs.text_processing import (
    STOP_WORDS,
    apply_learned_compounds,
    clear_extract_tokens_cache,
    compute_pmi,
    extract_tokens,
    extract_tokens_with_indices,
    join_message_content,
    normalize_text,
)
from libs.message_store import fetch_messages
from libs.meta_store import delete_meta_key, get_meta_value, set_meta_value
from libs.settings_store import update_schedule_last_executed
import matplotlib.pyplot as plt
from wordcloud import WordCloud


PMI_THRESHOLD = 3
COUNT_THRESHOLD = 10
DEFAULT_MESSAGE_LIMIT = 3000
JST = ZoneInfo("Asia/Tokyo")
LEARN_CURSOR_META_KEY = "last_learn_cursor"
LEGACY_LEARN_ID_META_KEY = "last_learn_id"


def _is_clickhouse(db) -> bool:
    return getattr(db, "backend", "mongo") in {"clickhouse", "hybrid"}


def _learning_db(db):
    if getattr(db, "backend", "mongo") == "hybrid":
        return db.db_clickhouse
    return db


def setup_learning_tables(db) -> None:
    if not _is_clickhouse(db):
        return

    db = _learning_db(db)

    db.command(
        """
        CREATE TABLE IF NOT EXISTS unigrams (
            word String,
            count UInt64
        )
        ENGINE = SummingMergeTree
        ORDER BY (word)
        """
    )

    db.command(
        """
        CREATE TABLE IF NOT EXISTS ngrams (
            ngram Array(String),
            count UInt64
        )
        ENGINE = SummingMergeTree
        ORDER BY (ngram)
        """
    )

    db.command(
        """
        CREATE TABLE IF NOT EXISTS compounds (
            word String,
            pmi Float64,
            updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (word)
        """
    )


def parse_during_days(during: Optional[str]) -> int | None:
    if during is None:
        return None

    during_days = int(during)

    if during_days <= 0:
        raise ValueError("during must be positive")

    return during_days


def parse_period_days(period: Optional[str]) -> int | None:
    # Backward-compatible wrapper for callers still using the old name.
    return parse_during_days(period)


def build_during_since_timestamp(during_days: int, *, tz=JST) -> str:
    if during_days <= 0:
        raise ValueError("during must be positive")

    now_local = discord_utcnow().astimezone(tz)
    since_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=during_days - 1
    )
    return since_local.astimezone(timezone.utc).isoformat()


def build_wordcloud_message_query(
    guild_id: str,
    *,
    during_days: int | None = None,
    user_id: str | None = None,
    channel_id: str | None = None,
    role_id: str | None = None,
) -> dict:
    query = {
        "guild_id": guild_id,
        "content": {"$type": "string", "$ne": ""},
        # tokens が存在するメッセージのみを対象にする（トークン化済みのみ取得）
        "tokens": {"$exists": True},
    }

    if during_days is not None:
        query["timestamp"] = {
            "$gte": build_during_since_timestamp(during_days)
        }

    if user_id is not None:
        query["user_id"] = user_id

    if channel_id is not None:
        query["channel_id"] = channel_id

    if role_id is not None:
        query["role_ids"] = {"$in": [role_id]}

    return query


def discord_utcnow() -> datetime:
    from discord.utils import utcnow

    return utcnow()


def fetch_wordcloud_documents(
    db,
    guild_id: str,
    *,
    during_days: int | None = None,
    user_id: str | None = None,
    channel_id: str | None = None,
    role_id: str | None = None,
    limit: int = DEFAULT_MESSAGE_LIMIT,
) -> list[dict]:
    query = build_wordcloud_message_query(
        guild_id,
        during_days=during_days,
        user_id=user_id,
        channel_id=channel_id,
        role_id=role_id,
    )
    return fetch_messages(
        db,
        query,
        {"content": 1, "tokens": 1},
        sort_field="timestamp",
        sort_order=-1,
        limit=limit,
    )


def save_unigram(db, token: str) -> None:
    if _is_clickhouse(db):
        db = _learning_db(db)
        db.insert_rows("unigrams", [[token, 1]], ["word", "count"])
        return

    db.unigrams.update_one(
        {"word": token},
        {"$inc": {"count": 1}},
        upsert=True,
    )


def save_ngram(db, ngram: tuple[str, ...]) -> None:
    if _is_clickhouse(db):
        db = _learning_db(db)
        db.insert_rows("ngrams", [[[str(word) for word in ngram], 1]], ["ngram", "count"])
        return

    db.ngrams.update_one(
        {"ngram": list(ngram)},
        {"$inc": {"count": 1}},
        upsert=True,
    )


def get_total_unigram_count(db) -> int:
    if _is_clickhouse(db):
        db = _learning_db(db)
        return int(db.query_scalar("SELECT sum(count) AS total FROM unigrams") or 0)

    result = db.unigrams.aggregate(
        [{"$group": {"_id": None, "total": {"$sum": "$count"}}}]
    )
    doc = next(result, None)

    if doc is None:
        return 0

    return doc["total"]


def learn_from_text(db, text: str) -> None:
    token_entries = extract_tokens_with_indices(text)
    tokens = [word for word, _ in token_entries]

    # 同一メッセージ内での繰り返しスパムを抑制するため、1トークンにつき1回だけ学習する。
    for token in set(tokens):
        save_unigram(db, token)

    for ngram_size in (2, 3):
        for index in range(len(token_entries) - ngram_size + 1):
            window = token_entries[index : index + ngram_size]

            # Keep only truly adjacent tokens in original text.
            if not all(
                window[pos + 1][1] - window[pos][1] == 1
                for pos in range(len(window) - 1)
            ):
                continue

            # 同じ単語が繰り返されるスパムngramはスキップ。
            ngram_words = [word for word, _ in window]
            if len(set(ngram_words)) < len(ngram_words):
                continue

            save_ngram(db, tuple(ngram_words))


def _count_tokens_for_text(text: str) -> tuple[Counter, Counter]:
    """ngramのPMI計算のために、テキストからunigramとngramの頻度を数える。DBアクセスは伴わないので、並列化して高速化する。"""
    unigram_counter: Counter = Counter()
    ngram_counter: Counter = Counter()

    token_entries = extract_tokens_with_indices(text)
    tokens = [word for word, _ in token_entries]

    for token in set(tokens):
        unigram_counter[token] += 1

    for ngram_size in (2, 3):
        for index in range(len(token_entries) - ngram_size + 1):
            window = token_entries[index : index + ngram_size]

            if not all(
                window[pos + 1][1] - window[pos][1] == 1
                for pos in range(len(window) - 1)
            ):
                continue

            ngram_words = [word for word, _ in window]
            if len(set(ngram_words)) < len(ngram_words):
                continue

            ngram_counter[tuple(ngram_words)] += 1

    return unigram_counter, ngram_counter


def learn_from_texts(db, texts: list[str], workers: int = 4) -> None:
    """
    ある程度の量のテキストを学習する際に、1テキストずつDBに更新をかけるとオーバーヘッドが大きいので、ある程度まとめて集計してから一括で更新する。
    """
    if not texts:
        return

    # CPU負荷の高いトークン化とPMI計算は並列化して高速化する。DBへの更新は一括で行うため、スレッドセーフな集計を行うためにCounterを使用する。
    # multiprocessing.Poolも試したが、pickleの制約でDBクライアントを渡せないため、ThreadPoolExecutorで代替する。
    unigram_agg: Counter = Counter()
    ngram_agg: Counter = Counter()

    def _aggregate_sequential() -> None:
        for text in texts:
            u_cnt, n_cnt = _count_tokens_for_text(text)
            unigram_agg.update(u_cnt)
            ngram_agg.update(n_cnt)

    if workers <= 1:
        _aggregate_sequential()
    else:
        try:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                for u_cnt, n_cnt in ex.map(_count_tokens_for_text, texts):
                    unigram_agg.update(u_cnt)
                    ngram_agg.update(n_cnt)
        except RuntimeError as error:
            # SudachiPy tokenizerはスレッドセーフではないため、環境によっては
            # 並列実行時に "Already borrowed" が発生する。その場合は逐次処理へフォールバックする。
            if "Already borrowed" not in str(error):
                raise
            _aggregate_sequential()

    if _is_clickhouse(db):
        db = _learning_db(db)
        unigram_rows = [[token, int(cnt)] for token, cnt in unigram_agg.items()]
        ngram_rows = [[[str(word) for word in ngram], int(cnt)] for ngram, cnt in ngram_agg.items()]

        if unigram_rows:
            db.insert_rows("unigrams", unigram_rows, ["word", "count"])

        if ngram_rows:
            db.insert_rows("ngrams", ngram_rows, ["ngram", "count"])

        return

    # 一括更新のためのUpdateOneオペレーションを作成して、DBに反映する。
    unigram_ops = [
        UpdateOne({"word": token}, {"$inc": {"count": cnt}}, upsert=True)
        for token, cnt in unigram_agg.items()
    ]
    ngram_ops = [
        UpdateOne({"ngram": list(ngram)}, {"$inc": {"count": cnt}}, upsert=True)
        for ngram, cnt in ngram_agg.items()
    ]

    if unigram_ops:
        db.unigrams.bulk_write(unigram_ops, ordered=False)

    if ngram_ops:
        db.ngrams.bulk_write(ngram_ops, ordered=False)


def _compute_bigram_pmi(db, left_word: str, right_word: str, total: int) -> float | None:
    bigram_doc = db.ngrams.find_one({"ngram": [left_word, right_word]})

    if not bigram_doc:
        return None

    count_xy = bigram_doc["count"]

    if count_xy < COUNT_THRESHOLD:
        return None

    unigram_x = db.unigrams.find_one({"word": left_word})
    unigram_y = db.unigrams.find_one({"word": right_word})

    if not unigram_x or not unigram_y:
        return None

    return compute_pmi(
        count_xy,
        unigram_x["count"],
        unigram_y["count"],
        total,
    )


def update_compounds(db) -> None:
    if _is_clickhouse(db):
        _update_compounds_clickhouse(_learning_db(db))
        clear_extract_tokens_cache()
        return

    # 全unigramの総数を取得。PMI計算で参照するため。
    total = get_total_unigram_count(db)

    if total == 0:
        return

    # 全unigramをロードして、PMI計算で頻度参照するための辞書を作る。
    unigram_counts: dict[str, int] = {
        doc["word"]: doc["count"] for doc in db.unigrams.find({}, {"word": 1, "count": 1})
    }

    # ngramを全件ロードして、PMI計算して、条件を満たすものをDBに保存する。
    ngram_docs = list(db.ngrams.find({}))

    accepted_bigrams: dict[tuple[str, str], float] = {}

    def _process_ngram_doc(doc):
        ngram_words = doc["ngram"]
        count_xy = doc["count"]
        ngram_size = len(ngram_words)

        if count_xy < COUNT_THRESHOLD:
            return None

        if ngram_size == 2:
            w1, w2 = ngram_words
            ux = unigram_counts.get(w1)
            uy = unigram_counts.get(w2)
            if ux is None or uy is None:
                return None

            pmi = compute_pmi(count_xy, ux, uy, total)
            if pmi is None:
                return None

            return ("bigram", (w1, w2), pmi)

        if ngram_size == 3:
            w1, w2, w3 = ngram_words
            ux = unigram_counts.get(w1)
            uy = unigram_counts.get(w2)
            uz = unigram_counts.get(w3)
            if ux is None or uy is None or uz is None:
                return None

            left_doc = db.ngrams.find_one({"ngram": [w1, w2]})
            right_doc = db.ngrams.find_one({"ngram": [w2, w3]})
            if not left_doc or not right_doc:
                return None

            left_pmi = compute_pmi(left_doc["count"], ux, uy, total)
            right_pmi = compute_pmi(right_doc["count"], uy, uz, total)

            if left_pmi is None or right_pmi is None:
                return None

            pmi = min(left_pmi, right_pmi)
            return ("trigram", tuple(ngram_words), pmi)

        return None

    # PMI計算はDBアクセスも伴うので、並列化して高速化する。
    with ThreadPoolExecutor() as ex:
        for res in ex.map(_process_ngram_doc, ngram_docs):
            if res is None:
                continue

            kind, key, pmi = res
            if kind == "bigram":
                w1, w2 = key
                if pmi >= PMI_THRESHOLD:
                    db.compounds.update_one({"word": w1 + w2}, {"$set": {"pmi": pmi}}, upsert=True)
                    accepted_bigrams[(w1, w2)] = pmi
            else:
                if pmi >= PMI_THRESHOLD:
                    db.compounds.update_one({"word": "".join(key)}, {"$set": {"pmi": pmi}}, upsert=True)

    # 条件を満たすbigramの中で、さらに両側に同じbigramが条件を満たすものは、重複bigramを避けるために3-gramとしても保存する。
    bigrams_by_left: dict[str, list[tuple[str, float]]] = {}

    for (left_word, right_word), pmi in accepted_bigrams.items():
        bigrams_by_left.setdefault(left_word, []).append((right_word, pmi))

    for (w1, w2), left_pmi in accepted_bigrams.items():
        for w3, right_pmi in bigrams_by_left.get(w2, []):
            trigram_pmi = min(left_pmi, right_pmi)

            if trigram_pmi >= PMI_THRESHOLD:
                db.compounds.update_one({"word": w1 + w2 + w3}, {"$set": {"pmi": trigram_pmi}}, upsert=True)

    # 学習の最後に、抽出トークンのキャッシュをクリアして、次回以降の抽出で新しい複合語を反映させる。
    clear_extract_tokens_cache()


def load_compounds(db) -> set[str]:
    if _is_clickhouse(db):
        db = _learning_db(db)
        rows = db.query_dicts(
            """
            SELECT word
            FROM (
                SELECT word,
                    row_number() OVER (PARTITION BY word ORDER BY updated_at DESC) AS rn
                FROM compounds
            )
            WHERE rn = 1
            """
        )
        return {row["word"] for row in rows if row.get("word")}

    return {doc["word"] for doc in db.compounds.find()}


def build_wordcloud_source_text(docs: list[dict]) -> str:
    return join_message_content(docs)


def build_token_list_from_docs(docs: list[dict]) -> list[str]:
    """メッセージドキュメントのリストからトークンリストを構築する。
    tokensフィールドが存在する場合はそれを使用し、存在しない場合はcontentからSudachiで抽出する（旧メッセージへのフォールバック）。"""
    all_tokens: list[str] = []
    for doc in docs:
        stored = doc.get("tokens")
        if stored:
            all_tokens.extend(stored)
        else:
            content = (doc.get("content") or "").strip()
            if content:
                all_tokens.extend(extract_tokens(normalize_text(content)))
    return all_tokens


def generate_wordcloud_image(db, docs: list[dict], *, compounds: set[str] | None = None) -> io.BytesIO:
    font_path = resolve_font_path()

    if font_path is None:
        raise RuntimeError("WordCloudフォントが見つかりません")

    if compounds is None:
        compounds = load_compounds(db)

    tokens = apply_learned_compounds(build_token_list_from_docs(docs), compounds)
    words = " ".join(tokens)

    if not words.strip():
        raise ValueError("no words")

    wordcloud = WordCloud(
        font_path=font_path,
        width=1500,
        height=900,
        stopwords=STOP_WORDS,
        background_color="white",
        max_words=400,
        prefer_horizontal=0.9,
        relative_scaling=0.5,
        collocations=False,
    ).generate(words)

    figure = plt.figure(figsize=(15, 10))
    buffer = io.BytesIO()

    try:
        plt.imshow(wordcloud)
        plt.axis("off")
        figure.savefig(buffer, format="png", bbox_inches="tight")
        buffer.seek(0)
    finally:
        plt.close(figure)

    return buffer


def parse_schedule_time(schedule_time: str) -> tuple[int, int] | None:
    try:
        hour_str, minute_str = schedule_time.split(":", maxsplit=1)
        hour = int(hour_str)
        minute = int(minute_str)
    except (ValueError, AttributeError):
        return None

    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return hour, minute

    return None


def parse_last_executed(value: Optional[str], timezone) -> datetime | None:
    if value is None:
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.UTC)

    return parsed.astimezone(timezone)


def should_execute_schedule(
    frequency: str,
    last_executed: Optional[str],
    now_jst: datetime,
    timezone,
) -> bool:
    last_executed_dt = parse_last_executed(last_executed, timezone)

    if frequency == "daily":
        if last_executed_dt is None:
            return True
        return last_executed_dt.date() != now_jst.date()

    if frequency == "weekly":
        if now_jst.weekday() != 0:
            return False
        if last_executed_dt is None:
            return True
        return (
            last_executed_dt.isocalendar().year != now_jst.isocalendar().year
            or last_executed_dt.isocalendar().week != now_jst.isocalendar().week
        )

    if frequency == "monthly":
        last_day = monthrange(now_jst.year, now_jst.month)[1]

        if now_jst.day != last_day:
            return False
        if last_executed_dt is None:
            return True
        return (
            last_executed_dt.year != now_jst.year
            or last_executed_dt.month != now_jst.month
        )

    return False


def update_last_executed(db, guild_id: str, channel_id: str, frequency: str) -> None:
    update_schedule_last_executed(
        db,
        guild_id,
        channel_id,
        frequency,
        discord_utcnow(),
    )


def build_learning_cursor_query(last_cursor: dict | None) -> dict:
    if last_cursor is None:
        return {}

    timestamp = last_cursor.get("timestamp")
    message_id = last_cursor.get("message_id")

    if not isinstance(timestamp, str) or not isinstance(message_id, str):
        return {}

    return {
        "$or": [
            {"timestamp": {"$gt": timestamp}},
            {
                "timestamp": timestamp,
                "message_id": {"$gt": message_id},
            },
        ]
    }


def fetch_last_learn_cursor(db) -> dict | None:
    value = get_meta_value(db, LEARN_CURSOR_META_KEY)

    if not isinstance(value, dict):
        return None

    if not isinstance(value.get("timestamp"), str):
        return None

    if not isinstance(value.get("message_id"), str):
        return None

    return {
        "timestamp": value["timestamp"],
        "message_id": value["message_id"],
    }


def fetch_legacy_last_learn_id(db):
    return get_meta_value(db, LEGACY_LEARN_ID_META_KEY)


def fetch_learning_documents(
    db,
    last_cursor: dict | None,
    *,
    legacy_last_id=None,
    limit: int = 500,
) -> list[dict]:
    is_clickhouse = _is_clickhouse(db)

    projection = {
        "content": 1,
        "timestamp": 1,
        "message_id": 1,
    }

    if not is_clickhouse:
        projection["_id"] = 1

    if not is_clickhouse and last_cursor is None and legacy_last_id is not None:
        query = {"_id": {"$gt": legacy_last_id}}
        return list(db.messages.find(query, projection).sort("_id", 1).limit(limit))

    query = build_learning_cursor_query(last_cursor)
    return fetch_messages(
        db,
        query,
        projection,
        sort_field=[("timestamp", 1), ("message_id", 1)],
        limit=limit,
    )


def extract_learning_cursor(doc: dict) -> dict | None:
    timestamp = doc.get("timestamp")
    message_id = doc.get("message_id")

    if not isinstance(timestamp, str) or not timestamp:
        return None

    if message_id is None:
        return None

    normalized_message_id = str(message_id)
    if not normalized_message_id:
        return None

    return {
        "timestamp": timestamp,
        "message_id": normalized_message_id,
    }


def update_last_learn_cursor(db, last_cursor: dict) -> None:
    set_meta_value(db, LEARN_CURSOR_META_KEY, last_cursor)


def update_last_learn_id(db, last_id) -> None:
    # Compatibility helper for existing deployments that still keep legacy progress.
    set_meta_value(db, LEGACY_LEARN_ID_META_KEY, last_id)


def reset_learning_state(db) -> None:
    if _is_clickhouse(db):
        learning_db = _learning_db(db)
        learning_db.command("TRUNCATE TABLE IF EXISTS unigrams")
        learning_db.command("TRUNCATE TABLE IF EXISTS ngrams")
        learning_db.command("TRUNCATE TABLE IF EXISTS compounds")
    else:
        db.unigrams.delete_many({})
        db.ngrams.delete_many({})
        db.compounds.delete_many({})

    reset_message_tokens(db)

    delete_meta_key(db, LEARN_CURSOR_META_KEY)
    delete_meta_key(db, LEGACY_LEARN_ID_META_KEY)
    clear_extract_tokens_cache()


def reset_message_tokens(db) -> int:
    if not _is_clickhouse(db):
        result = db.messages.update_many({}, {"$set": {"tokens": None}})
        return int(getattr(result, "modified_count", 0))

    msg_db = db
    if getattr(db, "backend", "mongo") == "hybrid":
        msg_db = db.db_clickhouse

    reset_count = int(msg_db.query_scalar("SELECT count() AS count FROM messages") or 0)
    msg_db.command("ALTER TABLE messages UPDATE tokens = [] WHERE 1")
    return reset_count


def get_frequency_label(frequency: str) -> str:
    return {
        "daily": "デイリー",
        "weekly": "ウィークリー",
        "monthly": "マンスリー",
    }.get(frequency, frequency)


def get_schedule_during_days(frequency: str, now_jst: datetime) -> int | None:
    if frequency == "daily":
        return 1

    if frequency == "weekly":
        return 7

    if frequency == "monthly":
        return now_jst.day

    return None


def migrate_message_tokens(db, batch_size: int = 500, *, force: bool = False) -> int:
    """起動時マイグレーション: tokensフィールドが未付与のメッセージをバッチ処理して一括保存する。"""
    total = 0

    # MongoDB 側の処理
    if not _is_clickhouse(db):
        if force:
            docs = list(db.messages.find({"content": {"$type": "string", "$ne": ""}}, {"_id": 1, "content": 1}))
            ops = [
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": {"tokens": extract_tokens(normalize_text(doc["content"]))}},
                )
                for doc in docs
                if (doc.get("content") or "").strip()
            ]

            if ops:
                db.messages.bulk_write(ops, ordered=False)
                total += len(ops)

            return total

        query = {
            "$or": [
                {"tokens": {"$exists": False}},
                {"tokens": None},
            ],
            "content": {"$type": "string", "$ne": ""},
        }

        while True:
            docs = list(db.messages.find(query, {"_id": 1, "content": 1}).limit(batch_size))
            if not docs:
                break

            ops = [
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": {"tokens": extract_tokens(normalize_text(doc["content"]))}},
                )
                for doc in docs
                if (doc.get("content") or "").strip()
            ]

            if ops:
                db.messages.bulk_write(ops, ordered=False)
                total += len(ops)

        return total

    # ClickHouse / hybrid 環境の処理: 既存レコードで tokens が NULL のものだけを更新する。
    # 空配列は「トークン化済み・抽出ゼロ」の正常ケースなので対象外。
    msg_db = db
    if getattr(db, "backend", "mongo") == "hybrid":
        msg_db = db.db_clickhouse

    if force:
        docs = msg_db.query_dicts(
            "SELECT message_id, content FROM messages WHERE content != ''"
        )

        for doc in docs:
            content = (doc.get("content") or "").strip()
            if not content:
                continue

            tokens = list(extract_tokens(normalize_text(content)))
            try:
                msg_db.command(
                    "ALTER TABLE messages UPDATE tokens = {tokens:Array(String)} WHERE message_id = {message_id:String}",
                    {"tokens": tokens, "message_id": str(doc.get("message_id", ""))},
                )
                total += 1
            except Exception:
                continue

        return total

    while True:
        docs = msg_db.query_dicts(
            (
                "SELECT message_id, content FROM messages "
                "WHERE tokens IS NULL AND content != '' "
                "LIMIT {limit:UInt32}"
            ),
            {"limit": int(batch_size)},
        )

        if not docs:
            break

        for doc in docs:
            content = (doc.get("content") or "").strip()
            if not content:
                continue

            tokens = list(extract_tokens(normalize_text(content)))
            try:
                msg_db.command(
                    "ALTER TABLE messages UPDATE tokens = {tokens:Array(String)} WHERE message_id = {message_id:String}",
                    {"tokens": tokens, "message_id": str(doc.get("message_id", ""))},
                )
                total += 1
            except Exception:
                continue

    return total


def count_unmigrated_tokens(db) -> int:
    """トークン未生成（tokens フィールドが存在しない）メッセージ件数を返す。
    ClickHouse 環境では常に 0 を返す（マイグレーション不要）。
    """
    # Mongo の場合は count_documents を使う
    if not _is_clickhouse(db):
        query = {
            "$or": [
                {"tokens": {"$exists": False}},
                {"tokens": None},
            ],
            "content": {"$type": "string", "$ne": ""},
        }
        return int(db.messages.count_documents(query))

    # ClickHouse / hybrid: tokens が NULL のレコードのみ未処理と見なす。空配列は正常ケース。
    msg_db = db
    if getattr(db, "backend", "mongo") == "hybrid":
        msg_db = db.db_clickhouse

    sql = (
        "SELECT count() AS count FROM messages "
        "WHERE tokens IS NULL AND content != ''"
    )
    return int(msg_db.query_scalar(sql) or 0)


def _update_compounds_clickhouse(db) -> None:
    total = get_total_unigram_count(db)
    if total == 0:
        return

    unigram_docs = db.query_dicts(
        "SELECT word, sum(count) AS count FROM unigrams GROUP BY word"
    )
    unigram_counts: dict[str, int] = {
        str(doc["word"]): int(doc["count"]) for doc in unigram_docs if doc.get("word")
    }

    if not unigram_counts:
        return

    ngram_docs = db.query_dicts(
        "SELECT ngram, sum(count) AS count FROM ngrams GROUP BY ngram"
    )

    bigram_counts: dict[tuple[str, str], int] = {}
    for doc in ngram_docs:
        words = tuple(str(word) for word in (doc.get("ngram") or []))
        if len(words) == 2:
            bigram_counts[words] = int(doc.get("count", 0))

    accepted_bigrams: dict[tuple[str, str], float] = {}
    compound_map: dict[str, float] = {}

    for doc in ngram_docs:
        ngram_words = tuple(str(word) for word in (doc.get("ngram") or []))
        count_xy = int(doc.get("count", 0))
        ngram_size = len(ngram_words)

        if count_xy < COUNT_THRESHOLD:
            continue

        if ngram_size == 2:
            w1, w2 = ngram_words
            ux = unigram_counts.get(w1)
            uy = unigram_counts.get(w2)
            if ux is None or uy is None:
                continue

            pmi = compute_pmi(count_xy, ux, uy, total)
            if pmi is None or pmi < PMI_THRESHOLD:
                continue

            compound_map[w1 + w2] = pmi
            accepted_bigrams[(w1, w2)] = pmi
            continue

        if ngram_size == 3:
            w1, w2, w3 = ngram_words
            ux = unigram_counts.get(w1)
            uy = unigram_counts.get(w2)
            uz = unigram_counts.get(w3)
            if ux is None or uy is None or uz is None:
                continue

            left_count = bigram_counts.get((w1, w2))
            right_count = bigram_counts.get((w2, w3))
            if left_count is None or right_count is None:
                continue

            left_pmi = compute_pmi(left_count, ux, uy, total)
            right_pmi = compute_pmi(right_count, uy, uz, total)
            if left_pmi is None or right_pmi is None:
                continue

            pmi = min(left_pmi, right_pmi)
            if pmi < PMI_THRESHOLD:
                continue

            compound_map["".join(ngram_words)] = pmi

    bigrams_by_left: dict[str, list[tuple[str, float]]] = {}
    for (left_word, right_word), pmi in accepted_bigrams.items():
        bigrams_by_left.setdefault(left_word, []).append((right_word, pmi))

    for (w1, w2), left_pmi in accepted_bigrams.items():
        for w3, right_pmi in bigrams_by_left.get(w2, []):
            trigram_pmi = min(left_pmi, right_pmi)
            if trigram_pmi >= PMI_THRESHOLD:
                compound_map[w1 + w2 + w3] = trigram_pmi

    rows = [[word, float(pmi)] for word, pmi in compound_map.items()]
    if rows:
        db.insert_rows("compounds", rows, ["word", "pmi"])