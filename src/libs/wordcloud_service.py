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
import matplotlib.pyplot as plt
from wordcloud import WordCloud


PMI_THRESHOLD = 3
COUNT_THRESHOLD = 10
DEFAULT_MESSAGE_LIMIT = 3000
JST = ZoneInfo("Asia/Tokyo")


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

    return list(
        db.messages.find(query, {"content": 1, "tokens": 1}).sort("timestamp", -1).limit(limit)
    )


def save_unigram(db, token: str) -> None:
    db.unigrams.update_one(
        {"word": token},
        {"$inc": {"count": 1}},
        upsert=True,
    )


def save_ngram(db, ngram: tuple[str, ...]) -> None:
    db.ngrams.update_one(
        {"ngram": list(ngram)},
        {"$inc": {"count": 1}},
        upsert=True,
    )


def get_total_unigram_count(db) -> int:
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


def generate_wordcloud_image(db, docs: list[dict]) -> io.BytesIO:
    font_path = resolve_font_path()

    if font_path is None:
        raise RuntimeError("WordCloudフォントが見つかりません")

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
    db.guild_settings.update_one(
        {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "frequency": frequency,
        },
        {"$set": {"last_executed": discord_utcnow().isoformat()}},
    )


def fetch_learning_documents(db, last_id, limit: int = 500) -> list[dict]:
    query = {}

    if last_id is not None:
        query["_id"] = {"$gt": last_id}

    return list(db.messages.find(query, {"content": 1}).sort("_id", 1).limit(limit))


def update_last_learn_id(db, last_id) -> None:
    db.meta.update_one(
        {"_id": "last_learn_id"},
        {"$set": {"value": last_id}},
        upsert=True,
    )


def reset_learning_state(db) -> None:
    db.unigrams.delete_many({})
    db.ngrams.delete_many({})
    db.compounds.delete_many({})
    db.meta.delete_one({"_id": "last_learn_id"})
    clear_extract_tokens_cache()


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


def migrate_message_tokens(db, batch_size: int = 500) -> int:
    """起動時マイグレーション: tokensフィールドが未付与のメッセージをバッチ処理して一括保存する。"""
    total = 0
    query = {"tokens": {"$exists": False}, "content": {"$type": "string", "$ne": ""}}

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