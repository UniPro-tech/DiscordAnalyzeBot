from datetime import datetime, timedelta
import io
from typing import Optional

import matplotlib.pyplot as plt
from wordcloud import WordCloud

from libs.text_processing import (
    STOP_WORDS,
    apply_learned_compounds,
    compute_pmi,
    extract_tokens,
    extract_tokens_with_indices,
    join_message_content,
    normalize_text,
)
from libs.visualization_common import resolve_font_path


PMI_THRESHOLD = 3
COUNT_THRESHOLD = 10
DEFAULT_MESSAGE_LIMIT = 3000


def parse_period_days(period: Optional[str]) -> int | None:
    if period is None:
        return None

    period_days = int(period)

    if period_days <= 0:
        raise ValueError("period must be positive")

    return period_days


def build_wordcloud_message_query(
    guild_id: str,
    *,
    period_days: int | None = None,
    user_id: str | None = None,
    channel_id: str | None = None,
    role_id: str | None = None,
) -> dict:
    query = {
        "guild_id": guild_id,
        "content": {"$type": "string", "$ne": ""},
    }

    if period_days is not None:
        query["timestamp"] = {
            "$gte": (discord_utcnow() - timedelta(days=period_days)).isoformat()
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
    period_days: int | None = None,
    user_id: str | None = None,
    channel_id: str | None = None,
    role_id: str | None = None,
    limit: int = DEFAULT_MESSAGE_LIMIT,
) -> list[dict]:
    query = build_wordcloud_message_query(
        guild_id,
        period_days=period_days,
        user_id=user_id,
        channel_id=channel_id,
        role_id=role_id,
    )

    return list(
        db.messages.find(query, {"content": 1}).sort("timestamp", -1).limit(limit)
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

    for token in tokens:
        save_unigram(db, token)

    for ngram_size in (2, 3):
        for index in range(len(token_entries) - ngram_size + 1):
            window = token_entries[index : index + ngram_size]

            # Keep only truly adjacent tokens in original text.
            if all(
                window[pos + 1][1] - window[pos][1] == 1
                for pos in range(len(window) - 1)
            ):
                save_ngram(db, tuple(word for word, _ in window))


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
    total = get_total_unigram_count(db)

    if total == 0:
        return

    accepted_bigrams: dict[tuple[str, str], float] = {}

    for doc in db.ngrams.find():
        ngram_words = doc["ngram"]
        count_xy = doc["count"]
        ngram_size = len(ngram_words)

        if count_xy < COUNT_THRESHOLD:
            continue

        if ngram_size not in (2, 3):
            continue

        if ngram_size == 2:
            w1, w2 = ngram_words
            pmi = _compute_bigram_pmi(db, w1, w2, total)

            if pmi is None:
                continue

            if pmi >= PMI_THRESHOLD:
                db.compounds.update_one(
                    {"word": w1 + w2},
                    {"$set": {"pmi": pmi}},
                    upsert=True,
                )
                accepted_bigrams[(w1, w2)] = pmi

            continue

        w1, w2, w3 = ngram_words
        left_pmi = _compute_bigram_pmi(db, w1, w2, total)
        right_pmi = _compute_bigram_pmi(db, w2, w3, total)

        if left_pmi is None or right_pmi is None:
            continue

        pmi = min(left_pmi, right_pmi)

        if pmi >= PMI_THRESHOLD:
            db.compounds.update_one(
                {"word": "".join(ngram_words)},
                {"$set": {"pmi": pmi}},
                upsert=True,
            )

    # Promote overlapping bigrams (A+B and B+C) to trigram compounds (A+B+C).
    bigrams_by_left: dict[str, list[tuple[str, float]]] = {}

    for (left_word, right_word), pmi in accepted_bigrams.items():
        if left_word not in bigrams_by_left:
            bigrams_by_left[left_word] = []

        bigrams_by_left[left_word].append((right_word, pmi))

    for (w1, w2), left_pmi in accepted_bigrams.items():
        for w3, right_pmi in bigrams_by_left.get(w2, []):
            trigram_pmi = min(left_pmi, right_pmi)

            if trigram_pmi >= PMI_THRESHOLD:
                db.compounds.update_one(
                    {"word": w1 + w2 + w3},
                    {"$set": {"pmi": trigram_pmi}},
                    upsert=True,
                )


def load_compounds(db) -> set[str]:
    return {doc["word"] for doc in db.compounds.find()}


def build_wordcloud_source_text(docs: list[dict]) -> str:
    return join_message_content(docs)


def generate_wordcloud_image(db, text: str) -> io.BytesIO:
    font_path = resolve_font_path()

    if font_path is None:
        raise RuntimeError("WordCloudフォントが見つかりません")

    normalized_text = normalize_text(text)
    compounds = load_compounds(db)
    tokens = apply_learned_compounds(extract_tokens(normalized_text), compounds)
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
    plt.imshow(wordcloud)
    plt.axis("off")

    buffer = io.BytesIO()
    figure.savefig(buffer, format="png", bbox_inches="tight")
    plt.close(figure)
    buffer.seek(0)

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
        if now_jst.day != 31:
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


def get_frequency_label(frequency: str) -> str:
    return {
        "daily": "デイリー",
        "weekly": "ウィークリー",
        "monthly": "マンスリー",
    }.get(frequency, frequency)