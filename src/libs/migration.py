import os
from itertools import islice

from pymongo import MongoClient

from libs.meta_store import set_meta_value


MESSAGE_COLUMNS = [
    "message_id",
    "guild_id",
    "guild_name",
    "user_id",
    "username",
    "channel_id",
    "channel_name",
    "content",
    "timestamp",
    "role_ids",
    "reply_to",
    "mentions",
    "attachments",
    "length",
    "emoji_count",
    "url_count",
    "tokens",
]

TARGET_TABLES = (
    "messages",
    "guild_settings",
    "channel_settings",
    "user_settings",
    "meta",
    "unigrams",
    "ngrams",
    "compounds",
)

HYBRID_TARGET_TABLES = (
    "messages",
    "unigrams",
    "ngrams",
    "compounds",
)


def migration_enabled() -> bool:
    value = os.getenv("MIGRATION", "false")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _is_clickhouse(db) -> bool:
    return getattr(db, "backend", "mongo") in {"clickhouse", "hybrid"}


def _is_hybrid(db) -> bool:
    return getattr(db, "backend", "mongo") == "hybrid"


def _target_db(db):
    if _is_hybrid(db):
        return db.db_clickhouse
    return db


def _target_tables(db) -> tuple[str, ...]:
    if _is_hybrid(db):
        return HYBRID_TARGET_TABLES
    return TARGET_TABLES


def _mongo_source_db():
    dsn = os.getenv("MONGODB_DSN")
    client = MongoClient(dsn)
    return client["discord_analyzer"]


def _batched(items, size: int):
    iterator = iter(items)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            return
        yield batch


def _normalize_scalar(value):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_normalize_scalar(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_scalar(item) for key, item in value.items()}
    return str(value)


def _truncate_target(target_db, table_names: tuple[str, ...]) -> None:
    for table_name in table_names:
        target_db.command(f"TRUNCATE TABLE IF EXISTS {table_name}")


def target_has_existing_data(target_db, table_names: tuple[str, ...] | None = None) -> bool:
    table_names = table_names or TARGET_TABLES
    for table_name in table_names:
        count = target_db.query_scalar(f"SELECT count() AS count FROM {table_name}")
        if int(count or 0) > 0:
            return True
    return False


def _message_row(doc: dict) -> list:
    return [
        str(doc.get("message_id", "")),
        str(doc.get("guild_id", "")),
        str(doc.get("guild_name", "")),
        str(doc.get("user_id", "")),
        str(doc.get("username", "")),
        str(doc.get("channel_id", "")),
        str(doc.get("channel_name", "")),
        str(doc.get("content", "")),
        doc.get("timestamp"),
        [str(value) for value in doc.get("role_ids", [])],
        str(doc.get("reply_to")) if doc.get("reply_to") is not None else None,
        [str(value) for value in doc.get("mentions", [])],
        [str(value) for value in doc.get("attachments", [])],
        int(doc.get("length", 0)),
        int(doc.get("emoji_count", 0)),
        int(doc.get("url_count", 0)),
        [str(value) for value in doc.get("tokens", [])],
    ]


def run_clickhouse_migration(target_db, source_db=None, *, batch_size: int = 1000) -> dict[str, int]:
    if not _is_clickhouse(target_db):
        return {}

    use_hybrid_scope = _is_hybrid(target_db)
    target_db = _target_db(target_db)
    table_names = HYBRID_TARGET_TABLES if use_hybrid_scope else TARGET_TABLES

    source_db = source_db or _mongo_source_db()
    _truncate_target(target_db, table_names)

    counts = {
        "messages": 0,
        "guild_settings": 0,
        "channel_settings": 0,
        "user_settings": 0,
        "meta": 0,
        "unigrams": 0,
        "ngrams": 0,
        "compounds": 0,
    }

    message_docs = list(source_db.messages.find({}, {"_id": 0}))
    for batch in _batched(message_docs, batch_size):
        rows = [_message_row(doc) for doc in batch]
        target_db.insert_rows("messages", rows, MESSAGE_COLUMNS)
        counts["messages"] += len(rows)

    guild_setting_docs = list(source_db.guild_settings.find({}, {"_id": 0}))
    if guild_setting_docs and not use_hybrid_scope:
        rows = [
            [
                str(doc.get("guild_id", "")),
                str(doc.get("channel_id", "")),
                str(doc.get("frequency", "")),
                str(doc.get("schedule_time", "09:00")),
                1 if doc.get("enabled", True) else 0,
                doc.get("last_executed"),
            ]
            for doc in guild_setting_docs
        ]
        target_db.insert_rows(
            "guild_settings",
            rows,
            [
                "guild_id",
                "channel_id",
                "frequency",
                "schedule_time",
                "enabled",
                "last_executed",
            ],
        )
        counts["guild_settings"] = len(rows)

    channel_setting_docs = list(source_db.channel_settings.find({}, {"_id": 0}))
    if channel_setting_docs and not use_hybrid_scope:
        rows = [
            [
                str(doc.get("guild_id", "")),
                str(doc.get("channel_id", "")),
                1 if doc.get("opt_out", False) else 0,
            ]
            for doc in channel_setting_docs
        ]
        target_db.insert_rows(
            "channel_settings",
            rows,
            ["guild_id", "channel_id", "opt_out"],
        )
        counts["channel_settings"] = len(rows)

    user_setting_docs = list(source_db.user_settings.find({}, {"_id": 0}))
    if user_setting_docs and not use_hybrid_scope:
        rows = [
            [
                str(doc.get("user_id", "")),
                1 if doc.get("opt_out", False) else 0,
            ]
            for doc in user_setting_docs
        ]
        target_db.insert_rows("user_settings", rows, ["user_id", "opt_out"])
        counts["user_settings"] = len(rows)

    unigram_docs = list(source_db.unigrams.find({}, {"_id": 0}))
    if unigram_docs:
        rows = [[str(doc.get("word", "")), int(doc.get("count", 0))] for doc in unigram_docs]
        target_db.insert_rows("unigrams", rows, ["word", "count"])
        counts["unigrams"] = len(rows)

    ngram_docs = list(source_db.ngrams.find({}, {"_id": 0}))
    if ngram_docs:
        rows = [
            [[str(value) for value in doc.get("ngram", [])], int(doc.get("count", 0))]
            for doc in ngram_docs
        ]
        target_db.insert_rows("ngrams", rows, ["ngram", "count"])
        counts["ngrams"] = len(rows)

    compound_docs = list(source_db.compounds.find({}, {"_id": 0}))
    if compound_docs:
        rows = [
            [str(doc.get("word", "")), float(doc.get("pmi", 0.0))]
            for doc in compound_docs
        ]
        target_db.insert_rows("compounds", rows, ["word", "pmi"])
        counts["compounds"] = len(rows)

    if not use_hybrid_scope:
        for doc in source_db.meta.find():
            key = str(doc.get("_id"))
            set_meta_value(target_db, key, _normalize_scalar(doc.get("value")))
            counts["meta"] += 1

    return counts


def maybe_run_clickhouse_migration(target_db) -> dict[str, int]:
    if not _is_clickhouse(target_db) or not migration_enabled():
        return {}

    table_names = _target_tables(target_db)
    target_clickhouse_db = _target_db(target_db)

    if target_has_existing_data(target_clickhouse_db, table_names):
        return {"skipped_existing_data": 1}

    return run_clickhouse_migration(target_db)