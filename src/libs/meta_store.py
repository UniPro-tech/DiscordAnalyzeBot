import json


def _is_clickhouse(db) -> bool:
    # Support both storage wrapper objects (which expose `backend`) and
    # raw ClickHouse client objects (which expose ClickHouse methods).
    backend = getattr(db, "backend", None)
    if backend == "clickhouse":
        return True
    # If this is a storage wrapper (MongoDatabase / HybridDatabase), prefer
    # using its explicit backend attribute instead of probing methods which
    # may trigger pymongo dynamic attribute access returning Collection
    # objects.
    if hasattr(db, "raw_db") or hasattr(db, "db_mongo") or hasattr(db, "db_clickhouse"):
        return False

    # Otherwise, detect a raw ClickHouse client by looking for the expected
    # callable methods. Avoid treating pymongo Collection objects as ClickHouse
    # by checking the attribute's class module.
    def _is_callable_nonpymongo(obj):
        if not callable(obj):
            return False
        cls = getattr(obj, "__class__", None)
        mod = getattr(cls, "__module__", "") if cls is not None else ""
        if mod.startswith("pymongo"):
            return False
        return True

    q = getattr(db, "query_dicts", None)
    if _is_callable_nonpymongo(q):
        return True
    s = getattr(db, "query_scalar", None)
    if _is_callable_nonpymongo(s):
        return True
    ins = getattr(db, "insert_rows", None)
    if _is_callable_nonpymongo(ins):
        return True

    return False


def _resolve_meta_db(db):
    if getattr(db, "backend", "mongo") == "hybrid":
        return db.db_mongo
    return db


def get_meta_value(db, key: str):
    db = _resolve_meta_db(db)

    if _is_clickhouse(db):
        rows = db.query_dicts(
            """
            SELECT value
            FROM meta
            WHERE key = {key:String}
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {"key": key},
        )
        if not rows:
            return None

        raw_value = rows[0].get("value")
        if raw_value is None:
            return None

        try:
            return json.loads(raw_value)
        except (TypeError, json.JSONDecodeError):
            return raw_value

    doc = db.meta.find_one({"_id": key})
    if doc is None:
        return None
    return doc.get("value")


def set_meta_value(db, key: str, value) -> None:
    db = _resolve_meta_db(db)

    if _is_clickhouse(db):
        db.insert_rows(
            "meta",
            [[key, json.dumps(value, ensure_ascii=False)]],
            ["key", "value"],
        )
        return

    db.meta.update_one(
        {"_id": key},
        {"$set": {"value": value}},
        upsert=True,
    )


def delete_meta_key(db, key: str) -> None:
    db = _resolve_meta_db(db)

    if _is_clickhouse(db):
        db.command(
            "ALTER TABLE meta DELETE WHERE key = {key:String}",
            {"key": key},
        )
        return

    db.meta.delete_one({"_id": key})
