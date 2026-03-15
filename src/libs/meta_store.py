import json


def _is_clickhouse(db) -> bool:
    # Support both storage wrapper objects (which expose `backend`) and
    # raw ClickHouse client objects (which expose ClickHouse methods).
    backend = getattr(db, "backend", None)
    if backend == "clickhouse":
        return True

    # Detect raw ClickHouse client by presence of query/insert methods
    if hasattr(db, "query_dicts") or hasattr(db, "query_scalar") or hasattr(db, "insert_rows"):
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
