from datetime import datetime, timezone


def _is_clickhouse(db) -> bool:
    return getattr(db, "backend", "mongo") in {"clickhouse", "hybrid"}


def _messages_db(db):
    if getattr(db, "backend", "mongo") == "hybrid":
        return db.db_clickhouse
    return db


def _settings_db(db):
    if getattr(db, "backend", "mongo") == "hybrid":
        return db.db_mongo
    return db


def _normalize_timestamp(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


def _build_where_clause(query: dict, params: dict, *, prefix: str = "p") -> str:
    conditions = []

    for key, expected in query.items():
        param_key = f"{prefix}_{key}_{len(params)}"

        if key == "$or" and isinstance(expected, list):
            nested = []
            for index, sub_query in enumerate(expected):
                nested_where = _build_where_clause(
                    sub_query,
                    params,
                    prefix=f"{prefix}_or_{index}",
                )
                if nested_where:
                    nested.append(f"({nested_where})")
            if nested:
                conditions.append("(" + " OR ".join(nested) + ")")
            continue

        if isinstance(expected, dict):
            if "$in" in expected:
                in_values = [str(value) for value in expected["$in"]]
                if key == "role_ids" and in_values:
                    params[param_key] = in_values[0]
                    conditions.append(f"has(role_ids, {{{param_key}:String}})")
                else:
                    params[param_key] = in_values
                    conditions.append(f"{key} IN {{{param_key}:Array(String)}}")

            if "$gte" in expected:
                gte_value = expected["$gte"]
                if key == "timestamp":
                    params[param_key] = str(gte_value)
                    conditions.append(
                        f"timestamp >= parseDateTime64BestEffortOrNull({{{param_key}:String}})"
                    )
                else:
                    params[param_key] = str(gte_value)
                    conditions.append(f"{key} >= {{{param_key}:String}}")

            if "$ne" in expected:
                params[param_key] = str(expected["$ne"])
                conditions.append(f"{key} != {{{param_key}:String}}")
            if "$exists" in expected:
                # ClickHouse 側では $exists を解釈できないため、必要なケースだけ SQL に変換する。
                # 現状は tokens の存在確認を配列要素が空でないことを確認する形で変換する。
                if key == "tokens" and expected["$exists"]:
                    conditions.append("arrayExists(x -> x != '', tokens)")

            # $type は ClickHouse 側での型設計前提なのでここでは無視する。
            continue

        params[param_key] = str(expected)
        conditions.append(f"{key} = {{{param_key}:String}}")

    return " AND ".join(conditions)


def _projection_columns(projection: dict) -> list[str]:
    columns = [key for key, include in projection.items() if include]
    if not columns:
        return ["*"]
    return columns


def is_channel_opted_out(db, guild_id: str, channel_id: str) -> bool:
    settings_db = _settings_db(db)

    if _is_clickhouse(settings_db):
        rows = settings_db.query_dicts(
            """
            SELECT opt_out
            FROM channel_settings
            WHERE guild_id = {guild_id:String} AND channel_id = {channel_id:String}
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {
                "guild_id": guild_id,
                "channel_id": channel_id,
            },
        )
        if not rows:
            return False
        return bool(rows[0].get("opt_out", 0))

    channel_opt_out = settings_db.channel_settings.find_one(
        {"guild_id": guild_id, "channel_id": channel_id}
    )

    if channel_opt_out is None:
        return False

    return channel_opt_out.get("opt_out", False)


def is_user_opted_out(db, user_id: str) -> bool:
    settings_db = _settings_db(db)

    if _is_clickhouse(settings_db):
        rows = settings_db.query_dicts(
            """
            SELECT opt_out
            FROM user_settings
            WHERE user_id = {user_id:String}
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {"user_id": user_id},
        )
        if not rows:
            return False
        return bool(rows[0].get("opt_out", 0))

    opt_out = settings_db.user_settings.find_one({"user_id": user_id})

    if opt_out is None:
        return False

    return opt_out.get("opt_out", False)


def get_opt_out_flags(
    db,
    guild_id: str,
    channel_id: str,
    user_id: str,
) -> tuple[bool, bool]:
    """オプトアウトフラグをまとめて取得するユーティリティ関数。DBアクセスが伴うため、必要に応じて非同期で呼び出すこと。"""
    return (
        is_channel_opted_out(db, guild_id, channel_id),
        is_user_opted_out(db, user_id),
    )


def normalize_message_ids(message_ids) -> list[str]:
    return [str(message_id) for message_id in message_ids]


def setup_message_indexes(db) -> None:
    db = _messages_db(db)

    if _is_clickhouse(db):
        db.command(
            """
            CREATE TABLE IF NOT EXISTS messages (
                message_id String,
                guild_id String,
                guild_name String,
                user_id String,
                username String,
                channel_id String,
                channel_name String,
                content String,
                timestamp DateTime64(3, 'UTC'),
                role_ids Array(String),
                reply_to Nullable(String),
                mentions Array(String),
                attachments Array(String),
                length UInt32,
                emoji_count UInt32,
                url_count UInt32,
                tokens Array(String)
            )
            ENGINE = MergeTree
            ORDER BY (guild_id, timestamp, message_id)
            TTL toDateTime(timestamp) + INTERVAL 30 DAY
            """
        )
        return

    db.messages.create_index("user_id")
    db.messages.create_index("channel_id")
    db.messages.create_index("guild_id")
    db.messages.create_index(
        "message_id",
        unique=True,
        partialFilterExpression={"message_id": {"$exists": True}},
    )
    db.messages.create_index("reply_to")
    # TTL Index: 30日後に自動的に削除
    db.messages.create_index("timestamp", expireAfterSeconds=30 * 24 * 60 * 60)


def fetch_messages(
    db,
    query: dict,
    projection: dict,
    *,
    sort_field: str | list[tuple[str, int]] | None = "timestamp",
    sort_order: int = -1,
    limit: int | None = None,
) -> list[dict]:
    db = _messages_db(db)

    if _is_clickhouse(db):
        params = {}
        where_clause = _build_where_clause(query, params)
        columns = ", ".join(_projection_columns(projection))
        sql = f"SELECT {columns} FROM messages"

        if where_clause:
            sql += f" WHERE {where_clause}"

        if sort_field is not None:
            if isinstance(sort_field, list):
                order_parts = []
                for field, field_order in sort_field:
                    direction = "DESC" if field_order == -1 else "ASC"
                    order_parts.append(f"{field} {direction}")
                sql += " ORDER BY " + ", ".join(order_parts)
            else:
                direction = "DESC" if sort_order == -1 else "ASC"
                sql += f" ORDER BY {sort_field} {direction}"

        if limit is not None:
            sql += " LIMIT {limit:UInt32}"
            params["limit"] = int(limit)

        rows = db.query_dicts(sql, params)
        for row in rows:
            if "timestamp" in row:
                row["timestamp"] = _normalize_timestamp(row["timestamp"])
        return rows

    cursor = db.messages.find(query, projection)

    if sort_field is not None:
        if isinstance(sort_field, list):
            cursor = cursor.sort(sort_field)
        else:
            cursor = cursor.sort(sort_field, sort_order)

    if limit is not None:
        cursor = cursor.limit(limit)

    return list(cursor)


def fetch_messages_by_ids(
    db,
    guild_id: str,
    message_ids,
    projection: dict,
) -> list[dict]:
    db = _messages_db(db)

    normalized_ids = normalize_message_ids(message_ids)

    if _is_clickhouse(db):
        columns = ", ".join(_projection_columns(projection))
        rows = db.query_dicts(
            f"""
            SELECT {columns}
            FROM messages
            WHERE guild_id = {{guild_id:String}}
              AND message_id IN {{message_ids:Array(String)}}
            """,
            {
                "guild_id": guild_id,
                "message_ids": normalized_ids,
            },
        )
        for row in rows:
            if "timestamp" in row:
                row["timestamp"] = _normalize_timestamp(row["timestamp"])
        return rows

    return list(
        db.messages.find(
            {
                "guild_id": guild_id,
                "message_id": {"$in": normalized_ids},
            },
            projection,
        )
    )


def insert_message(db, message: dict) -> None:
    db = _messages_db(db)

    if _is_clickhouse(db):
        timestamp = message.get("timestamp")
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except ValueError:
                timestamp = datetime.now(timezone.utc)

        row = [
            str(message.get("message_id", "")),
            str(message.get("guild_id", "")),
            str(message.get("guild_name", "")),
            str(message.get("user_id", "")),
            str(message.get("username", "")),
            str(message.get("channel_id", "")),
            str(message.get("channel_name", "")),
            str(message.get("content", "")),
            timestamp,
            [str(value) for value in message.get("role_ids", [])],
            str(message.get("reply_to")) if message.get("reply_to") is not None else None,
            [str(value) for value in message.get("mentions", [])],
            [str(value) for value in message.get("attachments", [])],
            int(message.get("length", 0)),
            int(message.get("emoji_count", 0)),
            int(message.get("url_count", 0)),
            [str(value) for value in message.get("tokens", [])],
        ]
        db.insert_rows(
            "messages",
            [row],
            [
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
            ],
        )
        return

    db.messages.insert_one(message)


def count_messages(db) -> int:
    db = _messages_db(db)

    if _is_clickhouse(db):
        return int(db.query_scalar("SELECT count() AS count FROM messages") or 0)

    return db.messages.estimated_document_count()


def count_distinct_message_users(db) -> int:
    db = _messages_db(db)

    if _is_clickhouse(db):
        return int(db.query_scalar("SELECT uniqExact(user_id) AS count FROM messages") or 0)

    return len(db.messages.distinct("user_id"))


def delete_messages_by_ids(db, message_ids) -> int:
    db = _messages_db(db)

    normalized_ids = normalize_message_ids(message_ids)

    if _is_clickhouse(db):
        deleted_count = int(
            db.query_scalar(
                """
                SELECT count() AS count
                FROM messages
                WHERE message_id IN {message_ids:Array(String)}
                """,
                {"message_ids": normalized_ids},
            )
            or 0
        )
        db.command(
            """
            ALTER TABLE messages
            DELETE WHERE message_id IN {message_ids:Array(String)}
            """,
            {"message_ids": normalized_ids},
        )
        return deleted_count

    result = db.messages.delete_many({"message_id": {"$in": normalized_ids}})
    return result.deleted_count


def delete_messages_by_query(db, query: dict) -> int:
    db = _messages_db(db)

    if _is_clickhouse(db):
        params = {}
        where_clause = _build_where_clause(query, params)
        if not where_clause:
            return 0

        deleted_count = int(
            db.query_scalar(
                f"SELECT count() AS count FROM messages WHERE {where_clause}",
                params,
            )
            or 0
        )
        db.command(
            f"ALTER TABLE messages DELETE WHERE {where_clause}",
            params,
        )
        return deleted_count

    result = db.messages.delete_many(query)
    return result.deleted_count


def delete_guild_data(db, guild_id: str) -> dict[str, int]:
    if getattr(db, "backend", "mongo") == "hybrid":
        message_db = db.db_clickhouse
        settings_db = db.db_mongo

        deleted_messages = int(
            message_db.query_scalar(
                "SELECT count() AS count FROM messages WHERE guild_id = {guild_id:String}",
                {"guild_id": guild_id},
            )
            or 0
        )
        message_db.command(
            "ALTER TABLE messages DELETE WHERE guild_id = {guild_id:String}",
            {"guild_id": guild_id},
        )

        deleted_schedules = settings_db.guild_settings.delete_many(
            {"guild_id": guild_id}
        ).deleted_count
        deleted_channel_settings = settings_db.channel_settings.delete_many(
            {"guild_id": guild_id}
        ).deleted_count

        return {
            "messages": deleted_messages,
            "guild_settings": deleted_schedules,
            "channel_settings": deleted_channel_settings,
        }

    if _is_clickhouse(db):
        deleted_messages = int(
            db.query_scalar(
                "SELECT count() AS count FROM messages WHERE guild_id = {guild_id:String}",
                {"guild_id": guild_id},
            )
            or 0
        )
        deleted_schedules = int(
            db.query_scalar(
                "SELECT count() AS count FROM guild_settings WHERE guild_id = {guild_id:String}",
                {"guild_id": guild_id},
            )
            or 0
        )
        deleted_channel_settings = int(
            db.query_scalar(
                "SELECT count() AS count FROM channel_settings WHERE guild_id = {guild_id:String}",
                {"guild_id": guild_id},
            )
            or 0
        )

        db.command(
            "ALTER TABLE messages DELETE WHERE guild_id = {guild_id:String}",
            {"guild_id": guild_id},
        )
        db.command(
            "ALTER TABLE guild_settings DELETE WHERE guild_id = {guild_id:String}",
            {"guild_id": guild_id},
        )
        db.command(
            "ALTER TABLE channel_settings DELETE WHERE guild_id = {guild_id:String}",
            {"guild_id": guild_id},
        )

        return {
            "messages": deleted_messages,
            "guild_settings": deleted_schedules,
            "channel_settings": deleted_channel_settings,
        }

    deleted_messages = db.messages.delete_many({"guild_id": guild_id}).deleted_count
    deleted_schedules = db.guild_settings.delete_many({"guild_id": guild_id}).deleted_count
    deleted_channel_settings = db.channel_settings.delete_many({"guild_id": guild_id}).deleted_count

    return {
        "messages": deleted_messages,
        "guild_settings": deleted_schedules,
        "channel_settings": deleted_channel_settings,
    }