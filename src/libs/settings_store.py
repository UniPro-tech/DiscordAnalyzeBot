from datetime import datetime


class DeleteResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count


def _is_clickhouse(db) -> bool:
    return getattr(db, "backend", "mongo") == "clickhouse"


def _resolve_settings_db(db):
    if getattr(db, "backend", "mongo") == "hybrid":
        return db.db_mongo
    return db


def setup_settings_indexes(db) -> None:
    db = _resolve_settings_db(db)

    if _is_clickhouse(db):
        db.command(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id String,
                channel_id String,
                frequency String,
                schedule_time String,
                enabled UInt8,
                last_executed Nullable(String),
                updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (guild_id, channel_id, frequency)
            """
        )
        db.command(
            """
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id String,
                opt_out UInt8,
                updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (user_id)
            """
        )
        db.command(
            """
            CREATE TABLE IF NOT EXISTS channel_settings (
                guild_id String,
                channel_id String,
                opt_out UInt8,
                updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (guild_id, channel_id)
            """
        )
        db.command(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key String,
                value String,
                updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (key)
            """
        )
        return

    db.guild_settings.create_index(
        [("guild_id", 1), ("channel_id", 1), ("frequency", 1)], unique=True
    )
    db.guild_settings.create_index("guild_id")
    db.guild_settings.create_index("enabled")

    db.user_settings.create_index("user_id", unique=True)
    db.user_settings.create_index("opt_out")

    db.channel_settings.create_index(
        [("guild_id", 1), ("channel_id", 1)], unique=True
    )
    db.channel_settings.create_index("opt_out")


def find_guild_schedule(
    db,
    guild_id: str,
    channel_id: str,
    frequency: str,
):
    db = _resolve_settings_db(db)

    if _is_clickhouse(db):
        rows = db.query_dicts(
            """
            SELECT guild_id, channel_id, frequency, schedule_time, enabled, last_executed
            FROM guild_settings
            WHERE guild_id = {guild_id:String}
              AND channel_id = {channel_id:String}
              AND frequency = {frequency:String}
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {
                "guild_id": guild_id,
                "channel_id": channel_id,
                "frequency": frequency,
            },
        )
        if not rows:
            return None
        row = rows[0]
        row["enabled"] = bool(row.get("enabled", 0))
        return row

    return db.guild_settings.find_one(
        {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "frequency": frequency,
        }
    )


def insert_guild_schedule(
    db,
    guild_id: str,
    channel_id: str,
    frequency: str,
    schedule_time: str,
) -> None:
    db = _resolve_settings_db(db)

    if _is_clickhouse(db):
        db.insert_rows(
            "guild_settings",
            [[guild_id, channel_id, frequency, schedule_time, 1, None]],
            [
                "guild_id",
                "channel_id",
                "frequency",
                "schedule_time",
                "enabled",
                "last_executed",
            ],
        )
        return

    db.guild_settings.insert_one(
        {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "frequency": frequency,
            "schedule_time": schedule_time,
            "enabled": True,
            "last_executed": None,
        }
    )


def list_guild_schedules(db, guild_id: str) -> list[dict]:
    db = _resolve_settings_db(db)

    if _is_clickhouse(db):
        rows = db.query_dicts(
            """
            SELECT guild_id, channel_id, frequency, schedule_time, enabled, last_executed
            FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY guild_id, channel_id, frequency
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM guild_settings
                WHERE guild_id = {guild_id:String}
            )
            WHERE rn = 1
            ORDER BY channel_id, frequency
            """,
            {"guild_id": guild_id},
        )
        for row in rows:
            row["enabled"] = bool(row.get("enabled", 0))
        return rows

    return list(db.guild_settings.find({"guild_id": guild_id}))


def list_enabled_schedules(db) -> list[dict]:
    db = _resolve_settings_db(db)

    if _is_clickhouse(db):
        rows = db.query_dicts(
            """
            SELECT guild_id, channel_id, frequency, schedule_time, enabled, last_executed
            FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY guild_id, channel_id, frequency
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM guild_settings
            )
            WHERE rn = 1 AND enabled = 1
            """
        )
        for row in rows:
            row["enabled"] = bool(row.get("enabled", 0))
        return rows

    return list(db.guild_settings.find({"enabled": True}))


def delete_guild_schedule(
    db,
    guild_id: str,
    channel_id: str,
    frequency: str,
):
    db = _resolve_settings_db(db)

    if _is_clickhouse(db):
        exists = find_guild_schedule(db, guild_id, channel_id, frequency)
        deleted_count = 1 if exists is not None else 0

        db.command(
            """
            ALTER TABLE guild_settings
            DELETE WHERE guild_id = {guild_id:String}
              AND channel_id = {channel_id:String}
              AND frequency = {frequency:String}
            """,
            {
                "guild_id": guild_id,
                "channel_id": channel_id,
                "frequency": frequency,
            },
        )
        return DeleteResult(deleted_count)

    return db.guild_settings.delete_one(
        {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "frequency": frequency,
        }
    )


def update_schedule_last_executed(
    db,
    guild_id: str,
    channel_id: str,
    frequency: str,
    executed_at: datetime,
) -> None:
    db = _resolve_settings_db(db)

    if _is_clickhouse(db):
        current = find_guild_schedule(db, guild_id, channel_id, frequency)
        if current is None:
            return

        db.insert_rows(
            "guild_settings",
            [[
                guild_id,
                channel_id,
                frequency,
                current.get("schedule_time", "09:00"),
                1 if current.get("enabled", True) else 0,
                executed_at.isoformat(),
            ]],
            [
                "guild_id",
                "channel_id",
                "frequency",
                "schedule_time",
                "enabled",
                "last_executed",
            ],
        )
        return

    db.guild_settings.update_one(
        {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "frequency": frequency,
        },
        {"$set": {"last_executed": executed_at.isoformat()}},
    )


def set_user_opt_out(db, user_id: str, opt_out: bool) -> None:
    db = _resolve_settings_db(db)

    if _is_clickhouse(db):
        db.insert_rows(
            "user_settings",
            [[user_id, 1 if opt_out else 0]],
            ["user_id", "opt_out"],
        )
        return

    db.user_settings.update_one(
        {"user_id": user_id},
        {"$set": {"opt_out": opt_out}},
        upsert=True,
    )


def set_channel_opt_out(
    db,
    guild_id: str,
    channel_id: str,
    opt_out: bool,
) -> None:
    db = _resolve_settings_db(db)

    if _is_clickhouse(db):
        db.insert_rows(
            "channel_settings",
            [[guild_id, channel_id, 1 if opt_out else 0]],
            ["guild_id", "channel_id", "opt_out"],
        )
        return

    db.channel_settings.update_one(
        {"guild_id": guild_id, "channel_id": channel_id},
        {"$set": {"opt_out": opt_out}},
        upsert=True,
    )
