def is_channel_opted_out(
    db,
    guild_id: str,
    channel_id: str,
    parent_channel_id: str | None = None,
) -> bool:
    channel_ids = [channel_id]

    if parent_channel_id is not None:
        channel_ids.append(parent_channel_id)

    channel_opt_out = db.channel_settings.find_one(
        {"guild_id": guild_id, "channel_id": {"$in": channel_ids}}
    )

    if channel_opt_out is None:
        return False

    return channel_opt_out.get("opt_out", False)


def is_user_opted_out(db, user_id: str) -> bool:
    opt_out = db.user_settings.find_one({"user_id": user_id})

    if opt_out is None:
        return False

    return opt_out.get("opt_out", False)


def get_opt_out_flags(
    db,
    guild_id: str,
    channel_id: str,
    user_id: str,
    parent_channel_id: str | None = None,
) -> tuple[bool, bool]:
    """オプトアウトフラグをまとめて取得するユーティリティ関数。DBアクセスが伴うため、必要に応じて非同期で呼び出すこと。"""
    return (
        is_channel_opted_out(db, guild_id, channel_id, parent_channel_id),
        is_user_opted_out(db, user_id),
    )


def get_guild_collection_stats(db) -> list[dict[str, int | str]]:
    pipeline = [
        {
            "$group": {
                "_id": {
                    "guild_id": "$guild_id",
                    "guild_name": {"$ifNull": ["$guild_name", "Unknown Guild"]},
                },
                "message_count": {"$sum": 1},
                "user_ids": {"$addToSet": "$user_id"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "guild_id": "$_id.guild_id",
                "guild_name": "$_id.guild_name",
                "message_count": 1,
                "collected_user_count": {"$size": "$user_ids"},
            }
        },
        {"$sort": {"message_count": -1}},
    ]
    return list(db.messages.aggregate(pipeline))


def normalize_message_ids(message_ids) -> list[str]:
    return [str(message_id) for message_id in message_ids]


def delete_messages_by_ids(db, message_ids) -> int:
    normalized_ids = normalize_message_ids(message_ids)
    result = db.messages.delete_many({"message_id": {"$in": normalized_ids}})
    return result.deleted_count


def delete_messages_by_query(db, query: dict) -> int:
    result = db.messages.delete_many(query)
    return result.deleted_count


def delete_guild_data(db, guild_id: str) -> dict[str, int]:
    deleted_messages = db.messages.delete_many({"guild_id": guild_id}).deleted_count
    deleted_schedules = db.guild_settings.delete_many({"guild_id": guild_id}).deleted_count
    deleted_channel_settings = db.channel_settings.delete_many({"guild_id": guild_id}).deleted_count

    return {
        "messages": deleted_messages,
        "guild_settings": deleted_schedules,
        "channel_settings": deleted_channel_settings,
    }