import pymongo


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


def get_opt_out_flags(db, guild_id, channel_id, user_id, parent_channel_id=None):
    # ユーザー設定: opt_out フィールドのみ取得
    user_settings = db.user_settings.find_one({"user_id": user_id}, {"opt_out": 1})
    # user_settings が None でなく、かつ opt_out が True の場合のみ True
    user_opted_out = bool(user_settings and user_settings.get("opt_out"))

    # ギルド設定: optout_channels フィールドのみ取得
    guild_settings = db.guild_settings.find_one(
        {"guild_id": guild_id}, {"optout_channels": 1}
    )

    channel_opted_out = False
    if guild_settings:
        # 配列が存在しない可能性も考慮して get([], ...)
        optout_list = guild_settings.get("optout_channels", [])

        # チャンネルID または 親チャンネルID (Forumの親など) が配列に含まれているか
        channel_opted_out = (channel_id in optout_list) or (
            parent_channel_id is not None and parent_channel_id in optout_list
        )

    return channel_opted_out, user_opted_out


def get_guild_collection_stats(
    db: pymongo.database.Database,
) -> list[dict[str, int | str]]:
    pipeline = [
        {
            "$group": {
                "_id": "$guild_id",
                "guild_name": {"$first": "$guild_name"},
                "message_count": {"$sum": 1},
                "collected_user_ids": {"$addToSet": "$user_id"},
                "last_message_time": {"$max": "$timestamp"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "guild_id": "$_id",
                "guild_name": 1,
                "message_count": 1,
                "collected_user_count": {"$size": "$collected_user_ids"},
                "last_message_time": 1,
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
    deleted_schedules = db.guild_settings.delete_many(
        {"guild_id": guild_id}
    ).deleted_count
    deleted_channel_settings = db.channel_settings.delete_many(
        {"guild_id": guild_id}
    ).deleted_count

    return {
        "messages": deleted_messages,
        "guild_settings": deleted_schedules,
        "channel_settings": deleted_channel_settings,
    }
