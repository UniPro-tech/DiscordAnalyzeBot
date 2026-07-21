import logging
import os
import sys
import asyncio
import re
import discord
import structlog
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from discord.ext import commands, tasks
from libs.message_store import (
    delete_guild_data,
    delete_messages_by_ids,
    get_opt_out_flags,
)
from libs.text_processing import extract_tokens, normalize_text

# Add src directory to sys.path for imports
sys.path.insert(0, os.path.dirname(__file__))

# --- structlog Configuration ---
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=logging.INFO,
)

if os.getenv("DEVELOP") == "TRUE":
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
else:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

logger = structlog.get_logger()
# -------------------------------

TOKEN = os.getenv("DISCORD_TOKEN")
DB_DSN = os.getenv("MONGODB_DSN")

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True


class AnalyzerBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)


bot = AnalyzerBot()

# MongoDB
client_db = MongoClient(DB_DSN)
bot.db = client_db["discord_analyzer"]

STATUS_ROTATION_SECONDS = 30
status_index = 0


def setup_db():
    # メッセージコレクションのインデックス設定
    bot.db.messages.create_index("user_id", name="user_id_idx")
    bot.db.messages.create_index("channel_id", name="channel_id_idx")
    bot.db.messages.create_index("parent_channel_id", name="parent_channel_id_idx")
    bot.db.messages.create_index("guild_id", name="guild_id_idx")
    bot.db.messages.create_index(
        "message_id",
        unique=True,
        partialFilterExpression={"message_id": {"$exists": True}},
        name="message_id_unique",
    )
    bot.db.messages.create_index("reply_to", name="reply_to_idx")

    # expires_atで消す
    bot.db.messages.create_index(
        "expires_at",
        expireAfterSeconds=0,
        name="expires_at_ttl",
    )

    # Guild設定: guild_idごとに1ドキュメント
    bot.db.guild_settings.create_index("guild_id", unique=True, name="guild_id_unique")
    bot.db.guild_settings.create_index("is_premium", name="is_premium_idk")

    # ユーザー設定
    bot.db.user_settings.create_index("user_id", unique=True, name="user_id_unique")

    # チャンネル設定コレクションのインデックス設定
    bot.db.channel_settings.create_index(
        [("guild_id", 1), ("channel_id", 1)],
        unique=True,
        name="channel_settings_unique",
    )
    bot.db.channel_settings.create_index("opt_out", name="opt_out_idx")


@bot.event
async def on_ready():
    if not rotate_status.is_running():
        rotate_status.start()
    await bot.tree.sync()
    logger.info("bot_logged_in", user=str(bot.user))


async def _get_status_messages():
    def collect_counts():
        messages_count = bot.db.messages.estimated_document_count()
        collected_user_count = len(bot.db.messages.distinct("user_id"))
        return messages_count, collected_user_count

    messages_count, collected_user_count = await asyncio.to_thread(collect_counts)
    guild_count = len(bot.guilds)

    return [
        f"{messages_count:,} 件のメッセージを分析中",
        f"{guild_count:,} サーバーに参加中",
        f"{collected_user_count:,} ユーザー分を分析中",
    ]


@tasks.loop(seconds=STATUS_ROTATION_SECONDS)
async def rotate_status():
    global status_index
    statuses = await _get_status_messages()
    if not statuses:
        return

    current_status = statuses[status_index % len(statuses)]
    status_index += 1
    await bot.change_presence(activity=discord.Game(name=current_status))


@rotate_status.before_loop
async def before_rotate_status():
    await bot.wait_until_ready()


@bot.event
async def on_message(message: discord.Message):
    # Bot自身やDMは無視
    if message.author.bot or message.guild is None:
        return
    guild_id = str(message.guild.id)
    channel_id = str(message.channel.id)
    user_id = str(message.author.id)
    # スレッドの場合は親チャンネルIDを取得（Forum以外もカバー）
    parent_channel_id = None
    if isinstance(message.channel, discord.Thread):
        parent_channel_id = str(message.channel.parent_id)

    # オプトアウト状況の確認
    # (内部で guild_settings.optout_channels を参照する前提)
    def collect_opt_out_flags() -> tuple[bool, bool]:
        return get_opt_out_flags(
            bot.db,
            guild_id,
            channel_id,
            user_id,
            parent_channel_id=parent_channel_id,
        )

    channel_opted_out, user_opted_out = await asyncio.to_thread(collect_opt_out_flags)
    # いずれかがオプトアウトなら処理終了
    if channel_opted_out or user_opted_out:
        return

    # プレミアム状況の確認
    premium_data = bot.db.guild_settings.find_one(
        {"guild_id": guild_id}, {"is_premium": True}
    )
    is_premium_status = bool(premium_data and premium_data.get("is_premium"))

    # メッセージデータの構築
    emoji_pattern = r"<a?:\w+:\d+>"
    emojis = re.findall(emoji_pattern, message.content)
    reply_to = str(message.reference.message_id) if message.reference else None

    # 有効期限を追加
    days = 365 if is_premium_status else 31
    expire_date = datetime.now(timezone.utc) + timedelta(days=days)
    data = {
        "message_id": str(message.id),
        "guild_id": guild_id,
        "guild_name": message.guild.name,
        "user_id": user_id,
        "username": str(message.author),
        "channel_id": channel_id,
        "parent_channel_id": parent_channel_id,
        "channel_name": str(message.channel),
        "content": message.content,
        "timestamp": message.created_at,
        "role_ids": [str(role.id) for role in message.author.roles]
        if hasattr(message.author, "roles")
        else [],
        "reply_to": reply_to,
        "mentions": [str(user.id) for user in message.mentions],
        "attachments": [a.url for a in message.attachments],
        "length": len(message.content),
        "emoji_count": len(emojis),
        "url_count": len(re.findall(r"https?://\S+", message.content)),
        "is_premium": is_premium_status,
        "expires_at": expire_date,
    }

    # トークン化とDB保存
    def _save_message(d: dict) -> None:
        try:
            content = d.get("content", "")
            if content:
                d["tokens"] = list(extract_tokens(normalize_text(content)))

            # insert_one ではなく、念のため upsert (あれば更新、なければ挿入) にする
            # これにより DuplicateKeyError を回避できます
            bot.db.messages.update_one(
                {"message_id": d["message_id"]}, {"$set": d}, upsert=True
            )
        except Exception as e:
            logger.error(
                "db_save_error",
                error=str(e),
                message_id=d.get("message_id"),
                guild_id=d.get("guild_id"),
            )

    await asyncio.to_thread(_save_message, data)

    # コマンドの実行（プレフィックスコマンド用）
    await bot.process_commands(message)


@bot.event
async def on_guild_remove(guild):
    logger.info("guild_left", guild_name=guild.name, guild_id=guild.id)
    deleted = delete_guild_data(bot.db, str(guild.id))
    logger.info(
        "guild_data_deleted",
        guild_name=guild.name,
        guild_id=guild.id,
        deleted_messages=deleted.get("messages", 0),
        deleted_guild_settings=deleted.get("guild_settings", 0),
        deleted_channel_settings=deleted.get("channel_settings", 0),
    )


@bot.event
async def on_raw_message_delete(payload):
    """
    メッセージが削除された際のイベントハンドラー
    """
    if payload.guild_id is None:
        return

    deleted_count = delete_messages_by_ids(bot.db, [payload.message_id])
    if deleted_count > 0:
        guild = bot.get_guild(payload.guild_id)
        channel = bot.get_channel(payload.channel_id)
        guild_name = guild.name if guild is not None else "Unknown Guild"
        channel_name = channel.name if channel is not None else "Unknown Channel"

        logger.info(
            "message_deleted",
            guild_id=payload.guild_id,
            channel_id=payload.channel_id,
            guild_name=guild_name,
            channel_name=channel_name,
            deleted_count=deleted_count,
        )


@bot.event
async def on_raw_bulk_message_delete(payload):
    """
    複数メッセージが一度に削除された際のイベントハンドラー
    """
    if payload.guild_id is None:
        return

    deleted_count = delete_messages_by_ids(bot.db, payload.message_ids)
    if deleted_count > 0:
        guild = bot.get_guild(payload.guild_id)
        guild_name = guild.name if guild is not None else "Unknown Guild"

        logger.info(
            "bulk_messages_deleted",
            guild_id=payload.guild_id,
            guild_name=guild_name,
            deleted_count=deleted_count,
        )


@bot.event
async def on_guild_join(guild):
    logger.info("guild_joined", guild_name=guild.name, guild_id=guild.id)
    try:
        owner = guild.owner  # サーバーオーナー

        if owner is None:
            logger.warning(
                "owner_info_not_found", guild_name=guild.name, guild_id=guild.id
            )
            return

        message = """
# Analyze Botをご利用いただきありがとうございます :tada:
Analyze Botは、サーバー内のメッセージを分析して、様々な統計情報を提供するDiscord Botです。
以下のコマンドを使用して、サーバーの分析を開始できます。
- `/ping`: Botの応答速度を確認します。
- `/wordcloud`: サーバー内の頻出単語をワードクラウド形式で表示します。
- `/about`: Botのバージョンや開発者情報を表示します。
- `/optout`: 統計データからのオプトアウト設定を行います。
ご質問やフィードバックがある場合は、開発者までお気軽にお問い合わせください。

## :warning: 注意点とサーバーオーナーのみなさまへのお願い
Analyze Botは、Discordサーバー内のメッセージを分析するため、プライバシーに配慮した設計となっていますが、以下の点にご注意ください。
また、下記内容をDiscordサーバー内のメンバーに広く周知していただくことを推奨します。(@everyoneなどを利用して告知してください。)
1. **データ収集の範囲**: Analyze Botは、サーバー内のテキストチャンネルのメッセージを収集します。
2. **データの保存期間**: 収集されたメッセージデータは、30日間保存され、その後自動的に削除されます。
3. **ユーザーのオプトアウト**: ユーザーは、`/optout` コマンドを使用して、統計データからオプトアウトすることができます。オプトアウトされたユーザーのメッセージは、分析の対象外となります。
4. **サーバーオーナーの責任**: サーバーオーナーは、Analyze Botの使用に関して、サーバー内のメンバーに適切な説明を行い、必要に応じて同意を得ることを推奨します。
Analyze Botは、ユーザーのプライバシーを尊重し、データの安全な取り扱いに努めていますが、サーバーオーナーの皆様には、Botの使用に関する透明性を保ち、メンバーの信頼を得るための適切な対応をお願い申し上げます。

### 周知用テンプレート
以下は、サーバー内での周知用テンプレートの例です。
メンバーの皆様にAnalyze Botの導入とプライバシーに関する注意点を周知する際にご活用ください。
```
## Analyze Bot導入のお知らせ :tada:
@everyone この度、当サーバーではAnalyze Botを導入しました！Analyze Botは、サーバー内のメッセージを分析して、様々な統計情報を提供するDiscord Botです。
### Analyze Botの主な機能
- サーバー内の頻出単語をワードクラウド形式で表示
- メッセージの送信頻度やアクティブな時間帯の分析
- ユーザーごとのメッセージ数やアクティブ度の分析
### プライバシーに関する注意点
Analyze Botは、サーバー内のテキストチャンネルのメッセージを収集しますが、収集されたデータは30日間保存され、その後自動的に削除されます。
また、ユーザーは`/optout`コマンドを使用して、統計データからご自身のメッセージをオプトアウトすることができます。オプトアウトされたユーザーのメッセージは、分析の対象外となります。
詳しくは、[プライバシー・ポリシー](https://analyze-bot.uniproject.jp/privacy )および[利用規約](https://analyze-bot.uniproject.jp/legal/terms )をご覧ください。
```
"""
        await owner.send(message)
    except discord.Forbidden:
        logger.error(
            "welcome_message_forbidden",
            guild_name=guild.name,
            guild_id=guild.id,
            reason="Insufficient permissions",
        )
    except Exception as e:
        logger.error(
            "welcome_message_failed",
            guild_name=guild.name,
            guild_id=guild.id,
            error=str(e),
        )


async def main():
    logger.info("bot_starting")
    setup_db()

    await bot.load_extension("cogs.ping")
    await bot.load_extension("cogs.wordcloud")
    await bot.load_extension("cogs.about")
    await bot.load_extension("cogs.optout")
    await bot.load_extension("cogs.network")
    await bot.load_extension("cogs.admin")
    await bot.load_extension("cogs.utils")
    await bot.load_extension("cogs.statistics")

    async with bot:
        await bot.start(TOKEN)


def migrate_timestamps_to_date():
    if not DB_DSN:
        logger.error("migration_missing_dsn", migration="timestamps_to_date")
        return

    client_db = MongoClient(DB_DSN)
    db = client_db["discord_analyzer"]

    logger.info("migration_started", migration="timestamps_to_date")

    # 対象: timestampフィールドが文字列(string)であるドキュメント
    filter_query = {"timestamp": {"$type": "string"}}

    # 更新内容: 文字列をDate型に変換する($toDate)
    update_pipeline = [{"$set": {"timestamp": {"$toDate": "$timestamp"}}}]

    try:
        # update_manyにパイプライン（リスト形式）を渡すことでサーバー側で一括変換
        result = db.messages.update_many(filter_query, update_pipeline)

        logger.info(
            "migration_completed",
            migration="timestamps_to_date",
            matched_count=result.matched_count,
            modified_count=result.modified_count,
        )

    except Exception as e:
        logger.error("migration_failed", migration="timestamps_to_date", error=str(e))
        raise
    finally:
        client_db.close()


def delete_all_index():
    if not DB_DSN:
        logger.error("migration_missing_dsn", migration="delete_all_index")
        return

    client_db = MongoClient(DB_DSN)
    db = client_db["discord_analyzer"]

    logger.info("migration_started", migration="delete_all_index")

    try:
        for collection_name in bot.db.list_collection_names():
            db[collection_name].drop_indexes()
        logger.info("migration_completed", migration="delete_all_index")

    except Exception as e:
        logger.error("migration_failed", migration="delete_all_index", error=str(e))
        raise
    finally:
        client_db.close()


def migrate_to_new_settings_structure():
    if not DB_DSN:
        logger.error("migration_missing_dsn", migration="new_settings_structure")
        return

    client = MongoClient(DB_DSN)
    db = client["discord_analyzer"]
    logger.info("migration_started", migration="new_settings_structure")

    # 1. channel_settings からオプトアウト済みのチャンネルを取得し、guild_settingsへ統合
    channels = db.channel_settings.find({"opt_out": True})
    for ch in channels:
        db.guild_settings.update_one(
            {"guild_id": ch["guild_id"]},
            {"$addToSet": {"optout_channels": ch["channel_id"]}},
            upsert=True,
        )
    logger.info("migration_step_completed", step="migrated_channel_opt_outs")

    # 2. 既存の古い guild_settings レコードを 1 つの guild_id ドキュメントに集約
    # frequency フィールドを持つ古い形式のドキュメントを抽出
    cursor = db.guild_settings.find({"frequency": {"$exists": True}})

    # 処理済みの _id を追跡（削除用）
    processed_ids = []

    for doc in cursor:
        guild_id = doc.get("guild_id")
        if not guild_id:
            continue

        schedule_item = {
            "channel_id": doc.get("channel_id"),
            "frequency": doc.get("frequency"),
            "enabled": doc.get("enabled", True),
            "type": "wordcloud",
        }

        # guild_id をキーにして upsert。schedules 配列にアイテムを追加し、不要なフィールドを unset
        db.guild_settings.update_one(
            {"guild_id": guild_id},
            {
                "$addToSet": {"schedules": schedule_item},
                "$set": {"updated_at": doc.get("timestamp") or discord.utils.utcnow()},
            },
            upsert=True,
        )
        processed_ids.append(doc["_id"])

    # 全てのデータを統合した後、古い「個別のレコード」を特定して削除
    db.guild_settings.delete_many(
        {"_id": {"$in": processed_ids}, "frequency": {"$exists": True}}
    )

    logger.info("migration_step_completed", step="migrated_guild_settings")

    # 3. インデックスの再設定
    db.guild_settings.drop_indexes()
    db.guild_settings.create_index("guild_id", unique=True, name="guild_id_unique")

    logger.info("migration_step_completed", step="recreated_indexes")
    logger.info("migration_completed", migration="new_settings_structure")
    client.close()


def migrate_add_expires_at():
    """
    expires_at フィールドがないドキュメントに対し、
    timestamp + 31日 を計算してセットするマイグレーション
    """
    if not DB_DSN:
        logger.error("migration_missing_dsn", migration="add_expires_at")
        return

    client_db = MongoClient(DB_DSN)
    db = client_db["discord_analyzer"]

    logger.info("migration_started", migration="add_expires_at")

    # 1. expires_at が存在しないドキュメントを対象にする
    try:
        # 集計パイプラインを使用して一括更新
        # 注意: MongoDB 4.2+ が必要です
        result = db.messages.update_many(
            {"expires_at": {"$exists": False}, "timestamp": {"$type": "date"}},
            [
                {
                    "$set": {
                        "expires_at": {"$add": ["$timestamp", 31 * 24 * 60 * 60 * 1000]}
                    }
                }
            ],
        )

        logger.info(
            "migration_completed",
            migration="add_expires_at",
            matched_count=result.matched_count,
            modified_count=result.modified_count,
        )

    except Exception as e:
        logger.error("migration_failed", migration="add_expires_at", error=str(e))
        raise
    finally:
        client_db.close()


if __name__ == "__main__":
    if os.getenv("RUN_TIMESTAMP_MIGRATION") == "1":
        migrate_timestamps_to_date()
        delete_all_index()
        migrate_to_new_settings_structure()
        migrate_add_expires_at()
    asyncio.run(main())
