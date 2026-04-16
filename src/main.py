import discord
import os
import sys
import asyncio
import re
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

    # TTL Index: 30日後に自動的に削除
    # 1. 一般ユーザー用（is_premium が true ではない、または存在しない場合）
    bot.db.messages.create_index(
        "timestamp",
        expireAfterSeconds=31 * 24 * 60 * 60,
        name="timestamp_ttl_normal",
        partialFilterExpression={"is_premium": {"$ne": True}},
    )

    # 2. Premiumユーザー用
    bot.db.messages.create_index(
        "timestamp",
        expireAfterSeconds=365 * 24 * 60 * 60,
        name="timestamp_ttl_premium",
        partialFilterExpression={"is_premium": True},
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
    print(f"Logged in as {bot.user}")


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

    # プレミアム状況の確認
    is_premium = bot.db.guild_settings.find_one(
        {"guild_id": guild_id}, {"is_premium": 1}
    )

    channel_opted_out, user_opted_out = await asyncio.to_thread(collect_opt_out_flags)

    # いずれかがオプトアウトなら処理終了
    if channel_opted_out or user_opted_out:
        return

    # メッセージデータの構築
    emoji_pattern = r"<a?:\w+:\d+>"
    emojis = re.findall(emoji_pattern, message.content)

    reply_to = str(message.reference.message_id) if message.reference else None

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
        "is_premium": is_premium,
    }

    # トークン化とDB保存
    def _save_message(d: dict) -> None:
        content = d.get("content", "")
        if content:
            # 形態素解析などはCPU負荷が高いためスレッドプールで実行
            d["tokens"] = list(extract_tokens(normalize_text(content)))
        bot.db.messages.insert_one(d)

    await asyncio.to_thread(_save_message, data)

    # コマンドの実行（プレフィックスコマンド用）
    await bot.process_commands(message)


@bot.event
async def on_guild_remove(guild):
    print(f"Left guild: {guild.name} (ID: {guild.id})")
    deleted = delete_guild_data(bot.db, str(guild.id))
    print(
        f"Deleted {deleted['messages']} messages from the database for guild {guild.name}"
    )
    print(
        f"Deleted {deleted['guild_settings']} guild settings from the database for guild {guild.name}"
    )
    print(
        f"Deleted {deleted['channel_settings']} channel settings from the database for guild {guild.name}"
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
        print(
            f"Deleted {deleted_count} message records from the database for deleted message in guild '{guild_name}' (ID: {payload.guild_id}), channel '{channel_name}' (ID: {payload.channel_id})"
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
        print(
            f"Deleted {deleted_count} message records from the database for bulk deleted messages in guild '{guild_name}' (ID: {payload.guild_id})"
        )


@bot.event
async def on_guild_join(guild):
    print(f"Joined guild: {guild.name} (ID: {guild.id})")
    try:
        owner = guild.owner  # サーバーオーナー

        if owner is None:
            print(f"Failed to get owner info: {guild.name}")
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
        print(f"{guild.name} のオーナーに権限不足のためDMを送れませんでした")
    except Exception as e:
        print(
            f"{guild.name} へのウェルカムメッセージの送信中にエラーが発生しました: {e}"
        )


async def main():
    print("Starting bot...")
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
        print("Error: Mongo DB_DSN is not set")
        return

    client_db = MongoClient(DB_DSN)
    db = client_db["discord_analyzer"]

    print("Starting migrate type of timestamp to datetime...")

    # 対象: timestampフィールドが文字列(string)であるドキュメント
    filter_query = {"timestamp": {"$type": "string"}}

    # 更新内容: 文字列をDate型に変換する($toDate)
    update_pipeline = [{"$set": {"timestamp": {"$toDate": "$timestamp"}}}]

    try:
        # update_manyにパイプライン（リスト形式）を渡すことでサーバー側で一括変換
        result = db.messages.update_many(filter_query, update_pipeline)

        print(f"Target Document Count: {result.matched_count}")
        print(f"Updated Document Count {result.modified_count}")
        print("Migration Successfully")

    except Exception as e:
        print(f"Migration failed: {e}")
        raise
    finally:
        client_db.close()


def delete_all_index():
    if not DB_DSN:
        print("Error: Mongo DB_DSN is not set")
        return

    client_db = MongoClient(DB_DSN)
    db = client_db["discord_analyzer"]

    print("Starting delete all indexes...")

    try:
        for collection_name in bot.db.list_collection_names():
            db[collection_name].drop_indexes()
        print("Deletion Successfully")

    except Exception as e:
        print(f"Migration failed: {e}")
        raise
    finally:
        client_db.close()


def migrate_to_new_settings_structure():
    if not DB_DSN:
        print("Error: Mongo DB_DSN is not set")
        return

    client = MongoClient(DB_DSN)
    db = client["discord_analyzer"]
    print("Starting structural migration...")

    # 1. channel_settings からオプトアウト済みのチャンネルを取得し、guild_settingsへ統合
    channels = db.channel_settings.find({"opt_out": True})
    for ch in channels:
        db.guild_settings.update_one(
            {"guild_id": ch["guild_id"]},
            {"$addToSet": {"optout_channels": ch["channel_id"]}},
            upsert=True,
        )
    print("Migrated channel opt-outs to guild_settings.optout_channels")

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
    # ただし、集約先のドキュメント自体も processed_ids に含まれている可能性があるため
    # 「集約後（schedulesが存在する）かつ 古いフィールド（frequency）が残っている」ものを消すか
    # 単純に frequency フィールドを持つ古い形式を全て unset/整理する

    # 集約が完了したので、古い個別ドキュメントの残骸を一掃（重複排除）
    # frequency フィールドを持つドキュメントを一括でクリーンアップ、または削除
    db.guild_settings.delete_many(
        {"_id": {"$in": processed_ids}, "frequency": {"$exists": True}}
    )

    print("Migrated and consolidated old guild_settings to unified schedules format")

    # 3. インデックスの再設定
    print("Re-creating indexes...")
    db.guild_settings.drop_indexes()
    db.guild_settings.create_index("guild_id", unique=True, name="guild_id_unique")

    print("Migration Successfully")
    client.close()


if __name__ == "__main__":
    if os.getenv("RUN_TIMESTAMP_MIGRATION") == "1":
        migrate_timestamps_to_date()
        delete_all_index()
        migrate_to_new_settings_structure()
    asyncio.run(main())
