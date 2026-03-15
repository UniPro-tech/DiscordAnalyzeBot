import discord
import os
import sys
import asyncio
import re
from discord.ext import commands, tasks
from libs.message_store import (
    count_distinct_message_users,
    count_messages,
    delete_guild_data,
    delete_messages_by_ids,
    get_opt_out_flags,
    insert_message,
    setup_message_indexes,
)
from libs.migration import maybe_run_clickhouse_migration
from libs.storage import HybridDatabase, get_storage_backend, init_storages
from libs.settings_store import setup_settings_indexes
from libs.text_processing import extract_tokens, normalize_text
from libs.wordcloud_service import setup_learning_tables

# Add src directory to sys.path for imports
sys.path.insert(0, os.path.dirname(__file__))

TOKEN = os.getenv("DISCORD_TOKEN")
DB_DSN = os.getenv("MONGODB_DSN")
STORAGE_BACKEND = get_storage_backend()

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True


class AnalyzerBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)


bot = AnalyzerBot()

# Storage backend (mongo / clickhouse)
mongo_db, clickhouse_db, _resolved_backend = init_storages()
bot.db_mongo = mongo_db
bot.db_clickhouse = clickhouse_db
if _resolved_backend == "hybrid" and mongo_db is not None and clickhouse_db is not None:
    bot.db = HybridDatabase(db_mongo=mongo_db, db_clickhouse=clickhouse_db)
elif _resolved_backend == "clickhouse":
    bot.db = clickhouse_db
else:
    bot.db = mongo_db

STATUS_ROTATION_SECONDS = 30
status_index = 0


def setup_db():
    setup_message_indexes(bot.db)
    setup_settings_indexes(bot.db)
    setup_learning_tables(bot.db)


@bot.event
async def on_ready():
    if not rotate_status.is_running():
        rotate_status.start()
    await bot.tree.sync()
    print(f"Logged in as {bot.user} (storage={STORAGE_BACKEND})")


async def _get_status_messages():
    def collect_counts():
        messages_count = count_messages(bot.db)
        collected_user_count = count_distinct_message_users(bot.db)
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
async def on_message(message):
    if message.author.bot:
        return

    if message.guild is None:
        return

    guild_id = str(message.guild.id)
    channel_id = str(message.channel.id)
    user_id = str(message.author.id)

    def collect_opt_out_flags() -> tuple[bool, bool]:
        return get_opt_out_flags(bot.db, guild_id, channel_id, user_id)

    channel_opted_out, user_opted_out = await asyncio.to_thread(collect_opt_out_flags)

    if channel_opted_out:
        return

    if user_opted_out:
        return

    roles = message.author.roles

    reply_to = None
    if message.reference:
        reply_to = str(message.reference.message_id)

    emoji_pattern = r"<a?:\w+:\d+>"
    emojis = re.findall(emoji_pattern, message.content)

    data = {
        "message_id": str(message.id),
        "guild_id": guild_id,
        "guild_name": message.guild.name,
        "user_id": user_id,
        "username": str(message.author),
        "channel_id": channel_id,
        "channel_name": str(message.channel),
        "content": message.content,
        "timestamp": message.created_at.isoformat(),
        "role_ids": [str(role.id) for role in roles] if roles else [],
        "reply_to": reply_to,
        "mentions": [str(user.id) for user in message.mentions],
        "attachments": [a.url for a in message.attachments],
        "length": len(message.content),
        "emoji_count": len(emojis),
        "url_count": len(message.content.split("http")),
    }

    def _save_message(d: dict) -> None:
        content = d.get("content", "")
        if content:
            d["tokens"] = list(extract_tokens(normalize_text(content)))
        insert_message(bot.db, d)

    await asyncio.to_thread(_save_message, data)

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
            print(f"Failed to get owner for guild {guild.name} (ID: {guild.id}), skipping welcome message.")
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
        print(f"Failed to send welcome message to {guild.name}'s owner: Forbidden")
    except Exception as e:
        print(
            f"Failed to send welcome message to {guild.name}'s owner: {e}"
        )


async def main():
    print("Starting bot...")
    setup_db()

    migration_counts = await asyncio.to_thread(maybe_run_clickhouse_migration, bot.db)
    if migration_counts.get("skipped_existing_data"):
        print("Startup migration skipped because ClickHouse already contains data.")
    elif migration_counts:
        print(f"Startup migration completed: {migration_counts}")

    await bot.load_extension("cogs.ping")
    await bot.load_extension("cogs.wordcloud")
    await bot.load_extension("cogs.about")
    await bot.load_extension("cogs.optout")
    await bot.load_extension("cogs.network")
    await bot.load_extension("cogs.admin")

    async with bot:
        await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
