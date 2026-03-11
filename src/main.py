import discord
import os
import sys
import asyncio
from pymongo import MongoClient
from discord.ext import commands
import re

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


def setup_db():
    # メッセージコレクションのインデックス設定
    bot.db.messages.create_index("user_id")
    bot.db.messages.create_index("channel_id")
    bot.db.messages.create_index("guild_id")
    bot.db.messages.create_index(
        "message_id",
        unique=True,
        partialFilterExpression={"message_id": {"$exists": True}},
    )
    bot.db.messages.create_index("reply_to")

    # TTL Index: 30日後に自動的に削除
    bot.db.messages.create_index("timestamp", expireAfterSeconds=30 * 24 * 60 * 60)

    # Guild設定のインデックス設定
    bot.db.guild_settings.create_index(
        [("guild_id", 1), ("channel_id", 1), ("frequency", 1)], unique=True
    )
    bot.db.guild_settings.create_index("guild_id")
    bot.db.guild_settings.create_index("enabled")

    # ユーザー設定コレクションのインデックス設定
    bot.db.user_settings.create_index("user_id", unique=True)
    bot.db.user_settings.create_index("opt_out")

    # チャンネル設定コレクションのインデックス設定
    bot.db.channel_settings.create_index(
        [("guild_id", 1), ("channel_id", 1)], unique=True
    )
    bot.db.channel_settings.create_index("opt_out")


@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f"Logged in as {bot.user}")


@bot.event
async def on_message(message):
    if message.author.bot:
        return

    if message.guild is None:
        return

    if channel_opt_out := bot.db.channel_settings.find_one(
        {"guild_id": str(message.guild.id), "channel_id": str(message.channel.id)}
    ):
        # チャンネル単位でOptout=trueならデータ収集しない
        if channel_opt_out.get("opt_out", False):
            return

    if opt_out := bot.db.user_settings.find_one({"user_id": str(message.author.id)}):
        # Optout=trueならデータ収集しない
        if opt_out.get("opt_out", False):
            return

    roles = message.author.roles

    reply_to = None
    if message.reference:
        reply_to = str(message.reference.message_id)

    emoji_pattern = r"<a?:\w+:\d+>"
    emojis = re.findall(emoji_pattern, message.content)

    data = {
        "message_id": str(message.id),
        "guild_id": str(message.guild.id),
        "guild_name": message.guild.name,
        "user_id": str(message.author.id),
        "username": str(message.author),
        "channel_id": str(message.channel.id),
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

    bot.db.messages.insert_one(data)

    await bot.process_commands(message)


def _delete_message_records(message_ids):
    normalized_ids = [str(message_id) for message_id in message_ids]
    result = bot.db.messages.delete_many({"message_id": {"$in": normalized_ids}})
    return result.deleted_count


@bot.event
async def on_guild_remove(guild):
    print(f"Left guild: {guild.name} (ID: {guild.id})")
    # サーバーから退出した際に、そのサーバーのメッセージデータを削除する
    result = bot.db.messages.delete_many({"guild_id": str(guild.id)})
    print(
        f"Deleted {result.deleted_count} messages from the database for guild {guild.name}"
    )
    # 設定の削除
    settings_result = bot.db.guild_settings.delete_many({"guild_id": str(guild.id)})
    print(
        f"Deleted {settings_result.deleted_count} guild settings from the database for guild {guild.name}"
    )
    channel_settings_result = bot.db.channel_settings.delete_many(
        {"guild_id": str(guild.id)}
    )
    print(
        f"Deleted {channel_settings_result.deleted_count} channel settings from the database for guild {guild.name}"
    )


@bot.event
async def on_raw_message_delete(payload):
    """
    メッセージが削除された際のイベントハンドラー
    """
    if payload.guild_id is None:
        return

    deleted_count = _delete_message_records([payload.message_id])
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

    deleted_count = _delete_message_records(payload.message_ids)
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
            print(f"{guild.name} のオーナー情報が取得できませんでした")
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

    async with bot:
        await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
