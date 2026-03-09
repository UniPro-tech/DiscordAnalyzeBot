import discord
import os
import asyncio
from pymongo import MongoClient
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN")
DB_DSN = os.getenv("MONGODB_DSN")

intents = discord.Intents.default()
intents.message_content = True


class AnalyzerBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)


bot = AnalyzerBot()

# MongoDB
client_db = MongoClient(DB_DSN)
bot.db = client_db["discord_analyzer"]


def setup_db():
    bot.db.messages.create_index("user_id")
    bot.db.messages.create_index("channel_id")
    bot.db.messages.create_index("guild_id")

    # TTL Index: 30日後に自動的に削除
    bot.db.messages.create_index("timestamp", expireAfterSeconds=30 * 24 * 60 * 60)


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

    data = {
        "guild_id": str(message.guild.id),
        "guild_name": message.guild.name,
        "user_id": str(message.author.id),
        "username": str(message.author),
        "channel_id": str(message.channel.id),
        "channel_name": str(message.channel),
        "content": message.content,
        "timestamp": message.created_at.isoformat(),
    }

    bot.db.messages.insert_one(data)

    await bot.process_commands(message)


async def main():
    print("Starting bot...")
    setup_db()

    await bot.load_extension("cogs.ping")
    await bot.load_extension("cogs.wordcloud")

    async with bot:
        await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
