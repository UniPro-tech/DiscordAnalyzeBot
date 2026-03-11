import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional
from datetime import timedelta
from collections import defaultdict

from libs.visualize import generate_conversation_network
from libs.embed import EmbedHelper


class ConversationNetwork(commands.Cog):

    network_group = app_commands.Group(
        name="network",
        description="会話ネットワーク分析",
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @network_group.command(
        name="generate",
        description="会話ネットワークを生成します",
    )
    @app_commands.describe(
        period="解析する期間（日）",
        user="特定ユーザーのみ解析",
        channel="特定チャンネルのみ解析",
    )
    async def generate_network(
        self,
        interaction: discord.Interaction,
        period: Optional[str] = None,
        user: Optional[discord.User] = None,
        channel: Optional[discord.TextChannel] = None,
    ):
        embed_helper = EmbedHelper(function_name="ConversationNetwork")

        if interaction.guild_id is None:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはサーバー内で使ってね。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        # period
        period_filter = {}
        if period:
            try:
                period_filter = {
                    "timestamp": {
                        "$gte": (
                            discord.utils.utcnow() - timedelta(days=int(period))
                        ).isoformat()
                    }
                }
            except ValueError:
                embed = embed_helper.create_error_embed(
                    title="エラー",
                    description="期間は数値で指定してね。",
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return

        user_filter = {}
        if user:
            user_filter = {"user_id": str(user.id)}

        channel_filter = {}
        if channel:
            channel_filter = {"channel_id": str(channel.id)}

        await interaction.response.defer(thinking=True)

        try:
            docs = list(
                self.bot.db.messages.find(
                    {
                        "guild_id": str(interaction.guild_id),
                        **period_filter,
                        **user_filter,
                        **channel_filter,
                    }
                )
                .sort("timestamp", -1)
                .limit(5000)
            )
        except Exception as e:
            embed = embed_helper.create_error_embed(
                title="DBエラー",
                description="データ取得中にエラーが発生しました",
            )
            await interaction.followup.send(embed=embed)
            print(e)
            return

        if not docs:
            embed = embed_helper.create_warning_embed(
                title="データ不足",
                description="解析対象メッセージがありません。",
            )
            await interaction.followup.send(embed=embed)
            return

        # message map
        msg_map = {doc["message_id"]: doc for doc in docs}

        edges = defaultdict(int)

        for msg in docs:

            author = msg.get("user_id")

            # reply
            reply_to = msg.get("reply_to")
            if reply_to and reply_to in msg_map:
                other = msg_map[reply_to]["user_id"]
                if author != other:
                    edges[tuple(sorted([author, other]))] += 1

            # mention
            for mentioned in msg.get("mentions", []):
                if mentioned != author:
                    edges[tuple(sorted([author, mentioned]))] += 1

        if not edges:
            embed = embed_helper.create_warning_embed(
                title="会話不足",
                description="会話ネットワークを作れるデータがありません。",
            )
            await interaction.followup.send(embed=embed)
            return

        # user id → name
        user_map = {}

        for a, b in edges.keys():
            if a not in user_map:
                member = interaction.guild.get_member(int(a))
                user_map[a] = member.display_name if member else a

            if b not in user_map:
                member = interaction.guild.get_member(int(b))
                user_map[b] = member.display_name if member else b

        named_edges = {
            (user_map[a], user_map[b]): count for (a, b), count in edges.items()
        }

        try:
            image_buffer = generate_conversation_network(named_edges)
        except Exception as e:
            embed = embed_helper.create_error_embed(
                title="生成エラー",
                description="ネットワーク生成中にエラーが発生しました",
            )
            await interaction.followup.send(embed=embed)
            print(e)
            return

        embed = embed_helper.create_success_embed(
            title="会話ネットワーク生成",
            description=f"{len(docs)}件のメッセージを解析しました",
            binary_data=image_buffer.getvalue(),
            binary_filename="network.png",
        )

        await interaction.followup.send(
            embed=embed,
            file=discord.File(image_buffer, filename="network.png"),
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(ConversationNetwork(bot))
