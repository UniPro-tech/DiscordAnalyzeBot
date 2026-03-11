import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional
from datetime import timedelta
from collections import defaultdict

from libs.visualize import generate_conversation_network
from libs.embed import EmbedHelper


class ConversationNetwork(commands.Cog):

    MAX_MESSAGE_COUNT = 5000

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
                description="このコマンドはサーバー内で使ってください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if interaction.guild is None:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="サーバー情報を取得できませんでした。時間をおいて再実行してください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        # period
        period_filter = {}
        if period:
            try:
                period_days = int(period)
                if period_days <= 0:
                    raise ValueError
                period_filter = {
                    "timestamp": {
                        "$gte": (
                            discord.utils.utcnow() - timedelta(days=period_days)
                        ).isoformat()
                    }
                }
            except ValueError:
                embed = embed_helper.create_error_embed(
                    title="エラー",
                    description="期間は1以上の数値で指定してください。",
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
            except Exception as e:
                embed = embed_helper.create_error_embed(
                    title="エラー",
                    description="期間の処理中にエラーが発生しました。",
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                print(f"Error processing network period: {e}")
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
                    },
                    {
                        "message_id": 1,
                        "user_id": 1,
                        "reply_to": 1,
                        "mentions": 1,
                    },
                )
                .sort("timestamp", -1)
                .limit(self.MAX_MESSAGE_COUNT)
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
        valid_docs = []
        invalid_doc_count = 0

        for doc in docs:
            message_id = doc.get("message_id")
            author_id = doc.get("user_id")

            if message_id is None or author_id is None:
                invalid_doc_count += 1
                continue

            valid_docs.append(
                {
                    "message_id": str(message_id),
                    "user_id": str(author_id),
                    "reply_to": (
                        str(doc["reply_to"])
                        if doc.get("reply_to") is not None
                        else None
                    ),
                    "mentions": [
                        str(mentioned)
                        for mentioned in doc.get("mentions", [])
                        if mentioned is not None
                    ],
                }
            )

        if not valid_docs:
            embed = embed_helper.create_warning_embed(
                title="データ不足",
                description="解析に使えるメッセージがありませんでした。",
            )
            await interaction.followup.send(embed=embed)
            return

        msg_map = {doc["message_id"]: doc for doc in valid_docs}

        edges = defaultdict(int)

        for msg in valid_docs:

            author = msg.get("user_id")
            if author is None:
                continue

            # reply
            reply_to = msg.get("reply_to")
            if reply_to and reply_to in msg_map:
                other = msg_map[reply_to].get("user_id")
                if author != other:
                    edges[tuple(sorted([author, other]))] += 1

            # mention
            mentions = msg.get("mentions", [])
            if not isinstance(mentions, list):
                continue

            for mentioned in mentions:
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
                try:
                    member = interaction.guild.get_member(int(a))
                    user_map[a] = member.display_name if member else a
                except (TypeError, ValueError):
                    user_map[a] = a

            if b not in user_map:
                try:
                    member = interaction.guild.get_member(int(b))
                    user_map[b] = member.display_name if member else b
                except (TypeError, ValueError):
                    user_map[b] = b

        named_edges = {
            (user_map[a], user_map[b]): count for (a, b), count in edges.items()
        }

        if not named_edges:
            embed = embed_helper.create_warning_embed(
                title="会話不足",
                description="ネットワーク図に変換できるデータがありませんでした。",
            )
            await interaction.followup.send(embed=embed)
            return

        try:
            image_buffer = generate_conversation_network(named_edges)
        except ValueError:
            embed = embed_helper.create_warning_embed(
                title="会話不足",
                description="表示条件を満たすつながりが少ないため、ネットワーク図を生成できませんでした。",
            )
            await interaction.followup.send(embed=embed)
            return
        except RuntimeError:
            embed = embed_helper.create_error_embed(
                title="内部エラー",
                description="描画に必要なフォントが見つからないため、生成できませんでした。",
            )
            await interaction.followup.send(embed=embed)
            return
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
            description=(
                f"{len(valid_docs)}件のメッセージを解析しました"
                + (
                    f"\n不正データ {invalid_doc_count}件 は自動でスキップしました"
                    if invalid_doc_count
                    else ""
                )
            ),
            binary_data=image_buffer.getvalue(),
            binary_filename="network.png",
        )

        await interaction.followup.send(
            embed=embed,
            file=discord.File(image_buffer, filename="network.png"),
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(ConversationNetwork(bot))
