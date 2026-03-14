import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional

from libs.embed import EmbedHelper
from libs.network_service import (
    build_node_labels,
    build_conversation_edges,
    fetch_network_documents,
    generate_conversation_network,
)
from libs.wordcloud_service import parse_during_days


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
        during="解析する期間（日）。1なら当日0:00以降、2なら前日0:00以降",
        user="特定ユーザーのみ解析",
        channel="特定チャンネルのみ解析",
    )
    async def generate_network(
        self,
        interaction: discord.Interaction,
        during: Optional[str] = None,
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

        if during:
            try:
                during_days = parse_during_days(during)
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
                print(f"Error processing network during: {e}")
                return
        else:
            during_days = None

        await interaction.response.defer(thinking=True)

        try:
            docs = fetch_network_documents(
                self.bot.db,
                str(interaction.guild_id),
                during_days=during_days,
                user_id=str(user.id) if user else None,
                channel_id=str(channel.id) if channel else None,
                limit=self.MAX_MESSAGE_COUNT,
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

        edges, invalid_doc_count = build_conversation_edges(docs)
        valid_doc_count = len(docs) - invalid_doc_count

        if valid_doc_count <= 0:
            embed = embed_helper.create_warning_embed(
                title="データ不足",
                description="解析に使えるメッセージがありませんでした。",
            )
            await interaction.followup.send(embed=embed)
            return

        if not edges:
            embed = embed_helper.create_warning_embed(
                title="会話不足",
                description="会話ネットワークを作れるデータがありません。",
            )
            await interaction.followup.send(embed=embed)
            return

        def resolve_name(user_id: str) -> str:
            try:
                member = interaction.guild.get_member(int(user_id))
                return member.display_name if member else user_id
            except (TypeError, ValueError):
                return user_id

        node_labels = build_node_labels(edges, resolve_name)

        if not node_labels:
            embed = embed_helper.create_warning_embed(
                title="会話不足",
                description="ネットワーク図に変換できるデータがありませんでした。",
            )
            await interaction.followup.send(embed=embed)
            return

        try:
            image_buffer = generate_conversation_network(edges, labels=node_labels)
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
                f"{valid_doc_count}件のメッセージを解析しました"
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
