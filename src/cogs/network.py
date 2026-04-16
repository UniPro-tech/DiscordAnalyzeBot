import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional
from datetime import datetime

from libs.embed import EmbedHelper
from libs.network_service import (
    build_node_labels,
    build_conversation_edges,
    fetch_network_documents,
    generate_conversation_network,
)
from libs.parser import parse_discord_timestamp


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
        start="解析する期間の初め (例: 2023-04-01)",
        end="解析する期間の終わり (例: 2023-04-30)",
        user="特定ユーザーのみ解析",
        channel="特定チャンネルのみ解析",
    )
    async def generate_network(
        self,
        interaction: discord.Interaction,
        start: Optional[str] = None,
        end: Optional[str] = None,
        user: Optional[discord.User] = None,
        channel: Optional[discord.TextChannel] = None,
    ):
        embed_helper = EmbedHelper(function_name="ConversationNetwork")

        if interaction.guild_id is None:
            embed = embed_helper.create_guild_only_error()
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if interaction.guild is None:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="サーバー情報を取得できませんでした。時間をおいて再実行してください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        await interaction.response.defer(thinking=True)

        # --- 文字列の期間引数を datetime に変換 ---
        start_dt: Optional[datetime] = None
        end_dt: Optional[datetime] = None

        try:
            # 注: プロジェクトに専用のパース関数があればそれを使ってください
            # ここでは簡易的に ISO形式(YYYY-MM-DD) を想定しています
            if start:
                start_dt = parse_discord_timestamp(start)
            if end:
                end_dt = parse_discord_timestamp(end)
        except ValueError:
            embed = embed_helper.create_error_embed(
                title="引数エラー",
                description="日付の形式が正しくありません (YYYY-MM-DD 形式で入力してください)",
            )
            await interaction.followup.send(embed=embed)
            return

        try:
            # fetch_network_documents の引数を during_days から start, end に変更
            docs = fetch_network_documents(
                self.bot.db,
                str(interaction.guild_id),
                start=start_dt,  # 修正
                end=end_dt,  # 修正
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
            print(f"Error fetching documents: {e}")
            return

        if not docs:
            embed = embed_helper.create_no_data_error()
            await interaction.followup.send(embed=embed)
            return

        edges, invalid_doc_count = build_conversation_edges(docs)
        valid_doc_count = len(docs) - invalid_doc_count

        if valid_doc_count <= 0:
            embed = embed_helper.create_no_data_error()
            await interaction.followup.send(embed=embed)
            return

        if not edges:
            embed = embed_helper.create_no_data_error()
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
            embed = embed_helper.create_no_data_error()
            await interaction.followup.send(embed=embed)
            return

        try:
            image_buffer = generate_conversation_network(edges, labels=node_labels)
        except ValueError:
            embed = embed_helper.create_no_data_error()
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
            print(f"Error generating network: {e}")
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
