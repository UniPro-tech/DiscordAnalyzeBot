import asyncio
import io
from datetime import datetime
from typing import Optional

import discord
import pandas as pd
import matplotlib.pyplot as plt
from discord import app_commands
from discord.ext import commands
import matplotlib.font_manager as fm

from libs.parser import parse_discord_timestamp
from libs.visualization_common import resolve_font_path


def setup_japanese_font():
    """Matplotlibのデフォルトフォントを日本語対応フォントに設定します"""
    font_path = resolve_font_path()
    if font_path:
        # フォントをMatplotlibのシステムに追加
        fm.fontManager.addfont(font_path)
        # フォントプロパティを取得し、ファミリー名として設定
        font_prop = fm.FontProperties(fname=font_path)
        plt.rcParams["font.family"] = font_prop.get_name()
    else:
        print("Warning: Japanese font not found.")


class Statistics(commands.Cog):
    graphs_group = app_commands.Group(
        name="graphs",
        description="サーバーの各種統計グラフを生成します",
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _build_query(
        self,
        guild_id: str,
        start_dt: Optional[datetime],
        end_dt: Optional[datetime],
        user_id: Optional[str],
        channel_id: Optional[str],
    ) -> dict:
        """MongoDBの検索クエリを構築します"""
        query = {"guild_id": guild_id}

        if start_dt or end_dt:
            query["timestamp"] = {}
            if start_dt:
                query["timestamp"]["$gte"] = start_dt
            if end_dt:
                query["timestamp"]["$lte"] = end_dt

        if user_id:
            query["user_id"] = user_id

        if channel_id:
            query["channel_id"] = channel_id

        return query

    def _generate_single_graph(
        self, data: list, graph_type: str
    ) -> Optional[discord.File]:
        """
        別スレッドで実行される単一グラフ生成関数
        """
        if not data:
            return None

        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # タイムゾーンがある場合は日本時間に変換してから除去
        if df["timestamp"].dt.tz is not None:
            df["timestamp"] = (
                df["timestamp"].dt.tz_convert("Asia/Tokyo").dt.tz_localize(None)
            )

        plt.style.use("default")
        setup_japanese_font()
        fig = plt.figure(figsize=(8, 5))
        buf = io.BytesIO()

        try:
            if graph_type == "posts":
                # 1. 投稿数の折れ線グラフ (月別)
                monthly_posts = df.resample("ME", on="timestamp").size()
                monthly_posts.index = monthly_posts.index.strftime("%Y-%m")

                # kind="line" に変更し、markerを追加
                monthly_posts.plot(
                    kind="line", marker="o", color="tab:blue", linewidth=2
                )

                plt.title("月別投稿数の推移")
                plt.xlabel("年月")
                plt.ylabel("投稿数")
                plt.xticks(rotation=45)
                plt.grid(
                    True, linestyle="--", alpha=0.7
                )  # 値を読み取りやすくするグリッド

            elif graph_type == "users":
                # 2. 投稿者数の折れ線グラフ (月別ユニークユーザー数)
                monthly_users = df.resample("ME", on="timestamp")["user_id"].nunique()
                monthly_users.index = monthly_users.index.strftime("%Y-%m")

                # kind="line" に変更し、markerを追加
                monthly_users.plot(
                    kind="line", marker="o", color="tab:green", linewidth=2
                )

                plt.title("月別アクティブユーザー数（投稿者数）の推移")
                plt.xlabel("年月")
                plt.ylabel("ユーザー数")
                plt.xticks(rotation=45)
                plt.grid(True, linestyle="--", alpha=0.7)

            elif graph_type == "channels":
                # 3. 投稿チャンネルの円グラフ
                plt.figure(figsize=(7, 7))  # 円グラフ用にサイズ上書き
                channel_counts = df["channel_name"].value_counts()
                if len(channel_counts) > 10:
                    top_10 = channel_counts[:10]
                    others = pd.Series([channel_counts[10:].sum()], index=["その他"])
                    channel_counts = pd.concat([top_10, others])
                channel_counts.plot(
                    kind="pie", autopct="%1.1f%%", startangle=90, cmap="Pastel1"
                )
                plt.title("投稿チャンネルの割合")
                plt.ylabel("")

            elif graph_type == "moving_avg":
                # 4. 年での移動平均 (日別投稿数の365日移動平均)
                fig.set_size_inches(10, 5)  # 横長に上書き
                daily_posts = df.resample("D", on="timestamp").size()
                yearly_moving_avg = daily_posts.rolling(
                    window=365, min_periods=1
                ).mean()
                plt.plot(
                    daily_posts.index,
                    daily_posts.values,
                    label="日別投稿数",
                    color="lightgray",
                    alpha=0.5,
                )
                plt.plot(
                    yearly_moving_avg.index,
                    yearly_moving_avg.values,
                    label="1年(365日)移動平均",
                    color="red",
                    linewidth=2,
                )
                plt.title("投稿数の推移と年(365日)移動平均")
                plt.xlabel("年月日")
                plt.ylabel("投稿数")
                plt.legend()
                plt.grid(True, linestyle="--", alpha=0.7)

            plt.tight_layout()
            plt.savefig(buf, format="png")
            buf.seek(0)
            return discord.File(buf, filename=f"graph_{graph_type}.png")

        finally:
            plt.close()  # メモリリーク防止

    async def _handle_graph_request(
        self,
        interaction: discord.Interaction,
        graph_type: str,
        start: Optional[str],
        end: Optional[str],
        user: Optional[discord.User],
        channel: Optional[discord.TextChannel],
    ):
        """全グラフコマンド共通のデータ取得・生成・送信ロジック"""
        if interaction.guild_id is None:
            await interaction.response.send_message(
                "このコマンドはサーバー内でご利用ください。", ephemeral=True
            )
            return

        # タイムスタンプのパース
        start_dt, end_dt = None, None
        try:
            if start:
                start_dt = parse_discord_timestamp(start)
            if end:
                end_dt = parse_discord_timestamp(end)
        except ValueError:
            await interaction.response.send_message(
                "時間の指定が正しくありません。Discordのタイムスタンプ機能を使って入力してください。",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)

        user_id = str(user.id) if user else None
        channel_id = str(channel.id) if channel else None
        guild_id = str(interaction.guild_id)

        # クエリ構築とDBアクセス
        query = self._build_query(guild_id, start_dt, end_dt, user_id, channel_id)

        def fetch_data():
            cursor = self.bot.db.messages.find(
                query, {"_id": 0, "user_id": 1, "channel_name": 1, "timestamp": 1}
            )
            return list(cursor)

        try:
            data = await asyncio.to_thread(fetch_data)
        except Exception as e:
            print(f"Database error in graphs: {e}")
            await interaction.followup.send(
                "データベースからのデータ取得中にエラーが発生しました。"
            )
            return

        if not data:
            await interaction.followup.send(
                "指定された条件に一致するメッセージが見つかりませんでした。"
            )
            return

        # グラフ生成
        file = await asyncio.to_thread(self._generate_single_graph, data, graph_type)

        if not file:
            await interaction.followup.send("グラフの生成に失敗しました。")
            return

        await interaction.followup.send(
            content=f":bar_chart: {interaction.user.mention} グラフの生成が完了しました！\n(対象データ: {len(data):,}件)",
            file=file,
        )

    # =========================================================
    # サブコマンド群
    # =========================================================

    @graphs_group.command(
        name="posts", description="月別の投稿数推移グラフを生成します"
    )
    @app_commands.describe(
        start="解析する期間の初め (@time機能を利用)",
        end="解析する期間の終わり (@time機能を利用)",
        user="特定のユーザーで絞り込み",
        channel="特定のチャンネルで絞り込み",
    )
    async def graphs_posts(
        self,
        interaction: discord.Interaction,
        start: Optional[str] = None,
        end: Optional[str] = None,
        user: Optional[discord.User] = None,
        channel: Optional[discord.TextChannel] = None,
    ):
        await self._handle_graph_request(
            interaction, "posts", start, end, user, channel
        )

    @graphs_group.command(
        name="users", description="月別のアクティブユーザー数推移グラフを生成します"
    )
    @app_commands.describe(
        start="解析する期間の初め (@time機能を利用)",
        end="解析する期間の終わり (@time機能を利用)",
        user="特定のユーザーで絞り込み",
        channel="特定のチャンネルで絞り込み",
    )
    async def graphs_users(
        self,
        interaction: discord.Interaction,
        start: Optional[str] = None,
        end: Optional[str] = None,
        user: Optional[discord.User] = None,
        channel: Optional[discord.TextChannel] = None,
    ):
        await self._handle_graph_request(
            interaction, "users", start, end, user, channel
        )

    @graphs_group.command(
        name="channels", description="投稿チャンネルの割合を円グラフで生成します"
    )
    @app_commands.describe(
        start="解析する期間の初め (@time機能を利用)",
        end="解析する期間の終わり (@time機能を利用)",
        user="特定のユーザーで絞り込み",
    )
    async def graphs_channels(
        self,
        interaction: discord.Interaction,
        start: Optional[str] = None,
        end: Optional[str] = None,
        user: Optional[discord.User] = None,
    ):
        # チャンネル円グラフでチャンネル絞り込みは矛盾するため、引数からchannelを除外しています
        await self._handle_graph_request(
            interaction, "channels", start, end, user, None
        )

    @graphs_group.command(
        name="trend", description="投稿数の推移と1年移動平均グラフを生成します"
    )
    @app_commands.describe(
        start="解析する期間の初め (@time機能を利用)",
        end="解析する期間の終わり (@time機能を利用)",
        user="特定のユーザーで絞り込み",
        channel="特定のチャンネルで絞り込み",
    )
    async def graphs_trend(
        self,
        interaction: discord.Interaction,
        start: Optional[str] = None,
        end: Optional[str] = None,
        user: Optional[discord.User] = None,
        channel: Optional[discord.TextChannel] = None,
    ):
        await self._handle_graph_request(
            interaction, "moving_avg", start, end, user, channel
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(Statistics(bot))
