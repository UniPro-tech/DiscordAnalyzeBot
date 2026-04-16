import asyncio
import io
import concurrent.futures
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


def _generate_graph_worker(data: list, graph_type: str) -> Optional[bytes]:
    """別プロセスで実行されるグラフ生成ワーカー"""
    if not data:
        return None

    # 1. データの準備
    df = pd.DataFrame(data)
    # 集約データの形式に応じてDataFrameを整形
    if graph_type == "channels":
        # {"_id": "channel_name", "count": N} 形式
        df = df.rename(columns={"_id": "channel_name"})
    else:
        # {"_id": "2023-10", "count": N} または {"_id": "2023-10-01", "count": N} 形式
        df = df.rename(columns={"_id": "date"})
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()

    # 2. 描画設定 (ここで1回だけ実行)
    plt.style.use("default")
    setup_japanese_font()

    fig, ax = plt.subplots(figsize=(8, 5))
    buf = io.BytesIO()

    try:
        if graph_type == "posts":
            # 1. 投稿数の折れ線グラフ (月別)
            df.index = df.index.strftime("%Y-%m")
            monthly_posts = df["count"]

            monthly_posts.plot(
                kind="line", marker="o", color="tab:blue", linewidth=2, ax=ax
            )

            ax.set_title("月別投稿数の推移")
            ax.set_xlabel("年月")
            ax.set_ylabel("投稿数")
            ax.tick_params(axis="x", rotation=45)
            ax.grid(True, linestyle="--", alpha=0.7)

        elif graph_type == "users":
            # 2. 投稿者数の折れ線グラフ (月別ユニークユーザー数)
            monthly_users = df.resample("ME", on="timestamp")["user_id"].nunique()
            monthly_users.index = monthly_users.index.strftime("%Y-%m")

            monthly_users.plot(
                kind="line", marker="o", color="tab:green", linewidth=2, ax=ax
            )

            ax.set_title("月別アクティブユーザー数（投稿者数）の推移")
            ax.set_xlabel("年月")
            ax.set_ylabel("ユーザー数")
            ax.tick_params(axis="x", rotation=45)
            ax.grid(True, linestyle="--", alpha=0.7)

        elif graph_type == "channels":
            # 3. 投稿チャンネルの円グラフ
            fig.set_size_inches(7, 7)
            channel_counts = df["channel_name"].value_counts()
            if len(channel_counts) > 10:
                top_10 = channel_counts[:10]
                others = pd.Series([channel_counts[10:].sum()], index=["その他"])
                channel_counts = pd.concat([top_10, others])

            channel_counts.plot(
                kind="pie",
                autopct="%1.1f%%",
                startangle=90,
                cmap="Pastel1",
                ax=ax,
            )
            ax.set_title("投稿チャンネルの割合")
            ax.set_ylabel("")

        elif graph_type == "moving_avg":
            # 4. 年での移動平均 (日別投稿数の365日移動平均)
            fig.set_size_inches(10, 5)
            daily_posts = df.resample("D", on="timestamp").size()
            yearly_moving_avg = daily_posts.rolling(window=365, min_periods=1).mean()

            ax.plot(
                daily_posts.index,
                daily_posts.values,
                label="日別投稿数",
                color="lightgray",
                alpha=0.5,
            )
            ax.plot(
                yearly_moving_avg.index,
                yearly_moving_avg.values,
                label="1年(365日)移動平均",
                color="red",
                linewidth=2,
            )
            ax.set_title("投稿数の推移と年(365日)移動平均")
            ax.set_xlabel("年月日")
            ax.set_ylabel("投稿数")
            ax.legend()
            ax.grid(True, linestyle="--", alpha=0.7)

        fig.tight_layout()
        fig.savefig(buf, format="png")
        # buf.seek(0) は getvalue() の場合は不要ですが残しても無害です
        return buf.getvalue()

    finally:
        # メモリリーク防止 (ProcessPool使用時はプロセス終了で解放されますが、安全のための明記は良い習慣です)
        plt.close(fig)


class Statistics(commands.Cog):
    graphs_group = app_commands.Group(
        name="graphs",
        description="サーバーの各種統計グラフを生成します",
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.process_pool = concurrent.futures.ProcessPoolExecutor(max_workers=2)

    def cog_unload(self):
        # Cogアンロード時にプールを安全に閉じる
        self.process_pool.shutdown(wait=False)

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
            # 1. ベースとなる検索条件
            pipeline = [{"$match": query}]

            # 2. グラフの種類に応じた集計パイプラインの追加
            if graph_type == "channels":
                pipeline.extend(
                    [
                        {"$group": {"_id": "$channel_name", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                    ]
                )
            elif graph_type == "users":
                # ユーザー数は「月とユーザーIDでグループ化」してから「月ごとの数をカウント」
                pipeline.extend(
                    [
                        {
                            "$group": {
                                "_id": {
                                    "date": {
                                        "$dateToString": {
                                            "format": "%Y-%m",
                                            "date": "$timestamp",
                                            "timezone": "Asia/Tokyo",
                                        }
                                    },
                                    "user_id": "$user_id",
                                }
                            }
                        },
                        {"$group": {"_id": "$_id.date", "count": {"$sum": 1}}},
                        {"$sort": {"_id": 1}},
                    ]
                )
            else:
                # posts(月別) と moving_avg(日別) の投稿数カウント
                format_str = "%Y-%m-%d" if graph_type == "moving_avg" else "%Y-%m"
                pipeline.extend(
                    [
                        {
                            "$group": {
                                "_id": {
                                    "$dateToString": {
                                        "format": format_str,
                                        "date": "$timestamp",
                                        "timezone": "Asia/Tokyo",
                                    }
                                },
                                "count": {"$sum": 1},
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                )

            # allowDiskUse=True を指定して大規模集計時のメモリ制限を回避
            cursor = self.bot.db.messages.aggregate(pipeline, allowDiskUse=True)
            return list(cursor)

        try:
            # 取得されるデータは [{"_id": "2023-10", "count": 150}, ...] のような軽量なリストになる
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
        try:
            loop = asyncio.get_running_loop()
            image_bytes = await loop.run_in_executor(
                self.process_pool, _generate_graph_worker, data, graph_type
            )
        except Exception as e:
            print(f"Graph generation error in graphs: {e}")
            await interaction.followup.send("グラフの生成中にエラーが発生しました。")
            return

        if not image_bytes:
            await interaction.followup.send("グラフの生成に失敗しました。")
            return

        file = discord.File(io.BytesIO(image_bytes), filename=f"graph_{graph_type}.png")
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
