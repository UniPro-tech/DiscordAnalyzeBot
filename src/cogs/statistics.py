import asyncio
import io
import concurrent.futures
from datetime import datetime
from typing import Optional

import discord
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from discord import app_commands
from discord.ext import commands
import matplotlib.font_manager as fm

from libs.parser import parse_discord_timestamp
from libs.visualization_common import resolve_font_path
from libs.embed import EmbedHelper


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


def _generate_graph_worker(
    data: list, graph_type: str, interval: str = "monthly"
) -> Optional[bytes]:
    """別プロセスで実行されるグラフ生成ワーカー"""
    if not data:
        return None

    # 1. データの準備
    df = pd.DataFrame(data)

    # 集約データの形式に応じてDataFrameを整形
    if graph_type == "channels":
        pass
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
            # 1. 投稿数の折れ線グラフ (月別 or 日別)
            title_prefix = "日別" if interval == "daily" else "月別"
            xlabel = "年月日" if interval == "daily" else "年月"

            # 修正: DatetimeIndexのままプロットすることで、PandasがよしなにX軸目盛りを間引いてくれます
            posts_data = df["count"]

            # データ点が多い（日別で長期など）場合はマーカーを消してスッキリさせる
            marker = "o" if len(posts_data) <= 60 else None

            posts_data.plot(
                kind="line", marker=marker, color="tab:blue", linewidth=2, ax=ax
            )

            ax.set_title(f"{title_prefix}投稿数の推移")
            ax.set_xlabel(xlabel)
            ax.set_ylabel("投稿数")
            ax.grid(True, linestyle="--", alpha=0.7)

        elif graph_type == "users":
            # 2. 投稿者数の折れ線グラフ (月別 or 日別ユニークユーザー数)
            title_prefix = "日別" if interval == "daily" else "月別"
            xlabel = "年月日" if interval == "daily" else "年月"

            users_data = df["count"]
            marker = "o" if len(users_data) <= 60 else None

            users_data.plot(
                kind="line", marker=marker, color="tab:green", linewidth=2, ax=ax
            )

            ax.set_title(f"{title_prefix}アクティブユーザー数（投稿者数）の推移")
            ax.set_xlabel(xlabel)
            ax.set_ylabel("ユーザー数")
            ax.grid(True, linestyle="--", alpha=0.7)

        elif graph_type == "channels":
            fig.set_size_inches(7, 7)

            # channel_id でインデックスを作成し、名前でラベル付け
            channel_counts = df.set_index("_id")["count"]
            channel_counts.index = df.set_index("_id")["channel_name"]
            # 重複する名前がある場合は channel_id を付与して区別
            if channel_counts.index.duplicated().any():
                channel_counts.index = [
                    f"{name} ({id_})" if dup else name
                    for name, id_, dup in zip(
                        df["channel_name"],
                        df["_id"],
                        df["channel_name"].duplicated(keep=False),
                    )
                ]

            if len(channel_counts) > 10:
                top_10 = channel_counts.iloc[:10]
                others = pd.Series([channel_counts.iloc[10:].sum()], index=["その他"])
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

            daily_posts = df["count"].resample("D").sum().fillna(0)
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
        return buf.getvalue()

    finally:
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
        self.process_pool.shutdown(wait=True, cancel_futures=True)

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
        interval: str = "monthly",  # 月別か日別かの指定を受け取る
    ):
        """全グラフコマンド共通のデータ取得・生成・送信ロジック"""
        embed_helper = EmbedHelper(function_name="Statistics")
        if interaction.guild_id is None:
            embed = embed_helper.create_guild_only_error()
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        # タイムスタンプのパース
        start_dt, end_dt = None, None
        try:
            if start:
                start_dt = parse_discord_timestamp(start)
            if end:
                end_dt = parse_discord_timestamp(end)
        except ValueError:
            embed = embed_helper.create_error_embed(
                title="入力エラー",
                description="時間の指定が正しくありません。Discordのタイムスタンプ機能を使って入力してください。",
            )
            await interaction.response.send_message(
                embed=embed,
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
                        {
                            "$group": {
                                "_id": "$channel_id",
                                "count": {"$sum": 1},
                                "channel_name": {"$first": "$channel_name"},
                            }
                        },
                        {"$sort": {"count": -1}},
                    ]
                )
            elif graph_type == "users":
                # intervalに応じてフォーマットを切り替え
                format_str = "%Y-%m-%d" if interval == "daily" else "%Y-%m"
                pipeline.extend(
                    [
                        {
                            "$group": {
                                "_id": {
                                    "date": {
                                        "$dateToString": {
                                            "format": format_str,
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
                # posts と moving_avg の投稿数カウント
                if graph_type == "moving_avg":
                    format_str = "%Y-%m-%d"
                else:
                    format_str = "%Y-%m-%d" if interval == "daily" else "%Y-%m"

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

            cursor = self.bot.db.messages.aggregate(pipeline, allowDiskUse=True)
            return list(cursor)

        try:
            data = await asyncio.to_thread(fetch_data)
        except Exception as e:
            print(f"Database error in graphs: {e}")
            embed = embed_helper.create_error_embed(
                title="内部エラー",
                description="データベースからのデータ取得中にエラーが発生しました。",
            )
            await interaction.followup.send(embed=embed)
            return

        if not data:
            embed = embed_helper.create_no_data_error(is_filtered=True)
            await interaction.followup.send(embed=embed)
            return

        # グラフ生成 (intervalをワーカーに渡す)
        try:
            loop = asyncio.get_running_loop()
            image_bytes = await loop.run_in_executor(
                self.process_pool, _generate_graph_worker, data, graph_type, interval
            )
        except Exception as e:
            print(f"Graph generation error in graphs: {e}")
            embed = embed_helper.create_error_embed(
                title="内部エラー", description="グラフの生成中にエラーが発生しました。"
            )
            await interaction.followup.send(embed=embed)
            return

        if not image_bytes:
            embed = embed_helper.create_error_embed(
                title="内部エラー", description="グラフの生成中にエラーが発生しました。"
            )
            await interaction.followup.send(embed=embed)
            return

        embed = embed_helper.create_success_embed(
            title=":bar_chart: グラフの生成完了",
            description=f"{interaction.user.mention} グラフの生成が完了しました！\n(集計バケット数: {len(data):,}件)",
            binary_data=image_bytes,
            filename=f"graph_{graph_type}.png",
        )
        await interaction.followup.send(embed=embed)

    # =========================================================
    # サブコマンド群
    # =========================================================

    @graphs_group.command(name="posts", description="投稿数推移グラフを生成します")
    @app_commands.describe(
        start="解析する期間の初め (@time機能を利用)",
        end="解析する期間の終わり (@time機能を利用)",
        user="特定のユーザーで絞り込み",
        channel="特定のチャンネルで絞り込み",
        interval="集計の間隔（月別 または 日別）",
    )
    @app_commands.choices(
        interval=[
            app_commands.Choice(name="月別", value="monthly"),
            app_commands.Choice(name="日別", value="daily"),
        ]
    )
    async def graphs_posts(
        self,
        interaction: discord.Interaction,
        start: Optional[str] = None,
        end: Optional[str] = None,
        user: Optional[discord.User] = None,
        channel: Optional[discord.TextChannel] = None,
        interval: Optional[app_commands.Choice[str]] = None,
    ):
        interval_value = interval.value if interval else "monthly"
        await self._handle_graph_request(
            interaction, "posts", start, end, user, channel, interval=interval_value
        )

    @graphs_group.command(
        name="users", description="アクティブユーザー数推移グラフを生成します"
    )
    @app_commands.describe(
        start="解析する期間の初め (@time機能を利用)",
        end="解析する期間の終わり (@time機能を利用)",
        user="特定のユーザーで絞り込み",
        channel="特定のチャンネルで絞り込み",
        interval="集計の間隔（月別 または 日別）",
    )
    @app_commands.choices(
        interval=[
            app_commands.Choice(name="月別", value="monthly"),
            app_commands.Choice(name="日別", value="daily"),
        ]
    )
    async def graphs_users(
        self,
        interaction: discord.Interaction,
        start: Optional[str] = None,
        end: Optional[str] = None,
        user: Optional[discord.User] = None,
        channel: Optional[discord.TextChannel] = None,
        interval: Optional[app_commands.Choice[str]] = None,
    ):
        interval_value = interval.value if interval else "monthly"
        await self._handle_graph_request(
            interaction, "users", start, end, user, channel, interval=interval_value
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
