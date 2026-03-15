import asyncio
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands, tasks

from libs.embed import EmbedHelper
from libs.wordcloud_service import (
    build_wordcloud_source_text,
    extract_learning_cursor,
    fetch_legacy_last_learn_id,
    fetch_last_learn_cursor,
    fetch_learning_documents,
    fetch_wordcloud_documents,
    generate_wordcloud_image,
    get_schedule_during_days,
    get_frequency_label,
    learn_from_texts,
    migrate_message_tokens,
    parse_during_days,
    parse_schedule_time,
    should_execute_schedule,
    update_compounds,
    update_last_executed,
    update_last_learn_cursor,
    update_last_learn_id,
)
from libs.settings_store import (
    delete_guild_schedule,
    find_guild_schedule,
    insert_guild_schedule,
    list_enabled_schedules,
    list_guild_schedules,
)


class WordCloud(commands.Cog):
    JST = ZoneInfo("Asia/Tokyo")

    wordcloud_group = app_commands.Group(
        name="wordcloud",
        description="ワードクラウド関連のコマンド",
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._background_tasks_started = False
        self.compounds: set[str] = set()

    async def cog_load(self) -> None:
        """Update compounds when the cog loads."""
        print("[WordCloud] Cog loaded. Updating compounds database...")
        await asyncio.to_thread(update_compounds, self.bot.db)
        print("[WordCloud] Compounds database updated on startup.")
        try:
            self.compounds = await asyncio.to_thread(__import__("libs.wordcloud_service", fromlist=["load_compounds"]).load_compounds, self.bot.db)
            print(f"[WordCloud] Loaded {len(self.compounds)} compounds into cache.")
        except Exception as e:
            print(f"[WordCloud] Failed loading compounds cache: {e}")
        asyncio.create_task(self._migrate_tokens_background())

    async def _migrate_tokens_background(self) -> None:
        """tokensフィールドがない旧メッセージをバックグラウンドで一括トークン化する。"""
        try:
            print("[WordCloud] Starting token migration for existing messages...")
            count = await asyncio.to_thread(migrate_message_tokens, self.bot.db)
            if count > 0:
                print(f"[WordCloud] Token migration complete: {count} messages updated.")
            else:
                print("[WordCloud] Token migration: all messages already have tokens.")
        except Exception as e:
            print(f"[WordCloud] Token migration error: {e}")

    def cog_unload(self):
        if self.check_scheduled_wordclouds.is_running():
            self.check_scheduled_wordclouds.cancel()
        if self.background_learn.is_running():
            self.background_learn.cancel()
        if self.update_compounds_task.is_running():
            self.update_compounds_task.cancel()

    def _start_background_task(
        self,
        task_loop: tasks.Loop,
        task_name: str,
        interval_label: str,
    ) -> None:
        if task_loop.is_running():
            print(
                f"[WordCloud] Background task '{task_name}' is already running ({interval_label})."
            )
            return

        task_loop.start()
        print(f"[WordCloud] Started background task '{task_name}' ({interval_label}).")

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        if self._background_tasks_started:
            return

        self._start_background_task(
            self.check_scheduled_wordclouds,
            "check_scheduled_wordclouds",
            "1 minute",
        )
        self._start_background_task(
            self.background_learn,
            "background_learn",
            "10 minutes",
        )
        self._start_background_task(
            self.update_compounds_task,
            "update_compounds_task",
            "24 hours",
        )
        self._background_tasks_started = True
        self._log_background_task_status()

    def _log_background_task_status(self) -> None:
        for task_name, interval_label, task_loop in (
            (
                "check_scheduled_wordclouds",
                "1 minute",
                self.check_scheduled_wordclouds,
            ),
            ("background_learn", "10 minutes", self.background_learn),
            ("update_compounds_task", "24 hours", self.update_compounds_task),
        ):
            next_iteration = task_loop.next_iteration
            next_iteration_text = "pending"

            if next_iteration is not None:
                next_iteration_text = next_iteration.astimezone(self.JST).isoformat()

            print(
                "[WordCloud] Background task status: "
                f"name='{task_name}', interval='{interval_label}', "
                f"running={task_loop.is_running()}, next_iteration={next_iteration_text}"
            )

    @wordcloud_group.command(
        name="generate",
        description="ワードクラウドを生成します",
    )
    @app_commands.describe(
        during="ワードクラウドの元になる期間（単位: 日。1なら当日0:00以降、2なら前日0:00以降）",
        user="特定のユーザーのメッセージからワードクラウドを生成します（省略した場合は全ユーザーのメッセージから生成）",
        channel="特定のチャンネルのメッセージからワードクラウドを生成します（省略した場合は全チャンネルのメッセージから生成）",
        role="特定のロールを持つユーザーのメッセージからワードクラウドを生成します（省略した場合は全ユーザーのメッセージから生成）",
    )
    async def generate(
        self,
        interaction: discord.Interaction,
        during: Optional[str] = None,
        user: Optional[discord.User] = None,
        channel: Optional[discord.TextChannel] = None,
        role: Optional[discord.Role] = None,
    ):
        embed_helper = EmbedHelper(function_name="WordCloud")
        if interaction.guild_id is None:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはサーバー内でご利用ください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        try:
            during_days = parse_during_days(during)
        except ValueError:
            embed = embed_helper.create_error_embed(
                title="エラー", description="期間は1以上の数値で指定してください。"
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        except Exception as error:
            embed = embed_helper.create_error_embed(
                title="エラー", description="期間の処理中にエラーが発生しました"
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            print(f"Error processing during: {error}")
            return

        await interaction.response.defer(thinking=True)

        try:
            docs = await asyncio.to_thread(
                fetch_wordcloud_documents,
                self.bot.db,
                str(interaction.guild_id),
                during_days=during_days,
                user_id=str(user.id) if user is not None else None,
                channel_id=str(channel.id) if channel is not None else None,
                role_id=str(role.id) if role is not None else None,
            )
        except Exception as error:
            embed = embed_helper.create_error_embed(
                title="データベースエラー",
                description="データベースクエリ中にエラーが発生しました",
            )
            await interaction.followup.send(embed=embed)
            print(f"Database query error: {error}")
            return

        if not docs:
            embed = embed_helper.create_warning_embed(
                title="会話不足",
                description="解析対象のメッセージがまだないようです。",
            )
            await interaction.followup.send(embed=embed)
            return

        # 速度優先: 実行時に tokens がないメッセージは無視する。
        docs = [doc for doc in docs if doc.get("tokens")]

        if not docs:
            embed = embed_helper.create_warning_embed(
                title="トークンなし",
                description=(
                    "解析済みトークンを持つメッセージが見つかりません。"
                    "バックグラウンド処理が完行されるのを待つか、管理者にマイグレーションを依頼してください。"
                ),
            )
            await interaction.followup.send(embed=embed)
            return

        try:
            image_buffer = await asyncio.to_thread(
                generate_wordcloud_image,
                self.bot.db,
                docs,
                compounds=self.compounds,
            )
        except ValueError:
            embed = embed_helper.create_warning_embed(
                title="語彙不足",
                description="表示できる単語が不足しています。もう少しメッセージが集まってから再度お試しください。",
            )
            await interaction.followup.send(embed=embed)
            return
        except RuntimeError:
            embed = embed_helper.create_error_embed(
                title="内部エラー",
                description="日本語フォントが見つからないため生成できませんでした。Bot管理者にお問い合わせください。",
            )
            await interaction.followup.send(embed=embed)
            return

        embed = embed_helper.create_success_embed(
            title="生成成功",
            description=f"最新{len(docs)}件のメッセージが取得でき、WordCloudが生成されました！",
            binary_data=image_buffer.getvalue(),
            binary_filename="wordcloud.png",
        )
        await interaction.followup.send(
            embed=embed, file=discord.File(fp=image_buffer, filename="wordcloud.png")
        )

    @wordcloud_group.command(
        name="schedule",
        description="指定されたチャンネルに定期的にワードクラウドを送信するようスケジュールします",
    )
    @app_commands.default_permissions(manage_channels=True)
    @app_commands.describe(
        channel="ワードクラウドを送信するチャンネル",
        frequency="送信頻度 (daily / weekly / monthly)",
        time="送信時刻 (HH:MM, 24時間表記 / JST)",
    )
    @app_commands.choices(
        frequency=[
            app_commands.Choice(name="デイリー (毎日)", value="daily"),
            app_commands.Choice(name="ウィークリー (毎週)", value="weekly"),
            app_commands.Choice(name="マンスリー (毎月)", value="monthly"),
        ]
    )
    async def schedule_wordcloud(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        frequency: app_commands.Choice[str],
        time: str = "09:00",
    ):
        embed_helper = EmbedHelper(function_name="WordCloud Schedule")

        if interaction.guild_id is None:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはサーバー内でご利用ください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        # チャンネル管理権限チェック
        if interaction.permissions.manage_channels is False:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはチャンネル管理権限が必要です。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        parsed_time = parse_schedule_time(time)
        if parsed_time is None:
            embed = embed_helper.create_error_embed(
                title="時刻形式エラー",
                description="時刻は HH:MM (例: 09:00, 21:30) で指定してね。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        # 既存の設定をチェック
        existing = await asyncio.to_thread(
            find_guild_schedule,
            self.bot.db,
            str(interaction.guild_id),
            str(channel.id),
            frequency.value,
        )

        if existing:
            embed = embed_helper.create_warning_embed(
                title="設定済み",
                description=f"{channel.mention} へのワードクラウド{frequency.name}送信は既に設定されています。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        # 新しい設定を追加
        try:
            await asyncio.to_thread(
                insert_guild_schedule,
                self.bot.db,
                str(interaction.guild_id),
                str(channel.id),
                frequency.value,
                f"{parsed_time[0]:02d}:{parsed_time[1]:02d}",
            )

            embed = embed_helper.create_success_embed(
                title="スケジュール設定完了",
                description=(
                    f"{channel.mention} へのワードクラウド{frequency.name}送信を設定したよ！\n"
                    f"送信時刻: {parsed_time[0]:02d}:{parsed_time[1]:02d} (JST)"
                ),
            )
            await interaction.response.send_message(embed=embed)
        except Exception as e:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="スケジュール設定中にエラーが発生しました。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            print(f"Error scheduling wordcloud: {e}")

    @wordcloud_group.command(
        name="list",
        description="このサーバーで設定されているワードクラウドスケジュールを一覧表示します",
    )
    @app_commands.default_permissions(manage_channels=True)
    async def list_schedules(self, interaction: discord.Interaction):
        embed_helper = EmbedHelper(function_name="WordCloud Schedule List")

        if interaction.guild_id is None:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはサーバー内でご利用ください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        # チャンネル管理権限チェック
        if interaction.permissions.manage_channels is False:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはチャンネル管理権限が必要です。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        try:
            settings = await asyncio.to_thread(
                list_guild_schedules,
                self.bot.db,
                str(interaction.guild_id),
            )

            if not settings:
                embed = embed_helper.create_warning_embed(
                    title="スケジュールなし",
                    description="このサーバーではワードクラウドのスケジュールが設定されていません。",
                )
                await interaction.response.send_message(embed=embed)
                return

            frequency_jp = {
                "daily": "デイリー",
                "weekly": "ウィークリー",
                "monthly": "マンスリー",
            }
            description_lines = []

            for idx, setting in enumerate(settings, 1):
                channel = self.bot.get_channel(int(setting["channel_id"]))
                channel_mention = (
                    channel.mention if channel else f"<#{setting['channel_id']}>"
                )
                freq = frequency_jp.get(setting["frequency"], setting["frequency"])
                schedule_time = setting.get("schedule_time", "09:00")
                status = "✅ 有効" if setting.get("enabled", True) else "❌ 無効"

                last_exec = setting.get("last_executed")
                if last_exec:
                    last_exec_dt = datetime.fromisoformat(last_exec)
                    last_exec_str = f"最終実行: <t:{int(last_exec_dt.timestamp())}:R>"
                else:
                    last_exec_str = "未実行"

                description_lines.append(
                    f"**{idx}.** {channel_mention} | {freq} | {schedule_time} (JST) | {status}\n　{last_exec_str}"
                )

            embed = discord.Embed(
                title="📅 ワードクラウドスケジュール一覧",
                description="\n\n".join(description_lines),
                color=discord.Color.blue(),
            )
            await interaction.response.send_message(embed=embed)

        except Exception as e:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="スケジュール一覧の取得中にエラーが発生しました。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            print(f"Error listing schedules: {e}")

    @wordcloud_group.command(
        name="remove",
        description="指定されたワードクラウドスケジュールを削除します",
    )
    @app_commands.default_permissions(manage_channels=True)
    @app_commands.describe(
        channel="削除するスケジュールのチャンネル",
        frequency="削除するスケジュールの頻度 (daily / weekly / monthly)",
    )
    @app_commands.choices(
        frequency=[
            app_commands.Choice(name="デイリー (毎日)", value="daily"),
            app_commands.Choice(name="ウィークリー (毎週)", value="weekly"),
            app_commands.Choice(name="マンスリー (毎月)", value="monthly"),
        ]
    )
    async def remove_schedule(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        frequency: app_commands.Choice[str],
    ):
        embed_helper = EmbedHelper(function_name="WordCloud Schedule Remove")

        if interaction.guild_id is None:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはサーバー内でご利用ください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        # チャンネル管理権限チェック
        if interaction.permissions.manage_channels is False:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはチャンネル管理権限が必要です。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        try:
            result = await asyncio.to_thread(
                delete_guild_schedule,
                self.bot.db,
                str(interaction.guild_id),
                str(channel.id),
                frequency.value,
            )

            if result.deleted_count == 0:
                embed = embed_helper.create_warning_embed(
                    title="スケジュールなし",
                    description=f"{channel.mention} への{frequency.name}スケジュールが見つかりませんでした。",
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return

            embed = embed_helper.create_success_embed(
                title="削除完了",
                description=f"{channel.mention} への{frequency.name}スケジュールを削除しました！",
            )
            await interaction.response.send_message(embed=embed)

        except Exception as e:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="スケジュール削除中にエラーが発生しました。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            print(f"Error removing schedule: {e}")

    @tasks.loop(minutes=1)
    async def check_scheduled_wordclouds(self):
        """定期的にスケジュールされたワードクラウド生成をチェック"""
        now = discord.utils.utcnow().astimezone(self.JST)

        # 有効な設定を取得
        try:
            settings = await asyncio.to_thread(
                list_enabled_schedules,
                self.bot.db,
            )
        except Exception as e:
            print(f"Error fetching scheduled wordcloud settings: {e}")
            return

        for setting in settings:
            guild_id = setting.get("guild_id")
            channel_id = setting.get("channel_id")
            frequency = setting.get("frequency")
            schedule_time = setting.get("schedule_time", "09:00")
            last_executed = setting.get("last_executed")

            parsed_time = parse_schedule_time(schedule_time)
            if parsed_time is None:
                continue

            # 指定時刻(JST)に一致した時だけ候補にする
            if now.hour != parsed_time[0] or now.minute != parsed_time[1]:
                continue

            if should_execute_schedule(frequency, last_executed, now, self.JST):
                await self._execute_scheduled_wordcloud(guild_id, channel_id, frequency)

    @check_scheduled_wordclouds.before_loop
    async def before_check_scheduled_wordclouds(self):
        """Bot準備完了を待つ"""
        await self.bot.wait_until_ready()

    async def _execute_scheduled_wordcloud(
        self, guild_id: str, channel_id: str, frequency: str
    ):
        """スケジュールされたワードクラウドを実行"""
        embed_helper = EmbedHelper(function_name="WordCloud (Scheduled)")

        try:
            guild = self.bot.get_guild(int(guild_id))
            if guild is None:
                print(f"Guild {guild_id} not found")
                return

            channel = guild.get_channel(int(channel_id))
            if channel is None:
                print(f"Channel {channel_id} not found in guild {guild_id}")
                return

            now_jst = discord.utils.utcnow().astimezone(self.JST)
            during_days = get_schedule_during_days(frequency, now_jst)
            if during_days is None:
                print(f"Unknown schedule frequency: {frequency}")
                return

            try:
                docs = await asyncio.to_thread(
                    fetch_wordcloud_documents,
                    self.bot.db,
                    guild_id,
                    during_days=during_days,
                )
            except Exception as error:
                print(f"Database query error for scheduled wordcloud: {error}")
                return

            if not docs:
                await asyncio.to_thread(
                    update_last_executed,
                    self.bot.db,
                    guild_id,
                    channel_id,
                    frequency,
                )
                return

            try:
                image_buffer = await asyncio.to_thread(
                    generate_wordcloud_image,
                    self.bot.db,
                    docs,
                    compounds=self.compounds,
                )
            except (ValueError, RuntimeError) as error:
                print(f"Error generating wordcloud: {error}")
                await asyncio.to_thread(
                    update_last_executed,
                    self.bot.db,
                    guild_id,
                    channel_id,
                    frequency,
                )
                return

            embed = embed_helper.create_success_embed(
                title=f"{get_frequency_label(frequency)}ワードクラウド",
                description=f"最新{len(docs)}件のメッセージから生成されました！",
                binary_data=image_buffer.getvalue(),
                binary_filename="wordcloud.png",
            )

            await channel.send(
                embed=embed,
                file=discord.File(fp=image_buffer, filename="wordcloud.png"),
            )

            await asyncio.to_thread(
                update_last_executed,
                self.bot.db,
                guild_id,
                channel_id,
                frequency,
            )

        except Exception as e:
            print(f"Error executing scheduled wordcloud: {e}")

    @tasks.loop(minutes=10)
    async def background_learn(self):
        try:
            def _learn_batch_sync() -> None:
                last_cursor = fetch_last_learn_cursor(self.bot.db)
                legacy_last_id = None

                if last_cursor is None:
                    legacy_last_id = fetch_legacy_last_learn_id(self.bot.db)

                docs = fetch_learning_documents(
                    self.bot.db,
                    last_cursor,
                    legacy_last_id=legacy_last_id,
                )

                if not docs:
                    return

                texts = [build_wordcloud_source_text([doc]) for doc in docs]
                learn_from_texts(self.bot.db, texts)

                new_cursor = extract_learning_cursor(docs[-1])
                if new_cursor is not None:
                    update_last_learn_cursor(self.bot.db, new_cursor)
                elif legacy_last_id is not None and docs[-1].get("_id") is not None:
                    update_last_learn_id(self.bot.db, docs[-1]["_id"])

            await asyncio.to_thread(_learn_batch_sync)
        except Exception as error:
            print(f"Error in background_learn loop: {error}")

    @background_learn.before_loop
    async def before_background_learn(self):
        try:
            await self.bot.wait_until_ready()
        except Exception as error:
            print(f"Error preparing background_learn loop: {error}")

    @tasks.loop(hours=24)
    async def update_compounds_task(self):
        try:
            await asyncio.to_thread(update_compounds, self.bot.db)
            try:
                self.compounds = await asyncio.to_thread(__import__("libs.wordcloud_service", fromlist=["load_compounds"]).load_compounds, self.bot.db)
                print(f"[WordCloud] Refreshed compounds cache ({len(self.compounds)} items)")
            except Exception as e:
                print(f"[WordCloud] Failed to refresh compounds cache: {e}")
        except Exception as error:
            print(f"Error in update_compounds_task loop: {error}")

    @update_compounds_task.before_loop
    async def before_update_compounds_task(self):
        try:
            await self.bot.wait_until_ready()
        except Exception as error:
            print(f"Error preparing update_compounds_task loop: {error}")


async def setup(bot: commands.Bot):
    await bot.add_cog(WordCloud(bot))
