import asyncio

import discord
from discord import app_commands
from discord.ext import commands

from config import ADMIN_USER_ID
from libs.embed import EmbedHelper
from libs.text_processing import inspect_tokens_with_pos
from libs.wordcloud_service import (
    extract_learning_cursor,
    fetch_learning_documents,
    learn_from_texts,
    reset_learning_state,
    update_compounds,
    update_last_learn_cursor,
    migrate_message_tokens,
    count_unmigrated_tokens,
)


class Admin(commands.Cog):
    admin_group = app_commands.Group(
        name="admin",
        description="管理者向けコマンド",
    )
    reset_group = app_commands.Group(
        name="reset",
        description="管理者向けリセットコマンド",
        parent=admin_group,
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._relearn_lock = asyncio.Lock()
        self._migrate_lock = asyncio.Lock()
        self._migrate_task: asyncio.Task | None = None

    def _is_admin_user(self, user_id: int) -> bool:
        return ADMIN_USER_ID is not None and user_id == ADMIN_USER_ID

    @staticmethod
    def _format_sudachi_pos_description(result: dict) -> str:
        normalized_text = result["normalized_text"] or "(空文字)"
        extracted_tokens = result["extracted_tokens"]
        tokens = result["tokens"]

        lines = [
            f"正規化後: {normalized_text}",
            "抽出対象: " + (", ".join(extracted_tokens) if extracted_tokens else "(なし)"),
            "",
            "トークン一覧:",
        ]

        max_visible_tokens = 25
        visible_tokens = tokens[:max_visible_tokens]
        for token in visible_tokens:
            pos = ", ".join(part for part in token["pos"] if part != "*") or "*"
            target = "yes" if token["is_target"] else "no"
            lines.append(
                f"[{token['index']}] {token['surface']} | {pos} | target={target}"
            )

        hidden_count = len(tokens) - len(visible_tokens)
        if hidden_count > 0:
            lines.append(f"... {hidden_count}件省略")

        description = "\n".join(lines)
        if len(description) <= 4000:
            return description

        truncated_lines = lines[:4]
        current_length = len("\n".join(truncated_lines))
        for token in visible_tokens:
            pos = ", ".join(part for part in token["pos"] if part != "*") or "*"
            target = "yes" if token["is_target"] else "no"
            line = f"[{token['index']}] {token['surface']} | {pos} | target={target}"
            if current_length + len(line) + 1 > 3900:
                break
            truncated_lines.append(line)
            current_length += len(line) + 1

        if hidden_count > 0 or len(truncated_lines) - 4 < len(visible_tokens):
            truncated_lines.append("... 出力を短縮したよ")

        return "\n".join(truncated_lines)

    def _reset_and_relearn_sync(self) -> tuple[int, int]:
        reset_learning_state(self.bot.db)
        remigrated_token_count = migrate_message_tokens(self.bot.db, force=True)
        last_cursor = None
        learned_message_count = 0

        while True:
            docs = fetch_learning_documents(self.bot.db, last_cursor)
            if not docs:
                break

            texts = [doc["content"] for doc in docs if (doc.get("content") or "").strip()]
            if texts:
                learn_from_texts(self.bot.db, texts, workers=1)
                learned_message_count += len(texts)

            next_cursor = extract_learning_cursor(docs[-1])
            if next_cursor is not None:
                last_cursor = next_cursor
                update_last_learn_cursor(self.bot.db, last_cursor)
            else:
                # timestamp/message_id が欠損したレコードで停止して無限再学習を防ぐ。
                break

        update_compounds(self.bot.db)
        return learned_message_count, remigrated_token_count

    @admin_group.command(
        name="sudachi_pos",
        description="SudachiPy の品詞解析結果を表示します",
    )
    @app_commands.describe(text="品詞を確認したい文字列")
    async def sudachi_pos(self, interaction: discord.Interaction, text: str):
        embed_helper = EmbedHelper(function_name="Admin Sudachi POS")

        if ADMIN_USER_ID is None:
            embed = embed_helper.create_error_embed(
                title="設定エラー",
                description="config.py に ADMIN_USER_ID が設定されていません。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if not self._is_admin_user(interaction.user.id):
            embed = embed_helper.create_error_embed(
                title="権限エラー",
                description="このコマンドは管理者のみ実行できます。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            result = await asyncio.to_thread(inspect_tokens_with_pos, text)
        except Exception as error:
            embed = embed_helper.create_error_embed(
                title="解析エラー",
                description="SudachiPy の解析中にエラーが発生しました。ログを確認してください。",
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            print(f"[Admin] sudachi_pos failed: {error}")
            return

        embed = embed_helper.create_info_embed(
            title="Sudachi POS Debug",
            description=self._format_sudachi_pos_description(result),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @reset_group.command(
        name="learn",
        description="学習データをリセットして全メッセージを再学習します",
    )
    async def reset_learn(self, interaction: discord.Interaction):
        embed_helper = EmbedHelper(function_name="Admin Reset learn")

        if ADMIN_USER_ID is None:
            embed = embed_helper.create_error_embed(
                title="設定エラー",
                description="config.py に ADMIN_USER_ID が設定されていません。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if not self._is_admin_user(interaction.user.id):
            embed = embed_helper.create_error_embed(
                title="権限エラー",
                description="このコマンドは管理者のみ実行できます。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if self._relearn_lock.locked():
            embed = embed_helper.create_warning_embed(
                title="処理中",
                description="現在、再学習処理を実行中です。完了まで待ってください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        async with self._relearn_lock:
            try:
                learned_message_count, remigrated_token_count = await asyncio.to_thread(
                    self._reset_and_relearn_sync
                )
            except Exception as error:
                embed = embed_helper.create_error_embed(
                    title="再学習エラー",
                    description="再学習中にエラーが発生しました。ログを確認してください。",
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                print(f"[Admin] Reset learn command failed: {error}")
                return

        embed = embed_helper.create_success_embed(
            title="再学習完了",
            description=(
                "学習データと message tokens をリセットして、"
                f"tokens を {remigrated_token_count}件再生成し、"
                f"{learned_message_count}件のメッセージを再学習しました。"
            ),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    migrate_group = app_commands.Group(
        name="migrate",
        description="マイグレーション関連のコマンド",
    )

    @migrate_group.command(
        name="status",
        description="トークンマイグレーションの未処理件数を表示します（管理者向け）",
    )
    async def migrate_status(self, interaction: discord.Interaction):
        embed_helper = EmbedHelper(function_name="Admin Migrate Status")

        if ADMIN_USER_ID is None:
            embed = embed_helper.create_error_embed(
                title="設定エラー",
                description="config.py に ADMIN_USER_ID が設定されていません。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if not self._is_admin_user(interaction.user.id):
            embed = embed_helper.create_error_embed(
                title="権限エラー",
                description="このコマンドは管理者のみ実行できます。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            remaining = await asyncio.to_thread(count_unmigrated_tokens, self.bot.db)
        except Exception as error:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="データベース問い合わせ中にエラーが発生しました。",
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            print(f"[Admin] migrate_status DB error: {error}")
            return

        embed = embed_helper.create_success_embed(
            title="マイグレーション状況",
            description=(
                f"tokens 未生成メッセージ数: {remaining}"
            ),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @migrate_group.command(
        name="start",
        description="起動時マイグレーションを手動で実行します（管理者向け、非同期で実行）",
    )
    async def migrate_start(self, interaction: discord.Interaction):
        embed_helper = EmbedHelper(function_name="Admin Migrate Start")

        if ADMIN_USER_ID is None:
            embed = embed_helper.create_error_embed(
                title="設定エラー",
                description="config.py に ADMIN_USER_ID が設定されていません。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if not self._is_admin_user(interaction.user.id):
            embed = embed_helper.create_error_embed(
                title="権限エラー",
                description="このコマンドは管理者のみ実行できます。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if self._migrate_lock.locked():
            embed = embed_helper.create_warning_embed(
                title="処理中",
                description="既にマイグレーションが実行中です。完了を待ってください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        # Acquire lock and start background migration task; the lock will be
        # released by the background task when finished.
        await self._migrate_lock.acquire()

        async def _bg_migrate(inter: discord.Interaction):
            try:
                try:
                    count = await asyncio.to_thread(migrate_message_tokens, self.bot.db)
                except Exception as error:
                    embed = embed_helper.create_error_embed(
                        title="マイグレーションエラー",
                        description="マイグレーション実行中にエラーが発生しました。ログを確認してください。",
                    )
                    await inter.followup.send(embed=embed, ephemeral=True)
                    print(f"[Admin] migrate_start failed: {error}")
                    return

                embed = embed_helper.create_success_embed(
                    title="マイグレーション完了",
                    description=f"更新済みメッセージ数: {count}",
                )
                await inter.followup.send(embed=embed, ephemeral=True)
            finally:
                try:
                    self._migrate_lock.release()
                except RuntimeError:
                    pass

        self._migrate_task = asyncio.create_task(_bg_migrate(interaction))

        embed = embed_helper.create_success_embed(
            title="マイグレーション開始",
            description="バックグラウンドでマイグレーションを開始しました。完了後に通知します。",
        )
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Admin(bot))
