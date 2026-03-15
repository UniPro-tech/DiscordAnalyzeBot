import asyncio

import discord
from discord import app_commands
from discord.ext import commands

from config import ADMIN_USER_ID
from libs.embed import EmbedHelper
from libs.wordcloud_service import (
    extract_learning_cursor,
    fetch_learning_documents,
    learn_from_texts,
    reset_learning_state,
    update_compounds,
    update_last_learn_cursor,
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

    def _is_admin_user(self, user_id: int) -> bool:
        return ADMIN_USER_ID is not None and user_id == ADMIN_USER_ID

    def _reset_and_relearn_sync(self) -> int:
        reset_learning_state(self.bot.db)
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
        return learned_message_count

    @reset_group.command(
        name="leran",
        description="学習データをリセットして全メッセージを再学習します",
    )
    async def reset_leran(self, interaction: discord.Interaction):
        embed_helper = EmbedHelper(function_name="Admin Reset Leran")

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
                learned_message_count = await asyncio.to_thread(self._reset_and_relearn_sync)
            except Exception as error:
                embed = embed_helper.create_error_embed(
                    title="再学習エラー",
                    description="再学習中にエラーが発生しました。ログを確認してください。",
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                print(f"[Admin] Reset leran command failed: {error}")
                return

        embed = embed_helper.create_success_embed(
            title="再学習完了",
            description=(
                "学習データ（unigrams / ngrams / compounds / last_learn_cursor）をリセットして、"
                f"{learned_message_count}件のメッセージを再学習しました。"
            ),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Admin(bot))
