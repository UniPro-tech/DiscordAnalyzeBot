import asyncio

import discord
from discord import app_commands
from discord.ext import commands

from config import ADMIN_USER_ID
from libs.embed import EmbedHelper
from libs.message_store import get_guild_collection_stats
from libs.wordcloud_service import (
    clear_all_message_tokens,
    fetch_learning_documents,
    learn_from_texts,
    migrate_message_tokens,
    reset_learning_state,
    update_compounds,
    update_last_learn_id,
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

    def _build_collection_summary_lines(
        self,
        stats: list[dict[str, int | str]],
    ) -> list[str]:
        guild_name_by_id = {str(guild.id): guild.name for guild in self.bot.guilds}
        merged_rows: list[tuple[str, int, int]] = []
        seen_guild_ids: set[str] = set()

        for row in stats:
            guild_id = str(row.get("guild_id") or "")
            message_count = int(row.get("message_count") or 0)
            collected_user_count = int(row.get("collected_user_count") or 0)

            if guild_id:
                seen_guild_ids.add(guild_id)

            db_guild_name = str(row.get("guild_name") or "Unknown Guild")
            display_name = guild_name_by_id.get(guild_id, db_guild_name)
            merged_rows.append((display_name, message_count, collected_user_count))

        for guild in self.bot.guilds:
            guild_id = str(guild.id)
            if guild_id in seen_guild_ids:
                continue
            merged_rows.append((guild.name, 0, 0))

        merged_rows.sort(key=lambda item: item[1], reverse=True)

        lines = [
            (
                f"{index}. {guild_name} | メッセージ: {message_count:,}件"
                f" | 収集ユーザー: {collected_user_count:,}人"
            )
            for index, (guild_name, message_count, collected_user_count) in enumerate(
                merged_rows,
                start=1,
            )
        ]
        return lines

    def _reset_and_relearn_sync(self) -> tuple[int, int, int]:
        reset_learning_state(self.bot.db)
        cleared_token_count = clear_all_message_tokens(self.bot.db)
        recalculated_token_count = migrate_message_tokens(self.bot.db)
        last_id = None
        learned_message_count = 0

        while True:
            docs = fetch_learning_documents(self.bot.db, last_id)
            if not docs:
                break

            texts = [
                doc["content"] for doc in docs if (doc.get("content") or "").strip()
            ]
            if texts:
                learn_from_texts(self.bot.db, texts, workers=1)
                learned_message_count += len(texts)

            last_id = docs[-1]["_id"]
            update_last_learn_id(self.bot.db, last_id)

        update_compounds(self.bot.db)
        return learned_message_count, cleared_token_count, recalculated_token_count

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
                (
                    learned_message_count,
                    cleared_token_count,
                    recalculated_token_count,
                ) = await asyncio.to_thread(self._reset_and_relearn_sync)
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
                "学習データ（unigrams / ngrams / compounds / last_learn_id）をリセットして、"
                f"{learned_message_count}件のメッセージを再学習しました。\n"
                f"tokensを初期化したメッセージ: {cleared_token_count}件\n"
                f"tokensを再計算したメッセージ: {recalculated_token_count}件"
            ),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @admin_group.command(
        name="server_stats",
        description="導入サーバーごとのメッセージ数と収集ユーザー数を表示します",
    )
    async def server_stats(self, interaction: discord.Interaction):
        embed_helper = EmbedHelper(function_name="Admin Server Stats")

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
            stats = await asyncio.to_thread(get_guild_collection_stats, self.bot.db)
        except Exception as error:
            embed = embed_helper.create_error_embed(
                title="集計エラー",
                description="サーバー集計の取得中にエラーが発生しました。",
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            print(f"[Admin] Collection stats command failed: {error}")
            return

        lines = self._build_collection_summary_lines(stats)
        if not lines:
            embed = embed_helper.create_warning_embed(
                title="データなし",
                description="表示可能なサーバー情報がありません。",
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        max_desc_len = 3800
        description_lines: list[str] = []
        current_len = 0

        for line in lines:
            extra_len = len(line) + (1 if description_lines else 0)
            if current_len + extra_len > max_desc_len:
                remaining = len(lines) - len(description_lines)
                description_lines.append(f"...他 {remaining} サーバー")
                break
            description_lines.append(line)
            current_len += extra_len

        embed = embed_helper.create_info_embed(
            title="導入サーバー集計",
            description="\n".join(description_lines),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Admin(bot))
