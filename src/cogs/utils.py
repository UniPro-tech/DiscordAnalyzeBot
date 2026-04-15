import asyncio

import discord
from discord import app_commands
from discord.ext import commands

from libs.embed import EmbedHelper
from libs.text_processing import analyze_sudachi_pos


class Utils(commands.Cog):
    utils_group = app_commands.Group(
        name="utils",
        description="ユーティリティコマンド",
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._relearn_lock = asyncio.Lock()

    @utils_group.command(
        name="sudachi_pos",
        description="SudachiPyで品詞情報を確認します",
    )
    @app_commands.describe(
        text="解析するテキスト",
        mode="分割モード (A / B / C)",
    )
    @app_commands.choices(
        mode=[
            app_commands.Choice(name="A (短単位)", value="A"),
            app_commands.Choice(name="B (中単位)", value="B"),
            app_commands.Choice(name="C (長単位)", value="C"),
        ]
    )
    async def sudachi_pos(
        self,
        interaction: discord.Interaction,
        text: str,
        mode: app_commands.Choice[str] | None = None,
    ):
        embed_helper = EmbedHelper(function_name="Admin Sudachi POS")

        if not text.strip():
            embed = embed_helper.create_warning_embed(
                title="入力エラー",
                description="解析するテキストを入力してください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        selected_mode = mode.value if mode is not None else "C"

        try:
            results = await asyncio.to_thread(analyze_sudachi_pos, text, selected_mode)
        except ValueError:
            embed = embed_helper.create_error_embed(
                title="入力エラー",
                description="分割モードは A / B / C のいずれかを指定してください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        except Exception as error:
            embed = embed_helper.create_error_embed(
                title="解析エラー",
                description="SudachiPyでの解析中にエラーが発生しました。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            print(f"[Admin] Sudachi POS command failed: {error}")
            return

        if not results:
            embed = embed_helper.create_warning_embed(
                title="解析結果なし",
                description="トークンが見つかりませんでした。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        max_display = 25
        lines = [
            f"{index}. {surface} -> {' / '.join(pos)} (原形: {base_form})"
            for index, (surface, pos, base_form) in enumerate(
                results[:max_display], start=1
            )
        ]

        if len(results) > max_display:
            lines.append(f"... and {len(results) - max_display} more")

        embed = embed_helper.create_info_embed(
            title=f"Sudachi POS ({selected_mode})",
            description="\n".join(lines),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Utils(bot))
