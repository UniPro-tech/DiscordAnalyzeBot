import discord
import asyncio
from discord.ext import commands
from discord import app_commands
from libs.embed import EmbedHelper


class Optout(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _delete_user_messages_background(self, user_id: str):
        try:
            # pymongo is synchronous, so run in a thread to avoid blocking the event loop.
            result = await asyncio.to_thread(
                self.bot.db.messages.delete_many, {"user_id": user_id}
            )
            print(
                f"[Optout] Background recursive delete completed for user_id={user_id}, deleted={result.deleted_count}"
            )
        except Exception as e:
            print(
                f"[Optout] Background recursive delete failed for user_id={user_id}: {e}"
            )

    @app_commands.command(name="optout", description="統計データからのオプトアウト")
    @app_commands.describe(
        optout="オプトアウトするかどうか",
        recursive="過去のメッセージまで遡ってオプトアウトするかどうか (オプトアウトする場合のみ有効。この変更は時間を要する可能性があります)",
    )
    @app_commands.choices(
        optout=[
            app_commands.Choice(name="はい", value="yes"),
            app_commands.Choice(name="いいえ", value="no"),
        ],
        recursive=[
            app_commands.Choice(name="はい", value="yes"),
            app_commands.Choice(name="いいえ", value="no"),
        ],
    )
    async def optout(
        self,
        interaction: discord.Interaction,
        optout: app_commands.Choice[str],
        recursive: app_commands.Choice[str] | None = None,
    ):
        embed_helper = EmbedHelper(function_name="Optout")
        user_id = str(interaction.user.id)
        opt_out_value = optout.value == "yes"
        recursive_value = (recursive.value if recursive else "no") == "yes"

        if recursive_value and not opt_out_value:
            embed = embed_helper.create_error_embed(
                title="入力エラー",
                description="`recursive` はオプトアウト時のみ指定できます。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        try:
            self.bot.db.user_settings.update_one(
                {"user_id": user_id}, {"$set": {"opt_out": opt_out_value}}, upsert=True
            )
        except Exception as e:
            print(f"Error in optout command: {e}")
            embed = embed_helper.create_error_embed(
                title="DBエラー",
                description="オプトアウトの設定中にエラーが発生しました。もう一度お試しください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if opt_out_value and recursive_value:
            asyncio.create_task(self._delete_user_messages_background(user_id))

        embed = embed_helper.create_success_embed(
            title="オプトアウト設定",
            description=(
                f"{interaction.user.mention}さんは統計データから"
                f"{'オプトアウト' if opt_out_value else 'オプトイン'}されました。"
                + (
                    "\n過去メッセージの削除をバックグラウンドで開始しました。"
                    if opt_out_value and recursive_value
                    else ""
                )
            ),
        )
        await interaction.response.send_message(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(Optout(bot))
