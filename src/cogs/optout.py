import discord
import asyncio
from discord.ext import commands
from discord import app_commands
from libs.embed import EmbedHelper
from libs.message_store import delete_messages_by_query
from libs.settings_store import set_channel_opt_out, set_user_opt_out


class Optout(commands.Cog):
    optout_group = app_commands.Group(
        name="optout",
        description="統計データからのオプトアウト設定",
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _delete_messages_background(self, query: dict, scope: str):
        try:
            deleted_count = await asyncio.to_thread(
                delete_messages_by_query,
                self.bot.db,
                query,
            )
            print(
                f"[Optout] Background recursive delete completed for {scope}, deleted={deleted_count}"
            )
        except Exception as e:
            print(f"[Optout] Background recursive delete failed for {scope}: {e}")

    @optout_group.command(name="user", description="ユーザー単位のオプトアウト")
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
    async def optout_user(
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
            await asyncio.to_thread(
                set_user_opt_out,
                self.bot.db,
                user_id,
                opt_out_value,
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
            asyncio.create_task(
                self._delete_messages_background(
                    {"user_id": user_id},
                    f"user_id={user_id}",
                )
            )

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

    @optout_group.command(name="channel", description="チャンネル単位のオプトアウト")
    @app_commands.default_permissions(manage_channels=True)
    @app_commands.describe(
        optout="このチャンネルをオプトアウトするかどうか",
        channel="対象チャンネル (省略時は実行中のチャンネル)",
        recursive="過去のメッセージまで遡って削除するかどうか (オプトアウトする場合のみ有効)",
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
    async def optout_channel(
        self,
        interaction: discord.Interaction,
        optout: app_commands.Choice[str],
        channel: discord.abc.GuildChannel | discord.Thread | None = None,
        recursive: app_commands.Choice[str] | None = None,
    ):
        embed_helper = EmbedHelper(function_name="Optout Channel")

        if interaction.guild is None:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはサーバー内で利用してください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        target_channel = channel
        if target_channel is None:
            if isinstance(
                interaction.channel,
                (discord.TextChannel, discord.ForumChannel, discord.VoiceChannel),
            ):
                target_channel = interaction.channel
            elif (
                isinstance(interaction.channel, discord.Thread)
                and interaction.channel.parent is not None
            ):
                target_channel = interaction.channel.parent
            else:
                embed = embed_helper.create_error_embed(
                    title="チャンネル指定エラー",
                    description="対象チャンネルを指定してください。テキスト/フォーラム/ボイスチャンネルでこのコマンドを実行するか、`channel` オプションでチャンネルを指定してください。",
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return

        guild_id = str(interaction.guild.id)
        channel_id = str(target_channel.id)
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
            await asyncio.to_thread(
                set_channel_opt_out,
                self.bot.db,
                guild_id,
                channel_id,
                opt_out_value,
            )
        except Exception as e:
            print(f"Error in optout channel command: {e}")
            embed = embed_helper.create_error_embed(
                title="DBエラー",
                description="チャンネルオプトアウト設定中にエラーが発生しました。もう一度お試しください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        if opt_out_value and recursive_value:
            delete_query = {"guild_id": guild_id, "channel_id": channel_id}
            delete_scope = f"guild_id={guild_id},channel_id={channel_id}"

            if isinstance(target_channel, discord.ForumChannel):
                delete_query = {
                    "guild_id": guild_id,
                    "$or": [
                        {"channel_id": channel_id},
                        {"parent_channel_id": channel_id},
                    ],
                }
                delete_scope = f"guild_id={guild_id},forum_id={channel_id}"

            asyncio.create_task(
                self._delete_messages_background(
                    delete_query,
                    delete_scope,
                )
            )

        embed = embed_helper.create_success_embed(
            title="チャンネルオプトアウト設定",
            description=(
                f"{target_channel.mention} を統計データから"
                f"{'オプトアウト' if opt_out_value else 'オプトイン'}しました。"
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
