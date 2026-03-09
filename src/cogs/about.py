import discord
from discord.ext import commands
from discord import app_commands, Embed
from version import __version__


class About(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="about", description="Botの情報を表示します")
    async def about(self, interaction: discord.Interaction):
        try:
            embed = Embed(
                title="About",
                description="Analyzer Botは、サーバー内の会話を分析して様々な統計情報やビジュアルを提供するDiscord Botです。",
                color=0x00FF00,
                timestamp=discord.utils.utcnow(),
            )
            embed.set_author(name="Analyzer Bot", icon_url=self.bot.user.avatar.url)
            embed.add_field(
                name="開発元",
                value="[デジタル創作サークルUniProject](https://uniproject.jp/)",
                inline=False,
            )
            embed.add_field(
                name="公式サイト",
                value="[リンク](https://analyze-bot.uniproject.jp/)",
                inline=True,
            )
            embed.add_field(
                name="利用規約",
                value="[リンク](https://analyze-bot.uniproject.jp/terms)",
                inline=True,
            )
            embed.add_field(
                name="プライバシー・ポリシー",
                value="[リンク](https://analyze-bot.uniproject.jp/privacy)",
                inline=True,
            )
            embed.add_field(
                name="Version",
                value=f"v{__version__} (リリースノート: [GitHub](https://github.com/UniProject/analyze-bot/releases/tag/v{__version__} ))",
                inline=False,
            )
            await interaction.response.send_message(embed=embed)
        except Exception as e:
            error_embed = Embed(
                title="エラー",
                description="情報の取得中にエラーが発生しました。後でもう一度お試しください。",
                color=0xFF0000,
            )
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            print(f"Error in about command: {e}")


async def setup(bot: commands.Bot):
    await bot.add_cog(About(bot))
