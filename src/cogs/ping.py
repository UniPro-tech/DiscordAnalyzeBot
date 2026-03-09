import discord
from discord.ext import commands
from discord import app_commands
from libs.embed import EmbedHelper


class Ping(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="ping", description="ping!")
    async def ping(self, interaction: discord.Interaction):
        embed_helper = EmbedHelper(function_name="Ping")
        embed = embed_helper.create_info_embed(title="pong!", description="pong!")
        await interaction.response.send_message(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(Ping(bot))
