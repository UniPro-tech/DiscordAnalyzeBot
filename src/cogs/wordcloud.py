import discord
from discord import app_commands
from discord.ext import commands
from libs.visualize import generate_wordcloud_image


class WordCloud(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="wordcloud", description="ワードクラウドを生成します")
    async def wordcloud(self, interaction: discord.Interaction):
        if interaction.guild_id is None:
            await interaction.response.send_message(
                "このコマンドはサーバー内でご利用ください。", ephemeral=True
            )
            return

        await interaction.response.defer(thinking=True)

        docs = list(
            self.bot.db.messages.find(
                {
                    "guild_id": str(interaction.guild_id),
                    "content": {"$type": "string", "$ne": ""},
                },
                {"content": 1},
            )
            .sort("timestamp", -1)
            .limit(3000)
        )

        if not docs:
            await interaction.followup.send("解析対象のメッセージがまだないようです。")
            return

        raw_text = " ".join(doc.get("content", "") for doc in docs)

        try:
            image_buffer = generate_wordcloud_image(raw_text)
        except ValueError:
            await interaction.followup.send(
                "表示できる単語が不足しています。もう少しメッセージが集まってから再度お試しください。"
            )
            return
        except RuntimeError:
            await interaction.followup.send(
                "日本語フォントが見つからないため生成できませんでした。"
            )
            return

        await interaction.followup.send(
            content=f"最新{len(docs)}件のメッセージから生成しました！",
            file=discord.File(fp=image_buffer, filename="wordcloud.png"),
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(WordCloud(bot))
