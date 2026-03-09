import discord
from discord import app_commands
from discord.ext import commands
from datetime import timedelta
from typing import Optional
from libs.visualize import generate_wordcloud_image


class WordCloud(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="wordcloud",
        description="ワードクラウドを生成します",
    )
    @app_commands.describe(
        period="ワードクラウドの元になる期間（単位: 日。省略した場合はデータ保持期間(デフォルト: 一ヶ月)）",
        user="特定のユーザーのメッセージからワードクラウドを生成します（省略した場合は全ユーザーのメッセージから生成）",
        channel="特定のチャンネルのメッセージからワードクラウドを生成します（省略した場合は全チャンネルのメッセージから生成）",
        role="特定のロールを持つユーザーのメッセージからワードクラウドを生成します（省略した場合は全ユーザーのメッセージから生成）",
    )
    async def wordcloud(
        self,
        interaction: discord.Interaction,
        period: Optional[str] = None,
        user: Optional[discord.User] = None,
        channel: Optional[discord.TextChannel] = None,
        role: Optional[discord.Role] = None,
    ):
        if interaction.guild_id is None:
            await interaction.response.send_message(
                "このコマンドはサーバー内でご利用ください。", ephemeral=True
            )
            return

        # periodを数値化してクエリに追加。指定がない場合はデータ保持期間(30日)を使用
        period_filter = {}
        if period is not None:
            try:
                period_filter = {
                    "timestamp": {
                        "$gte": (
                            discord.utils.utcnow() - timedelta(days=int(period))
                        ).isoformat()
                    }
                }
            except ValueError:
                await interaction.response.send_message(
                    "期間は数値で指定してください。", ephemeral=True
                )
                return
            except Exception as e:
                await interaction.response.send_message(
                    "期間の処理中にエラーが発生しました", ephemeral=True
                )
                print(f"Error processing period: {e}")
                return

        # userが指定された場合はクエリに追加
        user_filter = {}
        if user is not None:
            user_filter = {"user_id": str(user.id)}

        # channelが指定された場合はクエリに追加
        channel_filter = {}
        if channel is not None:
            channel_filter = {"channel_id": str(channel.id)}

        # roleが指定された場合はクエリに追加
        role_filter = {}
        if role is not None:
            role_filter = {"role_ids": {"$in": [str(role.id)]}}

        await interaction.response.defer(thinking=True)

        try:
            docs = list(
                self.bot.db.messages.find(
                    {
                        "guild_id": str(interaction.guild_id),
                        "content": {"$type": "string", "$ne": ""},
                        **period_filter,
                        **user_filter,
                        **channel_filter,
                        **role_filter,
                    },
                    {"content": 1},
                )
                .sort("timestamp", -1)
                .limit(3000)
            )
        except Exception as e:
            await interaction.followup.send(
                "データベースクエリ中にエラーが発生しました"
            )
            print(f"Database query error: {e}")
            return

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
