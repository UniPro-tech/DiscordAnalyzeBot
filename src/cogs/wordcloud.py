import discord
from discord import app_commands
from discord.ext import commands
from datetime import timedelta
from typing import Optional
from libs.visualize import generate_wordcloud_image
from libs.embed import EmbedHelper


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
        embed_helper = EmbedHelper(function_name="WordCloud")
        if interaction.guild_id is None:
            embed = embed_helper.create_error_embed(
                title="エラー",
                description="このコマンドはサーバー内でご利用ください。",
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
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
                embed = embed_helper.create_error_embed(
                    title="エラー", description="期間は数値で指定してください。"
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
            except Exception as e:
                embed = embed_helper.create_error_embed(
                    title="エラー", description="期間の処理中にエラーが発生しました"
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
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
            embed = embed_helper.create_error_embed(
                title="データベースエラー",
                description="データベースクエリ中にエラーが発生しました",
            )
            await interaction.followup.send(embed=embed)
            print(f"Database query error: {e}")
            return

        if not docs:
            embed = embed_helper.create_warning_embed(
                title="会話不足",
                description="解析対象のメッセージがまだないようです。",
            )
            await interaction.followup.send(embed=embed)
            return

        raw_text = " ".join(doc.get("content", "") for doc in docs)

        try:
            image_buffer = generate_wordcloud_image(raw_text)
        except ValueError:
            embed = embed_helper.create_warning_embed(
                title="語彙不足",
                description="表示できる単語が不足しています。もう少しメッセージが集まってから再度お試しください。",
            )
            await interaction.followup.send(embed=embed)
            return
        except RuntimeError:
            embed = embed_helper.create_error_embed(
                title="内部エラー",
                description="日本語フォントが見つからないため生成できませんでした。Bot管理者にお問い合わせください。",
            )
            await interaction.followup.send(embed=embed)
            return

        embed = embed_helper.create_success_embed(
            title="生成成功",
            description=f"最新{len(docs)}件のメッセージが取得でき、WordCloudが生成されました！",
            binary_data=image_buffer.getvalue(),
            binary_filename="wordcloud.png",
        )
        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(WordCloud(bot))
