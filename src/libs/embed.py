import discord


class EmbedHelper:
    def __init__(self, function_name: str):
        self.function_name = function_name

    def create_success_embed(
        self,
        title: str,
        description: str | None,
        binary_data: bytes | None = None,
        binary_filename: str | None = None,
    ) -> discord.Embed:
        embed = discord.Embed(
            title=f"{title} - {self.function_name}",
            description=description,
            color=0x00FF00,
        )
        if binary_data is not None:
            embed.set_image(url=f"attachment://{binary_filename or 'attachment.bin'}")
        return embed

    def create_warning_embed(
        self, title: str, description: str | None
    ) -> discord.Embed:
        return discord.Embed(
            title=f"{title} - {self.function_name}",
            description=description,
            color=0xFFFF00,
        )

    def create_error_embed(self, title: str, description: str | None) -> discord.Embed:
        return discord.Embed(
            title=f"{title} - {self.function_name}",
            description=description,
            color=0xFF0000,
        )

    def create_info_embed(self, title: str, description: str | None) -> discord.Embed:
        return discord.Embed(
            title=f"{title} - {self.function_name}",
            description=description,
            color=0x0000FF,
        )
