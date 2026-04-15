import re
from datetime import datetime, timezone
from typing import Optional


def parse_discord_timestamp(time_str: str) -> Optional[datetime]:
    """Discordのタイムスタンプ文字列(<t:1234567890:f>)をdatetimeに変換する"""
    if not time_str:
        return None

    # 正規表現で <t: と : または > の間にある数字を抽出
    match = re.search(r"<t:(\d+)(?::[a-zA-Z])?>", time_str)
    if match:
        timestamp = int(match.group(1))
        # DBでUTCとして扱っていると想定し、UTCのdatetimeに変換
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    # 万が一、ユーザーが数字(Unixタイムスタンプ)だけを直接入力した場合のフォールバック
    if time_str.isdigit():
        return datetime.fromtimestamp(int(time_str), tz=timezone.utc)

    raise ValueError("正しいDiscordタイムスタンプ形式ではありません")
