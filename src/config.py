import os


def _get_int_env(name: str) -> int | None:
	value = os.getenv(name)
	if value is None or value.strip() == "":
		return None

	try:
		return int(value)
	except ValueError:
		return None


# 環境変数から管理者のDiscordユーザーIDを読み込む。
# 例: ADMIN_USER_ID=123456789012345678
ADMIN_USER_ID: int | None = _get_int_env("ADMIN_USER_ID")
