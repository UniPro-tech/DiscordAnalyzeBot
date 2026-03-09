FROM ghcr.io/astral-sh/uv:debian-slim

WORKDIR /app

RUN apt update
RUN apt install -y ca-certificates

# 1. Pythonの出力をリアルタイムで表示させる設定（重要）
ENV PYTHONUNBUFFERED=1
# 2. .pycファイルを作成しない設定（コンテナを軽量に保つ）
ENV PYTHONDONTWRITEBYTECODE=1

COPY pyproject.toml .
COPY uv.lock* ./

# Disable development dependencies
ENV UV_NO_DEV=1
RUN uv sync --locked

COPY . .

CMD ["uv", "run", "/app/src/main.py"]