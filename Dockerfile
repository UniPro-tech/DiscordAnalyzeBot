FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

WORKDIR /app

# 1. ビルドに必要なツール一式をインストール
# gcc と libc6-dev がないと、Rustコンパイラがあってもビルドに失敗します
RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  gcc \
  libc6-dev \
  rustc \
  cargo \
  && rm -rf /var/lib/apt/lists/*

# 2. 環境設定
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 

# 3. 依存関係のインストール
COPY pyproject.toml .
COPY uv.lock* ./

ENV UV_NO_DEV=1
# --locked を使いつつ同期
RUN uv sync --locked --no-install-project

COPY . .

RUN uv sync --locked

CMD ["uv", "run", "/app/src/main.py"]