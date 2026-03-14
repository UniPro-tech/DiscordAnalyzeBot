# Discord Analyze Bot

Discord上のさまざまなアクティビティを記録し、分析するBot。

## 使い方(Self-host)

1. `_docker-compose.yaml`を`docker-compose.yaml`にコピーします。
2. `docker-compose.yaml`の環境変数を編集します。
3. 既定では `STORAGE_BACKEND=hybrid` で起動します（設定系はMongoDB、メッセージ/学習系はClickHouse）。

### 環境変数メモ

- `STORAGE_BACKEND`: `hybrid`（推奨）/ `mongo` / `clickhouse`
- `MIGRATION`: `clickhouse` 起動時に MongoDB から ClickHouse へ全件移行するかどうか
- `MONGODB_DSN`: MongoDB接続先
- `CLICKHOUSE_PASSWORD`: ローカル開発用 ClickHouse パスワード
- `CLICKHOUSE_DSN`: ClickHouse接続先（例: `http://default:clickhousepassword@localhost:8123/analyze_bot`）

`STORAGE_BACKEND=clickhouse` かつ `MIGRATION=true` の場合、Bot 起動時に MongoDB の `discord_analyzer` から ClickHouse の `analyze_bot` へデータをフルコピーします。`STORAGE_BACKEND=hybrid` の場合は、メッセージ/学習系テーブル（`messages`, `unigrams`, `ngrams`, `compounds`）のみ移行対象で、設定系（`guild_settings`, `channel_settings`, `user_settings`, `meta`）はMongoDBを正本として保持します。どちらの場合も ClickHouse 側の対象テーブル群に1件でも既存データがある場合は安全のため migration をスキップします。初回移行が終わったら `MIGRATION=false` に戻してください。

## ライセンス

このコードはGNU AGPL-3の下で使用可能です。
著作権表記は以下のとおりです。

```txt
(c) 2026 UniProject All rights reserved.
```

## 収録されているフォントについて

このBotでは標準でIPAフォントが収録されています。
下記ライセンスの下、`fonts`ディレクトリ直下に配置し、参照・使用しております。

- [IPAフォント ライセンス](https://moji.or.jp/ipafont/license/)
