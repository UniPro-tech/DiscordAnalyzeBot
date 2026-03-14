from dataclasses import dataclass
import os
from urllib.parse import urlparse

from pymongo import MongoClient


@dataclass
class ClickHouseDatabase:
    client: object
    database: str
    backend: str = "clickhouse"

    def command(self, query: str, parameters: dict | None = None):
        return self.client.command(query, parameters=parameters or {})

    def query_dicts(self, query: str, parameters: dict | None = None) -> list[dict]:
        result = self.client.query(query, parameters=parameters or {})
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]

    def query_scalar(self, query: str, parameters: dict | None = None):
        rows = self.query_dicts(query, parameters)
        if not rows:
            return None
        first_row = rows[0]
        if not first_row:
            return None
        return next(iter(first_row.values()))

    def insert_rows(self, table: str, rows: list[list], columns: list[str]) -> None:
        if not rows:
            return
        self.client.insert(table=table, data=rows, column_names=columns)


@dataclass
class MongoDatabase:
    raw_db: object
    backend: str = "mongo"

    def __getattr__(self, name: str):
        return getattr(self.raw_db, name)


@dataclass
class HybridDatabase:
    db_mongo: MongoDatabase
    db_clickhouse: ClickHouseDatabase
    backend: str = "hybrid"

    def __getattr__(self, name: str):
        # 既存コードとの後方互換のため、まず ClickHouse 側を優先して参照する。
        try:
            return getattr(self.db_clickhouse, name)
        except AttributeError:
            return getattr(self.db_mongo, name)


def get_storage_backend() -> str:
    return os.getenv("STORAGE_BACKEND", "mongo").strip().lower()


def _clickhouse_client_kwargs_from_dsn(dsn: str) -> dict:
    parsed = urlparse(dsn)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 8123,
        "username": parsed.username or "default",
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/") or "analyze_bot",
        # clickhouse-connect の同一 session 制約を避けるため、常に session を無効化する。
        "autogenerate_session_id": False,
    }


def _build_clickhouse_client_from_dsn(dsn: str):
    try:
        import clickhouse_connect
    except ImportError as error:
        raise RuntimeError(
            "clickhouse-connect が未インストールです。`uv add clickhouse-connect` を実行してください。"
        ) from error

    return clickhouse_connect.get_client(**_clickhouse_client_kwargs_from_dsn(dsn))


def init_mongo_storage() -> MongoDatabase:
    dsn = os.getenv("MONGODB_DSN")
    client_db = MongoClient(dsn)
    return MongoDatabase(raw_db=client_db["discord_analyzer"])


def init_clickhouse_storage() -> ClickHouseDatabase:
    dsn = os.getenv(
        "CLICKHOUSE_DSN",
        "http://default:clickhousepassword@localhost:8123/analyze_bot",
    )
    client = _build_clickhouse_client_from_dsn(dsn)
    database = urlparse(dsn).path.lstrip("/") or "analyze_bot"
    return ClickHouseDatabase(client=client, database=database)


def init_storages() -> tuple[MongoDatabase | None, ClickHouseDatabase | None, str]:
    backend = get_storage_backend()

    if backend == "hybrid":
        return init_mongo_storage(), init_clickhouse_storage(), backend

    if backend == "clickhouse":
        return None, init_clickhouse_storage(), backend

    return init_mongo_storage(), None, backend


def init_storage():
    backend = get_storage_backend()

    if backend == "hybrid":
        mongo_db = init_mongo_storage()
        clickhouse_db = init_clickhouse_storage()
        return HybridDatabase(db_mongo=mongo_db, db_clickhouse=clickhouse_db)

    if backend == "clickhouse":
        return init_clickhouse_storage()

    return init_mongo_storage()
