from libs.migration import (
    migration_enabled,
    maybe_run_clickhouse_migration,
    run_clickhouse_migration,
    target_has_existing_data,
)


class _FakeMongoCollection:
    def __init__(self, docs):
        self._docs = list(docs)

    def find(self, *_args, **_kwargs):
        return list(self._docs)


class _FakeMongoDB:
    def __init__(self):
        self.messages = _FakeMongoCollection(
            [
                {
                    "message_id": "1",
                    "guild_id": "g1",
                    "guild_name": "Guild",
                    "user_id": "u1",
                    "username": "user",
                    "channel_id": "c1",
                    "channel_name": "general",
                    "content": "hello",
                    "timestamp": "2026-03-14T00:00:00+00:00",
                    "role_ids": ["r1"],
                    "reply_to": None,
                    "mentions": ["u2"],
                    "attachments": [],
                    "length": 5,
                    "emoji_count": 0,
                    "url_count": 0,
                    "tokens": ["hello"],
                }
            ]
        )
        self.guild_settings = _FakeMongoCollection(
            [{"guild_id": "g1", "channel_id": "c1", "frequency": "daily", "schedule_time": "09:00", "enabled": True, "last_executed": None}]
        )
        self.channel_settings = _FakeMongoCollection(
            [{"guild_id": "g1", "channel_id": "c1", "opt_out": True}]
        )
        self.user_settings = _FakeMongoCollection(
            [{"user_id": "u1", "opt_out": False}]
        )
        self.unigrams = _FakeMongoCollection(
            [{"word": "hello", "count": 3}]
        )
        self.ngrams = _FakeMongoCollection(
            [{"ngram": ["hello", "world"], "count": 2}]
        )
        self.compounds = _FakeMongoCollection(
            [{"word": "helloworld", "pmi": 3.5}]
        )
        self.meta = _FakeMongoCollection(
            [{"_id": "last_learn_cursor", "value": {"timestamp": "2026-03-14T00:00:00+00:00", "message_id": "1"}}]
        )


class _FakeClickHouseDB:
    def __init__(self):
        self.backend = "clickhouse"
        self.tables = {
            "messages": [],
            "guild_settings": [],
            "channel_settings": [],
            "user_settings": [],
            "meta": [],
            "unigrams": [],
            "ngrams": [],
            "compounds": [],
        }
        self.commands = []

    def command(self, query, parameters=None):
        self.commands.append((query, parameters or {}))
        normalized = query.strip().upper()
        if normalized.startswith("TRUNCATE TABLE IF EXISTS"):
            table_name = query.strip().split()[-1]
            self.tables[table_name] = []

    def insert_rows(self, table, rows, columns):
        for row in rows:
            self.tables[table].append(dict(zip(columns, row)))

    def query_dicts(self, query, parameters=None):
        return []

    def query_scalar(self, query, parameters=None):
        table_name = query.strip().split()[-1]
        return len(self.tables.get(table_name, []))


def test_migration_enabled_accepts_truthy_values(monkeypatch):
    monkeypatch.setenv("MIGRATION", "true")
    assert migration_enabled() is True

    monkeypatch.setenv("MIGRATION", "false")
    assert migration_enabled() is False


def test_run_clickhouse_migration_copies_all_supported_collections():
    target = _FakeClickHouseDB()
    source = _FakeMongoDB()

    counts = run_clickhouse_migration(target, source)

    assert counts == {
        "messages": 1,
        "guild_settings": 1,
        "channel_settings": 1,
        "user_settings": 1,
        "meta": 1,
        "unigrams": 1,
        "ngrams": 1,
        "compounds": 1,
    }
    assert len(target.tables["messages"]) == 1
    assert len(target.tables["guild_settings"]) == 1
    assert len(target.tables["channel_settings"]) == 1
    assert len(target.tables["user_settings"]) == 1
    assert len(target.tables["meta"]) == 1
    assert len(target.tables["unigrams"]) == 1
    assert len(target.tables["ngrams"]) == 1
    assert len(target.tables["compounds"]) == 1


def test_maybe_run_clickhouse_migration_skips_when_flag_disabled(monkeypatch):
    monkeypatch.setenv("MIGRATION", "false")
    target = _FakeClickHouseDB()

    counts = maybe_run_clickhouse_migration(target)

    assert counts == {}


def test_target_has_existing_data_detects_any_non_empty_table():
    target = _FakeClickHouseDB()
    assert target_has_existing_data(target) is False

    target.tables["messages"].append({"message_id": "1"})
    assert target_has_existing_data(target) is True


def test_maybe_run_clickhouse_migration_skips_when_clickhouse_already_has_data(monkeypatch):
    monkeypatch.setenv("MIGRATION", "true")
    target = _FakeClickHouseDB()
    target.tables["messages"].append({"message_id": "existing"})

    counts = maybe_run_clickhouse_migration(target)

    assert counts == {"skipped_existing_data": 1}