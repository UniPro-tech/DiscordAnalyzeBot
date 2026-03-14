from libs.message_store import (
    count_distinct_message_users,
    count_messages,
    fetch_messages,
    fetch_messages_by_ids,
    insert_message,
    normalize_message_ids,
    setup_message_indexes,
)


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, field, order):
        reverse = order == -1
        self._docs.sort(key=lambda doc: doc.get(field), reverse=reverse)
        return self

    def limit(self, limit):
        self._docs = self._docs[:limit]
        return self

    def __iter__(self):
        return iter(self._docs)


class _FakeMessagesCollection:
    def __init__(self, docs):
        self._docs = list(docs)
        self.index_calls = []

    def find(self, query, projection):
        matched = []

        for doc in self._docs:
            if not self._matches(doc, query):
                continue

            projected = {
                key: value
                for key, value in doc.items()
                if key in projection or key == "_id"
            }
            matched.append(projected)

        return _FakeCursor(matched)

    def insert_one(self, doc):
        self._docs.append(dict(doc))

    def estimated_document_count(self):
        return len(self._docs)

    def distinct(self, key):
        return list({doc.get(key) for doc in self._docs if doc.get(key) is not None})

    def create_index(self, key, **kwargs):
        self.index_calls.append((key, kwargs))

    @staticmethod
    def _matches(doc, query):
        for key, expected in query.items():
            actual = doc.get(key)

            if isinstance(expected, dict):
                if "$in" in expected:
                    if actual not in expected["$in"]:
                        return False
                elif "$gte" in expected:
                    if actual is None or actual < expected["$gte"]:
                        return False
                else:
                    return False
            else:
                if actual != expected:
                    return False

        return True


class _FakeDB:
    def __init__(self, docs):
        self.messages = _FakeMessagesCollection(docs)


class _FakeClickHouseDB:
    def __init__(self):
        self.backend = "clickhouse"
        self.commands = []

    def command(self, query, parameters=None):
        self.commands.append((query, parameters or {}))


def test_normalize_message_ids_converts_all_to_strings():
    assert normalize_message_ids([1, "2", 3]) == ["1", "2", "3"]


def test_fetch_messages_applies_sort_and_limit():
    db = _FakeDB(
        [
            {"message_id": "1", "guild_id": "g1", "timestamp": 1, "content": "a"},
            {"message_id": "2", "guild_id": "g1", "timestamp": 3, "content": "b"},
            {"message_id": "3", "guild_id": "g1", "timestamp": 2, "content": "c"},
        ]
    )

    docs = fetch_messages(
        db,
        {"guild_id": "g1"},
        {"message_id": 1, "timestamp": 1},
        sort_field="timestamp",
        sort_order=-1,
        limit=2,
    )

    assert [doc["message_id"] for doc in docs] == ["2", "3"]


def test_fetch_messages_by_ids_filters_by_guild_and_id_set():
    db = _FakeDB(
        [
            {"message_id": "1", "guild_id": "g1", "user_id": "u1"},
            {"message_id": "2", "guild_id": "g2", "user_id": "u2"},
            {"message_id": "3", "guild_id": "g1", "user_id": "u3"},
        ]
    )

    docs = fetch_messages_by_ids(
        db,
        "g1",
        [1, "3"],
        {"message_id": 1, "user_id": 1},
    )

    assert {doc["message_id"] for doc in docs} == {"1", "3"}


def test_insert_message_and_counts_work():
    db = _FakeDB(
        [
            {"message_id": "1", "guild_id": "g1", "user_id": "u1"},
            {"message_id": "2", "guild_id": "g1", "user_id": "u2"},
        ]
    )

    insert_message(db, {"message_id": "3", "guild_id": "g1", "user_id": "u1"})

    assert count_messages(db) == 3
    assert count_distinct_message_users(db) == 2


def test_setup_message_indexes_creates_expected_indexes():
    db = _FakeDB([])

    setup_message_indexes(db)

    keys = [call[0] for call in db.messages.index_calls]
    assert "user_id" in keys
    assert "channel_id" in keys
    assert "guild_id" in keys
    assert "reply_to" in keys
    assert "timestamp" in keys

    message_id_call = next(
        call
        for call in db.messages.index_calls
        if call[0] == "message_id"
    )
    assert message_id_call[1]["unique"] is True


def test_setup_message_indexes_for_clickhouse_uses_datetime_ttl_expression():
    db = _FakeClickHouseDB()

    setup_message_indexes(db)

    assert len(db.commands) == 1
    query = db.commands[0][0]
    assert "TTL toDateTime(timestamp) + INTERVAL 30 DAY" in query
