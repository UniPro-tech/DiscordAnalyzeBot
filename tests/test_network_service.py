from libs.network_service import (
    build_conversation_edges,
    build_node_labels,
    fetch_network_documents,
)


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, limit: int):
        self._docs = self._docs[:limit]
        return self

    def __iter__(self):
        return iter(self._docs)


class _FakeMessagesCollection:
    def __init__(self, docs):
        self._docs = list(docs)

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


def test_build_conversation_edges_counts_reply_and_mentions():
    docs = [
        {"message_id": "1", "user_id": "10", "reply_to": None, "mentions": []},
        {"message_id": "2", "user_id": "20", "reply_to": "1", "mentions": []},
        {"message_id": "3", "user_id": "20", "reply_to": None, "mentions": ["10", "30"]},
        {"message_id": None, "user_id": "99", "reply_to": None, "mentions": []},
    ]

    edges, invalid_doc_count = build_conversation_edges(docs)

    assert invalid_doc_count == 1
    assert edges == {("10", "20"): 2, ("20", "30"): 1}


def test_build_node_labels_keeps_distinct_ids_for_same_display_name():
    edges = {("10", "20"): 2, ("20", "30"): 1}

    labels = build_node_labels(
        edges,
        lambda user_id: "same-name" if user_id in {"10", "20"} else "other-name",
    )

    assert labels == {
        "10": "same-name",
        "20": "same-name",
        "30": "other-name",
    }


def test_build_conversation_edges_skips_doc_when_mentions_is_not_list():
    docs = [
        {"message_id": "1", "user_id": "10", "reply_to": None, "mentions": []},
        {"message_id": "2", "user_id": "20", "reply_to": None, "mentions": "123"},
    ]

    edges, invalid_doc_count = build_conversation_edges(docs)

    assert invalid_doc_count == 1
    assert edges == {}


def test_fetch_network_documents_includes_missing_reply_targets():
    db = _FakeDB(
        [
            {
                "guild_id": "g1",
                "message_id": "parent-1",
                "user_id": "10",
                "reply_to": None,
                "mentions": [],
                "channel_id": "other",
                "timestamp": "2026-01-01T00:00:00+00:00",
            },
            {
                "guild_id": "g1",
                "message_id": "child-1",
                "user_id": "20",
                "reply_to": "parent-1",
                "mentions": [],
                "channel_id": "target",
                "timestamp": "2026-01-02T00:00:00+00:00",
            },
        ]
    )

    docs = fetch_network_documents(
        db,
        "g1",
        channel_id="target",
        limit=100,
    )

    edges, invalid_doc_count = build_conversation_edges(docs)

    assert invalid_doc_count == 0
    assert edges == {("10", "20"): 1}