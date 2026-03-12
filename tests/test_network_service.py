from libs.network_service import (
    build_conversation_edges,
    build_node_labels,
    calculate_label_display_width,
    calculate_label_font_size,
    calculate_layout_spacing,
    calculate_node_size,
    fetch_network_documents,
    generate_conversation_network,
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


def test_generate_conversation_network_uses_provided_labels(monkeypatch):
    captured = {}

    def _fake_draw(_graph, _positions, **kwargs):
        captured["node_size"] = kwargs["node_size"]
        captured["width"] = list(kwargs["width"])

    def _fake_draw_networkx_labels(_graph, _positions, labels, **_kwargs):
        captured["labels"] = dict(labels)
        captured["font_size"] = _kwargs["font_size"]
        return {}

    monkeypatch.setattr("libs.network_service.resolve_font_path", lambda: "fonts/ipaexg.ttf")
    monkeypatch.setattr("libs.network_service.nx.draw", _fake_draw)
    monkeypatch.setattr("libs.network_service.nx.draw_networkx_labels", _fake_draw_networkx_labels)

    generate_conversation_network(
        {("10", "20"): 2},
        labels={"10": "Alice", "20": "Bob"},
    )

    assert set(captured["labels"].values()) == {"Alice", "Bob"}
    assert captured["font_size"] == calculate_label_font_size(2, ["Alice", "Bob"])
    assert captured["node_size"] == calculate_node_size(2, ["Alice", "Bob"])
    assert captured["width"]


def test_calculate_label_display_width_counts_wide_characters():
    assert calculate_label_display_width("ABCD") == 4
    assert calculate_label_display_width("あい") == 4


def test_calculate_label_font_size_considers_node_count_and_label_length():
    assert calculate_label_font_size(2, ["Alice", "Bob"]) > calculate_label_font_size(
        12,
        ["Alice", "Bob"],
    )
    assert calculate_label_font_size(4, ["短い"]) > calculate_label_font_size(
        4,
        ["とても長いユーザー表示名です"],
    )


def test_calculate_node_size_grows_for_longer_labels():
    assert calculate_node_size(4, ["Amy", "Bob"]) < calculate_node_size(
        4,
        ["VeryLongDisplayName", "AnotherLongDisplayName"],
    )


def test_calculate_layout_spacing_expands_for_long_labels():
    assert calculate_layout_spacing(6, ["Amy", "Bob"]) < calculate_layout_spacing(
        6,
        ["とても長いユーザー表示名です", "かなり長い別名です"],
    )