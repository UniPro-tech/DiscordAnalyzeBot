from libs.network_service import build_conversation_edges, build_node_labels


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