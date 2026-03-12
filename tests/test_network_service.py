from libs.network_service import build_conversation_edges


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