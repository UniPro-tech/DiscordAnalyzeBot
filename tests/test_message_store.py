from libs.message_store import get_opt_out_flags, is_channel_opted_out


class _CollectionStub:
    def __init__(self, docs):
        self.docs = docs

    def find_one(self, query):
        channel_ids = query.get("channel_id", {}).get("$in")

        if channel_ids is not None:
            for doc in self.docs:
                if (
                    doc.get("guild_id") == query.get("guild_id")
                    and doc.get("channel_id") in channel_ids
                ):
                    return doc
            return None

        for doc in self.docs:
            matched = True
            for key, value in query.items():
                if doc.get(key) != value:
                    matched = False
                    break
            if matched:
                return doc

        return None


class _DBStub:
    def __init__(self, channel_docs, user_docs):
        self.channel_settings = _CollectionStub(channel_docs)
        self.user_settings = _CollectionStub(user_docs)


def test_is_channel_opted_out_matches_channel_id():
    db = _DBStub(
        channel_docs=[{"guild_id": "g1", "channel_id": "c1", "opt_out": True}],
        user_docs=[],
    )

    assert is_channel_opted_out(db, "g1", "c1") is True


def test_is_channel_opted_out_matches_parent_forum_channel_id():
    db = _DBStub(
        channel_docs=[{"guild_id": "g1", "channel_id": "forum1", "opt_out": True}],
        user_docs=[],
    )

    assert is_channel_opted_out(db, "g1", "thread1", parent_channel_id="forum1") is True


def test_get_opt_out_flags_checks_channel_and_user_with_parent_channel():
    db = _DBStub(
        channel_docs=[{"guild_id": "g1", "channel_id": "forum1", "opt_out": True}],
        user_docs=[{"user_id": "u1", "opt_out": True}],
    )

    assert get_opt_out_flags(
        db,
        guild_id="g1",
        channel_id="thread1",
        user_id="u1",
        parent_channel_id="forum1",
    ) == (True, True)