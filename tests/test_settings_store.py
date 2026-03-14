from libs.settings_store import setup_settings_indexes


class _FakeCollection:
    def __init__(self):
        self.index_calls = []

    def create_index(self, key, **kwargs):
        self.index_calls.append((key, kwargs))


class _FakeDB:
    def __init__(self):
        self.guild_settings = _FakeCollection()
        self.user_settings = _FakeCollection()
        self.channel_settings = _FakeCollection()


def test_setup_settings_indexes_creates_expected_indexes():
    db = _FakeDB()

    setup_settings_indexes(db)

    guild_keys = [call[0] for call in db.guild_settings.index_calls]
    user_keys = [call[0] for call in db.user_settings.index_calls]
    channel_keys = [call[0] for call in db.channel_settings.index_calls]

    assert ("guild_id", 1) in guild_keys[0]
    assert "enabled" in guild_keys
    assert "user_id" in user_keys
    assert "opt_out" in user_keys
    assert "opt_out" in channel_keys

    guild_compound = db.guild_settings.index_calls[0]
    assert guild_compound[1]["unique"] is True

    channel_compound = db.channel_settings.index_calls[0]
    assert channel_compound[1]["unique"] is True
