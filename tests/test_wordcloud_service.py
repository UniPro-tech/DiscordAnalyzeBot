from datetime import datetime
from zoneinfo import ZoneInfo

from libs.wordcloud_service import learn_from_text, parse_schedule_time, should_execute_schedule


JST = ZoneInfo("Asia/Tokyo")


def test_parse_schedule_time_accepts_hh_mm():
    assert parse_schedule_time("09:30") == (9, 30)
    assert parse_schedule_time("24:00") is None
    assert parse_schedule_time("bad") is None


def test_should_execute_schedule_for_weekly_only_once_per_week():
    now = datetime(2026, 3, 16, 9, 0, tzinfo=JST)

    assert should_execute_schedule("weekly", None, now, JST) is True
    assert (
        should_execute_schedule(
            "weekly",
            "2026-03-16T00:00:00+09:00",
            now,
            JST,
        )
        is False
    )


def test_should_execute_schedule_for_daily_compares_dates_only():
    now = datetime(2026, 3, 12, 9, 0, tzinfo=JST)

    assert should_execute_schedule("daily", None, now, JST) is True
    assert should_execute_schedule("daily", "2026-03-11T23:50:00+09:00", now, JST) is True
    assert should_execute_schedule("daily", "2026-03-12T00:00:00+09:00", now, JST) is False


class _CollectionStub:
    def __init__(self):
        self.calls = []

    def update_one(self, query, update, upsert=False):
        self.calls.append((query, update, upsert))


class _DBStub:
    def __init__(self):
        self.unigrams = _CollectionStub()
        self.ngrams = _CollectionStub()


def test_learn_from_text_does_not_create_ngram_across_particle():
    db = _DBStub()

    learn_from_text(db, "記憶の人間")

    # Unigrams are still learned.
    assert len(db.unigrams.calls) == 2
    # No ngram should be created across "の".
    assert db.ngrams.calls == []