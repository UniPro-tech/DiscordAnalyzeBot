from datetime import datetime
from zoneinfo import ZoneInfo

from libs.wordcloud_service import (
    learn_from_text,
    parse_schedule_time,
    should_execute_schedule,
    update_compounds,
)


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


def test_should_execute_schedule_for_monthly_on_actual_month_end():
    now_april_end = datetime(2026, 4, 30, 9, 0, tzinfo=JST)

    assert should_execute_schedule("monthly", None, now_april_end, JST) is True
    assert (
        should_execute_schedule(
            "monthly",
            "2026-04-01T00:00:00+09:00",
            now_april_end,
            JST,
        )
        is False
    )


def test_should_execute_schedule_for_monthly_not_on_non_month_end_day():
    now_not_end = datetime(2026, 4, 29, 9, 0, tzinfo=JST)

    assert should_execute_schedule("monthly", None, now_not_end, JST) is False


class _CollectionStub:
    def __init__(self):
        self.calls = []

    def update_one(self, query, update, upsert=False):
        self.calls.append((query, update, upsert))


class _DBStub:
    def __init__(self):
        self.unigrams = _CollectionStub()
        self.ngrams = _CollectionStub()


class _UnigramCollectionForUpdateStub:
    def __init__(self, docs):
        self._docs_by_word = {doc["word"]: doc for doc in docs}
        self._total = sum(doc["count"] for doc in docs)

    def aggregate(self, _pipeline):
        return iter([{"_id": None, "total": self._total}])

    def find_one(self, query):
        return self._docs_by_word.get(query["word"])


class _NgramCollectionForUpdateStub:
    def __init__(self, docs):
        self._docs = docs
        self._docs_by_ngram = {tuple(doc["ngram"]): doc for doc in docs}

    def find(self):
        return self._docs

    def find_one(self, query):
        return self._docs_by_ngram.get(tuple(query["ngram"]))


class _CompoundsCollectionForUpdateStub:
    def __init__(self):
        self.calls = []

    def update_one(self, query, update, upsert=False):
        self.calls.append((query, update, upsert))


class _UpdateCompoundsDBStub:
    def __init__(self, unigram_docs, ngram_docs):
        self.unigrams = _UnigramCollectionForUpdateStub(unigram_docs)
        self.ngrams = _NgramCollectionForUpdateStub(ngram_docs)
        self.compounds = _CompoundsCollectionForUpdateStub()


def test_learn_from_text_does_not_create_ngram_across_particle():
    db = _DBStub()

    learn_from_text(db, "記憶の人間")

    # Unigrams are still learned.
    assert len(db.unigrams.calls) == 2
    # No ngram should be created across "の".
    assert db.ngrams.calls == []


def test_learn_from_text_creates_trigram_for_adjacent_tokens():
    db = _DBStub()

    learn_from_text(db, "経済社会問題")

    learned_ngrams = [call[0]["ngram"] for call in db.ngrams.calls]

    assert ["経済", "社会"] in learned_ngrams
    assert ["社会", "問題"] in learned_ngrams
    assert ["経済", "社会", "問題"] in learned_ngrams


def test_learn_from_text_does_not_create_trigram_across_particle():
    db = _DBStub()

    learn_from_text(db, "経済の社会問題")

    learned_ngrams = [call[0]["ngram"] for call in db.ngrams.calls]

    assert ["社会", "問題"] in learned_ngrams
    assert ["経済", "社会", "問題"] not in learned_ngrams


def test_update_compounds_promotes_overlapping_bigrams_to_trigram():
    db = _UpdateCompoundsDBStub(
        unigram_docs=[
            {"word": "ミラノ", "count": 20},
            {"word": "風", "count": 20},
            {"word": "ドリア", "count": 20},
            {"word": "その他", "count": 340},
        ],
        ngram_docs=[
            {"ngram": ["ミラノ", "風"], "count": 10},
            {"ngram": ["風", "ドリア"], "count": 10},
        ],
    )

    update_compounds(db)

    saved_words = [call[0]["word"] for call in db.compounds.calls]

    assert "ミラノ風" in saved_words
    assert "風ドリア" in saved_words
    assert "ミラノ風ドリア" in saved_words