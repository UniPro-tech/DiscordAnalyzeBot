from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from libs.wordcloud_service import (
    build_learning_cursor_query,
    build_during_since_timestamp,
    extract_learning_cursor,
    fetch_last_learn_cursor,
    count_unmigrated_tokens,
    get_schedule_during_days,
    learn_from_text,
    learn_from_texts,
    migrate_message_tokens,
    parse_during_days,
    parse_schedule_time,
    reset_learning_state,
    setup_learning_tables,
    should_execute_schedule,
    update_compounds,
)


JST = ZoneInfo("Asia/Tokyo")


def test_parse_schedule_time_accepts_hh_mm():
    assert parse_schedule_time("09:30") == (9, 30)
    assert parse_schedule_time("24:00") is None
    assert parse_schedule_time("bad") is None


def test_parse_during_days_validates_positive_integer():
    assert parse_during_days(None) is None
    assert parse_during_days("1") == 1

    try:
        parse_during_days("0")
        assert False
    except ValueError:
        assert True


def test_build_during_since_timestamp_uses_jst_day_boundary(monkeypatch):
    monkeypatch.setattr(
        "libs.wordcloud_service.discord_utcnow",
        lambda: datetime(2026, 3, 13, 10, 45, tzinfo=timezone.utc),
    )

    assert build_during_since_timestamp(1) == "2026-03-12T15:00:00+00:00"
    assert build_during_since_timestamp(2) == "2026-03-11T15:00:00+00:00"


def test_get_schedule_during_days_matches_frequency():
    now = datetime(2026, 3, 31, 9, 0, tzinfo=JST)

    assert get_schedule_during_days("daily", now) == 1
    assert get_schedule_during_days("weekly", now) == 7
    assert get_schedule_during_days("monthly", now) == 31


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


def test_count_unmigrated_tokens_queries_null_tokens_for_clickhouse():
    class _DB:
        backend = "clickhouse"

        def __init__(self):
            self.queries = []

        def query_scalar(self, query, parameters=None):
            self.queries.append(query.strip())
            return 3

    db = _DB()

    assert count_unmigrated_tokens(db) == 3
    assert any("tokens IS NULL" in q for q in db.queries)


def test_migrate_message_tokens_updates_null_tokens_for_clickhouse(monkeypatch):
    class _DB:
        backend = "clickhouse"

        def __init__(self):
            self.rows = [
                {"message_id": "1", "content": "ミラノ風ドリア", "tokens": None},
                {"message_id": "2", "content": "", "tokens": None},
                {"message_id": "3", "content": "既に処理済み", "tokens": ["ok"]},
            ]
            self.commands = []

        def query_dicts(self, _query, parameters=None):
            limit = int((parameters or {}).get("limit", 0)) or len(self.rows)
            docs = [
                {"message_id": row["message_id"], "content": row["content"]}
                for row in self.rows
                if row.get("tokens") is None and (row.get("content") or "").strip()
            ]
            return docs[:limit]

        def command(self, _query, parameters=None):
            parameters = parameters or {}
            self.commands.append((_query, parameters))
            for row in self.rows:
                if row.get("message_id") == parameters.get("message_id"):
                    row["tokens"] = parameters.get("tokens")

    import libs.wordcloud_service as wc

    monkeypatch.setattr(wc, "extract_tokens", lambda _text: ["tok"])
    monkeypatch.setattr(wc, "normalize_text", lambda text: text)

    db = _DB()

    updated = migrate_message_tokens(db, batch_size=10)

    assert updated == 1
    assert db.rows[0]["tokens"] == ["tok"]
    assert db.rows[1]["tokens"] is None
    assert db.rows[2]["tokens"] == ["ok"]


def test_build_learning_cursor_query_returns_empty_without_cursor():
    assert build_learning_cursor_query(None) == {}


def test_build_learning_cursor_query_builds_lexicographic_progress_query():
    assert build_learning_cursor_query(
        {"timestamp": "2026-03-01T00:00:00+00:00", "message_id": "100"}
    ) == {
        "$or": [
            {"timestamp": {"$gt": "2026-03-01T00:00:00+00:00"}},
            {
                "timestamp": "2026-03-01T00:00:00+00:00",
                "message_id": {"$gt": "100"},
            },
        ]
    }


def test_extract_learning_cursor_requires_timestamp_and_message_id():
    assert extract_learning_cursor({"timestamp": "2026-03-01T00:00:00+00:00"}) is None
    assert extract_learning_cursor({"message_id": "100"}) is None

    assert extract_learning_cursor(
        {"timestamp": "2026-03-01T00:00:00+00:00", "message_id": 100}
    ) == {
        "timestamp": "2026-03-01T00:00:00+00:00",
        "message_id": "100",
    }


class _MetaFindOneStub:
    def __init__(self, doc):
        self._doc = doc

    def find_one(self, _query):
        return self._doc


class _MetaCursorDBStub:
    def __init__(self, doc):
        self.meta = _MetaFindOneStub(doc)


def test_fetch_last_learn_cursor_validates_document_shape():
    assert fetch_last_learn_cursor(_MetaCursorDBStub(None)) is None
    assert (
        fetch_last_learn_cursor(
            _MetaCursorDBStub({"_id": "last_learn_cursor", "value": "bad"})
        )
        is None
    )

    assert fetch_last_learn_cursor(
        _MetaCursorDBStub(
            {
                "_id": "last_learn_cursor",
                "value": {
                    "timestamp": "2026-03-01T00:00:00+00:00",
                    "message_id": "100",
                },
            }
        )
    ) == {
        "timestamp": "2026-03-01T00:00:00+00:00",
        "message_id": "100",
    }


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
        self._docs = docs
        self._docs_by_word = {doc["word"]: doc for doc in docs}
        self._total = sum(doc["count"] for doc in docs)

    def aggregate(self, _pipeline):
        return iter([{"_id": None, "total": self._total}])

    def find(self, *_args, **_kwargs):
        return self._docs

    def find_one(self, query):
        return self._docs_by_word.get(query["word"])


class _NgramCollectionForUpdateStub:
    def __init__(self, docs):
        self._docs = docs
        self._docs_by_ngram = {tuple(doc["ngram"]): doc for doc in docs}

    def find(self, *_args, **_kwargs):
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


class _BulkCollectionStub:
    def __init__(self):
        self.bulk_calls = []

    def bulk_write(self, operations, ordered=False):
        self.bulk_calls.append((operations, ordered))


class _BatchLearnDBStub:
    def __init__(self):
        self.unigrams = _BulkCollectionStub()
        self.ngrams = _BulkCollectionStub()


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


def test_learn_from_texts_uses_bulk_write_and_aggregates_counts():
    db = _BatchLearnDBStub()

    learn_from_texts(db, ["経済社会問題", "経済社会問題"], workers=1)

    assert len(db.unigrams.bulk_calls) == 1
    assert len(db.ngrams.bulk_calls) == 1

    unigram_ops = db.unigrams.bulk_calls[0][0]
    unigram_counts = {
        op._filter["word"]: op._doc["$inc"]["count"]
        for op in unigram_ops
    }
    assert unigram_counts["経済"] == 2
    assert unigram_counts["社会"] == 2
    assert unigram_counts["問題"] == 2


class _ClickHouseLearningStub:
    def __init__(self):
        self.backend = "clickhouse"
        self.tables = {
            "unigrams": [],
            "ngrams": [],
            "compounds": [],
            "meta": [],
        }
        self.commands = []

    def insert_rows(self, table, rows, columns):
        for row in rows:
            self.tables.setdefault(table, []).append(dict(zip(columns, row)))

    def command(self, query, parameters=None):
        self.commands.append((query.strip(), parameters or {}))

        normalized_query = query.strip().upper()
        if normalized_query.startswith("TRUNCATE TABLE IF EXISTS"):
            table_name = query.strip().split()[-1]
            self.tables[table_name] = []

    def query_scalar(self, query, parameters=None):
        normalized = " ".join(query.split()).upper()
        if "SELECT SUM(COUNT) AS TOTAL FROM UNIGRAMS" in normalized:
            return sum(int(doc.get("count", 0)) for doc in self.tables["unigrams"])
        return None

    def query_dicts(self, query, parameters=None):
        normalized = " ".join(query.split()).upper()

        if "FROM UNIGRAMS GROUP BY WORD" in normalized:
            grouped = {}
            for doc in self.tables["unigrams"]:
                grouped[doc["word"]] = grouped.get(doc["word"], 0) + int(doc["count"])
            return [{"word": word, "count": count} for word, count in grouped.items()]

        if "FROM NGRAMS GROUP BY NGRAM" in normalized:
            grouped = {}
            for doc in self.tables["ngrams"]:
                key = tuple(doc["ngram"])
                grouped[key] = grouped.get(key, 0) + int(doc["count"])
            return [{"ngram": list(ngram), "count": count} for ngram, count in grouped.items()]

        if "FROM COMPOUNDS" in normalized:
            return [{"word": doc["word"]} for doc in self.tables["compounds"]]

        return []


def test_setup_learning_tables_for_clickhouse_creates_tables():
    db = _ClickHouseLearningStub()

    setup_learning_tables(db)

    assert any("CREATE TABLE IF NOT EXISTS unigrams" in query for query, _ in db.commands)
    assert any("CREATE TABLE IF NOT EXISTS ngrams" in query for query, _ in db.commands)
    assert any("CREATE TABLE IF NOT EXISTS compounds" in query for query, _ in db.commands)


def test_learn_from_texts_for_clickhouse_inserts_aggregated_rows():
    db = _ClickHouseLearningStub()

    learn_from_texts(db, ["経済社会問題", "経済社会問題"], workers=1)

    unigram_count_by_word = {}
    for row in db.tables["unigrams"]:
        unigram_count_by_word[row["word"]] = unigram_count_by_word.get(row["word"], 0) + row["count"]

    assert unigram_count_by_word["経済"] == 2
    assert unigram_count_by_word["社会"] == 2
    assert unigram_count_by_word["問題"] == 2


def test_update_compounds_for_clickhouse_inserts_compound_rows():
    db = _ClickHouseLearningStub()
    db.insert_rows(
        "unigrams",
        [
            ["ミラノ", 20],
            ["風", 20],
            ["ドリア", 20],
            ["その他", 340],
        ],
        ["word", "count"],
    )
    db.insert_rows(
        "ngrams",
        [
            [["ミラノ", "風"], 10],
            [["風", "ドリア"], 10],
        ],
        ["ngram", "count"],
    )

    update_compounds(db)

    saved_words = {row["word"] for row in db.tables["compounds"]}
    assert "ミラノ風" in saved_words
    assert "風ドリア" in saved_words
    assert "ミラノ風ドリア" in saved_words


def test_reset_learning_state_for_clickhouse_truncates_learning_tables():
    db = _ClickHouseLearningStub()
    db.insert_rows("unigrams", [["a", 1]], ["word", "count"])
    db.insert_rows("ngrams", [[["a", "b"], 1]], ["ngram", "count"])
    db.insert_rows("compounds", [["ab", 3.1]], ["word", "pmi"])

    reset_learning_state(db)

    assert db.tables["unigrams"] == []
    assert db.tables["ngrams"] == []
    assert db.tables["compounds"] == []