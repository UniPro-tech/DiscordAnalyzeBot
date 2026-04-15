from libs.text_processing import (
    analyze_sudachi_pos,
    apply_learned_compounds,
    clear_extract_tokens_cache,
    extract_tokens,
    extract_tokens_with_indices,
    normalize_text,
    resolve_split_mode,
)


def test_normalize_text_strips_discord_markup_and_code_blocks():
    text = "<@123> 今日は ```print('x')``` https://example.com ||spoiler|| テストwｗｗ"

    normalized = normalize_text(text)

    assert "<@123>" not in normalized
    assert "https://example.com" not in normalized
    assert "print('x')" not in normalized
    assert "spoiler" not in normalized
    assert "www" in normalized


def test_apply_learned_compounds_joins_adjacent_words():
    words = ["自然", "言語", "処理", "入門"]

    assert apply_learned_compounds(words, {"自然言語", "処理入門"}) == [
        "自然言語",
        "処理入門",
    ]


def test_apply_learned_compounds_prefers_trigram_over_bigram():
    words = ["自然", "言語", "処理"]

    assert apply_learned_compounds(words, {"自然言語", "自然言語処理"}) == [
        "自然言語処理",
    ]


def test_apply_learned_compounds_promotes_overlapping_bigrams_to_trigram():
    words = ["ミラノ", "風", "ドリア"]

    assert apply_learned_compounds(words, {"ミラノ風", "風ドリア"}) == [
        "ミラノ風ドリア",
    ]


def test_extract_tokens_keeps_style_suffix_for_compound_words():
    assert extract_tokens("ミラノ風ドリア") == ["ミラノ", "風", "ドリア"]


def test_extract_tokens_drops_counter_suffix_noise():
    assert "つ" not in extract_tokens("ひとつ食べた")


def test_extract_tokens_drops_taiku_noise_word():
    tokens = extract_tokens("多くの参加者が集まった")

    assert "多く" not in tokens
    assert "参加者" in tokens


def test_extract_tokens_with_indices_keeps_original_positions():
    assert extract_tokens_with_indices("記憶の人間") == [("記憶", 0), ("人間", 2)]


class _TokenStub:
    def __init__(self, word: str, pos: tuple[str, ...]):
        self._word = word
        self._pos = pos

    def part_of_speech(self):
        return self._pos

    def surface(self):
        return self._word

    def dictionary_form(self):
        return self._word


class _TokenizerStub:
    def __init__(self):
        self.calls = 0

    def tokenize(self, _text, _mode):
        self.calls += 1
        return [
            _TokenStub("参加者", ("名詞", "一般")),
            _TokenStub("多く", ("名詞", "一般")),
        ]


def test_extract_tokens_cache_and_clear(monkeypatch):
    import libs.text_processing as text_processing

    tokenizer_stub = _TokenizerStub()
    clear_extract_tokens_cache()
    monkeypatch.setattr(text_processing, "tokenizer_obj", tokenizer_stub)

    assert extract_tokens("same input") == ["参加者"]
    assert extract_tokens("same input") == ["参加者"]
    assert tokenizer_stub.calls == 1

    clear_extract_tokens_cache()
    assert extract_tokens("same input") == ["参加者"]
    assert tokenizer_stub.calls == 2


def test_resolve_split_mode_rejects_invalid_value():
    import pytest

    with pytest.raises(ValueError):
        resolve_split_mode("z")


def test_analyze_sudachi_pos_returns_surface_pos_and_base_form(monkeypatch):
    import libs.text_processing as text_processing

    class _AnalyzerTokenizerStub:
        def tokenize(self, _text, _mode):
            return [
                _TokenStub("参加者", ("名詞", "一般")),
                _TokenStub("です", ("助動詞", "*")),
            ]

    monkeypatch.setattr(text_processing, "tokenizer_obj", _AnalyzerTokenizerStub())

    assert analyze_sudachi_pos("参加者です", "C") == [
        ("参加者", ("名詞", "一般"), "参加者"),
        ("です", ("助動詞", "*"), "です"),
    ]
