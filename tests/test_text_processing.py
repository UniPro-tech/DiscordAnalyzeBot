from libs.text_processing import (
    apply_learned_compounds,
    extract_tokens,
    extract_tokens_with_indices,
    normalize_text,
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