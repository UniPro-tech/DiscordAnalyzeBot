import math
import re
import unicodedata
from functools import lru_cache

from sudachipy import dictionary, tokenizer


STOP_WORDS = {
    "ので",
    "そう",
    "から",
    "ため",
    "あと",
    "こと",
    "もの",
    "よう",
    "さん",
    "これ",
    "それ",
    "あれ",
    "どれ",
    "なに",
    "なん",
    "どこ",
    "いつ",
    "だれ",
    "なぜ",
    "どう",
    "なにか",
    "なんか",
    "どこか",
    "いつか",
    "だれか",
    "なぜか",
    "どうか",
    "する",
    "いる",
    "ある",
    "www",
    "感じ",
    "やつ",
    "ここ",
    "ところ",
    "みたい",
    "やっぱ",
    "多く",
    "お疲れ様",
    "うち",
    "はず"
}

tokenizer_obj = dictionary.Dictionary().create()
MODE = tokenizer.Tokenizer.SplitMode.C
SPLIT_MODE_MAP = {
    "A": tokenizer.Tokenizer.SplitMode.A,
    "B": tokenizer.Tokenizer.SplitMode.B,
    "C": tokenizer.Tokenizer.SplitMode.C,
}
SINGLE_HIRAGANA_PATTERN = re.compile(r"^[ぁ-ゖ]$")
URL_PATTERN = re.compile(r"https?://\S+")
MENTION_PATTERN = re.compile(r"<@!?\d+>")
CHANNEL_PATTERN = re.compile(r"<#\d+>")
EMOJI_PATTERN = re.compile(r"<a?:\w+:\d+>")
CODE_BLOCK_PATTERN = re.compile(r"```.*?```", flags=re.DOTALL)
INLINE_CODE_PATTERN = re.compile(r"`.*?`")
STRIKETHROUGH_PATTERN = re.compile(r"~~.*?~~")
SPOILER_PATTERN = re.compile(r"\|\|.*?\|\|")
WWW_PATTERN = re.compile(r"[wｗ]{2,}")
MULTISPACE_PATTERN = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    text = text.replace("\n", " ")

    text = URL_PATTERN.sub("", text)
    text = MENTION_PATTERN.sub("", text)
    text = CHANNEL_PATTERN.sub("", text)
    text = EMOJI_PATTERN.sub("", text)
    text = CODE_BLOCK_PATTERN.sub("", text)
    text = INLINE_CODE_PATTERN.sub("", text)
    text = STRIKETHROUGH_PATTERN.sub("", text)
    text = SPOILER_PATTERN.sub("", text)
    text = WWW_PATTERN.sub("www", text)
    text = MULTISPACE_PATTERN.sub(" ", text)

    return unicodedata.normalize("NFKC", text).strip()


def resolve_split_mode(mode: str) -> tokenizer.Tokenizer.SplitMode:
    normalized_mode = mode.strip().upper()

    if normalized_mode not in SPLIT_MODE_MAP:
        raise ValueError("split mode must be one of A, B, C")

    return SPLIT_MODE_MAP[normalized_mode]


def _is_target_token(word: str, pos: tuple[str, ...]) -> bool:
    if word in STOP_WORDS:
        return False

    if pos[0] == "接尾辞":
        if len(pos) < 2 or pos[1] not in {"形状詞的", "名詞的"}:
            return False
        # Exclude counters like "つ" (接尾辞-名詞的-助数詞)
        if len(pos) > 2 and pos[2] == "助数詞":
            return False

        # Exclude noisy one-char hiragana suffixes
        if SINGLE_HIRAGANA_PATTERN.fullmatch(word):
            return False

        return len(word) >= 1

    if pos[0] == "名詞" and pos[1] != "数":
        return len(word) >= 2

    return False


@lru_cache(maxsize=4096)
def _extract_tokens_cached(text: str) -> tuple[str, ...]:
    tokens = tokenizer_obj.tokenize(text, MODE)
    words = []

    for token in tokens:
        pos = token.part_of_speech()
        word = token.surface()

        if _is_target_token(word, pos):
            words.append(word)

    return tuple(words)


def extract_tokens(text: str) -> list[str]:
    return list(_extract_tokens_cached(text))


def clear_extract_tokens_cache() -> None:
    _extract_tokens_cached.cache_clear()


def extract_tokens_with_indices(text: str) -> list[tuple[str, int]]:
    tokens = tokenizer_obj.tokenize(text, MODE)
    words_with_indices = []

    for index, token in enumerate(tokens):
        pos = token.part_of_speech()
        word = token.surface()

        if _is_target_token(word, pos):
            words_with_indices.append((word, index))

    return words_with_indices


def analyze_sudachi_pos(
    text: str,
    mode: str = "C",
) -> list[tuple[str, tuple[str, ...], str]]:
    split_mode = resolve_split_mode(mode)
    tokens = tokenizer_obj.tokenize(text, split_mode)

    return [
        (token.surface(), tuple(token.part_of_speech()), token.dictionary_form())
        for token in tokens
    ]


def generate_ngrams(tokens: list[str], n: int) -> list[tuple[str, ...]]:
    return [tuple(tokens[index : index + n]) for index in range(len(tokens) - n + 1)]


def compute_pmi(xy: int, x: int, y: int, total: int) -> float:
    p_xy = xy / total
    p_x = x / total
    p_y = y / total

    return math.log2(p_xy / (p_x * p_y))


def apply_learned_compounds(words: list[str], compounds: set[str]) -> list[str]:
    joined = []
    index = 0

    while index < len(words):
        if index + 2 < len(words):
            first_bigram = words[index] + words[index + 1]
            second_bigram = words[index + 1] + words[index + 2]

            # Fallback: if overlapping bigrams are learned, promote to a 3-word phrase.
            if first_bigram in compounds and second_bigram in compounds:
                joined.append(words[index] + words[index + 1] + words[index + 2])
                index += 3
                continue

        if index + 2 < len(words):
            trigram_compound = words[index] + words[index + 1] + words[index + 2]

            if trigram_compound in compounds:
                joined.append(trigram_compound)
                index += 3
                continue

        if index + 1 < len(words):
            compound = words[index] + words[index + 1]

            if compound in compounds:
                joined.append(compound)
                index += 2
                continue

        joined.append(words[index])
        index += 1

    return joined


def join_message_content(docs: list[dict]) -> str:
    return " ".join((doc.get("content", "") or "").strip() for doc in docs)