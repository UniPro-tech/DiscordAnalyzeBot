import math
import re
import unicodedata

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
}

tokenizer_obj = dictionary.Dictionary().create()
MODE = tokenizer.Tokenizer.SplitMode.C
SINGLE_HIRAGANA_PATTERN = re.compile(r"^[ぁ-ゖ]$")


def normalize_text(text: str) -> str:
    text = text.replace("\n", " ")

    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"<@!?\d+>", "", text)
    text = re.sub(r"<#\d+>", "", text)
    text = re.sub(r"<a?:\w+:\d+>", "", text)
    text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
    text = re.sub(r"`.*?`", "", text)
    text = re.sub(r"~~.*?~~", "", text)
    text = re.sub(r"\|\|.*?\|\|", "", text)
    text = re.sub(r"[wｗ]{2,}", "www", text)
    text = re.sub(r"\s+", " ", text)

    return unicodedata.normalize("NFKC", text).strip()


def _is_target_token(word: str, pos: tuple[str, ...]) -> bool:
    if word in STOP_WORDS:
        return False

    if pos[0] == "接尾辞":
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


def extract_tokens(text: str) -> list[str]:
    tokens = tokenizer_obj.tokenize(text, MODE)
    words = []

    for token in tokens:
        pos = token.part_of_speech()
        word = token.surface()

        if _is_target_token(word, pos):
            words.append(word)

    return words


def extract_tokens_with_indices(text: str) -> list[tuple[str, int]]:
    tokens = tokenizer_obj.tokenize(text, MODE)
    words_with_indices = []

    for index, token in enumerate(tokens):
        pos = token.part_of_speech()
        word = token.surface()

        if _is_target_token(word, pos):
            words_with_indices.append((word, index))

    return words_with_indices


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