import io
import os
import re
import unicodedata

import matplotlib
from janome.tokenizer import Tokenizer
from wordcloud import WordCloud

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

matplotlib.use("Agg")
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
}

DEFAULT_FONT_PATHS = [
    "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/fonts-japanese-gothic.ttf",
    "/app/fonts/ipaexg.ttf",
    "/app/fonts/ipaexm.ttf",
]


def resolve_font_path() -> str | None:
    env_font = os.getenv("WORDCLOUD_FONT_PATH")
    if env_font and os.path.exists(env_font):
        return env_font

    for font_path in DEFAULT_FONT_PATHS:
        if os.path.exists(font_path):
            return font_path

    return None


def normalize_text(text: str) -> str:
    normalized = text.replace("\n", " ")
    normalized = re.sub("\u3000", "", normalized)
    normalized = re.sub("・", "", normalized)
    normalized = re.sub("「", "", normalized)
    normalized = re.sub("」", "", normalized)
    normalized = re.sub("（", "", normalized)
    normalized = re.sub("）", "", normalized)
    normalized = re.sub("\\\\n", " ", normalized)
    return unicodedata.normalize("NFKC", normalized)


def strip_decoration(text: str) -> str:
    """テキスト中のコードブロック（```...```, ~~~...~~~）とインラインコード（`...`）、取り消し線（~~...~~）、スポイラー（||...||）を除去して返す。

    意図: ワードクラウド生成時にコードのトークンがノイズになるため除去する。
    """
    if not text:
        return ""

    # フェンス付きコードブロック（```...``` や ~~~...~~~）を削除
    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    text = re.sub(r"~~~.*?~~~", " ", text, flags=re.S)

    # インラインコード `...` を削除
    text = re.sub(r"`[^`]*`", " ", text)

    # 取り消し線 ~~...~~ を削除
    text = re.sub(r"~~[^~]*~~", " ", text)

    # スポイラー ||...|| を削除
    text = re.sub(r"\|\|[^|]*\|\|", " ", text)

    # 複数空白を単一空白に
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_nouns(text: str, tokenizer: Tokenizer | None = None) -> str:
    t = tokenizer or Tokenizer()
    words_list: list[str] = []
    for token in t.tokenize(text):
        hinshi = token.part_of_speech.split(",")[0]
        hinshi2 = token.part_of_speech.split(",")[1]
        if hinshi == "名詞" and hinshi2 not in {"数", "代名詞", "非自立"}:
            words_list.append(token.surface)
    return " ".join(words_list)


def generate_wordcloud_image(text: str, font_path: str | None = None) -> io.BytesIO:
    import matplotlib.pyplot as plt

    chosen_font = font_path or resolve_font_path()
    if chosen_font is None:
        raise RuntimeError("WordCloudフォントが見つかりません")

    normalized_text = normalize_text(text)
    words_wakachi = extract_nouns(normalized_text)
    if not words_wakachi.strip():
        raise ValueError("名詞が抽出できませんでした")

    word_cloud = WordCloud(
        font_path=chosen_font,
        width=1500,
        height=900,
        stopwords=STOP_WORDS,
        min_font_size=5,
        collocations=False,
        background_color="white",
        max_words=400,
    ).generate(words_wakachi)

    figure = plt.figure(figsize=(15, 10))
    plt.imshow(word_cloud, interpolation="bilinear")
    plt.axis("off")
    plt.tick_params(labelbottom=False, labelleft=False)
    plt.xticks([])
    plt.yticks([])

    buffer = io.BytesIO()
    figure.savefig(buffer, format="png", bbox_inches="tight")
    plt.close(figure)
    buffer.seek(0)
    return buffer


def generate_wordcloud_from_file(
    input_file_path: str, output_file_path: str = "Word_Cloud.png"
) -> None:
    with open(input_file_path, encoding="utf-8") as f:
        text = f.read().replace("\n", "").replace(" ", "")
    image_buffer = generate_wordcloud_image(text)
    with open(output_file_path, "wb") as f:
        f.write(image_buffer.getvalue())


def generate_sample_conversation_network_view(
    output_file_path: str = "sample_network.png",
) -> None:
    edges = {
        ("Alice", "Bob"): 5,
        ("Alice", "Charlie"): 3,
        ("Bob", "Charlie"): 2,
        ("Bob", "David"): 4,
        ("Charlie", "David"): 1,
        ("Alice", "David"): 2,
        ("Eve", "Alice"): 1,
    }
    with open(output_file_path, "wb") as f:
        f.write(generate_conversation_network(edges).getvalue())


def generate_conversation_network(edges: dict) -> io.BytesIO:

    font_path = resolve_font_path()
    font_prop = None

    if font_path:
        font_prop = fm.FontProperties(fname=font_path, size=64)

    G = nx.Graph()

    # ユーザー名 → ノードID
    node_map = {}
    labels = {}
    node_index = 0

    for (a, b), weight in edges.items():

        if weight < 2:
            continue

        if a not in node_map:
            node_map[a] = node_index
            labels[node_index] = a
            node_index += 1

        if b not in node_map:
            node_map[b] = node_index
            labels[node_index] = b
            node_index += 1

        G.add_edge(node_map[a], node_map[b], weight=weight)

    pos = nx.kamada_kawai_layout(G)

    plt.figure(figsize=(24, 24))

    weights = [G[u][v]["weight"] for u, v in G.edges()]

    nx.draw(
        G,
        pos,
        node_color="#A0CBE2",
        node_size=5000,
        width=[w * 0.8 for w in weights],
        with_labels=False,
    )

    texts = nx.draw_networkx_labels(
        G,
        pos,
        labels,
        font_size=128,
    )

    # 日本語フォント適用
    if font_prop:
        for t in texts.values():
            t.set_fontproperties(font_prop)

    buffer = io.BytesIO()

    plt.axis("off")
    plt.savefig(buffer, format="png", bbox_inches="tight")
    plt.close()

    buffer.seek(0)

    return buffer


if __name__ == "__main__":
    # generate_wordcloud_from_file("sample.txt")
    generate_sample_conversation_network_view()
