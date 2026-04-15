from collections import defaultdict
import io
import math
from typing import Callable
import unicodedata
from datetime import datetime

from libs.visualization_common import resolve_font_path

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import networkx as nx

DEFAULT_MESSAGE_LIMIT = 5000
CANVAS_WIDTH_PX = 3200
CANVAS_HEIGHT_PX = 1800
CANVAS_DPI = 100


def build_network_message_query(
    guild_id: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    user_id: str | None = None,
    channel_id: str | None = None,
) -> dict:
    query = {"guild_id": guild_id}

    # timestampの範囲指定を構築
    timestamp_query = {}
    if start is not None:
        timestamp_query["$gte"] = start
    if end is not None:
        timestamp_query["$lte"] = end

    if timestamp_query:
        query["timestamp"] = timestamp_query

    if user_id is not None:
        query["user_id"] = user_id

    if channel_id is not None:
        query["channel_id"] = channel_id

    return query


def fetch_network_documents(
    db,
    guild_id: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    user_id: str | None = None,
    channel_id: str | None = None,
    limit: int = DEFAULT_MESSAGE_LIMIT,
) -> list[dict]:
    # build_network_message_queryの引数を修正
    query = build_network_message_query(
        guild_id,
        start=start,
        end=end,
        user_id=user_id,
        channel_id=channel_id,
    )

    docs = list(
        db.messages.find(
            query,
            {
                "message_id": 1,
                "user_id": 1,
                "reply_to": 1,
                "mentions": 1,
            },
        )
        .sort("timestamp", -1)
        .limit(limit)
    )

    existing_message_ids = {
        str(doc["message_id"]) for doc in docs if doc.get("message_id") is not None
    }
    missing_reply_target_ids = {
        str(doc["reply_to"])
        for doc in docs
        if doc.get("reply_to") is not None
        and str(doc["reply_to"]) not in existing_message_ids
    }

    if not missing_reply_target_ids:
        return docs

    reply_target_docs = db.messages.find(
        {
            "guild_id": guild_id,
            "message_id": {"$in": list(missing_reply_target_ids)},
        },
        {
            "message_id": 1,
            "user_id": 1,
        },
    )

    for reply_target_doc in reply_target_docs:
        message_id = reply_target_doc.get("message_id")

        if message_id is None:
            continue

        normalized_message_id = str(message_id)
        if normalized_message_id in existing_message_ids:
            continue

        docs.append(
            {
                "message_id": normalized_message_id,
                "user_id": reply_target_doc.get("user_id"),
                "reply_to": None,
                "mentions": [],
            }
        )
        existing_message_ids.add(normalized_message_id)

    return docs


def normalize_network_documents(docs: list[dict]) -> tuple[list[dict], int]:
    valid_docs = []
    invalid_doc_count = 0

    for doc in docs:
        message_id = doc.get("message_id")
        author_id = doc.get("user_id")

        if message_id is None or author_id is None:
            invalid_doc_count += 1
            continue

        mentions = doc.get("mentions", [])
        if mentions is None:
            mentions = []

        if not isinstance(mentions, list):
            invalid_doc_count += 1
            continue

        valid_docs.append(
            {
                "message_id": str(message_id),
                "user_id": str(author_id),
                "reply_to": str(doc["reply_to"])
                if doc.get("reply_to") is not None
                else None,
                "mentions": [
                    str(mentioned) for mentioned in mentions if mentioned is not None
                ],
            }
        )

    return valid_docs, invalid_doc_count


def build_conversation_edges(
    docs: list[dict],
) -> tuple[dict[tuple[str, str], int], int]:
    valid_docs, invalid_doc_count = normalize_network_documents(docs)

    if not valid_docs:
        return {}, invalid_doc_count

    msg_map = {doc["message_id"]: doc for doc in valid_docs}
    edges = defaultdict(int)

    for msg in valid_docs:
        author = msg.get("user_id")

        if author is None:
            continue

        reply_to = msg.get("reply_to")
        if reply_to and reply_to in msg_map:
            other = msg_map[reply_to].get("user_id")
            if other is not None and author != other:
                edges[tuple(sorted([author, other]))] += 1

        mentions = msg.get("mentions", [])
        if not isinstance(mentions, list):
            continue

        for mentioned in mentions:
            if mentioned != author:
                edges[tuple(sorted([author, mentioned]))] += 1

    return dict(edges), invalid_doc_count


def build_node_labels(
    edges: dict[tuple[str, str], int],
    resolve_name: Callable[[str], str],
) -> dict[str, str]:
    labels = {}

    for user_a, user_b in edges:
        labels[user_a] = resolve_name(user_a)
        labels[user_b] = resolve_name(user_b)

    return labels


def calculate_label_display_width(label: str) -> int:
    width = 0

    for char in str(label):
        width += 2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1

    return max(1, width)


def summarize_label_metrics(label_texts: list[str] | None) -> tuple[int, float]:
    if not label_texts:
        return 1, 1.0

    widths = [calculate_label_display_width(text) for text in label_texts]
    return max(widths), sum(widths) / len(widths)


def calculate_label_font_size(
    node_count: int,
    label_texts: list[str] | None = None,
    *,
    canvas_width_px: int = CANVAS_WIDTH_PX,
    canvas_height_px: int = CANVAS_HEIGHT_PX,
) -> int:
    if node_count <= 0:
        return 128

    max_label_width, average_label_width = summarize_label_metrics(label_texts)
    base_size = min(canvas_width_px, canvas_height_px) * 0.1
    density_factor = math.sqrt(max(node_count, 1))
    length_factor = max(1.0, math.sqrt(max_label_width / 8), average_label_width / 10)

    return max(24, min(140, int(base_size / density_factor / length_factor)))


def calculate_node_size(
    node_count: int,
    label_texts: list[str] | None = None,
    *,
    canvas_width_px: int = CANVAS_WIDTH_PX,
    canvas_height_px: int = CANVAS_HEIGHT_PX,
) -> int:
    label_font_size = calculate_label_font_size(
        node_count,
        label_texts,
        canvas_width_px=canvas_width_px,
        canvas_height_px=canvas_height_px,
    )
    max_label_width, average_label_width = summarize_label_metrics(label_texts)
    label_width_factor = max(
        4.0, min(18.0, (max_label_width + average_label_width) / 2)
    )

    return max(1800, min(14000, int(label_font_size * label_width_factor * 14)))


def calculate_layout_spacing(
    node_count: int,
    label_texts: list[str] | None = None,
    *,
    canvas_width_px: int = CANVAS_WIDTH_PX,
    canvas_height_px: int = CANVAS_HEIGHT_PX,
) -> float:
    max_label_width, average_label_width = summarize_label_metrics(label_texts)
    aspect_ratio = canvas_width_px / canvas_height_px
    base_k = 1 / math.sqrt(max(node_count, 1))
    spacing_multiplier = (
        2.2
        + min(max_label_width / 10, 1.8)
        + min(average_label_width / 14, 1.0)
        + min(node_count / 24, 1.0)
    )

    return base_k * spacing_multiplier * max(1.0, aspect_ratio / 1.4)


def calculate_layout_iterations(node_count: int) -> int:
    return max(120, min(400, 80 + node_count * 10))


def normalize_layout_positions(
    positions: dict[int, tuple[float, float]],
    *,
    font_size: int,
    max_label_width: int,
    canvas_width_px: int = CANVAS_WIDTH_PX,
    canvas_height_px: int = CANVAS_HEIGHT_PX,
) -> dict[int, tuple[float, float]]:
    if not positions:
        return positions

    x_values = [position[0] for position in positions.values()]
    y_values = [position[1] for position in positions.values()]
    x_min, x_max = min(x_values), max(x_values)
    y_min, y_max = min(y_values), max(y_values)

    padding_x = min(
        0.18, max(0.04, (font_size * max_label_width) / (canvas_width_px * 1.6))
    )
    padding_y = min(0.16, max(0.04, (font_size * 1.8) / canvas_height_px))

    normalized_positions = {}
    for node, (x_value, y_value) in positions.items():
        normalized_x = 0.5 if x_max == x_min else (x_value - x_min) / (x_max - x_min)
        normalized_y = 0.5 if y_max == y_min else (y_value - y_min) / (y_max - y_min)
        normalized_positions[node] = (
            padding_x + normalized_x * (1 - padding_x * 2),
            padding_y + normalized_y * (1 - padding_y * 2),
        )

    return normalized_positions


def calculate_edge_widths(
    weights: list[int], node_count: int, label_font_size: int
) -> list[float]:
    density_factor = max(0.7, 1.2 - math.log2(node_count + 1) * 0.12)
    return [
        max(1.5, min(18.0, math.sqrt(weight) * (label_font_size / 18) * density_factor))
        for weight in weights
    ]


def generate_conversation_network(
    edges: dict[tuple[str, str], int],
    labels: dict[str, str] | None = None,
) -> io.BytesIO:
    if not edges:
        raise ValueError("会話エッジがありません")

    font_path = resolve_font_path()
    if font_path is None:
        raise RuntimeError("フォント無し")

    font_prop = fm.FontProperties(fname=font_path)
    graph = nx.Graph()
    node_map = {}
    display_labels = {}
    index = 0

    for (user_a, user_b), weight in edges.items():
        if weight < 2:
            continue

        if user_a not in node_map:
            node_map[user_a] = index
            label_text = labels.get(user_a, user_a) if labels else user_a
            display_labels[index] = label_text
            index += 1

        if user_b not in node_map:
            node_map[user_b] = index
            label_text = labels.get(user_b, user_b) if labels else user_b
            display_labels[index] = label_text
            index += 1

        graph.add_edge(node_map[user_a], node_map[user_b], weight=weight)

    if graph.number_of_edges() == 0:
        raise ValueError("表示条件を満たす会話エッジがありません")

    label_texts = list(display_labels.values())
    max_label_width, _ = summarize_label_metrics(label_texts)
    label_font_size = calculate_label_font_size(graph.number_of_nodes(), label_texts)
    node_size = calculate_node_size(graph.number_of_nodes(), label_texts)
    raw_positions = nx.spring_layout(
        graph,
        k=calculate_layout_spacing(graph.number_of_nodes(), label_texts),
        iterations=calculate_layout_iterations(graph.number_of_nodes()),
        seed=42,
        weight="weight",
    )
    positions = normalize_layout_positions(
        raw_positions,
        font_size=label_font_size,
        max_label_width=max_label_width,
    )
    figure, ax = plt.subplots(
        figsize=(CANVAS_WIDTH_PX / CANVAS_DPI, CANVAS_HEIGHT_PX / CANVAS_DPI),
        dpi=CANVAS_DPI,
    )
    buffer = io.BytesIO()

    try:
        weights = [graph[node_u][node_v]["weight"] for node_u, node_v in graph.edges()]
        edge_widths = calculate_edge_widths(
            weights, graph.number_of_nodes(), label_font_size
        )
        figure.patch.set_facecolor("#F8FAFC")
        ax.set_facecolor("#F8FAFC")

        nx.draw(
            graph,
            positions,
            node_color="#8ECAE6",
            edge_color="#94A3B8",
            node_size=node_size,
            width=edge_widths,
            linewidths=max(1.5, label_font_size / 36),
            edgecolors="#E2E8F0",
            with_labels=False,
            ax=ax,
        )

        texts = nx.draw_networkx_labels(
            graph,
            positions,
            display_labels,
            font_size=label_font_size,
            ax=ax,
        )

        for text in texts.values():
            text.set_fontproperties(font_prop)
            text.set_fontsize(label_font_size)
            text.set_color("#0F172A")

        ax.set_axis_off()
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_position([0.0, 0.0, 1.0, 1.0])
        ax.margins(0)
        figure.savefig(buffer, format="png", bbox_inches=None, pad_inches=0)
        buffer.seek(0)
    finally:
        plt.close(figure)

    return buffer
