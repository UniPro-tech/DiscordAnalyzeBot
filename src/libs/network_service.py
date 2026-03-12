from collections import defaultdict
from datetime import timedelta
import io
from typing import Callable

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import networkx as nx

from libs.visualization_common import resolve_font_path
from libs.wordcloud_service import discord_utcnow


DEFAULT_MESSAGE_LIMIT = 5000


def build_network_message_query(
    guild_id: str,
    *,
    period_days: int | None = None,
    user_id: str | None = None,
    channel_id: str | None = None,
) -> dict:
    query = {"guild_id": guild_id}

    if period_days is not None:
        query["timestamp"] = {
            "$gte": (discord_utcnow() - timedelta(days=period_days)).isoformat()
        }

    if user_id is not None:
        query["user_id"] = user_id

    if channel_id is not None:
        query["channel_id"] = channel_id

    return query


def fetch_network_documents(
    db,
    guild_id: str,
    *,
    period_days: int | None = None,
    user_id: str | None = None,
    channel_id: str | None = None,
    limit: int = DEFAULT_MESSAGE_LIMIT,
) -> list[dict]:
    query = build_network_message_query(
        guild_id,
        period_days=period_days,
        user_id=user_id,
        channel_id=channel_id,
    )

    return list(
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


def normalize_network_documents(docs: list[dict]) -> tuple[list[dict], int]:
    valid_docs = []
    invalid_doc_count = 0

    for doc in docs:
        message_id = doc.get("message_id")
        author_id = doc.get("user_id")

        if message_id is None or author_id is None:
            invalid_doc_count += 1
            continue

        valid_docs.append(
            {
                "message_id": str(message_id),
                "user_id": str(author_id),
                "reply_to": str(doc["reply_to"]) if doc.get("reply_to") is not None else None,
                "mentions": [
                    str(mentioned)
                    for mentioned in doc.get("mentions", [])
                    if mentioned is not None
                ],
            }
        )

    return valid_docs, invalid_doc_count


def build_conversation_edges(docs: list[dict]) -> tuple[dict[tuple[str, str], int], int]:
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


def label_edges(
    edges: dict[tuple[str, str], int],
    resolve_name: Callable[[str], str],
) -> dict[tuple[str, str], int]:
    return {
        (resolve_name(a), resolve_name(b)): count for (a, b), count in edges.items()
    }


def generate_conversation_network(edges: dict[tuple[str, str], int]) -> io.BytesIO:
    if not edges:
        raise ValueError("会話エッジがありません")

    font_path = resolve_font_path()
    if font_path is None:
        raise RuntimeError("フォント無し")

    font_prop = fm.FontProperties(fname=font_path, size=64)
    graph = nx.Graph()
    node_map = {}
    labels = {}
    index = 0

    for (user_a, user_b), weight in edges.items():
        if weight < 2:
            continue

        if user_a not in node_map:
            node_map[user_a] = index
            labels[index] = user_a
            index += 1

        if user_b not in node_map:
            node_map[user_b] = index
            labels[index] = user_b
            index += 1

        graph.add_edge(node_map[user_a], node_map[user_b], weight=weight)

    if graph.number_of_edges() == 0:
        raise ValueError("表示条件を満たす会話エッジがありません")

    positions = nx.kamada_kawai_layout(graph)
    plt.figure(figsize=(24, 24))

    weights = [graph[node_u][node_v]["weight"] for node_u, node_v in graph.edges()]

    nx.draw(
        graph,
        positions,
        node_color="#A0CBE2",
        node_size=5000,
        width=[weight * 0.8 for weight in weights],
        with_labels=False,
    )

    texts = nx.draw_networkx_labels(
        graph,
        positions,
        labels,
        font_size=128,
    )

    for text in texts.values():
        text.set_fontproperties(font_prop)

    buffer = io.BytesIO()
    plt.axis("off")
    plt.savefig(buffer, format="png", bbox_inches="tight")
    plt.close()
    buffer.seek(0)

    return buffer