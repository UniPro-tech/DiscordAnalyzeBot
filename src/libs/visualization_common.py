import os

import matplotlib


matplotlib.use("Agg")

DEFAULT_FONT_PATHS = [
    "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/app/fonts/ipaexg.ttf",
]


def resolve_font_path() -> str | None:
    env_font = os.getenv("WORDCLOUD_FONT_PATH")

    if env_font and os.path.exists(env_font):
        return env_font

    for path in DEFAULT_FONT_PATHS:
        if os.path.exists(path):
            return path

    return None
