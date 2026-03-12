#!/usr/bin/env python3
"""Debug script to examine Sudachi tokenization and POS tags."""

from sudachipy import dictionary, tokenizer

tokenizer_obj = dictionary.Dictionary().create()
MODE = tokenizer.Tokenizer.SplitMode.C

test_texts = [
    "ミラノ風ドリア",
    "ナポリタン",
    "オムレツ",
    "塩辛い",
    "風",
    "式",
    "的",
]

print("=" * 80)
print("Sudachi Tokenization Analysis")
print("=" * 80)

for text in test_texts:
    print(f"\nText: 『{text}』")
    print("-" * 60)
    tokens = tokenizer_obj.tokenize(text, MODE)
    
    for i, token in enumerate(tokens):
        surface = token.surface()
        pos = token.part_of_speech()
        print(f"  Token[{i}]: '{surface}'")
        print(f"    POS[0] (major): {pos[0]}")
        print(f"    POS[1] (minor): {pos[1]}")
        if len(pos) > 2:
            print(f"    POS[2]: {pos[2]}")
        print()

print("=" * 80)
