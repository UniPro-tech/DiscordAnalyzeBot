#!/usr/bin/env python3
"""Test the improved extract_tokens() function."""

import sys
sys.path.insert(0, '/Users/yutaka/Documents/Dev/UniProject/analyze-bot/src')

from libs.text_processing import normalize_text, extract_tokens

test_cases = [
    ("ミラノ風ドリア", ["ミラノ", "風", "ドリア"]),
    ("ナポリタン大好き", ["ナポリタン"]),  # "大好き" is not a noun/suffix >= 2 chars
    ("チーズオムレツ", ["チーズ", "オムレツ"]),
]

print("=" * 80)
print("Test: improved extract_tokens()")
print("=" * 80)

for text, expected_keywords in test_cases:
    normalized = normalize_text(text)
    tokens = extract_tokens(normalized)
    
    print(f"\nInput: 『{text}』")
    print(f"Normalized: 『{normalized}』")
    print(f"Tokens: {tokens}")
    print(f"Expected to contain: {expected_keywords}")
    
    for kw in expected_keywords:
        if kw in tokens:
            print(f"  ✓ {kw} found")
        else:
            print(f"  ✗ {kw} NOT found (FAIL)")

print("\n" + "=" * 80)
