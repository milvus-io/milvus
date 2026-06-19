# Arabic and Thai Analyzer Support

## Summary

Add built-in Arabic and Thai text analyzers to Milvus's tantivy-binding layer, enabling native full-text search for Arabic and Thai languages. This includes two new tokenizers/analyzers, two new token filters, and language-specific stop word lists.

## Motivation

Milvus currently supports full-text search for English, Chinese (Jieba), and a set of European languages via the standard/ICU tokenizer pipeline. Arabic and Thai have unique linguistic characteristics that require dedicated processing:

- **Arabic**: Right-to-left script with diacritical marks (harakat), letter-form variations (hamza variants, teh marbuta, alef maksura), decorative stretching (tatweel/kashida), and its own digit system (Arabic-Indic numerals ٠-٩).
- **Thai**: No whitespace between words — word boundaries must be determined by a segmentation model (LSTM-based ICU4X WordSegmenter).

Without dedicated support, Arabic text search produces poor recall (diacritics and letter variants cause mismatches), and Thai text cannot be tokenized at all by whitespace-based tokenizers.

## Design

### Architecture Overview

Both analyzers follow the existing pattern in tantivy-binding: a **tokenizer** splits text into tokens, then a chain of **filters** normalizes them.

```
Arabic:  StandardTokenizer → LowerCaser → DecimalDigitFilter → ArabicNormalizationFilter → Stemmer(Arabic) → StopWordFilter
Thai:    ThaiTokenizer      → LowerCaser → DecimalDigitFilter → StopWordFilter
```

### New Components

#### 1. ThaiTokenizer (`thai_tokenizer.rs`)

- Uses **ICU4X `WordSegmenter::try_new_lstm()`** for LSTM-based Thai word segmentation.
- Filters out non-word segments (whitespace, punctuation) — only tokens where `is_alphanumeric()` is true are emitted.
- **Position scheme**: Character-based (Unicode scalar value count from input start), including skipped segments. This is consistent with `IcuTokenizer` and `JiebaTokenizer`.
  - `position`: cumulative character offset from input start (counts characters in skipped segments too).
  - `position_length`: character count of the current token segment.
  - `offset_from` / `offset_to`: byte offsets into the original text.
- Available as both a standalone tokenizer (`"tokenizer": "thai"`) and a built-in analyzer (`"type": "thai"`).

#### 2. ArabicNormalizationFilter (`arabic_normalization_filter.rs`)

Implements Lucene-compatible Arabic normalization:

| Transformation | From | To |
|---|---|---|
| Hamza + Alef variants | آ أ إ (U+0622, U+0623, U+0625) | ا (U+0627, bare Alef) |
| Teh Marbuta | ة (U+0629) | ه (U+0647, Heh) |
| Alef Maksura | ى (U+0649) | ي (U+064A, Yeh) |
| Harakat (diacritics) | U+064B..U+065F | removed |
| Tatweel (kashida) | ـ (U+0640) | removed |

Only runs the normalization pass when at least one normalizable character is detected (fast-path check).

Available as a standalone filter: `"filter": ["arabic_normalization"]`.

#### 3. DecimalDigitFilter (`decimal_digit_filter.rs`)

Converts non-ASCII Unicode decimal digits (General Category Nd) to ASCII 0-9. Covers 34 digit systems including Arabic-Indic (٠-٩), Thai (๐-๙), Devanagari, Bengali, Fullwidth, etc.

Uses a lookup table of known "zero" code points — since Unicode guarantees digits 0-9 are contiguous within each block, `ascii_value = '0' + (codepoint - block_zero)`.

Available as a standalone filter: `"filter": ["decimaldigit"]`.

#### 4. Stop Word Lists

- **Arabic** (`arabic.txt`): 119 stop words sourced from Apache Lucene (BSD license, Jacques Savoy).
- **Thai** (`thai.txt`): 115 stop words sourced from Apache Lucene.

Both are registered in the stop word system and accessible via `"_arabic_"` / `"_thai_"` language identifiers.

### Usage

**Built-in analyzer** (recommended):

```json
{"type": "arabic"}
{"type": "arabic", "stop_words": ["custom1", "custom2"]}

{"type": "thai"}
{"type": "thai", "stop_words": ["custom1", "custom2"]}
```

**Custom pipeline**:

```json
{
  "tokenizer": "standard",
  "filter": ["lowercase", "arabic_normalization", "decimaldigit"]
}

{
  "tokenizer": "thai",
  "filter": ["lowercase", "decimaldigit"]
}
```

### Dependencies

- **icu_segmenter** (ICU4X): Already used by the existing `IcuTokenizer`. The `ThaiTokenizer` uses the same crate with `try_new_lstm()` (LSTM model) instead of `try_new_auto()` (dictionary model), keeping it focused on Thai without pulling in CJK dictionary data.

### Position Semantics (ThaiTokenizer)

The position field uses **character-based absolute positioning** — each token's position equals the cumulative Unicode scalar count from the start of the input, counting characters in all segments (including skipped whitespace/punctuation).

Example: `"สวัสดี ครับ"` (6 Thai chars + 1 space + 3 Thai chars)
- Token "สวัสดี": position=0, position_length=6
- Token "ครับ": position=7, position_length=3

This matches the behavior of `IcuTokenizer` and `JiebaTokenizer`, ensuring consistent phrase query and proximity query semantics across all non-Latin tokenizers.

## Test Plan

- Unit tests for `ThaiTokenizer`: basic Thai segmentation, mixed Thai/English/CJK input, punctuation filtering, character-based position verification.
- Unit tests for `ArabicNormalizationFilter`: hamza normalization, teh marbuta → heh, harakat removal, tatweel removal.
- Unit tests for `DecimalDigitFilter`: Arabic-Indic and Thai digit conversion, ASCII passthrough.
- Integration tests for built-in `arabic` and `thai` analyzers: end-to-end tokenization with stop words, custom stop words, digit conversion.
