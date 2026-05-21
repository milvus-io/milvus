# MEP: Pinyin Filter for Text Analyzer

- **Created:** 2026-02-09
- **Author(s):** @aoiasd
- **Status:** Implemented
- **Component:** QueryNode/DataNode
- **Related Issues:** [#45811](https://github.com/milvus-io/milvus/issues/45811)
- **Released:** [TBD]

## Summary

Add a Pinyin filter to Milvus's tantivy-based text analyzer pipeline, enabling Chinese character tokens to be converted into their Pinyin (romanized) representations. This allows users to search Chinese text using Pinyin input, supporting common scenarios such as name lookup, autocomplete, and search-as-you-type for Chinese content.

## Motivation

Chinese text search in Milvus currently relies on tokenizers like Jieba for word segmentation, but there is no built-in way to search Chinese content using Pinyin input. This is a fundamental requirement for many Chinese-language applications:

- **Name search:** Users frequently search for people or places by typing Pinyin instead of Chinese characters (e.g., typing "zhangsan" to find "张三").
- **Autocomplete / search-as-you-type:** Input methods on most devices convert Pinyin keystrokes to Chinese characters, so supporting Pinyin matching enables faster, more natural search experiences.
- **Cross-input-method search:** Users may not have a Chinese input method available and need to search using Latin characters.

Without a Pinyin filter, users would need to maintain a separate Pinyin-mapped field or implement application-level conversion, adding complexity and overhead.

## Public Interfaces

The Pinyin filter is exposed as a new filter type `"pinyin"` in the analyzer configuration JSON. It can be used in any analyzer's filter pipeline.

**Analyzer configuration example:**

```json
{
  "tokenizer": "jieba",
  "filter": [
    {
      "type": "pinyin",
      "keep_original": true,
      "keep_full_pinyin": true,
      "keep_joined_full_pinyin": false,
      "keep_separate_first_letter": false
    }
  ]
}
```

**Filter parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `keep_original` | bool | `true` | Retain the original Chinese token in output |
| `keep_full_pinyin` | bool | `true` | Emit individual Pinyin for each Chinese character (e.g., "中文" → "zhong", "wen") |
| `keep_joined_full_pinyin` | bool | `false` | Emit concatenated Pinyin for the entire token (e.g., "中文" → "zhongwen") |
| `keep_separate_first_letter` | bool | `false` | Emit concatenated first letters of each character's Pinyin (e.g., "中文" → "zw") |

When used with no parameters (i.e., `"pinyin"` as a plain string filter), the default options apply.

## Design Details

### Implementation Location

The filter is implemented in the tantivy-binding Rust crate, which provides the text analysis infrastructure for Milvus's full-text search:

- `internal/core/thirdparty/tantivy/tantivy-binding/src/analyzer/filter/pinyin_filter.rs` — core filter implementation
- `internal/core/thirdparty/tantivy/tantivy-binding/src/analyzer/filter/filter.rs` — registration in the filter dispatch system

### Architecture

The Pinyin filter follows the same pattern as existing tantivy-binding filters (RegexFilter, SynonymFilter, etc.):

1. **`PinyinFilter`** — implements `tantivy::tokenizer::TokenFilter` trait, holds `PinyinOptions` configuration.
2. **`PinyinFilterWrapper<T>`** — wraps an inner tokenizer, produced by `TokenFilter::transform()`.
3. **`PinyinFilterStream<T>`** — the token stream that performs the actual conversion. Uses a cache-based approach to expand a single input token into multiple output tokens.

### Token Expansion Logic

For each incoming token from the upstream tokenizer:

1. If `keep_original` is true, the original token is preserved in output.
2. Each Chinese character in the token is converted to Pinyin using the `pinyin` crate (`ToPinyin` trait). Non-Chinese characters are skipped.
3. Based on the configured options:
   - `keep_full_pinyin`: Each character's Pinyin is emitted as a separate token.
   - `keep_joined_full_pinyin`: All characters' Pinyin are concatenated into a single token.
   - `keep_separate_first_letter`: The first letter of each character's Pinyin is concatenated into a single token.
4. All generated tokens share the same `offset_from`, `offset_to`, and `position` as the original token.

### Dependency

The filter uses the [`pinyin`](https://crates.io/crates/pinyin) Rust crate (version 0.10) for Chinese-to-Pinyin conversion. This crate provides accurate conversion including tone-less plain Pinyin and first-letter extraction.

### Filter Registration

The `"pinyin"` filter type is registered in the `SystemFilter` enum alongside existing filters. It supports both:
- **String shorthand:** `"pinyin"` (uses default options)
- **JSON object:** `{"type": "pinyin", ...}` (with custom parameters)

### Example Token Output

Input text: "中文测试" with Jieba tokenizer (segments into "中文" and "测试"):

| Configuration | Output tokens |
|---|---|
| `keep_original=true, keep_full_pinyin=true` | "中文", "zhong", "wen", "测试", "ce", "shi" |
| `keep_original=true, keep_joined_full_pinyin=true` | "中文", "zhongwen", "测试", "ceshi" |
| `keep_original=true, keep_separate_first_letter=true` | "中文", "zw", "测试", "cs" |
| All options enabled | "中文", "zhong", "wen", "zhongwen", "zw", "测试", "ce", "shi", "ceshi", "cs" |

## Compatibility, Deprecation, and Migration Plan

- **Backward compatible:** This is a purely additive change. No existing analyzer configurations are affected.
- **No migration needed:** Users opt in by adding the `"pinyin"` filter to their analyzer configuration.
- **Tantivy binding dependency:** Adds `pinyin = "0.10"` to the tantivy-binding Cargo.toml. This increases the compiled binary size marginally.

## Test Plan

Unit tests are included in `pinyin_filter.rs` covering three scenarios:

1. **Joined full Pinyin:** Verifies that `keep_joined_full_pinyin=true` produces concatenated Pinyin tokens ("zhongwen", "ceshi") for Jieba-segmented Chinese input.
2. **Full Pinyin per character:** Verifies that `keep_full_pinyin=true` produces individual Pinyin tokens ("zhong", "wen", "ce", "shi").
3. **First letter extraction:** Verifies that `keep_separate_first_letter=true` produces first-letter tokens ("zw", "cs").

All tests use the Jieba tokenizer as the upstream tokenizer and verify output using subset matching.

Integration/E2E tests should cover:
- Creating a collection with a VARCHAR field using an analyzer configured with the Pinyin filter.
- Inserting Chinese text and searching with Pinyin queries.
- Verifying that both original Chinese and Pinyin queries return the expected results.

## Rejected Alternatives

- **Application-level Pinyin conversion:** Requires users to maintain separate Pinyin fields or pre-process data, adding complexity and storage overhead. An in-pipeline filter is more ergonomic and efficient.
- **Standalone Pinyin tokenizer:** A filter-based approach is more composable — it can be combined with any tokenizer (Jieba, standard, etc.) and stacked with other filters (stop words, lowercase, etc.).

## References

- [`pinyin` Rust crate](https://crates.io/crates/pinyin) — Chinese-to-Pinyin conversion library
- Tantivy tokenizer pipeline: `internal/core/thirdparty/tantivy/tantivy-binding/src/analyzer/`
