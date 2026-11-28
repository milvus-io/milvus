# MEP: Standalone punctuation removal

- **Created:** 2026-11-20
- **Author(s):** Codex
- **Status:** Draft
- **Component:** Index
- **Related Issues:** #55521

## Summary

Add an opt-in `standalone` mode to the `removepunct` analyzer filter. Keep the
existing substring predicate as the default.

## Motivation

The existing filter drops a whole token when any substring matches punctuation
or whitespace. ICU preserves contractions, decimal quantities, abbreviations
and domains as tokens, so `don't`, `3.5kg`, `u.s.a`, `modash.io` and `b.com`
are lost. Users need to remove punctuation-only and whitespace-only tokens
without losing these words.

## Public Interfaces

```json
{
  "tokenizer": "icu",
  "filter": [{"type": "removepunct", "mode": "standalone"}, "lowercase"]
}
```

The string `"removepunct"`, the object `{"type":"removepunct"}`, and an object
with `"mode":"contains"` all use the legacy predicate `[\p{Punct}\s]+`.
`standalone` uses `\A[\p{Punct}\s]+\z`: remove only nonempty tokens consisting
entirely of Unicode punctuation or whitespace. Symbols such as emoji remain.
Unknown modes and non-string values, including explicit null, are invalid
analyzer arguments.

## Design Details

The JSON filter factory constructs a mode-aware `RemovePunctFilter`. Both modes
use lazily compiled static expressions and the existing `FixedRegex` matcher,
which owns a separate search cache for each analyzer and clone. Only the token
selection predicate changes; retained token text, offsets, positions and position
lengths pass through unchanged. The same factory serves analyzer validation,
indexing, query analysis and the v5 analyzer adapter.

Analyzer parameters continue to use their existing JSON representation. This
change introduces no metadata schema, storage layout, external resource or
additional state-changing operation. It requires no new replication message.
It does not repair ICU's character-based phrase positions (issue #55776).

## Compatibility, Deprecation, and Migration Plan

Existing collections keep their current tokenization. The default must not
change, because query analysis must agree with already indexed tokens. Use the
new mode when creating an analyzer for a new collection; rebuilding existing
indexed data is necessary when adopting different tokenization. Older binaries
do not support the new object form, so all consuming nodes must support the
option before it is used. No persisted index format changes are required.

## Test Plan

Reproduce the five missing words through the JSON analyzer entrypoint with the
reported ICU input, then verify the standalone output, reuse and cloning. For
versions without configurable removepunct, the regression falls back only on the
specific unsupported-filter error to expose the original token loss.

Compare all retained token fields against independently filtered upstream tokens
for ICU, jieba, standard and whitespace tokenizers. Exercise legacy/default and
explicit modes, serialization, validation, invalid values, Unicode punctuation
and whitespace, empty input, word-edge punctuation and mixed tokens. Check the
v5 adapter preserves every token field and raw tokens enforce whole-token
matching, including trailing newlines. Run the existing fixed-regex integration
suite and analyzer unit tests.

## Rejected Alternatives

Changing the default breaks existing indexes. A regex keep-filter remains a
workaround, but requires users to understand the legacy removal predicate.
Changing ICU token positions is a separate tokenizer issue.

## References

- https://github.com/milvus-io/milvus/issues/55521
- https://github.com/milvus-io/milvus/issues/55776
- The public analyzer guide lives in milvus-docs; its description was corrected
  separately by https://github.com/milvus-io/milvus-docs/pull/3856.
