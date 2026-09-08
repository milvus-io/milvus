# Analyzer Char Filters

- **Feature DRI:** TBD
- **Primary Approver:** @zhengbuqian
- **Independent Approver:** TBD
- **Design Review:** TBD

## Motivation And Scope

Milvus custom analyzers currently follow Tantivy's tokenizer-then-token-filter
pipeline. They cannot normalize the complete input before tokenization, and a
token filter cannot provide equivalent behavior after token boundaries have
already been chosen.

This design adds an ordered pre-tokenization character-filter stage while
preserving offsets into the original input. Its intended final state is the
scope delivered by this change: custom analyzers support inline character
filters, the first implementation is `mapping`, and analyzers without character
filters retain their current behavior.

File-backed mappings, additional character-filter types, character filters on
built-in analyzer templates, and UTF-16 offsets are out of scope. This change
does not add a new public RPC or protobuf field.

## Summary

Custom analyzers can run character filters over the complete input before
tokenization. A character filter is a `String -> String` transformation: it
modifies the tokenizer's input string, and its output becomes the tokenizer's
next input directly. It does not consume or emit tokens. A wrapper tokenizer
owns the configured character filters and the user-selected Tantivy analyzer,
then corrects token offsets back to the original input.

The first supported character filter is `mapping`. It is a pre-tokenization
string rewriter backed by `source => target` rules. It scans the current input
from left to right, selects the longest matching source at each position,
appends its target, and advances past the source. If no rule matches, it copies
the next Unicode scalar unchanged. A target may replace, expand, contract, or
delete source text.

Replacement output is not scanned again by the same mapping character filter.
A later character filter processes the complete output of the previous filter.
Offset corrections preserve the relationship between rewritten text and the
original tokenizer input.

Mapping rules trim syntax whitespace on both sides of `=>`, matching
Elasticsearch. Whitespace that is part of a source or target must use an escape
such as `\u0020`. The parser supports `\\`, `\n`, `\t`, `\r`, `\b`, `\f`, and
`\uXXXX`; valid UTF-16 surrogate-pair escapes are combined into one Unicode
scalar before processing UTF-8 text. When a rule contains multiple raw `=>`
sequences, the last one is the separator. A literal arrow inside either side
must be escaped as `\=\>`, so it contains no raw separator. Duplicate sources
are rejected after trimming and unescaping.

## Public Interface And Validation

`char_filter` is an ordered array on a custom analyzer:

```json
{
  "char_filter": [
    {
      "type": "mapping",
      "mappings": ["& => and"]
    }
  ],
  "char_filter_offset_mode": "source_span",
  "tokenizer": "standard",
  "filter": ["lowercase"]
}
```

Character filters run in array order before the tokenizer. Token filters run
after the tokenizer as before. Built-in analyzer templates do not accept
character-filter options.

The configuration remains part of the existing analyzer JSON used by field
`analyzer_params`, analyzer validation, and `RunAnalyzer`. Collection creation
and schema changes use the existing validation path, which constructs the
analyzer on a query node. Invalid character-filter configuration is returned as
the existing invalid-parameter error; no new error code is introduced.

Validation rejects a non-array `char_filter`, non-object entries, missing or
unsupported `type`, missing or non-array `mappings`, non-string mapping entries,
empty or duplicate sources, malformed escapes, and unsupported offset modes.
`char_filter_offset_mode` requires `char_filter` to be present.

`char_filter_offset_mode` is optional:

- `source_span` is the default. Every token that overlaps replacement output is
  attributed to the complete source span of that replacement.
- `boundary` maps each replacement character boundary to a source character
  boundary. Expansions may therefore produce zero-length token offsets.

Both modes return UTF-8 byte offsets, matching Tantivy and Rust strings. Given
valid tokenizer offsets, boundary mode advances by Unicode scalar values through
`char_indices()`, so a returned offset never divides a UTF-8 encoded scalar
value. It is similar to
Lucene boundary correction but is not numerically compatible with
Elasticsearch, whose offsets use UTF-16 code units.

## Architecture

`CharFilterTokenizer` wraps the selected analyzer because Tantivy has no
pre-tokenization character-filter role. For each input it:

1. Builds a `FilteredText` from the original UTF-8 input.
2. Applies each character filter and composes its offset corrections.
3. Creates the inner token stream over the filtered text.
4. Lazily corrects each emitted token's start and end offsets.

The wrapper retains the filtered text for the lifetime of the inner stream. It
does not buffer emitted tokens.

Analyzer JSON continues to be stored with the field schema and consumed through
the existing analyzer construction paths for indexing, BM25 execution, and
`RunAnalyzer`. Character filters add no independent identifiers, metadata,
persistence, cache, WAL record, recovery procedure, lock, retry, or background
task. Each analyzer instance owns immutable filter configuration, and each token
stream owns the transformed text and offset corrections for one input.

## Offset Correction

Offset metadata is sparse and monotonic.

Source-span mode stores one tuple per replacement. For tuple `(a, b, c, d)`,
`a` is the filtered span start, `b` is its filtered byte length, `c` is the
source-minus-filtered length delta, and `d` is the cumulative delta before the
span. A token start inside the span maps to its source start; a token end inside
the span maps to its source end.

Boundary mode stores only filtered-to-source boundary corrections. Replacement
boundaries are generated from UTF-8 character boundaries rather than individual
bytes.

Correcting a token performs one binary search for its start. Because token spans
are normally short and correction points are ordered, the end correction scans
forward from that position. The cost is `O(log C + K)` for `C` correction
records and `K` records crossed by the token.

When filters are chained, existing corrections are composed into the next
filtered text. Source-span records are merged with a forward cursor. Boundary
replacement construction performs one initial binary search and then advances
through source corrections monotonically.

## Compatibility, Rollout, And Limits

- Existing analyzers without `char_filter` are unchanged.
- Old binaries reject the new configuration. Operators must upgrade all nodes
  that validate or execute analyzers before creating fields that use it.
- There is no feature flag or data migration. Before data is analyzed, rollback
  consists of removing the new options. After derived text or BM25 data has
  been produced, changing the analyzer alone would make index-time and
  query-time analysis inconsistent; affected derived data must be rebuilt.
- `source_span` avoids ambiguous or zero-length provenance for expanded text.
- `boundary` is available when consumers require character-boundary behavior.
- UTF-16 offsets are not supported. Adding them would require a coordinate
  contract across tokenizers, Rust/Cgo bindings, Go, and API consumers rather
  than a local character-filter change.
- Every tokenizer, including the gRPC tokenizer, owns the contract of returning
  ordered UTF-8 byte offsets for its input. The character-filter wrapper trusts
  those offsets and does not validate or repair tokenizer output.
- Mapping output size grows with replacement expansion. Inline mappings add no
  file access or other external I/O; existing input and configuration transport
  limits are unchanged.

No new metrics or logs are added. Invalid configurations use the existing
analyzer-validation response, and `RunAnalyzer` with detailed tokens exposes
the corrected offsets for troubleshooting.

## Open Decisions

- Decide whether mappings containing U+0000 are rejected or the Rust/C string
  boundary is made length-aware. The current C-string transport cannot carry a
  NUL safely.
- Define a transformed-output and correction-record limit, including the error
  returned when chained mappings exceed it.

Both decisions block Design Review approval.

## Alternatives

- Applying normalization in a token filter was rejected because tokenization
  has already fixed token boundaries and cannot represent whole-input rewrites.
- Preprocessing outside the analyzer was rejected because indexing and query
  paths could diverge and original-input offset correction would be lost.
- Buffering all emitted tokens and storing one correction per output byte were
  rejected in favor of lazy token streaming and sparse corrections.

## Verification

Rust unit and integration tests cover mapping, expansion, contraction, deletion,
UTF-8 input and replacement text, chained character filters, both offset modes,
configuration validation, and lazy token streaming. Cgo analyzer tests cover the
default source-span behavior through the Go boundary.

No design-review meeting conclusion has been recorded yet. The Feature DRI,
Independent Approver, and review date remain required before design approval.
