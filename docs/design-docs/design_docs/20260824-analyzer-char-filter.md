# Analyzer Char Filters

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
scalar before processing UTF-8 text. When a rule contains multiple unescaped
`=>` sequences, the last one is the separator. Duplicate sources are rejected
after trimming and unescaping.

## Configuration

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

## Compatibility And Limits

- Existing analyzers without `char_filter` are unchanged.
- `source_span` avoids ambiguous or zero-length provenance for expanded text.
- `boundary` is available when consumers require character-boundary behavior.
- UTF-16 offsets are not supported. Adding them would require a coordinate
  contract across tokenizers, Rust/Cgo bindings, Go, and API consumers rather
  than a local character-filter change.
- A gRPC tokenizer used with character filters must return ordered UTF-8 byte
  offsets for the filtered input. Contract validation is future work.

## Verification

Rust unit and integration tests cover mapping, expansion, contraction, deletion,
UTF-8 input and replacement text, chained character filters, both offset modes,
configuration validation, and lazy token streaming. Cgo analyzer tests cover the
default source-span behavior through the Go boundary.
