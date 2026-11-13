# MEP: Add Czech Stemmer Support

- **Created:** 2026-06-22
- **Author(s):** @faileon
- **Status:** Blocked on #54100 (compatibility of existing indexes)
- **Component:** Index
- **Related Issues:** #53696
- **Released:** N/A

## Summary

Add Czech to the set of predefined stemmer languages supported by the Milvus
text analyzer. Users can now configure `{"type":"stemmer","language":"czech"}`
in their `analyzer_params` filter chain, joining the 18 existing languages
(arabic, danish, dutch, english, finnish, french, german, greek, hungarian,
italian, norwegian, portuguese, romanian, russian, spanish, swedish, tamil,
turkish).

Czech requires Snowball v3.1.1 (Czech was introduced in v3.1.0 and refined in
v3.1.1). The stemmer filter is owned by Milvus and depends on
`rust_stemmers::Algorithm` directly, independently of Tantivy (#54100):

```
stemmer_filter.rs (milvus)  →  rust_stemmers::Algorithm (milvus-io/rust-stemmers)
  →  Snowball-generated .rs
```

## Motivation

Milvus full-text search supports a configurable stemmer filter so that
morphological variants of a word (e.g. *play / plays / played / playing*) map to
a common stem, improving recall. The set of supported languages is defined by
the Snowball project's algorithm catalog.

The Snowball project introduced a Czech stemmer algorithm in v3.1.0 and refined
it in v3.1.1. Czech-speaking users of Milvus full-text search currently have no
way to apply Czech stemming, forcing them to either skip stemming (hurting
recall) or fall back on a non-Czech stemmer (producing incorrect stems). Adding
Czech closes this gap. Because it requires a newer Snowball, the dependency bump
also changes stems for some existing languages; see Compatibility.

## Public Interfaces

### Analyzer `language` parameter

The `"language"` field of the `{"type":"stemmer","language":"<lang>"}` filter
object gains one accepted value:

| Value | Behavior |
|-------|----------|
| `czech` | Applies the Snowball Czech stemmer. Case-insensitive (matched after `to_lowercase()`). |

Example `analyzer_params`:

```json
{
  "tokenizer": "standard",
  "filter": [
    { "type": "lowercase" },
    { "type": "stemmer", "language": "czech" }
  ]
}
```

No protobuf, schema, SDK, REST, metrics, or YAML config changes are required.
The stemmer language is carried as an opaque JSON string inside `analyzer_params`
on a VARCHAR `FieldSchema`; this design adds one accepted string value to the
existing validation `match` in `stemmer_filter.rs`.

### Dependency updates

- The `rust-stemmers` pin in `tantivy-binding/Cargo.toml` (`milvus-io/rust-stemmers`
  fork) moves from `3b25452` (1.2.0, Snowball 3.0.0) to `ac4aee9` (1.3.0,
  Snowball 3.1.1), which exposes `Algorithm::Czech`.
- Tantivy is not touched. `ac4aee9` is already in `Cargo.lock` as the
  `rust-stemmers` used by `tantivy` 0.23, so the lock change only drops the
  now-unused 1.2.0 `?rev=3b25452` entry.

## Design Details

### Change chain

#### `milvus-io/rust-stemmers`

Already done upstream: `ac4aee9` regenerates all bundled Snowball modules from
v3.1.1 and adds `Algorithm::Czech` (milvus-io/rust-stemmers#2, #3). The
generated modules use the `rust-stemmers` runtime (`SnowballEnv` / `Among`).

#### `milvus-io/milvus` (this repository)

`internal/core/thirdparty/tantivy/tantivy-binding/src/analyzer/filter/stemmer_filter.rs`
defines `StemmerLanguageParser`, which maps the JSON `"language"` string to a
`rust_stemmers::Algorithm`.

1. Add `"czech" => Ok(Algorithm::Czech),` to the match.
2. Bump the `rust-stemmers` `rev` in `Cargo.toml` to `ac4aee9` and refresh
   `Cargo.lock` with `cargo update -p rust-stemmers`.
3. Add unit tests for the `czech` mapping, case-insensitivity, and the
   unsupported-language error path.

### Position in the analyzer pipeline

The stemmer filter runs in the position specified by the user in the `"filter"`
array of `analyzer_params`, exactly like the existing 18 languages. Tokens are
expected to be lowercased before reaching the stemmer (the Snowball Czech
algorithm, like all Snowball algorithms, assumes lowercase input). No change to
filter ordering semantics is introduced.

### Built-in analyzers

No new built-in analyzer template is added. Czech is reachable only through the
custom analyzer `"filter"` path (the built-in `"english"` and `"arabic"`
analyzers remain the only templates that hard-wire a stemmer). A future
enhancement could add a built-in Czech analyzer with Czech stop words; that is
out of scope for this change.

## Compatibility, Deprecation, and Migration Plan

**Not purely additive.** Snowball 3.0.0 → 3.1.1 changes the output of some
existing stemmers (e.g. English `internal` stays `internal` instead of
`intern`). Stems are persisted in text-match indexes and BM25 sparse vectors,
so after the bump, queries against data indexed with the old stemmer would no
longer match the stored terms. The guard tests on master,
`test_legacy_english_stems` and `test_existing_english_index`, fail with this
change for exactly that reason.

This change therefore depends on the compatibility work tracked in #54100
(keep the old stemmer output for existing indexes / BM25 functions, use the new
version only for new ones). It must not merge before that lands; once it does,
Czech should be wired as a new-version-only language and the guard tests should
pass unchanged.

The `"czech"` value itself is additive: no existing `analyzer_params` becomes
invalid, and there are no schema, proto, or on-disk format changes.

## Test Plan

### Unit tests (Rust, this repository)

New tests in the existing `#[cfg(test)]` module of `stemmer_filter.rs`:

- `test_czech_language` — `"czech".into_language()` returns `Ok(Algorithm::Czech)`.
- `test_czech_language_case_insensitive` — `"Czech"` and `"CZECH"` resolve to
  `Ok(Algorithm::Czech)`.
- `test_unsupported_language` — `"klingon".into_language()` is an error.

The existing guard tests `test_legacy_english_stems` and
`test_existing_english_index` are left unchanged; they currently fail and are
expected to pass once #54100 compatibility handling is in place.

### Upstream unit tests

- `rust-stemmers`: `czech_test` validates the generated Czech module against
  `snowballstem/snowball-data` Czech voc/output pairs; the full suite validates
  the 3.1.1 regeneration of the other languages.

### Integration test (Python)

`tests/python_client/testcases/test_query.py` gains
`test_query_text_match_custom_analyzer_with_czech_stemmer_filter`, mirroring the
existing `test_query_text_match_custom_analyzer_with_stemmer_filter` but driving
Czech text through a `{"type": "stemmer", "language": "czech"}` filter and
asserting that an inflected query form matches documents containing other
inflections of the same lemma. This is the only end-to-end coverage that
exercises the full Go → cgo → Rust → Snowball path, and it runs in Milvus CI
against a live cluster, which is where it is executed for the first time — it
cannot be run locally without a cluster built from this branch.

## Rejected Alternatives

- **Hand-write the Czech stemmer in Rust.** Rejected: the `rust-stemmers` project
  (and Milvus's fork) intentionally ships Snowball-compiler-generated modules
  rather than hand-written algorithms, to stay faithful to the upstream Snowball
  reference implementation and to make upgrades mechanical. Hand-writing would
  diverge from the reference and create a maintenance burden.
- **Add Czech stop words alongside the stemmer.** Rejected for this PR: stop
  words are a separate filter (`stop_words.rs`) with a separate language list
  that does not align 1:1 with the stemmer list. Bundling Czech stop words would
  expand scope and require sourcing/maintaining a Czech stop-word list. It can
  be done in a follow-up.
- **Ship Czech through a separate Czech-only dependency** to avoid touching
  existing stemmers. Rejected: it duplicates the Snowball runtime and diverges
  from the single pinned `rust-stemmers` that #54100 introduced.
- **Expose the other algorithms that `rust-stemmers` 1.3.0 now ships**
  (Armenian, Basque, Catalan, Polish, …). Rejected: out of scope; each is one
  more `Algorithm` arm and can follow separately.
- **Add a built-in Czech analyzer template.** Rejected: built-in templates
  hard-wire a full pipeline (tokenizer + filters + stop words). This PR only
  adds the stemmer language to the custom-analyzer filter path, which is the
  minimal user-facing change.

## References

- Snowball project: https://snowballstem.org/
- Snowball v3.1.1 release tag: `snowballstem/snowball@v3.1.1`
- Snowball Czech algorithm source: `snowballstem/snowball` `algorithms/czech.sbl`
  (introduced v3.1.0, sha `8f02da6`)
- Snowball test data: `snowballstem/snowball-data` `czech/`
- `milvus-io/rust-stemmers` fork (PR#1 set the precedent for Snowball-version
  regeneration)
- Related Milvus design doc: `docs/design-docs/design_docs/20260403-arabic-thai-analyzer.md`
- Upstream PR (rust-stemmers, Czech algorithm):
  https://github.com/milvus-io/rust-stemmers/pull/2
- Upstream PR (rust-stemmers, Snowball 3.1.1 sync and full algorithm set):
  https://github.com/milvus-io/rust-stemmers/pull/3
- Decouple Milvus stemmer version from Tantivy: #54100 (#54096, #54097, #54098)
