# MEP: Add Czech Stemmer Support

- **Created:** 2026-06-22
- **Author(s):** @faileon
- **Status:** Under Review
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

This requires regenerating the bundled Snowball algorithm modules to Snowball
v3.1.1 (Czech was introduced in v3.1.0 and refined in v3.1.1) and wiring the
new language through the three-layer stemmer chain:

```
stemmer_filter.rs (milvus)  →  tantivy::tokenizer::Language (zilliztech/tantivy)
  →  rust_stemmers::Algorithm (milvus-io/rust-stemmers)  →  Snowball-generated .rs
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
Czech closes this gap and is purely additive — no existing language or behavior
changes for users who do not opt in.

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

- `rust-stemmers` (via the `milvus-io/rust-stemmers` fork) is updated to a
  version that regenerates all bundled Snowball algorithm modules from Snowball
  v3.1.1 and exposes `Algorithm::Czech`.
- `tantivy` (via the `zilliztech/tantivy` fork) is updated to a version that
  exposes `Language::Czech` and maps it to `rust_stemmers::Algorithm::Czech`.

Note on breadth: the updated `rust-stemmers` exposes the full set of ~36
standard Snowball algorithms, while `tantivy::tokenizer::Language` surfaces 19
of them. This design deliberately widens only the Czech path; surfacing the
remaining algorithms would require further `Language` variants upstream in
`zilliztech/tantivy` and is left as a separate enhancement.

## Design Details

### Three-repo change chain

The stemmer is layered across three repositories. Czech must be added at each
layer; no single repo suffices on its own.

#### Layer 1 — `milvus-io/rust-stemmers`

This fork ships Snowball algorithm modules **pre-compiled to Rust** by the
Snowball compiler (the `.sbl` → `.rs` generator). The procedure mirrors the
existing PR#1 ("Update snowball code by snowball 3.0.0"):

1. Build the Snowball compiler from `snowballstem/snowball` at tag `v3.1.1`.
2. Regenerate every algorithm module that maps to an `Algorithm` enum variant
   (the 19 existing languages) plus the new Czech module:
   `./snowball algorithms/<lang>.sbl -rust -o <path>/<lang>_stemmer.rs`.
3. Add `algorithms/czech.sbl` (source from Snowball v3.1.1).
4. Add `src/snowball/algorithms/czech_stemmer.rs` (generated output).
5. Declare `pub mod czech_stemmer;` in `src/snowball/algorithms/mod.rs`.
6. Add `Czech` to the `Algorithm` enum and the `Stemmer::create` match in
   `src/lib.rs`.
7. Add Czech test data (`test_data/voc_cs.txt`, `test_data/res_cs.txt`) from
   `snowballstem/snowball-data` and a `czech_test` mirroring the existing
   per-language tests.
8. Bump crate version (minor) and update the README "Supported Algorithms" list.

The generated modules use the `rust-stemmers` runtime (`SnowballEnv` / `Among`),
not the Snowball project's own Rust runtime. The Snowball v3.1.1 compiler output
is compatible with this runtime (verified: all `SnowballEnv` methods and fields
referenced by the generated code exist in the runtime).

#### Layer 2 — `zilliztech/tantivy`

`src/tokenizer/stemmer.rs` defines the `Language` enum and an exhaustive
`algorithm()` match mapping each `Language` to a `rust_stemmers::Algorithm`.

1. Add `Czech,` to the `Language` enum.
2. Add `Czech => Algorithm::Czech,` to the `algorithm()` match (this is the only
   exhaustive match on `Language` with no catch-all, so it is the only
   compile-critical edit; `stop_word_filter` has a `_ => None` catch-all and
   needs no change).
3. Point the `rust-stemmers` git dependency at the new commit.
4. Add a `#[cfg(test)]` module verifying `Stemmer::new(Language::Czech)` stems
   known Czech word pairs.

#### Layer 3 — `milvus-io/milvus` (this repository)

`internal/core/thirdparty/tantivy/tantivy-binding/src/analyzer/filter/stemmer_filter.rs`
defines the user-facing `StemmerLanguageParser` match that maps the JSON
`"language"` string to a `tantivy::tokenizer::Language`.

1. Add `"czech" => Ok(Language::Czech),` to the match (alphabetical, after the
   `arabic` / `arabig` block, before `danish`).
2. Refresh the `tantivy` git dependency to the new `zilliztech/tantivy` commit
   via `cargo update -p tantivy@0.23.0`. `Cargo.toml` declares the dependency
   without a `rev`, so only `Cargo.lock` changes.
3. Add a `#[cfg(test)]` module (none previously existed — a pre-existing test
   gap) covering: the `czech` mapping, case-insensitivity, and the
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

**Compatibility:** Purely additive. The new `"czech"` value is accepted by
validation; all previously accepted values behave identically. No existing
`analyzer_params` configuration becomes invalid. No schema, proto, or on-disk
format change. No upgrade or rollback impact — existing segments and indexes are
unaffected because analyzer params are resolved at query/index build time from
the field schema.

**Snowball v3.0.0 → v3.1.1 regeneration:** Regenerating the 19 existing
algorithm modules from Snowball v3.1.1 may pick up minor algorithm refinements
upstream. The Snowball project treats these as bug-fix-level changes within the
v3.x series. Existing per-language tests in `rust-stemmers` (which use curated
test-data subsets) are expected to continue passing; if any need updating, the
curated subset should be refreshed from `snowballstem/snowball-data` for the
affected language only. This is the same risk accepted by PR#1's 3.0.0
regeneration.

The regeneration reaches **both** index paths, not only the current one. The
binding links two tantivy versions: `tantivy` 0.23 (current) and `tantivy-5`
(0.21.1-fix4, the legacy V5 index writer). `tantivy` 0.23 pins `rust-stemmers`
by `rev`, whereas `tantivy-5` depends on it by git branch, so `Cargo.lock` now
holds two `rust-stemmers` entries that both resolve to the same Snowball 3.1.1
commit. Consequently the V5 writer's stemmers move from 3.0.0 to 3.1.1 as well.
Czech itself is *not* reachable from the V5 path — that path has no
`Language::Czech` — so the only effect there is the shared 3.0.0 → 3.1.1
refinement of the existing languages. The built-in Arabic and English analyzer
tests are the in-repo guard for that upgrade.

**Dependency pinning:** Unchanged by this design. `Cargo.toml` declares
`tantivy` as a git dependency without a `rev`, and reproducibility continues to
come from `Cargo.lock` alone. Adding an explicit `rev` was considered and
rejected to keep the diff minimal; the pre-existing caveat that a future
`cargo update` can advance the pin to a newer `zilliztech/tantivy` commit is
unchanged by this work.

## Test Plan

### Unit tests (Rust, this repository)

New `#[cfg(test)]` module in `stemmer_filter.rs`:

- `test_czech_language` — `"czech".into_language() == Ok(Language::Czech)`.
- `test_czech_language_case_insensitive` — `"Czech"` and `"CZECH"` resolve to
  `Ok(Language::Czech)` (exercises the existing `to_lowercase()` normalization).
- `test_unsupported_language` — `"klingon".into_language().is_err()` (closes the
  pre-existing gap where the error path had no coverage).

These live in the tantivy-binding cargo test suite. Note that Milvus CI does not
currently invoke `cargo test` for this crate (`make rustfmt` / `make rustcheck`
only run `cargo fmt`), and the crate's test target does not build on `master`
for reasons unrelated to this change — a stale `rand = "0.7"` dev-dependency
collides with the `rand = "0.9.1"` runtime dependency, and a test in
`index_reader_text.rs` calls `create_text_writer` with a stale arity. The tests
above were therefore validated locally with those two pre-existing defects
patched out; fixing them in the repository is left to a separate change so that
this one stays scoped to Czech.

### Upstream unit tests

- `rust-stemmers`: `czech_test` validates the generated Czech module against
  `snowballstem/snowball-data` Czech voc/output pairs; the full suite validates
  the 3.1.1 regeneration of the other 18 languages.
- `tantivy`: `test_czech_stemmer` validates `Stemmer::new(Language::Czech)`
  end-to-end through the `StemmerFilter` token stream on known Czech word pairs;
  `test_czech_language_algorithm_mapping` validates the `Language::Czech` →
  `Algorithm::Czech` mapping.

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
- **Expose the other algorithms that `rust_stemmers` now ships but
  `tantivy::Language` does not surface** (Armenian, Basque, Catalan, Esperanto,
  Estonian, Hindi, Indonesian, Irish, Lithuanian, Nepali, Persian, Polish,
  Serbian, Sesotho, Yiddish, …). Rejected: out of scope for this PR, which
  targets Czech specifically. Each additional language needs a `Language`
  variant and `algorithm()` arm in `zilliztech/tantivy` first, then a further
  dependency bump here; that is a separate enhancement.
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
- `zilliztech/tantivy` fork (`src/tokenizer/stemmer.rs`)
- Related Milvus design doc: `docs/design-docs/design_docs/20260403-arabic-thai-analyzer.md`
- Upstream PR (rust-stemmers, Czech algorithm):
  https://github.com/milvus-io/rust-stemmers/pull/2
- Upstream PR (rust-stemmers, Snowball 3.1.1 sync and full algorithm set):
  https://github.com/milvus-io/rust-stemmers/pull/3
- Upstream PR (tantivy, `Language::Czech`):
  https://github.com/zilliztech/tantivy/pull/29
