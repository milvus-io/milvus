# Segcore Grep: ripgrep Integration and a Memory-Only C API

- **Created:** 2026-10-08
- **Status:** Design specification.
- **Components:** Segcore, Rust binding, query execution
- **Engine dependency:** ripgrep `15.2.0`, commit `e89fff89ac9af12e8d4ce9d5fd07beb408ca730f`

## Summary

Add an in-process Grep component that searches text already loaded by Milvus.
It compiles patterns once, searches independent field values in batches, and
returns matching lines as `content` and content-relative `spans`.

The selected Query integration is specified in
[Query Grep with Candidate Records and Deferred Field Retrieval](20261013-query-grep-candidate-execution.md).
It uses an independent QueryScan GrepOp whose candidate DataFrame contains only
segment offsets, without final-limit truncation. Segment identity, field bindings
and snapshot lifetime reside in the execution context. A native runner can use
indexes after offsets exist, reads text in batches, and retrieves identity values
only for matching candidates. User output fields are materialized after selection. Proxy performs generic Query/chain
planning; execution-side construction parses arguments and compiles the Rust
matcher.
Normalized patterns/options and native handles stay local to the execution node.
The engine sections below define the matching contract; the companion design
defines limited batch execution and Query integration. The eventual
capabilities in Section 2 are a roadmap, not additional accepted options.

The implementation uses a new `milvus-grep-core` Rust crate. The existing
`tantivy-binding` crate consumes it through a local `path` dependency and
exports a small C API. The independent `src/grep` C++ module wraps that API;
the Query adapter supplies field values and row mappings. The final
Rust artifact remains `libtantivy_binding.a`, linked into `milvus_core`.

Version one uses the complete upstream `grep` facade and its default
dependencies from a Git dependency pinned to the ripgrep 15.2.0 release commit.
No local upstream source snapshot or parser patch is maintained. Do not migrate
the binding into the upstream Cargo workspace. The C boundary exposes only
the memory-search functionality needed by Milvus. Source-level trimming can be
evaluated after correctness, integration, and performance measurements.

## 1. Motivation and Integration Context

Milvus already has RE2-based string matching in
[`RegexQuery.h`](../../../internal/core/src/common/RegexQuery.h). Its full and
partial matchers primarily return Boolean results and enable `dot_nl`.
That behavior is useful for existing filters, but is not the same contract as
line-oriented Grep with line numbers, context, matching fragments, and an
explicit multiline mode. Existing filter semantics must remain unchanged.

The repository also has an established Rust-to-C++ integration:

- [`tantivy-binding/Cargo.toml`](../../../internal/core/thirdparty/tantivy/tantivy-binding/Cargo.toml)
  declares Rust dependencies and produces `staticlib` and `rlib` artifacts.
- [`tantivy/CMakeLists.txt`](../../../internal/core/thirdparty/tantivy/CMakeLists.txt)
  invokes `cargo +1.89`, imports the resulting static library, and installs its
  headers.
- [`build.rs`](../../../internal/core/thirdparty/tantivy/tantivy-binding/build.rs)
  runs cbindgen. Its current
  [configuration](../../../internal/core/thirdparty/tantivy/tantivy-binding/cbindgen.toml)
  generates a C++ header.
- [`src/CMakeLists.txt`](../../../internal/core/src/CMakeLists.txt) links
  `tantivy_binding` into `milvus_core`. The
  [Segcore target](../../../internal/core/src/segcore/CMakeLists.txt) is an
  object library, not the final shared-library link target.

This design reuses that build path while keeping Grep logic separate from
Tantivy index objects.

## 2. Goals and Scope

### Goals

1. Search a batch of in-memory UTF-8 field values without opening files,
   issuing network requests, or executing external commands.
2. Parse an explicit whitelist of rg-style options at the execution node: patterns,
   regex/literal mode, case sensitivity, and word/line boundaries.
3. Return matching lines and content-relative UTF-8 byte spans, preserving
   positional correspondence with independently searched field values.
4. Reuse compiled patterns and task-local execution buffers.
5. Define ownership, cancellation, error propagation, and resource limits
   before implementation.
6. Keep the initial C API limited to the selected Milvus functionality rather
   than mirror ripgrep's CLI or internal configuration surface.

### Capability Roadmap

Milvus accepts an rg-style argument string as a request parameter, with the
field name supplied separately. It is neither a shell command nor a complete
rg invocation: do not include an executable name or file paths. Section 5.1
is the authoritative first-phase whitelist and grammar. Only successful
execution-side parsing and engine compilation permit Grep execution to begin.
The tables below the first-phase contract describe eventual capabilities;
their presence does not enable those flags in the current parser.

Each non-NULL varchar/text field value is one independent search input, like
one file's contents in rg. Never concatenate values across records. C++ retains
the mapping from batch positions to segment rows; primary keys and field names
belong to the Milvus query/result layer. NULL values are excluded. Grep selection
must happen before database record limit/offset processing.

#### First-phase Result Contract

Return an ordered array of matching lines for each input field value. Each
line has exactly these business payload fields:

```json
[
  {
    "content": "error: error",
    "spans": [[0, 5], [7, 12]]
  }
]
```

- `content` is the entire matching line with its line terminator removed;
  preserve all other whitespace. Spans index these returned bytes.
- `spans` contains non-overlapping match intervals `[start, end)`, measured in
  zero-based UTF-8 bytes relative to `content`, not characters or field offsets.
  Multiple occurrences on one line share one result element.
- Return lines in original order. Identical text on different lines remains
  separate. A field with no selected lines returns an empty array.
- Empty occurrences are represented as `[n, n]` at UTF-8 character boundaries
  only. Section 5.3 defines enumeration and empty-input rules; upstream
  empty-match iteration alone does not establish this guarantee.
- First-phase matching is line-oriented. No context, multiline blocks,
  fragment-only output, line numbers, or field offsets are exposed initially.
- Batch results preserve input order. Transport wrappers and record identity
  are separate from these two payload fields.

The first-phase options are repeated `-e`, `-F`, `-i`/`-s`, and `-w`/`-x`,
including the exact long aliases in Section 5.1. Matching is LF line-oriented.
All other options fail, including familiar rg options and planned extensions.

#### Eventual Matching Capabilities

| rg analogue | Proposed Milvus parameter | Intended scope |
|---|---|---|
| `-e` | `patterns: string[]` | At least one pattern; combined OR enumeration rather than duplicate per-pattern scans |
| `-F` | `pattern_kind: regex/literal` | Regex by default; literal mode escapes pattern syntax |
| `-i/-s/-S` | `case_mode: sensitive/insensitive/smart` | Explicit case policy; smart-case is a later addition |
| `-w/-x` | `boundary: none/word/line` | Mutually exclusive boundary modes; word means regex Unicode word boundaries, not Chinese tokenization |
| `-U` | `multiline: bool` | Matches may cross lines within one field value only |
| `--multiline-dotall` | `dot_all: bool` | Dot may consume newline; requires regex multiline mode |
| `--crlf` | `newline: lf/crlf` | Explicit line-ending and anchor semantics without rewriting stored bytes |

#### Eventual Result Capabilities

| Capability | Proposed direction | Required contract before exposure |
|---|---|---|
| Context (`-A/-B/-C`) | `before_context`, `after_context` | Stay within one field; introduce a match/context role or a separate context container, never infer role solely from spans |
| Fragment output (`-o`) | `output_mode: lines/matches` | Explicitly distinguish whole-line content from exact fragments and define empty-match behavior |
| Positions (`-n/-b`) | Optional `line_number`, `start_byte` metadata | Field-relative coordinates; spans remain content-relative |
| Per-field result limit (similar to `-m`) | Explicitly named per-field limit | Define whether it counts lines or occurrences, multiline grouping, trailing context, and completeness; never equate it with database record limit |
| Counts (`-c/--count-matches`) | Separate statistics or query aggregation | Distinguish selected records, selected lines, and regex occurrences; exclude context from match counts |

Multiline output requires a deliberate extension from a single-line `content`
to a multi-line block, with spans relative to that entire returned block.
Define block grouping and terminator preservation before exposing it. The
future per-field limit requires a separate counting contract: Matcher occurrences and grouped Sink callbacks are different units.
Likewise, re-enumerating a selected line can discover occurrences beyond a
search cutoff; decide whether those are included before implementing limits.

A user-requested per-field result limit is a deliberate restriction of output.
A service resource budget is different: exceeding compile or output budgets
must fail explicitly rather than silently return apparently complete results.
Soft truncation requires its own completeness contract.

#### Capabilities Owned by the Query Layer

| rg analogue | Milvus responsibility |
|---|---|
| Matching file names (`-l`) | Project primary keys of matching records |
| Existence (`-q`) | A separately specified query-level existence operation |
| Files without matches | Record-level predicate meaning no match anywhere in the field; define NULL semantics separately |
| File selection and sorting | Collection, Partition, Filter, target field, and database ordering semantics |
| Output selection | `output_fields` and Grep result projection |

Do not expose an ambiguous `invert` switch. rg `-v` selects nonmatching lines;
it does not mean that the entire input contains no match. A field can contain
both matching and nonmatching lines. Record-level negation and line-level
inversion require different names and contracts; line-level inversion is not
in the selected scope.

#### Excluded or Separately Gated Capabilities

- File traversal: glob/type filters, hidden/ignore rules, symlink following,
  depth limits, file-based pattern loading, and filesystem paths.
- Terminal formatting: colors, headings, separators, pretty/vimgrep output,
  and CLI JSON formatting. SDK responses are structured data.
- Encoding and binary switches: the field contract uses UTF-8 with embedded
  NUL allowed; no user-controlled transcoding, binary detection, or NUL line
  delimiters. Unicode disabling remains unsupported.
- Execution knobs: threads, mmap, regex-size limits, and DFA-cache limits are
  controlled by service configuration and scheduling, not unrestricted user
  flags. The query layer owns deadlines and enforces hard resource ceilings.
- Content rewriting: replacement and trimming are excluded because they change
  the relationship between original text and returned spans.
- External processing: preprocessors, subprocesses, and archive decompression
  are outside field-memory search.

The default Rust regex engine is the selected backend. PCRE2 is not part of the
committed scope, even for the eventual capability set. It may be considered
separately for demonstrated lookaround or backreference requirements. Such an
extension must use explicit engine selection, not automatic fallback, and must
validate execution limits, cancellation, dependency integration, and semantic
compatibility. Both engines could use `content` and `spans`; changing engines
does not by itself require a different result shape. Multiline support does
not require PCRE2.

### Component Boundaries

The memory-only C header exposes normalized-spec validation and matching only.
It has no file/path input, Reader/Writer callback, encoding selection, mmap,
thread count, output mode, context, per-field match limit, engine selection,
PCRE2, inverted selection, count, or request-level existence option. It has no
reserved feature flags or generic extension dictionary.

The component does not own Collection/Partition selection, authorization,
visibility, primary keys, field names, database limit/offset, projection,
storage, or scheduling. Full GNU grep, POSIX shell, and ripgrep CLI compatibility
are not acceptance criteria. Later capabilities require explicit implementation,
protocol support, and a whitelist update. There is no resumable scan or stable
cross-version plugin ABI in phase one.

## 3. Selected Dependency and Build Integration

### 3.1 Version and License

Use the **15.2.0 release commit**, not upstream `master`. The release declares
Rust 1.85 as its minimum version and is compatible with the selected Milvus
Rust 1.89 toolchain at the independent-build level.

| Crate | Version in the selected source commit | Use |
|---|---|---|
| `grep` | 0.4.1 | Facade used by `milvus-grep-core` |
| `grep-matcher` | 0.1.9 | Matching interface and occurrence enumeration |
| `grep-regex` | 0.1.14 | Pattern compilation and matching |
| `grep-searcher` | 0.1.17 | In-memory search and line/context events |
| `grep-cli` | 0.1.12 | Retained default dependency |
| `grep-printer` | 0.3.1 | Retained default dependency |
| `grep-pcre2` | 0.1.10 | Optional; not enabled in version one |

These are the package versions in the release source, not the minimum version
ranges appearing in individual dependency declarations. Pinning only the
crates.io facade version would not pin all of its transitive dependencies.

ripgrep is dual-licensed `Unlicense OR MIT`. Select MIT, retain its copyright
and license text, and review notices for the final resolved dependency set.

### 3.2 Source Management and Layout

Use the public upstream `grep` crate through a Cargo Git dependency pinned
by full commit SHA. Cargo owns its source checkout/cache; Milvus does not
maintain a source snapshot, Git submodule, fork, or parser-export patch.
The [dependency-management README](../../../internal/core/thirdparty/grep/README.md)
describes the dependency and upgrade workflow. Refer to the selected
[upstream MIT notice](https://github.com/BurntSushi/ripgrep/blob/e89fff89ac9af12e8d4ce9d5fd07beb408ca730f/LICENSE-MIT)
for project-wide third-party distribution notices; no local copy is maintained.

Parameter tokenization, whitelist enforcement, conflicts and normalization
belong in shared Go code called by the query execution entry. Rust owns engine validation, compilation and matching.
The core manifest records the upstream revision; both the standalone and binding
lockfiles pin the resolved source and transitive dependencies.

```text
internal/core/thirdparty/grep/
  README.md                 # Dependency ownership, build and upgrade rules.
  Cargo.toml                # Pinned Git facade dependency, consumed by binding.
  Cargo.lock                # Reproducible standalone core tests.
  build.rs                  # Generate the independent C header with cbindgen.
  cbindgen.toml
  include/milvus_grep.h      # Generated; do not edit.
  src/lib.rs
  src/ffi.rs                # C ABI types, handles, validation, panic boundary.
  src/plan.rs              # Shared validation and matcher compilation.
  src/session.rs
  src/result.rs
  src/error.rs
  tests/

internal/core/thirdparty/tantivy/tantivy-binding/
  Cargo.toml                 # Add local path dependency.
  Cargo.lock                 # Resolve and lock the integrated dependency graph.
  build.rs                   # Existing Tantivy header generation.
  src/lib.rs                 # Re-export the Grep FFI module for static aggregation.

internal/util/function/grep/
  options.go, lexer.go, parser.go  # Pure Go argument parsing and budgets.
  *_test.go                       # Grammar, limits, context and fuzz tests.

internal/core/src/grep/
  Grep.h/.cpp               # Independent RAII wrapper and typed local errors.
  GrepTest.cpp              # Real Rust integration tests, also in all_tests.
  CMakeLists.txt            # Object library, linked into milvus_core.
  tests/                    # Standalone project and isolated ABI fault tests.

# Query adapter:
# Filter/MVCC candidates, field pinning, batching, row mapping and reduction.
```

`milvus-grep-core` is an `rlib`. Add this dependency to `tantivy-binding`:

```toml
milvus-grep-core = { path = "../../grep" }
```

In `milvus-grep-core`, use the pinned Git dependency:

```toml
[dependencies]
grep = { git = "https://github.com/BurntSushi/ripgrep.git", rev = "e89fff89ac9af12e8d4ce9d5fd07beb408ca730f" }
```

Use `grep::matcher`, `grep::regex`, and `grep::searcher`. Keep the default
dependency set; do not use `--all-features` to imply completeness. Merely
including the facade does not launch the CLI or expose all its behaviors.

The pinned upstream command-line parser lives in the executable's private
`crates/core/flags` module, not in the facade's public API. `grep::cli` is not
an argument parser for rg. Milvus does not export or depend on that parser.
The pure Go package `internal/util/function/grep` implements the argument grammar below: tokenize the
string, check each option occurrence against the whitelist, validate conflicts,
and produce typed patterns/options. The Query protocol carries the original
argument string. There is no shell, rg configuration loading,
process argv access, or upstream `HiArgs` construction on this path.

Rust reuses `RegexMatcherBuilder` for engine validation and compilation at the
execution node. Do not substitute Go's regexp compiler for the selected Rust
engine's syntax validation.

Use one pinned Git source for the Grep crates. Initial builds may fetch it;
CI can cache Cargo Git and registry sources. `--locked` enforces the committed
resolution but is not an offline flag. Fully offline builds require all
resolved sources to be cached or vendored through a separate workflow.

### 3.3 Build Changes

Keep the existing Rust artifact name, toolchain, and release profile, including
`panic = "unwind"`. Cargo builds Grep as a dependency of the binding; there is
no separate Grep static library or independent Cargo invocation from CMake.

Merge new dependencies into the existing binding lockfile. Do not replace it
with ripgrep's lockfile or perform an unrelated full dependency update. Use
`--locked` in the integrated build to preserve the resolved dependency graph.

Generate a separate **C-compatible** `milvus_grep.h`; the existing C++-oriented
binding header should not expose Grep's internal Rust types. Configure exports
and common includes so the two headers can coexist without duplicate type
definitions. Add header installation and generation dependencies to CMake,
including dependencies needed before consumers compile in a clean parallel
build. The final link remains the existing `milvus_core` link.

Update build-context packaging, CI cache inputs, and source-change tracking to
include the sibling Grep directory. Changes in that directory must rebuild the
Rust artifact. No workspace root, lockfile relocation, or binding rename is
part of this work.

Proxy needs no Grep-specific cgo call or native linkage. Production query
integration passes the execution-local Go specification to the C++ query adapter,
compiles through `mg_grep_compile`, and retains the Plan for execution.
The final shared library must retain and export the symbols used by the
execution adapter. No Rust object or Go-managed pointer may cross an asynchronous boundary
without explicit ownership. The internal request carries raw arguments, not
Go-owned normalized patterns/options or a Rust handle.

## 4. Architecture and Query Integration

The authoritative integration design is
[Query Grep with Candidate Records and Deferred Field Retrieval](20261013-query-grep-candidate-execution.md).

```text
Query with an independent QueryScan GrepOp
  -> ordinary filter and MVCC
  -> offsets-only candidate frame plus segment/read-view context, no final limit
  -> GrepOp passes offsets and request-owned read view to a native runner
  -> optional index bitmap checked at the candidate offsets
  -> bounded text gathering and rg exact matching
  -> identity lookup only for matches, PK-group finalization and Grep limit
  -> OperatorResult with input positions, selected identities and matching details
  -> deferred output fields
  -> aligned worker/delegator/Proxy reduction and final pagination
```

[Function Chain Row Selection](20261010-function-chain-row-selection.md)
([PR 54190](https://github.com/milvus-io/milvus/pull/54190)) defines `OperatorResult`,
`RowSelection` and central assembly. Positions are implicit per logical input
chunk; there is no materialized `$row_id`. Computed columns are already in
selected order. The selection interface supports arbitrary index gathering
of retained scalar columns but not retained List/Struct columns. The initial
single-Grep scope avoids claiming arbitrary downstream nested-column selection.

Indexes need not run before candidate export: the native runner can compute a
segment-range candidate bitmap once and test `bitmap[offsets[i]]` before reading
text. An earlier index pushdown can reduce metadata export costs, but
must preserve rg semantics and index coverage. Index absence/unsupported patterns
fall back to exact matching, without silently discarding uncovered records.

Each per-segment GrepOp has a required independent positive `limit`, capped by
`function.grep.maxLimit` in milvus.yaml and paramtable. This setting is a hard
maximum, not a default for an omitted operator limit. GrepOp has no offset;
Query's offset/limit remain separate final-pagination controls. Candidate
generation does not apply either result cap before matching. Grep stops at its
own accepted limit, even if subsequent Query pagination returns fewer rows.
Early stopping must preserve the existing PK/duplicate/version rules; physical
matcher hits alone are insufficient. The runner resolves identities for hits
and finalizes duplicate groups before counting them. Without an available group
boundary, it may need the next distinct matching PK or candidate exhaustion to
close the final group. Candidate metadata and matching details
remain subject to independent memory and time budgets.

The companion Query design defines read-view lifetime, native/Arrow ownership,
per-segment quotas, protocol capabilities and `extra_results` reduction. Request
admission requires all participating components to support those contracts.

## 5. Parsing and C API

The memory-only C API uses three opaque handles. Its all-input operations are
listed below; limited batch execution adds a processed-prefix contract. Go owns parsing
and normalized configuration. Plan owns a compiled matcher, Session owns
execution state, and Result owns matching lines and spans.

| Function | Responsibility |
|---|---|
| `mg_grep_compile` | Validate/compile an execution-local typed spec; no CLI parsing |
| `mg_grep_plan_free` | Release the caller's Plan reference |
| `mg_grep_session_create` | Create task-local execution state retaining the Plan |
| `mg_grep_session_cancel` | Request sticky cancellation |
| `mg_grep_session_free` | Release a quiescent Session |
| `mg_grep_execute_batch` | Match an input batch synchronously |
| `mg_grep_result_view` | Borrow a read-only batch result |
| `mg_grep_result_free` | Release all result-owned storage |

### 5.1 Argument String Grammar and Whitelist

The query execution entry implements this grammar using the shared Go parser. It applies to the UTF-8 string after the
SDK/transport has decoded it once. It does not include host-language string delimiters or JSON escapes.
For example, the actual argument string is:

```text
-i -e "user_id=[0-9]{3,6}\b.*timeout"
```

The same string in a JSON request property would be:

```json
{"arguments":"-i -e \"user_id=[0-9]{3,6}\\b.*timeout\""}
```

The example shows the Grep operator arguments after transport decoding. Parsing
produces the following logical spec (field identity is resolved by GrepOp on QueryNode):

```json
{
  "patterns": ["user_id=[0-9]{3,6}\\b.*timeout"],
  "pattern_kind": "regex",
  "case_mode": "insensitive",
  "boundary": "none"
}
```

#### Lexical rules

- Unquoted ASCII space, tab, CR, and LF separate tokens. Other Unicode
  whitespace is literal. Quotes are removed and adjacent quoted/unquoted
  pieces concatenate into one token. Empty quotes create an empty token.
- Single quotes preserve every enclosed byte except the closing single quote;
  a backslash has no special meaning inside single quotes.
- In double quotes, backslash escapes only `"` and `\`. Before any other byte
  it is preserved, so `\b`, `\d`, and `\n` reach the regex parser unchanged.
- Outside quotes, backslash escapes ASCII token-separating whitespace, either
  quote, or another backslash. Before other bytes it is preserved. A dangling
  unquoted backslash or an unclosed quote is an error. Escaped whitespace is
  literal, not a shell line-continuation instruction.
- No shell is invoked. No variable, command, glob, tilde, or environment
  expansion occurs. Shell metacharacters inside a pattern are literal pattern
  bytes; outside a pattern, unexpected tokens fail. `#` does not start a comment.
- The argument string must be valid UTF-8 and contain no literal NUL. Bound its
  bytes before expensive setup. Never read process argv, stdin,
  rg configuration files, or pattern files.

#### Accepted options

| Short | Exact long alias | Normalized effect |
|---|---|---|
| `-e PATTERN` | `--regexp PATTERN` | Append one pattern, preserving order; at least one required |
| `-F` | `--fixed-strings` | All patterns are literal; default is regex |
| `-i` | `--ignore-case` | Case-insensitive |
| `-s` | `--case-sensitive` | Case-sensitive; also the default |
| `-w` | `--word-regexp` | Word boundary mode |
| `-x` | `--line-regexp` | Whole-line boundary mode |

Support attached `-ePATTERN` and `--regexp=PATTERN`. Short flags can be grouped
(e.g. `-Fi`); `e` consumes the remainder of its token or the next token.
The next token after a separated `-e` is always its value, even if it starts
with `-`: `-e -C` searches for the pattern `-C`, not context. A missing next token
is an error. Boolean flags take no values; `--ignore-case=false` is invalid.
Do not support abbreviated long names or unlisted negated aliases.

Repeated `-e` appends patterns, including empty ones; `-e ""` is valid. Patterns
are compiled as one ordered OR expression, not searched independently. Repeated
`-F`, `-w`, or `-x` is idempotent. For `-i`/`-s`, the last occurrence wins.
Combining `-w` and `-x` is an error, irrespective of order, rather than inheriting
rg's precedence. `--` ends options, but all following positional tokens are
rejected. A final bare `--` is allowed if a pattern was already supplied.
There is no positional pattern, filename, executable name, or implicit empty
pattern. Use `-e` for every pattern.

Unknown flags, rg flags outside this whitelist, and roadmap-only flags fail
at the execution node as typed request-input errors. The diagnostic names the offending
option and reason; do not parse error messages for control flow. These Go
parsing failures do not cross the Rust C API. There is no best-effort
acceptance, ignored option, or fallback to ordinary Query. An unsupported flag
fails even if its value would disable its effect, such as `-C 0`.

| Actual argument string | First-phase outcome |
|---|---|
| `-i -e "user_id=[0-9]{3,6}\b.*timeout"` | Accepted; preserves the regex word-boundary escape |
| `-F -e 'a.b' -e timeout` | Accepted; two literal patterns |
| `-i -s -e error` | Accepted; case-sensitive |
| `-e ""` | Accepted; explicit empty pattern |
| `-i -C 2 -e "user_id=[0-9]{3,6}\b.*timeout"` | Error: context is outside the initial scope |
| `-P -e error` / `-U -e error` / `-S -e error` | Error: unsupported option |
| `-w -x -e error` | Error: conflicting boundary modes |
| `-e` / `-e "unterminated` / `-e error app.log` | Error: missing value, unclosed quote, or positional token |
| `-e '['` | Error: invalid regex (`MG_GREP_INVALID_PATTERN`) |

#### Go parser API and error contract

`internal/util/function/grep` provides the pure-Go `grep.Parse` API. It performs
no shell execution, filesystem lookup, environment expansion, cgo calls or regex
compilation. GrepOp calls it during per-segment construction; the
[candidate execution design](20261013-query-grep-candidate-execution.md#responsibilities-and-execution-flow)
defines Proxy forwarding, operator construction and native plan lifetime.

```go
spec, err := grep.Parse(
    `-i -e "user_id=[0-9]{3,6}\b.*timeout"`,
    paramtable.Get().FunctionCfg.GrepMaxArgumentBytes.GetAsInt64())
```

The byte limit is positive and supplied by the factory from
`function.grep.maxArgumentBytes` (default 65536). `Spec` contains ordered patterns and
matching enums, not field IDs, database metadata, native handles or a wire
representation. Do not mutate it concurrently with validation.

The parser is bounded by the arguments byte limit and does not
accept a context. It returns no partial Spec on failure. Malformed user arguments
produce existing merr input errors (`ParameterInvalid`, `ParameterMissing` or
`ParameterTooLarge`); invalid service budgets produce system errors. The caller
checks request cancellation before and after construction. The parser does not
translate C ABI statuses. Parsing success does not establish regex validity: `-e '['` produces a
Spec, but native compilation rejects it before matching.

### 5.2 Normalized Spec and Input Contract

The normalized matching options are `pattern_kind` (regex/literal), `case_mode`
(sensitive/insensitive), and `boundary` (none/word/line). Zero initialization
selects regex, case-sensitive, no boundary restriction. Reject unknown enum
values when constructing the Go specification and at the Rust compile boundary.
Word mode uses the pinned builder's
Unicode word matching semantics; it is not a tokenizer or Chinese segmentation.

The shared builder fixes Unicode on, LF line-oriented matching, and disables
cross-line search. Set the matcher line terminator to LF and configure line
anchors consistently. No literal LF is accepted in a normalized pattern;
regex expressions requiring LF are rejected by the line matcher builder.
Character classes follow that builder's line-terminator restriction. Literal
mode treats backslashes as ordinary pattern bytes after argument tokenization.
In regex mode, reject Unicode-disabling inline constructs with syntax-aware
validation; other inline flags follow the pinned builder and cannot enable
cross-line scanning. Engine validation and execution use the same compiled matcher.

`MgGrepBytes` is length-delimited, not a C string. Patterns and field values
must be valid UTF-8. Normalized patterns cannot contain literal NUL (consistent
with the argument grammar); a regex escape such as `\x00` can match NUL in
text. Field text may contain NUL and BOM bytes, which remain unchanged.

The C layer never resolves fields. C++ excludes database NULLs; `{NULL, 0}`
represents an empty non-NULL field. Reject a NULL data pointer with nonzero
length. Pattern count is at least one. An empty batch permits a NULL text-array
pointer and returns a Result with zero entries. Otherwise a successful Result
has exactly one entry per input, including inputs without matches.

Validate pointer/length combinations, counts, checked arithmetic (including
Rust slice length constraints) and result metadata budgets before allocating.
Validate all text UTF-8 before matching; validation checks cancellation. Actual non-NULL pointer validity and
memory extent are caller obligations, not something the ABI can safely probe.

### 5.3 Matching Line Results

Phase one has one output shape and no `MgGrepOutputOptions`:
`BatchView -> TextResult[] -> Line[] -> {content, Span[]}`.

- LF separates logical lines. Remove only that terminal LF from `content`;
  a preceding CR remains ordinary content in phase one. Do not trim whitespace.
  A final unterminated line is searchable. A trailing LF does not invent an
  additional line after EOF. CRLF normalization is a later explicit capability.
- Empty input has no logical lines and produces no matches. `"\n"` has one
  empty-content line, which an empty pattern selects with span `[0, 0]`.
- Matching and occurrence enumeration use the same logical line boundaries.
  Do not rerun the regex on a cropped matching fragment. Searcher candidate
  optimizations may be used only when equivalent to this contract, including
  line anchors and absolute anchors evaluated against the logical line input.
- Enumerate the combined matcher in non-overlapping order, retaining upstream
  tie/alternative order and empty-after-nonempty suppression. Omit any empty
  occurrence inside a UTF-8 code point. Continue enumeration to subsequent
  boundaries without duplication or looping. Nonempty span endpoints must also
  be valid character boundaries. These are acceptance requirements, not an
  assumption that `grep-regex`'s byte-oriented empty iterator already does it.
- Each selected logical line appears once, with all eligible spans; retain
  identical content from different lines as distinct entries in line order.
  Every returned line has at least one span, including an empty span where
  appropriate. `line_count > 0` is the record selection decision.
- Spans satisfy `0 <= start <= end <= content.len` and address UTF-8 bytes of
  that line's returned content. They are not original-field offsets. Do not
  duplicate matched text or return line numbers, roles, or field identifiers.
- Success includes all matching lines and spans for every input in the batch.
  The unlimited batch API has no first-hit early return or silent truncation.
  The proposed limited API stops only between complete records and reports a
  processed prefix; it does not enable per-field `-m`. Resource failure, deadline
  and cancellation produce no Result in either mode.

Result views are immutable and owned by their Rust handle. SpecView is a
borrowed C representation assembled by the caller from execution-local normalized
configuration. Neither layout is a serialized wire representation.

## 6. Ownership and Concurrency

Proxy forwards the raw argument string. The execution entry owns the parsed
Go configuration, which is not sent back through the distributed request. `mg_grep_compile` borrows a SpecView and its nested
patterns only during the call, and retains no caller pointer. The Go wrapper
must obey cgo pointer rules when constructing this temporary view; it cannot
pass a Go struct containing unpinned Go pointers as though it were C memory.

Patterns and configuration are copied or owned by the Plan before compilation
returns. A Session retains a strong reference to its Plan, so freeing the
caller's Plan handle does not invalidate existing Sessions. Sharing a Plan
must be backed by verified Matcher concurrency/cache behavior; no mutable
Searcher or Sink is shared between concurrently executing tasks.

One Session may execute multiple batches sequentially. It cannot execute two
batches concurrently. Cancellation is the only operation allowed concurrently
with execution on the same Session. Different Sessions may run in parallel
under Milvus scheduling.

Input text is borrowed only until `execute_batch` returns. A Result owns its
arrays and text storage. All nested view pointers remain stable until
`result_free`, independent of the input, Session, and Plan lifetimes. Empty
arrays have NULL pointers. Read-only Result views may be shared, provided
release is synchronized with all readers.

Only Rust release functions free Rust objects. C++ wraps handles in RAII;
it never calls `free`/`delete` on returned storage. NULL release is a no-op.
The caller must synchronize releases: Plan release must not race with Session
creation; all Session execution and cancellation calls must have returned
before Session release; Result readers must finish before their handle is
released. Release functions do not wait for concurrent users.

Example lifecycle:

```text
Proxy: request checks -> serialize field ID + unchanged argument string
Executor: decode request -> Go parse -> local SpecView -> compile -> plan
session_create(plan) -> session
plan_free(plan)                         # Session retains ownership.
execute_batch(session, texts, ...) -> result
session_free(session)                   # No execution/cancel call remains.
result_view(result) -> copy/convert     # Input/session may already be gone.
result_free(result)
```

The installed header and library are delivered together for the same build and
architecture. Use C POD, fixed-width integers, `size_t`, and pointers; no Rust
`String`/`Vec`, C++ containers, packed structures, or cross-language `bool`.
Rust uses `repr(C)` structures and integer aliases. Do not add unused
`abi_version`, `struct_size`, or reserved fields for hypothetical consumers.
An incompatible future ABI change requires an explicit versioning decision.

### C++ Wrapper Contract

[`grep/Grep.h`](../../../internal/core/src/grep/Grep.h) exposes move-only Plan,
Session and Result owners. The public header contains no generated C view
structures. Result accessors return borrowed `std::string_view` content and
Span values, avoiding another copy of the complete Rust output. Index checks
throw a typed local error; borrowed text must not outlive the owning Result.

A copyable CancellationToken pins shared Session state through cancellation
callbacks, including callbacks after the C++ Session owner is gone. Callers
still synchronize move/destruction of a C++ object with its own method calls.
An execute call pins its Session state locally; ordinary errors leave it
reusable, while internal execution failures remain poisoned even after cancel.
The wrapper checks result metadata budgets before descriptor allocation and
checks cancellation during descriptor preparation.

`milvus::grep::Error` retains the exact private status (including unknown
future values), the operation phase, and a bounded owned diagnostic. It does
not assign input/system blame or invent a Segcore code. The Query/cgo adapter
must explicitly translate this error before the existing generic
`std::exception` catch would flatten its code. No wire/retry behavior is
claimed by this standalone wrapper.

## 7. Budgets, Cancellation, and Failure Semantics

### 7.1 Resource Limits

| Limit | Meaning |
|---|---|
| `function.grep.maxArgumentBytes` | Maximum raw arguments byte length; default 65536 |
| Propagated request deadline | Executor checks before/after chain construction and Rust compilation |
| `function.grep.maxRegexBytes` | Engine compiled-program byte budget; default 33554432 |
| `function.grep.maxDFACacheBytes` | Engine DFA-cache byte budget; default 8388608 |
| `max_result_bytes` | Logical result structures/arrays and owned text, including unselected-row metadata |

Compile and result limits are strictly positive and supplied by the service,
not raw CLI switches. The matching API has no input-byte budget or independent
timeout. QueryScan bounds read batches by row count and propagates request
cancellation, including request deadline expiry, through the session token.
Pointer/length and result-size arithmetic remain checked; zero-length texts
still consume descriptor/result metadata. Test the metadata-only exhaustion
case rather than checking only text bytes.

These are not aggregate RSS guarantees. Engine cache limits may apply to
multiple caches; logical result bytes exclude allocator overhead and temporary
engine storage. C++ also bounds batch size, concurrent Sessions, and the sum of
retained results, and passes remaining request budgets on each call.

The unlimited batch API has **whole-batch success or failure**. The proposed
limited API returns a complete processed prefix on a normal quota stop; it never
turns a budget or execution failure into partial success. Resource exhaustion,
invalid input and cancellation discard that call's output and return
`*out_result=NULL`. No details are silently truncated to fit a budget. Success
returns all matching lines and spans for every input field in that batch. If a
later batch fails, the caller fails the request rather than publishing earlier batches as a
complete success. Soft detail truncation is deferred.

### 7.2 Cancellation and Timeout

`session_cancel` sets Rust-owned atomic state. It is idempotent and sticky:
cancellation before execution cannot be lost, and future executions on that
Session also return CANCELLED. There is no reset operation; use a new Session
for a new task. C++ does not share an atomic memory layout with Rust.

Check cancellation at validation loops, text boundaries, Matcher call
boundaries, and Sink events. The final cancellation check before publishing a
result is the success linearization point; later cancellation does not revoke
an already completed result. A cancellation callback must retain the Session
until it returns. Freeing a handle is not cancellation.

Cancellation and deadlines are cooperative. They cannot immediately interrupt
every individual regex call. Pattern compilation has no Session cancellation
handle. The execution entry checks the propagated request deadline before/after chain
construction and before/after compilation; a single builder call
cannot be preempted. Compilation is also bounded by service compile limits. Hard preemptive timeout is not promised.

A cancelling Matcher wrapper must check before/after matching calls, including
candidate searches and detail enumeration. Sink callbacks alone cannot catch
long no-match scans promptly. Test cancellation on long fields and expensive
patterns before accepting a service latency objective.

### 7.3 Errors and Milvus Classification

The C API returns a `uint32_t` status and optionally fills a caller-owned
512-byte UTF-8 diagnostic buffer. Success clears the buffer; failure may
truncate at a character boundary and always terminates it with NUL. Control
flow uses the status code, never error-string parsing. No error allocator,
`error_free`, global `last_error`, or I/O error category is needed.

The ten values in Appendix A are **private Grep statuses**, not Milvus
`ErrorCode` values or wire codes. The execution-side Go and C++
adapter must classify by origin before constructing merr or typed Segcore
errors respectively:

| Origin | Required treatment |
|---|---|
| Malformed argument string, unsupported option, or invalid option combination from the request | Request-input failure at the execution entry, propagated to Proxy |
| Malformed user pattern or unsupported user pattern syntax | Request-input failure |
| Invalid user pattern UTF-8 | Request-input failure |
| Invalid UTF-8 in a persisted field | Data-integrity/system failure; do not blame the current request |
| Invalid descriptor, enum, output pointer, or internally supplied configuration | Internal adapter/plan contract failure unless proven to originate from request input |
| Deterministic pattern/result policy limit | Preserve the budget failure and its policy origin; do not blindly map to a transient resource shortage |
| Request cancellation or deadline | Preserve cancellation/deadline semantics; do not translate to success or empty results |
| Recovered Rust panic | Internal failure, never invalid input |

The Rust core exposes typed local errors. Upstream
`grep-regex::Error::regex/generic` stringify some syntax and compiled-size
failures into the same `ErrorKind::Regex`. AST/HIR validation catches syntax
early; remaining `Error::EngineBuild` failures map to the distinct private
`MG_GREP_ENGINE_BUILD_ERROR` status (8), without message parsing. Actual
`try_reserve` failures map to `MG_GREP_ALLOCATION_ERROR` (9), separate from
policy limits. The original error source is retained inside Rust. The
execution adapter must preserve these private categories and classify by origin,
without guessing from diagnostic text. Malformed normalized specs and unexpected
descriptor failures remain system errors. Context cancellation/deadline causes
must be preserved through remote error propagation so callers can apply the
corresponding retry policy.

Use the repository's
[error handling guide](../../dev/error_handling_guide.md) and
[casebook](../../dev/error_handling_casebook.md). Do not invent numeric 20xx
codes or cast a Grep status into `CStatus.error_code`. Audit producers and the
actual consumer path before choosing the existing Milvus code mapping.

Existing typed boundary patterns are visible in
[`tantivy-error.h`](../../../internal/core/thirdparty/tantivy/tantivy-error.h),
[`CGoCatch.h`](../../../internal/core/src/common/CGoCatch.h), and
[`merr/segcore.go`](../../../pkg/util/merr/segcore.go). The Grep adapter
must preserve its chosen classification through executor wrapping and the
cgo boundary.

Every failed handle-producing function sets a valid output slot to NULL. A
recoverable ordinary error clears per-call state. An internally panicked
Session is poisoned and returns INTERNAL_ERROR on later executions until it
is freed. The C handle also retains core internal failures so cancellation
cannot mask a poisoned state. Unwinding panics are caught at the ABI boundary; C++ exceptions do
not cross it. Abort and allocation failure cannot all be claimed recoverable.

## 8. Memory-Only Execution and Upstream Dependencies

The current core implements the equivalent line adapter described in Section
5.3: scan LF boundaries in bounded chunks, match each complete logical line
through `grep::matcher::Matcher`, and collect owned line/span results. It
does not construct a Searcher, transcode input, inspect BOM/binary markers,
map files, print, or write files. Each Session clones the compiled matcher;
the resolved regex-automata clone shares compiled state but creates its own
cache pool. Buffers owned by results cannot be reused while a result is live.

If a future optimization uses `search_slice`, explicitly disable encoding/BOM
processing, binary detection and file mapping, and prove equivalence for line
and absolute anchors and empty matches. Keep its builder private. A Reader
abstraction over a slice does not itself imply disk I/O.

The facade retains I/O-related source and dependencies. `encoding_rs`,
`encoding_rs_io`, and `memmap2` are non-optional Searcher dependencies in this
version; a downstream `default-features=false` does not remove them. Cargo
feature unification can also re-enable shared capabilities through other
consumers. Do not promise that all unused code disappears from the final
shared library or use linker elimination as a security boundary.

If introduced later, Searcher constructors have memory costs: the pinned Searcher allocates an 8 KiB
decode buffer and a default 64 KiB line buffer. Reuse Session state rather than
constructing a Searcher for every record. Do not set `heap_limit=0` as a way to
disable buffers; it has different semantics.

The library uses Rust `log`; trace/debug messages may contain patterns or
extracted literals. Integrate with the existing process logger, suppress these
module-specific payload logs, and do not register a second global logger or
disable all Milvus logging. At the Go boundary, follow the repository's
[`mlog` guidance](../../agent_guides/observability/logging.md) and carry the real
request context. No new logging callbacks or telemetry knobs belong in this C
API. Metrics use low-cardinality labels rather than patterns or record IDs.

The contract forbids active external I/O by the matcher. It does not promise
that the entire query avoids storage reads: Milvus loads fields, mapped input
can fault pages, and allocators may use anonymous mmap. Verify the isolated
search path separately from the whole query's I/O behavior.

## 9. Compatibility and Alternatives

### Compatibility

- Existing RE2 filters and their semantics remain unchanged.
- This component does not change persisted data, indexes, WAL, manifests, or
  storage paths. It introduces no data migration.
- Existing requests keep their behavior. Public Grep exposure requires explicit
  plan/protocol support and mixed-version capability checks; unsupported nodes
  must reject the operation rather than ignore it.
- NULL rows are excluded by C++; empty fields and zero-length matches have the
  explicit rules above. One complete field value is one independent input;
  storage chunks must not accidentally become matching boundaries.
- MIT and transitive notices accompany distribution. Full dependency-license
  and security review is based on the integrated lockfile, not the facade alone.

### Alternatives Considered

| Alternative | Decision |
|---|---|
| Launch `rg` as a subprocess | Adds process/file/output-protocol work to an in-memory operation; not selected |
| Extend the existing RE2 Boolean matcher | Requires implementing the Grep line/context contract separately; retain RE2 for existing filters |
| Depend directly on only three search crates | Deferred; the complete facade keeps dependency integration straightforward |
| Fork and remove I/O source immediately | Creates maintenance work before measured benefit; defer |
| Build a second Rust `staticlib` | Requires extra runtime/dependency/symbol coordination; use one binding artifact |
| Create a Cargo workspace now | Useful for shared independent testing/lockfile management, but adds unrelated build migration; use a path dependency first |
| Return partial details or callbacks across C | Expands lifetime, failure, and continuation contracts; use owned all-or-error results in version one |

The integrated path-dependency build resolves Grep under the binding
lockfile and root profile. Migrating the binding's workspace is not needed to
produce one artifact. Standalone core tests use their own lockfile and may
resolve a different graph, so binding
integration tests are mandatory even when the crate's own tests pass.

## 10. Component Responsibilities

The Rust engine owns pattern compilation and bounded in-memory matching. The C
ABI defines status and ownership boundaries. The independent C++ wrapper owns
Plan/Session/Result handles, and the Query adapter owns segment access and row
mapping. Go owns bounded argument parsing and chain orchestration.

The [candidate execution design](20261013-query-grep-candidate-execution.md#module-responsibilities)
defines candidate/read-view preparation, native runner integration, row selection,
late materialization and distributed computed-result propagation. The initial
execution model does not require a general segcore iterator.

## 11. Verification Plan

### Go parser verification

Run from the repository root:

```sh
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/grep/...
go test -race -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/grep/...
go test -tags dynamic,test -gcflags="all=-N -l" -run '^$' -fuzz '^FuzzParse$' -fuzztime=15s ./internal/util/function/grep
```

The parser package can also be tested with `CGO_ENABLED=0`, without native Milvus
libraries or pkg-config configuration. Parser tests verify grammar and bounded
normalization; native matching and Query integration require their own tests.

### Acceptance Cases

The following matrix is for phase one. Context, multiline, fragments, counts,
and per-field match limits are not enabled by these tests and need separate
acceptance contracts when implemented.

| Area | Required cases |
|---|---|
| Argument parsing | Every short/long alias; grouped flags; attached/separated values; literal backslashes; single/double quotes; empty token; Unicode; repeated patterns; last-wins case mode; `--`; all rejected examples in Section 5.1 |
| Rejection | Unknown and roadmap-only flags including no-op values, positional paths, missing values, malformed quotes, conflicting boundaries, invalid UTF-8/NUL, invalid regex; no execution Plan published on compilation failure |
| Pattern semantics | Regex/literal, ordered OR, case modes, word/line boundaries, inline flags, forbidden Unicode disabling and literal LF, line/absolute anchors |
| Text/results | Empty field, blank line, trailing LF, retained CR, unterminated final line, NUL/BOM, invalid UTF-8, all occurrences, empty matches on UTF-8 boundaries, identical lines retained, exact content-relative spans |
| Limits | Arguments byte limit, configurable compile/cache limits, metadata-only batches, result bytes, checked arithmetic, request cancellation before/after compilation |
| Lifecycle | Compile retains no caller pointers; Plan release before Session; inputs/Session released before Result view; independent Sessions; synchronized cancellation/free |
| Failure | Whole-batch failure on any invalid input, budget, cancel or deadline; sticky cancellation; panic poisoning; no unwind across ABI; no partial result publication |
| Query integration | Raw arguments survive transport unchanged; shared grammar with per-segment operator construction and native compilation; Filter polarity, snapshot and NULL handling; selection before limit; row/detail alignment through reduction; unsupported-node rejection |
| Error propagation | Remote parse failures retain input classification through Proxy; execution failures through C++/executor/cgo to Go; input/system origin and retry classification audited end to end |
| Build | Execution-adapter symbol retention/export; header coexistence and C/Rust layout; clean parallel generation; merged graph; Linux Release/LTO; existing Tantivy regression |

Use a standalone search probe to trace file/network/process activity after
initialization, with relevant detailed logs suppressed. Separate loader,
allocator, and input page-fault effects from active external operations.

Benchmark short/long records, low/high match density, multiple batch sizes,
and concurrent Sessions. Measure Proxy forwarding, execution-side parsing/compilation, and steady-state
line/span extraction separately. Compare
final `libmilvus_core` size, build time, peak memory, result-copy cost, and
throughput against the same toolchain/profile baseline. Do not infer runtime
improvement from a smaller dependency count.

Acceptance includes failure-path verification from the error producer to the
consumer; successful matching alone does not establish error or retry behavior.

## 12. Open Design Parameters

- Service defaults and hard ceilings for batch size, compilation/cache budgets,
  output bytes, and concurrent Sessions require workload measurements.
- The execution adapter maps private EngineBuild/Allocation statuses into the
  existing Milvus error families by origin, without parsing diagnostic strings.
  A malformed internal descriptor is a system error, not a user parameter error.
- Cancellation latency on long individual matcher calls must meet the service
  objective. Cooperative checks do not provide hard preemptive interruption.

The selected ripgrep revision, local path dependency, single Rust static artifact
and memory-only C boundary are fixed by this design.

## References

- [ripgrep 15.2.0 release manifest](https://github.com/BurntSushi/ripgrep/blob/15.2.0/Cargo.toml)
- [Facade and default dependencies](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/grep/Cargo.toml)
- [Searcher implementation](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/searcher/src/searcher/mod.rs)
- [Private CLI parser boundary](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/core/flags/mod.rs)
- [Matcher builder](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/regex/src/matcher.rs)
- [CLI matcher/searcher configuration](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/core/flags/hiargs.rs)
- [Dual-license selection](https://github.com/BurntSushi/ripgrep/blob/15.2.0/COPYING)
- [MIT license](https://github.com/BurntSushi/ripgrep/blob/15.2.0/LICENSE-MIT)
- [Cargo dependency and path rules](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html)
- [Rust foreign-code linking](https://doc.rust-lang.org/reference/linkage.html#mixed-rust-and-foreign-codebases)

## Appendix A. Generated C Header

The source of truth is `internal/core/thirdparty/grep/src/ffi.rs`; its build script
regenerates [milvus_grep.h](../../../internal/core/thirdparty/grep/include/milvus_grep.h).
Read the generated header together with the implementation and native tests.
The C API is a same-build in-process ABI, not a wire format or stable plugin ABI.

The API provides compile, Plan/Session/Result ownership, cancellation and
all-input batch matching. A limited batch interface additionally accepts a
positive maximum number of selected inputs and returns a processed prefix with
complete per-input details. Quota completion is normal success; invalid input,
resource exhaustion, cancellation and deadline failures return no result. This
memory-level quota does not encode Query pagination or primary-key deduplication.
