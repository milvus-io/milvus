# MEP: Growing JSON path extraction and index-state contracts

- **Created:** 2026-10-18
- **Author(s):** @pardeep-singh
- **Status:** Draft
- **Component:** QueryNode (segcore `index/growing`)
- **Related Issues:** #53855 (GJ01), #53856 (GJ02), #53857 (GJ03)

## Summary

Add `GrowingJsonPathExtractor`. It reads growing JSON rows and extracts one
configured path as VARCHAR into row-aligned batches. Each row gets a value or
a state: NULL row, missing path, or type mismatch. This document also fixes
the contracts that the later Growing JSON index work builds on:

- path and type identity;
- what "indexed coverage" means;
- how batches are published;
- when a query falls back to the scan.

This is the Stanford CS146S catalog project GJ01. It does not change query
behavior, the write path, plans or any public API. Nothing calls the
component yet. Delivering it does not make any query faster.

## Motivation

A sealed segment can carry a JSON path index (`json_path` + `json_cast_type`),
but a growing segment always scans JSON filters row by row. GJ02 (#53856)
adds mutable string-to-row postings for one path. GJ03 (#53857) wires them
into inserts and filtering. Both need the same input: for every Segment row,
the string at the path, or the exact reason there is none. The index answers
must match the scan, so the reasons must be defined by what the scan does
today, not by a new rule.

`IJsonIndexReader.h` already states the sealed rule: "JSON value extraction
belongs before the builder." `JsonValueProjection.cpp` does that extraction
for sealed builds over `FieldDataBase` chunks. Growing rows live in
`ConcurrentVector<Json>` and arrive in increments, so GJ01 provides the
growing-side equivalent with the same per-row calls.

## Scope

In scope:

- one fixed path per extractor, VARCHAR only, no cast function;
- borrowed batches of `Json` plus validity, at a Segment row offset;
- owned, immutable, row-aligned output batches;
- a contiguous extracted-row watermark;
- the coverage, publication and fallback contract for GJ02/GJ03.

Out of scope (from the #53855 boundaries, or owned by other issues):

- path discovery, several paths per extractor, arrays, numeric types and
  ranges, casts (`json_cast_function`);
- postings, read views, memory budgets and cancellation (GJ02);
- index metadata hooks, insert integration, query routing, deletes and
  timestamps (GJ03);
- any change to sealed segments, JSON stats or the public API.

## Interface

`internal/core/src/index/growing/GrowingJsonPathExtractor.h`:

```cpp
using JsonBatch = ScalarBatch<Json>;  // IGrowingIndex.h input view

enum class JsonPathRowState : uint8_t { Value, NullRow, Missing, TypeMismatch };

class GrowingJsonPathKey {
 public:
    static GrowingJsonPathKey FromPointer(int64_t field_id, std::string_view pointer);
    static GrowingJsonPathKey FromNestedPath(int64_t field_id,
                                             const std::vector<std::string>& nested_path);
    static bool IsSupportedCast(JsonCastType::DataType, std::string_view cast_function);
    int64_t FieldId() const;
    const std::string& Pointer() const;              // canonical RFC 6901
    const std::vector<std::string>& Tokens() const;  // unescaped
    JsonCastType::DataType CastType() const;         // always VARCHAR
    bool Matches(int64_t field_id, const std::vector<std::string>& nested_path) const;
};

JsonPathRowState ClassifyJsonPathString(const Json&, std::string_view pointer,
                                        std::string& value);

class JsonPathStringBatch {  // move-only, owns all strings
 public:
    int64_t RowBegin() const;  int64_t RowEnd() const;  size_t RowCount() const;
    JsonPathRowState State(size_t i) const;
    std::string_view Value(size_t i) const;   // empty unless State(i) == Value
    const JsonPathRowState* States() const;
    size_t CountOf(JsonPathRowState) const;
    ScalarBatch<std::string_view> AsScalarBatch() const;  // valid = Value
    int64_t ByteSize() const;
};

class GrowingJsonPathExtractor {
 public:
    explicit GrowingJsonPathExtractor(GrowingJsonPathKey key);
    const GrowingJsonPathKey& Key() const;
    int64_t ExtractedRowEnd() const;
    JsonPathStringBatch Extract(int64_t row_begin, const JsonBatch& batch);
};
```

The input reuses `ScalarBatch<T>` from `IGrowingIndex.h`, so the insert path
can build a `JsonBatch` from a `ConcurrentVector<Json>` chunk and the field's
validity the same way `FeedGrowingIndexRange` builds a `TextBatch` today.

## Path and type identity

An extractor serves one key: (field id, JSON pointer, cast type).

- **Pointer form.** The key stores the RFC 6901 pointer that
  `json_path` uses for sealed JSON indexes. Keys are unescaped with `~1` to
  `/` and `~0` to `~`. `""` is the document root, `"/"` is the empty key, and
  `"/a/"` is `["a", ""]`. These are three different paths.
- **Agreement with the scan.** `FromNestedPath` builds the pointer with
  `Json::pointer`, the same function the scan calls on
  `ColumnInfo::nested_path_`. `FromPointer` splits the pointer into tokens and
  checks that `Json::pointer(tokens)` gives the same string back. Both forms of
  one path therefore compare equal. GJ03 routes an expression to a key only
  when `Matches(field_id, nested_path)` is true, which compares unescaped
  tokens. For example, the key `"a/b"` (`/a~1b`) and the nested path
  `a -> b` (`/a/b`) never match each other.
- **Rejected pointers.** A pointer that does not start with `/`, or that has
  `~` not followed by `0` or `1`, throws `InvalidParameter`.
- **No `parse_json_pointer`.** `common/JsonUtils.cpp` `parse_json_pointer`
  drops empty tokens: `"/"` gives `[]` and `"/a/"` gives `["a"]`. simdjson
  and `Json::pointer` keep them. The key uses its own RFC 6901 split so the
  empty key works like the scan. The sealed path in
  `JsonValueProjection.cpp` uses `parse_json_pointer` for `path_exists`, so
  sealed indexes on empty-key paths may differ from the scan. This document
  does not change that code; it is listed under open questions.
- **Type.** The cast type is part of the identity even though only VARCHAR
  exists today. A later numeric extractor for the same pointer is a different
  key. `IsSupportedCast` accepts only VARCHAR with an empty cast function. Any
  other index metadata gets no extractor, and queries on it scan.

## Row states

Each state is defined by the scan's own calls on the same row:

| State | Condition | `==`, `!=`, `IN`, `NOT IN` | `EXISTS` |
|---|---|---|---|
| `Value` | `Json::at<std::string_view>(pointer)` succeeds | compare the value; valid | true |
| `NullRow` | validity bit clear | UNKNOWN `(0, 0)` | false |
| `Missing` | no string, `Json::exist(pointer)` false | UNKNOWN `(0, 0)` | false |
| `TypeMismatch` | no string, `Json::exist(pointer)` true | UNKNOWN `(0, 0)` | true |

- `Value` uses the call made by `PhyUnaryRangeFilterExpr` and `PhyTermFilterExpr`
  for VARCHAR JSON operands. The sealed `ProcessJsonFieldData` makes the same
  call. An empty string `""` is a `Value`, not NULL.
- `Missing` and `TypeMismatch` are split by `Json::exist`, the call
  `PhyExistsFilterExpr` makes. Following that call, `Missing` covers an absent
  key, an out-of-range array index, an intermediate value that is not a
  container, JSON `null`, and an empty object or array (including ones that
  hold only nulls and empties). `TypeMismatch` covers numbers, booleans,
  non-empty objects and arrays, and a string token simdjson cannot decode.
- `NullRow` rows are never parsed, so their payload may be empty bytes.
- Duplicate object keys, array indexes, `\uXXXX` escapes and escaped quotes
  resolve exactly as the scan resolves them, because the extractor makes the
  same simdjson ondemand calls.
- `ClassifyJsonPathString` copies the string at once. The view simdjson
  ondemand returns points into a thread-local parser buffer, which the next
  parse on that thread overwrites (see `Json::at_string_or_int64`).

Every non-`Value` state is UNKNOWN for the in-scope operators. GJ02 may
therefore treat them alike for postings, as the sealed JSON path index does:
`AsScalarBatch()` marks them invalid. The batch keeps the split anyway, so a
consumer can answer `EXISTS` or count states without parsing the row again.

## Batch publication

- **Alignment.** Entry `i` of a batch is Segment row `RowBegin() + i`. Every
  input row appears once, NULL rows included, so values and row ids cannot
  drift across batches.
- **Ownership.** A batch owns its strings in one arena. It borrows nothing
  from the input, so it stays valid after the growing chunk, the `Json`
  objects and the parser buffer change. The batch is immutable after
  `Extract` returns and is safe to read from several threads. It is
  move-only because its views point into its own arena.
- **Watermark.** `ExtractedRowEnd()` is the end of the contiguous prefix
  `[0, end)` that has been extracted. `Extract` rejects a `row_begin` above
  the watermark with `UnexpectedError`, because that would leave a hole.
  Ranges that start at or below the watermark are accepted. Growing rows are
  immutable, so extracting the same rows again gives identical batches. A
  consumer whose own append failed after extraction can extract again and
  retry. An empty batch is accepted and changes nothing.
- **All or nothing.** The batch is built locally, and the watermark moves
  only after every row succeeds. A malformed document throws
  `DataFormatBroken` with the row number. That is either the error from
  `Json::doc()`, or a simdjson exception from `Json::exist` mapped by
  `SimdjsonParseErrorToErrorCode`. No partial batch is returned.
- **Threading.** The extractor is not thread-safe. The Segment write path
  serializes calls, as `IAppendable::Append` requires.

## Indexed coverage

Three row boundaries, all Segment row prefixes:

```
extracted  E = GrowingJsonPathExtractor::ExtractedRowEnd()
accepted   A = rows GJ02 has taken into its mutable postings
covered    C = GrowingIndexSnapshotPin::CoveredRowEnd() of a published record
                                     C <= A <= E
```

GJ01 provides `E` and the batches. GJ02 owns `A` and `C`. The rules from
`index/contracts/README.md` apply as written:

- `C` is a complete prefix and never decreases. NULL, `Missing` and
  `TypeMismatch` rows count toward it, because their answer is known. Element
  counts, the number of values, or the highest row seen cannot stand in for
  `C`.
- A published record fixes `C`, the postings view and the per-row NULL
  information together.
- A failed extraction or append never advances `C`.

## Fallback

The scan stays the reference. GJ03 uses the index only when every condition
below holds. Otherwise it scans the whole expression:

1. The expression is VARCHAR `==` or `IN` (and `!=`/`NOT IN` as the
   three-valued complement) on a JSON column, with no cast.
2. A key exists with `IsSupportedCast` and `Matches(field_id, nested_path)`.
3. A non-empty pin has `CoveredRowEnd() >= ` the query-visible row count.

The contracts README also allows a covered prefix to be joined with an
exactly scanned tail. The scan is an exact raw evaluator for these operators,
so GJ03 may do that later. The minimum rule, and the one #53857 states, is a
whole-expression scan whenever `C` is below the visible rows. An extraction
error affects coverage only. Queries still scan, and the scan reaches the
same malformed row on its own terms, so a failure never changes an answer.
Deletes, timestamps and MVCC are applied by GJ03 on top of index results;
extraction depends only on row content.

## What GJ02 and GJ03 build on

- **GJ02 (#53856).** Take `JsonPathStringBatch` (or `AsScalarBatch()`) as the
  append input. Insert `Value(i)` for `Value` rows at row `RowBegin() + i`.
  Record all other rows as NULL for `INullReader`, and keep `CountOf` for
  statistics. Use `ByteSize()` when charging memory for a batch in flight.
  Publish `C` only at batch ends that have been fully accepted.
- **GJ03 (#53857).** Create one extractor per supported `(field, json_path)`
  index entry. Feed it from `FeedGrowingIndexRange`-style chunks of
  `ConcurrentVector<Json>` with the field's validity. Route expressions with
  `Matches`, and use the fallback rule above. `GrowingIndexSet` keys appenders
  by `FieldId` today. Several paths on one JSON field need a field+path key
  there, which belongs to GJ03's integration work.

## Compatibility

The change is additive: one header, one source file, one test file and this
document. The source is picked up by `add_source_at_current_directory_recursively`
in `index/CMakeLists.txt`. The test is added to `INDEX_TEST_FILES` in
`unittest/CMakeLists.txt`, so it runs in `index_tests` next to
`GrowingIndexTest.cpp` and stays out of `all_tests`. No existing code
changes behavior.

## Validation

`GrowingJsonPathExtractorTest.cpp` checks every extracted row against copies
of the scan's per-row loops for `==`, `!=`, `IN` and `EXISTS`, and checks row
alignment across batches. It covers:

- path identity: escaping, `~01`, the root versus the empty key versus a
  trailing empty key, field ids, rejected pointers, supported casts;
- a state matrix: NULL rows, absent keys, JSON null, empty and null-only
  containers, numbers, booleans, arrays, objects, empty strings, escaped and
  Unicode strings, case-sensitive keys, a non-object root;
- escaped and nested paths across batch cuts, including a key that contains
  `/` next to a real nested path with the same spelling;
- root pointers, all-valid and all-NULL batches, empty payloads under NULL;
- state counts and the `AsScalarBatch` view;
- batch ownership after the input, the parser buffer and the batch itself
  move;
- the watermark: holes rejected, retry identical, overlap, empty batches,
  bad arguments;
- a malformed row failing the whole batch with no watermark change;
- 200 randomized rounds of documents, paths and batch cuts.

## Open questions

- Should `parse_json_pointer` keep empty tokens? Doing so would align sealed
  `path_exists` with the scan for empty-key paths. It changes sealed behavior
  and is outside GJ01.
- `Missing` follows `Json::exist`, which treats `{}`, `[]` and containers of
  only nulls as absent. That is the current `EXISTS` rule. A change to it
  would have to change both together.
- Should GJ03 join a covered prefix with a scanned tail, or keep the
  whole-expression rule from #53857?

## References

- #53855: [Stanford GJ01] Define Growing JSON extraction and index-state contracts
- #53856: [Stanford GJ02] Implement dynamic postings for Growing JSON equality
- #53857: [Stanford GJ03] Integrate Growing JSON indexing with writes and filtering
- `internal/core/src/index/contracts/README.md` (growing publication protocol)
- `internal/core/src/index/scalar/json/JsonValueProjection.cpp` (sealed extraction)
