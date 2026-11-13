# Grep NGRAM Index Acceleration

Created: 2026-11-03. Updated: 2026-11-09.

This document describes the current implementation and is the single maintained reference for Grep/NGRAM index design. Grep arguments, matching results, and the overall QueryScan protocol are covered by the [Grep integration design](20261008-segcore-grep-ripgrep-integration.md). Historical proposals and development logs are excluded; measured performance and its verification limits are summarized in Section 7.

## 1. Current Scope

- TEXT supports explicitly created NGRAM indexes through the existing scalar index build, persistence, and load paths.
- Grep extracts necessary literal conditions from Rust regex HIR and transfers them to C++ through Protobuf.
- Sealed VARCHAR/TEXT segments can filter candidates through the dedicated `INgramReader` condition-tree interface, then read original values and perform exact matching in Rust.
- Eligible VARCHAR queries scan a prefix bounded by input rows and UTF-8 bytes, returning when the limit is satisfied or querying the unread suffix through NGRAM when the budget is exhausted. TEXT remains index-first.
- This design does not introduce TEXT AUTOINDEX/HYBRID, integrate FM-index, add an index file format, or enable ordinary LIKE, `=~`, and `!~` filters on TEXT.

Long, mostly distinct documents are a target workload, not a data prerequisite. Short texts, duplicate values, NULLs, empty strings, Unicode, embedded NULs, and newlines must remain correct. Distinct documents may share many grams, so full-value cardinality does not establish substring selectivity.

## 2. TEXT Index Creation and Building

### 2.1 Admission Rules

| TEXT CreateIndex request | `scalarAutoIndex.enable=false` | `scalarAutoIndex.enable=true` |
|---|---|---|
| Explicit NGRAM with valid gram parameters | Create NGRAM | Create NGRAM, preserving the explicit type |
| AUTOINDEX | Reject | Reject |
| Missing or empty index_type | Reject | Reject |
| Another user scalar index type | Reject | Reject |

Both `min_gram` and `max_gram` must be integers satisfying `0 < min_gram <= max_gram`. Length follows the existing tokenizer's Unicode-character semantics, not UTF-8 byte length. No TEXT-specific gram defaults are introduced.

`ScalarAutoIndexEnable` does not automatically select NGRAM for TEXT. Proxy checks the explicit type before automatic index handling, excludes TEXT from scalar automatic resolution, and verifies that the resolved type remains NGRAM with `isAutoIndex=false`. VARCHAR automatic index policy is unchanged; its global policy can override an explicitly requested index type.

“Only NGRAM” refers to user scalar indexes and does not exclude existing full-text search features. NGRAM indexes raw text with its own gram tokenizer; the field's full-text analyzer does not determine its case handling, stop words, or stemming.

Code: [Proxy parameter resolution](../../../internal/proxy/ddl/task_index.go), [NGRAM parameter validation](../../../internal/util/indexparamcheck/ngram_index_checker.go).

### 2.2 Native Data Path

Index definitions, task scheduling, and publication use existing paths. The field schema remains TEXT rather than being relabeled VARCHAR. This change adds no independent metadata write path, DDL message, or index file format.

For Milvus-written manifest-backed TEXT, `BuildSession` uses `VisitTextFieldDataFromManifest` to decode inline/LOB cells before supplying text to the scalar NGRAM builder. External mapped columns use the existing generic read path. Encoded LOB references must not be indexed as document contents. NULLs, empty strings, and duplicate texts retain their original row coordinates, and index row count must match segment row count.

The existing NGRAM builder/loader and heap/mmap loading capabilities are reused. Successful creation or `state=Finished` alone does not establish a complete build: functional verification also requires full indexed-row progress, actual index files, and query results after loading.

Code: [BuildSession](../../../internal/core/src/indexbuilder/BuildSession.cpp), [input materialization](../../../internal/core/src/indexbuilder/BuildInputMaterializer.cpp), [NgramIndexBuilder](../../../internal/core/src/index/scalar/ngram/NgramIndexBuilder.cpp), [NgramIndexLoader](../../../internal/core/src/index/scalar/ngram/NgramIndexLoader.cpp).

## 3. Necessary Condition Tree

The required invariant is:

```text
Rust Grep matches a value => the value satisfies the index condition
```

The converse does not hold. The condition tree does not encode final match order, same-line requirements, boundaries, line numbers, or spans, and cannot replace Rust matching results.

The tree contains `True`, `Literal`, `And`, and `Or`. A Literal denotes a case-sensitive byte substring; True means no pruning. Multiple `-e` patterns are combined with OR. Literal-mode (`-F`) patterns are escaped before extraction. Case options and scoped inline flags are handled through Rust HIR. A case-insensitive pattern must not become a single lowercase literal.

| HIR structure | Conservative condition |
|---|---|
| Literal | A substring that must occur |
| Concatenation | AND of child conditions, with bounded combination of adjacent literal sequences |
| Alternation | OR covering every branch |
| Capture | Inner condition |
| Repetition with minimum zero | True |
| Repetition with minimum at least one | Inner necessary condition; the repetition count may be omitted |
| Empty expression, assertion, or character class without a useful bounded expansion | True |

Small character classes and case variants may be enumerated within the budget. Dropping some AND constraints is safe; dropping a viable OR branch is not. If an OR branch cannot be represented, the condition must be conservatively weakened. Regex validation and final matcher compilation still run: extraction fallback must not make an invalid pattern acceptable.

### 3.1 Extraction and Encoding Budgets

| Layer | Current bound | Behavior on exhaustion |
|---|---|---|
| Rust condition extraction | At most 127 nodes; literals are valid UTF-8 substrings of 2..64 bytes | Weaken conditions, using True when necessary |
| HIR traversal | 4,096 visits and 64 recursive levels | Weaken unanalyzed portions |
| Finite sequence expansion | At most 32 strings, each at most 64 bytes | Stop combination or discard the sequence cover |
| Additional literal-mode HIR preparation | 4,096 estimated bytes | Use True |
| Rust Protobuf encoding | 16 KiB and 192 Protobuf message levels | Encode a complete True root |
| NGRAM condition planning | More than 127 nodes, or literals unsuitable for min_gram | Conservatively weaken the tree or affected conditions |

These are optimization budgets, not additional regex request rejection criteria. Actual allocation failures, malformed internal trees, and matcher compilation errors remain errors. Protobuf depth includes intermediate messages such as `Conditions`; it is distinct from logical tree depth.

Code: [Rust Plan](../../../internal/core/thirdparty/grep/src/plan.rs), [condition extraction](../../../internal/core/thirdparty/grep/src/condition.rs), [encoding](../../../internal/core/thirdparty/grep/src/condition_proto.rs).

## 4. Rust-to-C++ Protocol and Ownership

The shared schema is in [cgo_msg.proto](../../../pkg/proto/cgo_msg.proto):

```proto
message LiteralConditionTree {
  LiteralCondition root = 1;
}
message LiteralCondition {
  oneof kind {
    AlwaysTrue always_true = 1;
    bytes literal = 2;
    Conditions all = 3;
    Conditions any = 4;
  }
}
message AlwaysTrue {}
message Conditions {
  repeated LiteralCondition children = 1;
}
```

This is in-process binary Protobuf, not JSON, regex text, or a persisted index format. There is no version field, reserved field, custom header, or separate Grep proto file. Rust and C++ generate their own bindings from the same schema; bytes are transferred without sharing language-specific object layouts.

Rust Plan caches the matcher, `max_count`, condition tree, and encoded payload. The C ABI exposes a view of the Plan-owned payload. C++ `Plan::ConditionPayload()` uses that view while the Plan remains alive, and `DecodeLiteralConditionTree` copies it into a `RegexLiteralCondition` that owns its literals and children. The reader borrows the tree only for the call and retains neither Rust pointers nor Session state.

C++ checks complete Protobuf parsing, root/kind presence, nonempty literals, and at least two children for AND/OR. It trusts Rust's size, depth, and UTF-8 guarantees rather than adding duplicate Grep payload limits; the Protobuf recursion limit is relaxed to accept valid Rust output. Structural errors report `DataFormatBroken` rather than becoming True. A structurally valid tree exceeding the planning budget can decode successfully and subsequently be rejected for pruning by the planner.

Standard Protobuf unknown-field and merge semantics apply. The decoder does not promise to detect every concatenation of valid fields. The pointer/length supplied by the call defines the frame.

Code: [Rust FFI](../../../internal/core/thirdparty/grep/src/ffi.rs), [C++ Plan wrapper](../../../internal/core/src/grep/Grep.cpp), [C++ decoding](../../../internal/core/src/grep/IndexCondition.cpp), [Rust binding generation](../../../internal/core/thirdparty/grep/build.rs).

## 5. NGRAM Reader and Candidate Bitmap

Grep uses dedicated methods on [INgramReader](../../../internal/core/src/index/contracts/query/INgramReader.h):

```cpp
bool CanHandleConditionTree(const RegexLiteralCondition& condition) const;
void ConditionTreeCandidates(const RegexLiteralCondition& condition,
                             TargetBitmap& candidates) const;
```

The reader receives a C++ condition tree, not Protobuf. Grep adds no PatternOp and does not use RE2 to parse Rust regexes. Ordinary filters using `RegexMatch` and Grep retain their respective matcher semantics.

`PlanNgramRegexCondition` restricts conditions to lookups supported by min_gram. Short literals become True. AND may omit True children, whereas OR containing True cannot query only the remaining branches. `ApplyNgramRegexPlan` applies intersections and branch unions to the incoming candidate set. Direct literal children of an AND are passed together to the reader; each OR branch starts from the incoming set.

Candidates use a bitmap over the full segment row domain and are intersected with the caller's input range. This is not a streaming interface that returns only enough candidates to satisfy limit. Grep currently has no lightweight candidate-estimation API or posting iterator; smaller offsets windows do not guarantee less underlying posting work. Regardless of candidate precision, Grep verifies values in Rust and generates complete matching details.

Code: [condition planning](../../../internal/core/src/index/scalar/ngram/NgramRegex.cpp), [tree evaluation](../../../internal/core/src/index/scalar/ngram/NgramRegex.h), [NgramIndexReader](../../../internal/core/src/index/scalar/ngram/NgramIndexReader.cpp).

## 6. Segment Routing and Execution

### 6.1 Index Eligibility

`PrepareGrepIndex` considers only:

- Sealed segments with nonempty input offsets.
- Fields with a whole-field NGRAM registration and an empty JSON path.
- Non-True conditions that the reader can handle.
- Capabilities providing ngram candidates, with no nesting and a matching field type.
- Pinned readers in the Row coordinate domain, with Count equal to segment row count and a matching ValueType.

Missing eligible indexes, growing segments, True conditions, and unsuitable domains or coverage use scanning. Actual errors during loading, pinning, decoding, index lookup, or reading propagate rather than being hidden as fallback. There is no additional explicit generation-ID comparison here; sealed read leases/snapshots and index pins protect access under the existing lifecycle contracts.

### 6.2 Bounded VARCHAR Prefix Scan

An eligible NGRAM query on a VARCHAR field first tries a bounded input prefix when original values can be borrowed as string views. All prunable condition shapes and all positive local execution limits are eligible: there is no OR-root, literal-count, or limit-128 gate. TEXT, including inline and LOB-backed values, and VARCHAR without borrowed raw views use Index directly. Missing usable indexes still use raw scanning.

The prefix has two independent internal budgets:

| Budget | Bound |
|---|---|
| Input positions | `min(input_count, clamp(2 * limit, 32, 256))`, saturating before multiplication |
| Logical text bytes | 256 KiB of complete UTF-8 field values passed to prefix matching |
| Batch size | Existing read window, remaining row budget, remaining result quota, and bounded batch growth |

The first batch has at most 32 positions; subsequent batches grow up to twice the largest accepted batch. Borrowed views expose value lengths before matching. Only the longest contiguous prefix fitting both budgets is accepted. A value that exceeds the remaining byte budget is left in the unread suffix, including when it is the first value; it is never skipped or split. NULLs and empty strings consume a row position but no bytes. Duplicate input positions consume budget independently.

```text
Prepare an eligible index and validate the original read window.
Within the row and byte budgets:
    borrow a batch of VARCHAR views
    accept and exactly match only its budget-fitting prefix
    limit satisfied or input exhausted => return
    budget exhausted or next value cannot fit => switch to Index
Query NGRAM once for the unread suffix, then verify candidates in input order.
```

There is no match-ratio decision and no transition from Probe to an unbounded raw scan. A misleading high-hit prefix cannot authorize scanning the whole suffix. Matches found in the prefix remain in the result and reduce the remaining quota. Original validation-window boundaries, duplicate-offset semantics, matching details, and error propagation remain intact.

The byte budget bounds logical text admitted to prefix matching, not physical I/O, page faults, chunk loading, or elapsed time. Borrowing views can still pin/load a column chunk. It does not impose a new request-size rejection rule, and it does not limit ordinary Scan/Index reads. A low-hit query pays for this bounded prefix before its index lookup; the constants are a conservative implementation choice, not a proven universal optimum.

These budgets are C++ implementation constants, not paramtable settings or RPC/C ABI parameters. `function.grep.batchSize` continues to bound the existing read windows. Re-enabling TEXT Probe requires separate validation of LOB read cost.

### 6.3 Ordering, Reads, and Resource Lifetime

- `BuildGrepCandidates` initializes the row-domain bitmap from the unread input suffix before calling the reader. A physical row appearing again in the suffix must not be removed merely because it was read in the prefix.
- Execution follows the original offsets array rather than sorting by row offset. Returned positions refer to the original input, preserving duplicate-record semantics.
- `GrepReadCount` validates the original window and handles VARCHAR chunk boundaries. Probe sub-batches do not reset those validation boundaries.
- The Index path collects candidate offsets per window rather than materializing every candidate offset at once. Ordinary Scan/Index raw reads still use window-sized batches and are not uniformly capped by the remaining limit; Rust matching stops according to the remaining quota.
- Owned TEXT reads deduplicate repeated physical offsets within a batch, then map the fetched values back to all input positions.
- After candidate generation, `index.reset()` releases the reader pin while the candidate bitmap remains independently owned. RAII releases resources on early return or exceptions; the sealed read lease continues to cover raw reads.
- `-m` caps matching lines per record, not index candidate count or the Query limit. `-m 0` returns an empty result after validating and compiling the pattern, without index lookup or raw reads. Positive values retain the routing described above.

Code: [SegmentGrep.cpp](../../../internal/core/src/grep/SegmentGrep.cpp).

### 6.4 Limits Across Segments

[QueryScan](../../../internal/querynodev2/segments/query_scan.go) runs an independent chain for each segment with the same request limit, waits for completion, and then merges results. Reaching the global limit does not immediately cancel other segments. Therefore, at most one NGRAM lookup per native call does not mean at most one lookup per request.

## 7. Performance Test Results

### 7.1 Measurement Method

All measurements below use one synchronous request thread and `MilvusClient.grep`. Unlimited cases explicitly pass `limit=None`; untimed request inspection verifies that the protobuf request contains no `limit` parameter. Small-limit cases use the same API with explicit limits.

Each native variant runs in an isolated standalone instance with identical raw and NGRAM-indexed collections. Raw/indexed request order alternates. The direct-Index library bypasses prefix scanning; the candidate library implements the bounded VARCHAR prefix in Section 6.2. Loaded library paths and stable binary hashes are checked. NGRAM uses `min_gram=3`, `max_gram=4`, with complete indexed-row progress and nonempty artifacts verified.

Timing includes the SDK call and complete result materialization, including lazy JSON decoding. Sorting and correctness checks are excluded. Validation checks IDs, ordinary projected fields, matching content, line numbers, line-start byte offsets, and UTF-8 spans. Full-corpus runs use streamed ingestion and bounded-memory oracles: independent match counts establish completeness, returned rows are checked against original text, and subsequent responses must match the independently verified result digest.

All table values are milliseconds, summarized as the median of round p50 values. Raw A is the no-NGRAM baseline from the direct-Index run; Raw B is the baseline from the candidate run. Each baseline is reported alongside its corresponding indexed run. TEXT bypasses prefix scanning, so its candidate path is also direct Index. Other indexes, including the fixture's vector FLAT index, are identical across collections and do not participate in Grep matching.

### 7.2 Million-Row Controlled Workload

Each collection contains 1,000,000 VARCHAR records of exactly 1 KiB. The uniform corpus matches where `id % 10 < 6`, including the first row. The back-loaded corpus has 400,000 nonmatching rows followed by 600,000 matching rows. Both therefore contain 600,000 matches. Data is generated and inserted in batches; only the first required oracle results are retained. Three rounds of five timed calls per path cover each case.

| Placement / pattern | Limit | Raw A | Direct Index | Raw B | Bounded prefix |
|---|---:|---:|---:|---:|---:|
| Uniform / literal | 1 | 20.21 | 39.73 | 20.20 | 20.76 |
| Uniform / literal | 10 | 27.87 | 41.53 | 20.04 | 20.21 |
| Uniform / literal | 100 | 30.11 | 45.66 | 24.34 | 24.74 |
| Uniform / and | 1 | 21.66 | 83.50 | 20.97 | 20.53 |
| Uniform / and | 10 | 21.31 | 83.87 | 21.00 | 23.06 |
| Uniform / and | 100 | 30.79 | 86.32 | 25.66 | 25.93 |
| Uniform / case_fold | 1 | 21.86 | 58.53 | 27.46 | 20.91 |
| Uniform / case_fold | 10 | 22.60 | 59.96 | 21.33 | 27.46 |
| Uniform / case_fold | 100 | 26.34 | 62.47 | 25.93 | 25.70 |
| Back-loaded / literal | 1 | 633.31 | 35.18 | 601.18 | 23.64 |
| Back-loaded / literal | 10 | 473.77 | 32.48 | 484.08 | 23.93 |
| Back-loaded / literal | 100 | 472.62 | 35.97 | 482.63 | 27.42 |
| Back-loaded / and | 1 | 747.64 | 66.28 | 738.23 | 46.16 |
| Back-loaded / and | 10 | 688.20 | 68.37 | 628.04 | 44.70 |
| Back-loaded / and | 100 | 616.88 | 72.01 | 610.24 | 48.54 |
| Back-loaded / case_fold | 1 | 868.85 | 46.61 | 786.30 | 30.20 |
| Back-loaded / case_fold | 10 | 740.56 | 41.22 | 677.00 | 28.16 |
| Back-loaded / case_fold | 100 | 780.89 | 42.53 | 690.12 | 30.95 |

Automatic sealing produced five raw segments and three indexed segments in each million-row collection. The largest segments contained approximately 560,000–577,000 rows.

### 7.3 Sampled Real Corpora

Original text was preserved without injected match markers or truncation. Seed-7419 reservoir samples were restored to source order. HDFS and Thunderbird each use 10,000 records sampled from 100,000 source records. Go and Python each use 5,000 snippets sampled from the CodeSearchNet test partition; Linux C uses 500 of 1,000 source files. Logs and snippets fit VARCHAR; Linux C uses TEXT. Each case has three rounds of twelve calls per path, with a paired raw baseline.

| Corpus / query | Limit | Returned rows | Raw A | Direct Index | Raw B | Candidate |
|---|---:|---:|---:|---:|---:|---:|
| hdfs / block | Unlimited | 10,000 | 265.92 | 263.55 | 266.64 | 278.94 |
| hdfs / receive | Unlimited | 1,486 | 70.26 | 65.22 | 73.06 | 65.89 |
| hdfs / warn | Unlimited | 300 | 23.88 | 20.36 | 23.44 | 20.91 |
| hdfs / exception | Unlimited | 3 | 11.28 | 7.56 | 11.05 | 7.47 |
| hdfs / failure | Unlimited | 7 | 13.64 | 9.31 | 12.92 | 9.31 |
| hdfs / block | 1 | 1 | 6.89 | 7.06 | 6.83 | 6.98 |
| hdfs / receive | 1 | 1 | 7.11 | 7.20 | 7.18 | 7.08 |
| hdfs / warn | 1 | 1 | 7.95 | 7.64 | 7.46 | 7.18 |
| hdfs / exception | 1 | 1 | 7.67 | 7.24 | 7.39 | 7.21 |
| hdfs / failure | 1 | 1 | 10.27 | 8.36 | 11.23 | 8.88 |
| thunderbird / error | Unlimited | 385 | 52.88 | 27.85 | 51.77 | 27.43 |
| thunderbird / warning | Unlimited | 301 | 43.68 | 24.72 | 48.81 | 26.12 |
| thunderbird / failed | Unlimited | 986 | 77.87 | 60.58 | 80.11 | 59.69 |
| thunderbird / kernel | Unlimited | 4,941 | 568.52 | 596.75 | 554.54 | 584.31 |
| thunderbird / severity | Unlimited | 1,309 | 91.68 | 68.62 | 95.22 | 73.91 |
| thunderbird / error | 1 | 1 | 7.85 | 8.20 | 7.61 | 7.77 |
| thunderbird / warning | 1 | 1 | 8.08 | 8.66 | 7.77 | 7.79 |
| thunderbird / failed | 1 | 1 | 7.99 | 8.48 | 7.77 | 7.84 |
| thunderbird / kernel | 1 | 1 | 7.45 | 7.89 | 7.04 | 7.17 |
| thunderbird / severity | 1 | 1 | 6.55 | 7.09 | 9.02 | 9.17 |
| linux_c / return | Unlimited | 338 | 170.00 | 161.92 | 163.65 | 176.43 |
| linux_c / static | Unlimited | 322 | 166.28 | 151.86 | 165.09 | 154.55 |
| linux_c / allocation | Unlimited | 29 | 62.12 | 16.80 | 60.67 | 16.28 |
| linux_c / mutex | Unlimited | 57 | 64.51 | 23.50 | 62.68 | 23.46 |
| linux_c / todo | Unlimited | 25 | 62.13 | 15.49 | 60.87 | 15.53 |
| linux_c / return | 1 | 1 | 11.91 | 8.73 | 12.36 | 9.18 |
| linux_c / static | 1 | 1 | 12.36 | 8.74 | 12.09 | 9.06 |
| linux_c / allocation | 1 | 1 | 14.63 | 7.41 | 14.13 | 7.24 |
| linux_c / mutex | 1 | 1 | 14.04 | 7.25 | 13.93 | 7.29 |
| linux_c / todo | 1 | 1 | 18.54 | 7.24 | 18.13 | 7.18 |
| code_go / function | Unlimited | 5,000 | 137.54 | 142.81 | 143.07 | 170.24 |
| code_go / return | Unlimited | 4,527 | 211.57 | 208.64 | 213.53 | 216.31 |
| code_go / error | Unlimited | 3,018 | 206.92 | 245.09 | 210.70 | 241.51 |
| code_go / error_check | Unlimited | 1,549 | 90.00 | 96.32 | 96.79 | 96.30 |
| code_go / panic | Unlimited | 159 | 28.71 | 14.10 | 29.34 | 15.74 |
| code_go / function | 1 | 1 | 6.98 | 7.09 | 6.95 | 6.93 |
| code_go / return | 1 | 1 | 6.91 | 6.93 | 6.81 | 6.75 |
| code_go / error | 1 | 1 | 6.81 | 6.87 | 6.80 | 6.75 |
| code_go / error_check | 1 | 1 | 6.37 | 6.49 | 7.33 | 7.16 |
| code_go / panic | 1 | 1 | 7.14 | 7.09 | 7.01 | 7.12 |
| code_python / function | Unlimited | 5,000 | 150.36 | 162.52 | 147.65 | 156.46 |
| code_python / return | Unlimited | 3,734 | 152.13 | 148.08 | 168.40 | 154.73 |
| code_python / raise | Unlimited | 919 | 73.97 | 54.63 | 69.30 | 57.27 |
| code_python / none_check | Unlimited | 955 | 69.68 | 61.15 | 71.24 | 63.33 |
| code_python / append | Unlimited | 545 | 56.98 | 38.10 | 57.70 | 39.77 |
| code_python / function | 1 | 1 | 6.51 | 6.51 | 6.33 | 6.39 |
| code_python / return | 1 | 1 | 6.36 | 6.66 | 6.81 | 6.81 |
| code_python / raise | 1 | 1 | 6.91 | 6.84 | 6.97 | 7.16 |
| code_python / none_check | 1 | 1 | 7.75 | 7.72 | 7.60 | 7.59 |
| code_python / append | 1 | 1 | 6.88 | 6.86 | 7.10 | 7.20 |

### 7.4 Full Local Real Corpora

The full run removes sampling. HDFS contains 100,000 records; Linux C contains 1,000 files; Go contains 346,365 code records and Python 457,461, including all archive partitions. Full Go/Python contain values exceeding the VARCHAR length limit, so their entire collections use TEXT to preserve all content. HDFS uses VARCHAR. Each case uses three rounds of three timed calls per path.

The following comparisons are complete for both native variants:

| Corpus / query | Limit | Returned rows | Raw A | Direct Index | Raw B | Candidate |
|---|---:|---:|---:|---:|---:|---:|
| hdfs/block | Unlimited | 100,000 | 2677.57 | 2771.78 | 2683.56 | 2793.20 |
| hdfs/block | 1 | 1 | 8.38 | 9.00 | 8.22 | 7.87 |
| hdfs/block | 10 | 10 | 9.08 | 10.00 | 9.51 | 9.59 |
| hdfs/block | 100 | 100 | 12.88 | 13.21 | 12.44 | 12.39 |
| hdfs/warn | Unlimited | 3,211 | 128.48 | 130.07 | 123.97 | 133.46 |
| hdfs/warn | 1 | 1 | 12.29 | 8.73 | 13.71 | 9.54 |
| hdfs/warn | 10 | 10 | 10.56 | 9.53 | 10.75 | 9.54 |
| hdfs/warn | 100 | 100 | 10.81 | 10.23 | 13.53 | 12.42 |
| linux_c/return | Unlimited | 687 | 340.84 | 300.96 | 337.41 | 270.94 |
| linux_c/return | 1 | 1 | 8.60 | 8.58 | 7.90 | 7.82 |
| linux_c/return | 10 | 10 | 10.34 | 10.43 | 10.50 | 10.22 |
| linux_c/return | 100 | 100 | 85.93 | 52.20 | 82.93 | 50.77 |
| linux_c/allocation | Unlimited | 56 | 154.02 | 23.45 | 145.52 | 24.63 |
| linux_c/allocation | 1 | 1 | 12.34 | 7.50 | 11.24 | 7.46 |
| linux_c/allocation | 10 | 10 | 78.69 | 13.79 | 80.66 | 14.27 |
| linux_c/allocation | 100 | 56 | 145.75 | 23.88 | 148.00 | 25.92 |
| code_go/function | Unlimited | 346,365 | 7942.40 | 8018.81 | 7579.48 | 7668.46 |
| code_go/function | 1 | 1 | 11.43 | 14.92 | 12.69 | 13.16 |
| code_go/function | 10 | 10 | 13.21 | 15.39 | 13.35 | 15.17 |
| code_go/function | 100 | 100 | 17.06 | 18.31 | 16.87 | 18.84 |
| code_go/panic | Unlimited | 7,239 | 517.81 | 203.30 | 535.23 | 203.13 |
| code_go/panic | 1 | 1 | 13.25 | 14.20 | 13.38 | 13.97 |
| code_go/panic | 10 | 10 | 15.03 | 14.78 | 17.25 | 14.96 |
| code_go/panic | 100 | 100 | 31.39 | 17.44 | 33.71 | 17.43 |
| code_python/function | Unlimited | 457,459 | 11569.11 | 11946.67 | 11548.57 | 11290.21 |
| code_python/function | 1 | 1 | 16.15 | 19.23 | 15.08 | 18.14 |
| code_python/function | 10 | 10 | 16.70 | 20.04 | 16.93 | 19.76 |
| code_python/function | 100 | 100 | 21.74 | 24.97 | 20.62 | 23.92 |
| code_python/raise | Unlimited | 83,157 | 3815.97 | 3082.67 | 3783.07 | 3022.27 |
| code_python/raise | 1 | 1 | 17.11 | 27.67 | 16.82 | 28.15 |
| code_python/raise | 10 | 10 | 17.29 | 27.24 | 16.53 | 27.76 |
| code_python/raise | 100 | 100 | 22.92 | 32.04 | 22.37 | 32.80 |

## 8. Follow-up

### 8.1 Performance Investigation and Optimization

The following performance concerns are deferred to follow-up investigation and
optimization. They remain unverified or unresolved; the measurements in Section 7
do not establish that these cases are regression-free.

- **Concurrent short-text queries:** Reproduce the reported PyMilvus throughput
  drop with and without NGRAM. Verify that all three clients send identical
  requests, including record limits and consistency settings, and collect
  QueryNode-side latency for the PyMilvus run. Profile any remaining server-side
  regression before choosing an optimization; client overhead is not an
  established explanation. See the [review discussion](https://github.com/milvus-io/milvus/pull/55136#discussion_r4538210369).
- **TEXT with few filtered offsets:** Compare direct row scanning with NGRAM
  lookup when a selective filter leaves only a small input, including
  case-insensitive patterns with many OR branches. Measure postings and bitmap
  costs against TEXT/LOB read costs, then evaluate a small-input scan threshold
  or reduced bitmap work. No threshold or performance benefit is validated yet.
  See the [review discussion](https://github.com/milvus-io/milvus/pull/55136#discussion_r4538209130).
- **Large TEXT index-build memory:** Test segments containing many 1–2 MB TEXT
  values, including manifest sources after sort compaction. Measure peak decoded
  data memory and compare it with segment-size and build-slot estimates; use the
  results to evaluate memory accounting and build buffering improvements. The
  review raised this as an existing-path concern exposed to more workloads by
  TEXT NGRAM, not a newly demonstrated regression. See the [review discussion](https://github.com/milvus-io/milvus/pull/55136#pullrequestreview-5859154647).

### 8.2 Production Metrics

TODO: Design production Grep metrics comprehensively in the next PR. Cover execution routes (Scan, direct Index, prefix completion, and prefix-to-Index), prefix rows/bytes and budget-exhaustion reasons, index candidate counts, stage latency, result volume, cancellation, and errors. Define metric types, aggregation boundaries (request versus segment), label cardinality, and collection overhead as part of that design.
