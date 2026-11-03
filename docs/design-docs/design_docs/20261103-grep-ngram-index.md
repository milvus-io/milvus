# Grep NGRAM Index Acceleration

Created: 2026-11-03. Updated: 2026-11-05.

This document describes the current implementation and is the single maintained reference for Grep/NGRAM index design. Grep arguments, matching results, and the overall QueryScan protocol are covered by the [Grep integration design](20261008-segcore-grep-ripgrep-integration.md). Historical proposals and development logs are excluded; measured performance and its verification limits are summarized in Section 7.

## 1. Current Scope

- TEXT supports explicitly created NGRAM indexes through the existing scalar index build, persistence, and load paths.
- Grep extracts necessary literal conditions from Rust regex HIR and transfers them to C++ through Protobuf.
- Sealed VARCHAR/TEXT segments can filter candidates through the dedicated `INgramReader` condition-tree interface, then read original values and perform exact matching in Rust.
- Eligible wide-OR queries with small limits first probe the input, then use the internal fixed 0.05 match ratio to choose continued scanning or index lookup.
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

### 6.2 Probe and Fixed Match-Ratio Heuristic

Probe requires all of the following: an eligible NGRAM reader, an incoming condition rooted at OR, at least 32 Literal leaves in the whole tree, and a local execution limit at most 128. Other queries with an eligible NGRAM reader query the index directly.

The Probe path addresses a specific performance boundary. A high-frequency literal on the direct Index path can enumerate a large postings bitmap even when Query requests only a few results; the raw scan can satisfy that limit after reading only a few rows. Probe samples and refines a bounded prefix before deciding whether the unread suffix justifies a postings lookup. It therefore targets small-limit, high-hit queries without changing the exact matcher or claiming that every indexed query is faster.

```text
sampleRows = min(input_count, clamp(2 * limit, 32, 256))

Read and match small batches:
    limit satisfied or inputs exhausted => return
Once sampleRows inputs have been sampled:
    matches / sampled_rows >= 0.05 => keep scanning; no later index switch
    otherwise => query the index once for the unread suffix
```

```yaml
function:
  grep:
    batchSize: 128
```

| Parameter or boundary | Behavior |
|---|---|
| Fixed match ratio | Internal constant 0.05 in SegmentGrep.cpp; not configurable |
| Ratio equal to threshold | Continue scanning |
| L1, L10 | Sample cap 32; the fixed threshold requires at least 2 matches to continue scanning after a full sample |
| L100 | Sample cap 200; the fixed threshold requires at least 10 matches |
| L128 | Sample cap 256; the fixed threshold requires at least 13 matches |
| Fewer inputs than the sample cap | Scan to input exhaustion without an additional index lookup |

The match ratio counts input record positions, not matching lines or spans. NULLs count as nonmatches, and repeated offsets count at each input position. The first TEXT batch contains at most one row; the first VARCHAR batch contains at most 32. Subsequent batches are at most twice the largest prior batch, further bounded by the original read window, remaining sample size, and remaining match quota. A first batch with zero matches does not immediately trigger index lookup.

There is no Probe byte budget, fixed total sample of 32 rows, or `8 * limit` completion estimate. The sample cap limits observation before choosing a path; the query need not finish within that sample. An unrepresentative prefix can still select a slower path, and larger samples increase read work for low-hit-rate long texts. The internal 5% threshold is a heuristic, not a proven universal optimum.

The match ratio is fixed in C++ and is not carried by paramtable, Go execution limits, or the C ABI. Other execution budgets flow through paramtable, GrepOp, and the Go native adapter. The C ABI is built together with Go; structure changes require rebuilding both. No RPC parameter is added.

### 6.3 Ordering, Reads, and Resource Lifetime

- `BuildGrepCandidates` initializes the row-domain bitmap from the unread input suffix before calling the reader. A physical row appearing again in the suffix must not be removed merely because it was read in the prefix.
- Execution follows the original offsets array rather than sorting by row offset. Returned positions refer to the original input, preserving duplicate-record semantics.
- `GrepReadCount` validates the original window and handles VARCHAR chunk boundaries. Probe sub-batches do not reset those validation boundaries.
- The Index path collects candidate offsets per window rather than materializing every candidate offset at once. Ordinary Scan/Index raw reads still use window-sized batches and are not uniformly capped by the remaining limit; Rust matching stops according to the remaining quota.
- Owned TEXT reads deduplicate repeated physical offsets within a batch, then map the fetched values back to all input positions.
- After candidate generation, `index.reset()` releases the reader pin while the candidate bitmap remains independently owned. Choosing continued scanning also releases the pin. RAII releases resources on early return or exceptions; the sealed read lease continues to cover raw reads.
- `-m` caps matching lines per record, not index candidate count or the Query limit. `-m 0` returns an empty result after validating and compiling the pattern, without index lookup or raw reads. Positive values retain the routing described above.

Code: [SegmentGrep.cpp](../../../internal/core/src/grep/SegmentGrep.cpp).

### 6.4 Limits Across Segments

[QueryScan](../../../internal/querynodev2/segments/query_scan.go) runs an independent chain for each segment with the same request limit, waits for completion, and then merges results. Reaching the global limit does not immediately cancel other segments. Therefore, at most one NGRAM lookup per native call does not mean at most one lookup per request.

## 7. Functional Verification and Remaining Work

Functional verification covers explicit TEXT NGRAM admission, actual metadata persistence, DataNode building, index file existence, load/reload/restart, and complete-result equality between indexed and raw Grep paths. This does not establish acceptance for every deployment mode or failure scenario.

Primary test entry points:

- [TEXT index admission](../../../internal/proxy/ddl/task_index_text_test.go) and [NGRAM parameter validation](../../../internal/util/indexparamcheck/ngram_index_checker_test.go).
- [Native TEXT building](../../../internal/core/src/indexbuilder/NgramTextBuildSessionTest.cpp).
- [Rust extraction correctness](../../../internal/core/thirdparty/grep/src/condition_tests.rs).
- [NGRAM condition trees](../../../internal/core/src/index/scalar/ngram/NgramGrepTest.cpp) and [segment execution](../../../internal/core/src/grep/SegmentGrepTest.cpp): candidate supersets, original ordering, repeated offsets, NULLs, LOBs, limits, cancellation, index/read errors, and Probe transitions.
- [Configuration](../../../pkg/util/paramtable/function_param_test.go), [operator snapshots](../../../internal/util/function/chain/operator_grep_test.go), and the [Go/native adapter](../../../internal/util/segcore/grep_test.go).

### 7.1 Probe Evaluation Method and Remaining Acceptance Matrix

The fixed 0.05 match ratio was evaluated with controlled workload comparisons rather than a single synthetic query. Threshold sweeps require isolated native builds with different internal constants; the threshold is not exposed as a runtime configuration. Candidate values must follow the integer decisions made by the bounded sample. With enough inputs, `S = clamp(2 * limit, 32, 256)` and continuing Scan requires at least `K = ceil(threshold * S)` matches. Reaching `limit` returns before making this decision, so `K >= limit` cannot select continued Scan. A threshold of zero always continues Scan after the sample; every threshold at least one half selects Index whenever a decision is reached. Exhausting a shorter input returns without another index lookup.

Use the two-decimal initial grid `0.00`, `0.01`, `0.02`, `0.03`, `0.05`, `0.06`, `0.12`, `0.25`, and `0.50`. Dense spacing below 0.06 covers low-hit prefixes. At sample sizes 32, 64, 128, and 256, 0.03 preserves the integer decisions of 1/32 and 0.06 preserves those of 1/16. The 0.12 point approximates 1/8 (requiring 4, 8, 16, and 31 matches), followed by 0.25 and the 0.50 Probe-then-Index endpoint. Keep 0.05 as the current-default baseline. Also measure 1.00 independently as a Probe-then-Index control: its decision is equivalent to 0.50, but it provides an independent timing check. Neither value disables Probe. With two-decimal precision, the smallest positive threshold requires three matches in a 256-row sample, so one- and two-match cutoffs at that sample size are outside this initial grid. If the best region lies below 0.01, explicitly revisit this precision constraint before finer screening. For each limit, group thresholds with the same `min(K, limit)` decision before scheduling measurements. Limit 1 has only two classes: zero and any positive threshold; retain it as a control rather than a source of evidence distinguishing positive defaults. For representative limits 16, 32, 64, and 128, the sample sizes are 32, 64, 128, and 256. Validate finalists at common and boundary limits such as 2, 4, 8, 10, 17, and 100, where early returns and integer rounding differ. Around the best region, add adjacent integer-match boundaries `k/S` rather than arbitrary decimal steps. Reports must retain `S`, `K`, early returns, equivalent threshold classes, and actual route observations; eligibility or a predicted route alone does not prove that a query reached the threshold decision.

The remaining acceptance matrix below is a validation target, not a claim that all combinations have been measured. The completed screening in Section 7.3 uses fewer rounds and narrower query coverage.

The corpus matrix covers application and system logs, Go/C++/Python/Rust source, JSON-like events, natural-language documents, short and long VARCHAR, inline TEXT, and TEXT LOB. Record counts span 1K, 10K, 100K, and 1M where practical. Length distribution, duplicate content, NULL rate, and segment layout remain fixed across threshold runs.

The query catalog includes:

- case-sensitive and case-insensitive literals;
- explicit alternation and case-insensitive wide OR;
- multiple required literals such as `error.*timeout`;
- prefixes, suffixes, word boundaries, line anchors, character classes, optional branches, and repetition;
- log-oriented IP, status-code, timestamp, request-ID, and path expressions;
- patterns with no usable NGRAM condition, absent patterns, high-frequency tokens, and multiple `-e` patterns.

Queries are classified by extracted condition shape, Probe eligibility, actual match ratio, returned-match volume, and final Scan or Index decision. Non-Probe queries remain in the matrix as controls but do not determine the threshold. Controlled match ratios include 0%, 0.1%, 1%, 2%, 5%, 10%, 25%, 50%, and 100%. Row ordering covers uniform matches, front-loaded and back-loaded matches, clustered segments, a high-hit sample prefix followed by a low-hit suffix, and the inverse. These distributions test whether a bounded prefix is representative.

Query limits include 1, 4, 16, 32, 64, and 128, with limits above 128 retained as non-Probe controls. Concurrency includes 1, 4, 16, and 32. Measurements report p50, p95, p99, throughput, server CPU, and RSS for both warm-cache and cold-start cases. Index construction is outside query timing. Each threshold uses an isolated server and identical persisted input; threshold order is randomized, each group is warmed up, and performance groups run for at least 20–30 seconds across at least five rounds. Reports retain outliers and include confidence intervals.

Correct result equality with the raw matcher is mandatory. Among correct configurations, the selection metric is the geometric mean of per-workload throughput ratios with explicit penalties for p95 and worst-case regressions. A candidate default must not regress important workloads by more than 10% at p95 or 15% in the worst measured group. If candidates differ by less than 3% overall, prefer the threshold with lower cross-round variance and fewer severe regressions. Further changes to the internal heuristic require route counters exposing Probe eligibility, sampled rows and matches, selected mode, NGRAM candidate count, and time spent in Probe, index lookup, field reads, and exact refinement.

### 7.2 NGRAM Performance Results

The following measurements compare the same sealed collection before and after creating a whole-field NGRAM index. The baseline has no NGRAM index on the text field; ordinary scalar and vector indexes may already exist. Both phases use the same queries and data. The NGRAM index uses `min_gram=3`, `max_gram=4`. Requests use `MilvusClient.grep` from PyMilvus 3.1.0rc103 against an isolated standalone server with `GOMAXPROCS=48`. Index construction and loading are excluded from query timing. These are single-round measurements without OS page-cache eviction.

Full measures single-request latency across log and source-code patterns, sequential and bucket filters, and input sizes ranging from zero records to the full corpus. Query limits are 1 or 100. Concurrency measures sustained throughput at concurrency 4, 8, 16, and 32, using queries with ten matching records and a severity query on the 10K short-text corpus. Sparse varies matching-record counts across 0, 1, 10, 100, and 1,000, plus a severity query, at concurrency 1 and 4. Both throughput suites use the full input and Query limit 10,000.

Each table entry is the geometric mean of per-case speedup ratios within that dataset. Full uses baseline median latency divided by indexed median latency. Concurrency and Sparse use indexed QPS divided by baseline QPS. A value below 1 indicates a regression. The suites use different queries and limits, so their aggregate ratios should not be interpreted as the isolated effect of concurrency.

| Suite | Dataset | Corpus characteristics | Compared cases | Raw baseline | With NGRAM |
|---|---|---|---:|---:|---:|
| Full: latency | HDFS log lines | 100K VARCHAR records; average 141 bytes; individual log lines | 90 | 1.000x | 1.064x |
| Full: latency | HDFS log blocks | 1K TEXT records; average 32.1 KiB; multiline log blocks | 90 | 1.000x | 1.102x |
| Full: latency | Linux C source | 1K TEXT records; average 24.3 KiB; source-code records | 70 | 1.000x | 1.194x |
| Full: latency | Python source | 5K TEXT records; average 1.32 KiB; CodeSearchNet snippets | 70 | 1.000x | 1.061x |
| Full: latency | Go source | 5K TEXT records; average 651 bytes; CodeSearchNet snippets | 70 | 1.000x | 1.045x |
| Full: latency | Java source | 5K TEXT records; average 473 bytes; CodeSearchNet snippets | 70 | 1.000x | 1.059x |
| Concurrency: QPS | Short text, 10K records | Full input; ten-match and severity queries | 8 | 1.000x | 0.974x |
| Concurrency: QPS | Short text, 100K records | Full input; ten-match query | 4 | 1.000x | 4.988x |
| Concurrency: QPS | Long text, 1K records | Full input; ten-match query | 4 | 1.000x | 36.892x |
| Sparse: QPS | Short text, 10K records | Full input; varying match counts and severity | 12 | 1.000x | 1.807x |
| Sparse: QPS | Short text, 100K records | Full input; varying match counts and severity | 12 | 1.000x | 4.024x |
| Sparse: QPS | Long text, 1K records | Full input; varying match counts and severity | 12 | 1.000x | 14.191x |

Average record sizes describe UTF-8 text payload only. Full has 460 paired cases; Concurrency has 16 and Sparse has 36. Full records an initial request followed by five repetitions; its reported p95 has limited statistical resolution. Concurrency groups run for approximately 30 seconds. Repeated rounds are needed to quantify run-to-run variance.

The results show substantial gains for selective queries over large inputs or long records. Full gains are smaller because it also includes tiny filtered inputs, frequent patterns, and queries that can satisfy a small limit early. Aggregate gains do not eliminate regressions: the 10K short-text concurrency suite averages 0.974x, and its ten-match query at concurrency 32 drops from 1,391.1 QPS to 709.5 QPS, with p50 increasing from 20.90 ms to 37.76 ms. Per-stage execution times were not recorded, so the cause of these regressions requires further profiling. The measurements establish workload-dependent NGRAM benefits and costs.

### 7.3 Completed Probe Screening (2026-11-05)

These experiments isolate Probe behavior; they do **not** compare creating an NGRAM index with having no index. All variants have a real, fully built NGRAM index (`min_gram=3`, `max_gram=4`). No-Probe starts directly in Index mode for indexed inputs; 1.00 still pays for Probe and can finish within the sample. Experiments retain PyMilvus 3.1.0rc103. The repository E2E dependency update to rc104 does not change these measurements.

**Threshold screening.** HDFS and enwik9 VARCHAR, Go and Python CodeSearchNet TEXT, and Linux source TEXT were tested at limits 16, 32, 64, 100, and 128. Values were 0.00, 0.01, 0.02, 0.03, 0.05, 0.06, 0.12, 0.25, 0.50, and an independently measured 1.00. Equivalent integer-decision classes in the initial grid share samples and are explicitly marked as reused. Each independently measured workload has three rounds of 30 timed calls. The four added corpora reuse one sealed collection per threshold across rounds; the earlier Linux series uses a different ingestion schedule and is a separate reference, not part of the four-corpus aggregate.

Queries use case-insensitive five-letter literals whose actual extracted conditions pass the wide-OR eligibility check. This is narrower than general regex coverage. Query selection covers adjacent threshold decisions where available; HDFS and enwik9 do not cover every adjacent boundary. Route labels are predictions from the input prefix, not native route counters. Go/Python input comes from deterministic CodeSearchNet test-partition prefixes rather than a representative random source-code sample.

**Probe versus No-Probe.** An isolated native variant changes only the initial mode selection to skip Probe. Re-linking the unmodified object first reproduced the baseline library byte-for-byte after restoring its RPATH. The loaded library path is checked in the actual server process. Each corpus has six query patterns, five limits, three rounds, and 30 calls per group. The following p50 values in milliseconds are geometric means of per-workload, per-round medians, including high-hit queries that finish in the sample. They are not pooled latency percentiles.

| Corpus / field / records | Probe 0.05 | Probe 1.00 | No-Probe |
|---|---:|---:|---:|
| HDFS / VARCHAR / 100K | 10.65 | 11.51 | 11.33 |
| enwik9 / VARCHAR / 100K | 11.89 | 11.73 | 11.91 |
| Go / TEXT / 5K | 10.09 | 9.74 | 9.19 |
| Python / TEXT / 5K | 11.17 | 10.76 | 9.99 |
| Linux / TEXT / 1K | 45.55 | 39.28 | 34.26 |

Timing includes the SDK call and materializing returned IDs, which also materializes lazy result JSON. Index construction is excluded; tests use warm caches and concurrency one. Added-corpus runs and No-Probe runs validate complete returned rows and match details against the raw oracle; the original Linux threshold series checked IDs only. These table aggregates include early-return controls, whereas the threshold-selection summary excludes predicted quota-in-sample cases; those summaries must not be mixed.

The four-corpus threshold aggregate favors 0.25 over 0.05 by about 3.35% at p50 and 3.46% at p95, but individual workloads regress, including HDFS `termi`, limit 32, with about 23.6% higher cross-round geometric-mean p95. This does not establish a better general default. No-Probe improves the source-code aggregates while losing on HDFS and on several high-hit early-return subsets. The evidence supports workload-dependent routing, not a universal Probe speedup or a TEXT-specific limit cutoff.

Entry points: [corpus benchmark](../../../tests/scripts/grep_probe_corpus.py), [threshold report](../../../tests/scripts/grep_probe_corpus_report.py), and [No-Probe comparison](../../../tests/scripts/grep_probe_compare_no_probe.py). Local raw evidence is retained under `/tmp/grep-probe-multi-corpus-20261105` and `/tmp/grep-no-probe-20261105`; these are local artifacts, not repository-published datasets.

### 7.4 Controlled Severity Replay (2026-11-05)

The historical severity fixture was regenerated exactly: 100K VARCHAR records of 1 KiB each, seed 53960, approximately 60% matching records, and regex `(?i:warning|error|failed)`. Concatenated UTF-8 payload SHA-256 is `6bec062464fab7e7cf07c98e3bcbab266dc8ab9aafb2ae7d04147a77d405cf1f`. Actual extraction produces an OR with 100 literal leaves. This is a 100K-record experiment, not the earlier 10M-record throughput run.

The comparison uses the current fixed-0.05 implementation and a matching No-Probe native variant. The server executable and native dependencies are frozen, hashes are checked before and after each run, and process mappings confirm the selected library. Run order is No-Probe 1 → Probe 1 → Probe 2 → No-Probe 2. Complete results agree with the raw matcher and across all four runs. Earlier runs affected by executable/library drift were rejected and are excluded.

Serial cases have three warmups and 30 timed SDK calls. Unlike Section 7.3, timing excludes lazy JSON decoding and correctness checks. The two pairs are retained separately because absolute latency drifts substantially between phases.

| Limit / case | No-Probe 1 p50 ms | Probe 1 p50 ms | Probe 2 p50 ms | No-Probe 2 p50 ms |
|---|---:|---:|---:|---:|
| 1 | 13.22 | 4.72 | 9.96 | 14.94 |
| 10 | 9.27 | 4.00 | 10.04 | 15.15 |
| 100 | 11.73 | 5.83 | 12.33 | 17.55 |
| 127 | 12.32 | 6.74 | 12.95 | 18.22 |
| 128 | 11.48 | 5.70 | 13.08 | 18.21 |
| 129: non-Probe control | 10.17 | 10.34 | 18.01 | 18.12 |
| 100: 1% input filter | 6.94 | 8.95 | 10.70 | 12.36 |

Probe lowers serial severity latency in both pairs for limits at most 128; at limit 100 the reductions are about 50% and 30%. The limit-129 control is close within each pair. The filtered case has mixed results. At concurrency 16 and limit 100, two 15-second closed-loop groups give No-Probe → Probe QPS of 384.94 → 357.42 and 323.44 → 329.18. They do not demonstrate a repeatable throughput improvement. Concurrent timing includes full result validation.

Entry point: [severity replay](../../../tests/scripts/grep_severity_probe_ab.py). Local frozen inputs, hashes, raw samples, and comparison CSV/JSON are under `/tmp/grep-severity-frozen-ab-20261105`. Historical large-TEXT and adaptive-budget results describe a different Probe implementation; they do not establish current TEXT performance. Both field types currently remain eligible through limit 128, with a smaller initial TEXT batch; a stricter TEXT limit gate and byte/time budgets remain unimplemented.

### 7.5 Boundaries and Follow-up Work

- Ordinary TEXT LIKE, `=~`, and `!~` filters remain subject to the [parser's TEXT restriction](../../../internal/parser/planparserv2/parser_visitor.go). Creating NGRAM does not enable these filters. Existing full-text operators are separate from this restriction.
- TEXT AUTOINDEX/HYBRID, FM-index Grep integration, index-estimation APIs, limit pushdown into postings, and uniformly limit-aware raw reads are not implemented by this design.
- Complete CDC, compaction, remote object storage, and fault-injection acceptance remains outstanding. Reusing existing index definitions and lifecycle mechanisms is not evidence that these scenarios have been verified.
- Native build tests have encountered teardown failures, and embedded standalone instances have encountered shutdown problems. Passing functional assertions does not establish clean shutdown; index admission and Probe changes do not resolve those issues.
