# Expression cache follow-ups

Issue: https://github.com/milvus-io/milvus/issues/55658

Baseline: `0c421ad706`, master. This work is independent of the single-flight
implementation in #55567. The expression result cache remains disabled by default.

## Source audit

1. **Disk locking:** confirmed. `ExprResCacheManager::GetWithStatus` retains
   `state_mutex_` and `disk_files_mutex_` through `DiskSlotFile::Get`; `PutInternal`
   retains them through file creation and writes. Readers can run concurrently;
   writers exclude unrelated readers and writers. Retirement takes the exclusive
   state lock. `EntryPool::EvictForPut` examines up to three times the entry count.
2. **Growing:** confirmed. Eligibility is configurable, keys use generation zero,
   and active-count equality rejects old snapshots. The QueryNode release path
   conditionally calls a best-effort C API, while StreamingNode has no equivalent
   cache cleanup. Low hit rates during continuous ingestion are a workload
   expectation, not a measured result; in-memory predicates can still save CPU.
3. **Whole-filter cache:** confirmed gating, not a defect. Only the two-stage
   request builders opt in, and defaults disable two-stage search (thresholds
   are topk 2000 and five segments). Actual usage is unmeasured. Retain this
   existing optimization: stage 2 can reuse the entire predicate bitmap from
   stage 1. Leaf caching is not equivalent, especially for compound filters
   whose later children execute with short-circuit masks.
4. **TTL:** confirmed. `CreateTTLFieldFilterExpression` constructs a changing
   comparison without excluding it from raw or indexed leaf caching.
5. **Signatures:** confirmed repeated `ToString` construction, hashing and
   retained strings. `TermFilterExpr` preserves input order. Exact byte counts
   depend on literal types and sizes; the issue's numbers are examples.
6. **Hit copies:** confirmed full decode before offset gathering, batch copying
   even for a whole-segment output, and a further whole-filter clone.

The four separately listed observations are audited separately from these reviewed
items. This document does not claim measured throughput or tail-latency gains.

## Changes and invariants

### Disk lifetime and locks

Global locks protect lookup, registration, configuration and detachment only.
Readers and writers retain a shared file owner before releasing global locks.
Retirement removes discoverability immediately; operations already holding the
file can finish without recreating a retired generation. The last owner closes
and removes that file. File identities must distinguish replacement files so
delayed cleanup cannot unlink a newer file for the same snapshot.

Per-file readers hold a shared lock. Writers serialize with one another, reserve
an unpublished slot, write outside the index lock, then publish under the index
lock. Replacement preserves the old slot until the new write succeeds. Readers
must never see partially written slots. If no staging capacity is available,
caching may be skipped. Failures remain optional-cache misses, preserving query
evaluation and existing error semantics. Memory Clock work has a fixed scan cap;
an insertion that cannot fit within that cap is rejected without evicting an
insufficient collection of candidates.

### Sealed-only leaf caching

Remove the growing configuration and runtime eligibility, generation-zero
production identities, and the legacy erase API. Keep explicit negative tests
that growing expressions bypass caching in either backend.

### Preserve the two-stage whole-filter cache

Retain FilterBitsNode caching, request-level EnableExprCache, leaf write policy,
proto fields and merge compatibility checks. The earlier removal is withdrawn.
The two-stage algorithm has its own `autoIndex.twoStageSearch.enabled` switch;
its cache reuse also requires the shared `queryNode.exprCache.enabled` backend.
Both default to false. Normal requests can use leaf caching without two-stage
search. Whole-filter entries store the complete excluded-row bitmap before
MVCC/deletion visibility; leaf entries store a predicate result/validity pair.
They share the backend and budgets but use different signature domains.

Derive whole-filter keys from the memoized logical signature with domain 3,
including the TTL field and physical time whenever the read schema has TTL.
Move private whole-filter hits into output columns while retaining their leases.
Keep miss/budget-failure fallback and snapshot generation retirement semantics.

The request audit follows both producers (delegator and QueryView two-stage
builders) through worker requests, QueryNode/StreamingNode execution options,
segcore SearchRequest, AsyncSearch, SegmentInterface and both ExecPlanNodeVisitor
branches into QueryContext. It includes fallback restoration and task merging.
The proto fields and Go forwarding paths match the baseline. At FilterBitsNode,
request/backend gates, non-cacheable children, generation eligibility and
best-effort lookup failures still select normal predicate evaluation. The
existing `IsCacheable` propagation excludes predicates containing bloom_match;
this boundary was source-audited; its dedicated BloomFilter test was not run.

### TTL eligibility

Mark the injected time-dependent comparison and its enclosing OR as ineligible
for result caching before physical compilation. Keep `TTL IS NULL` under its
default policy: its result is stable within a snapshot. Preserve the local policy
on compiled expressions and consult it at raw decoration and indexed cache
boundaries. It does not spread to the user predicate or parent AND. TTL still
evaluates with the request's physical time and retains NULL semantics. This
leaf policy does not disable opt-in whole-filter caching, whose key includes
the TTL context.

`PhyNullExpr` currently has no shared result-cache integration: it reads validity
from the field or index and flips it for IS NULL; the index path retains validity
within one evaluator. Restoring its default policy does not itself introduce
cross-request caching. Non-nullable fields have a constant-result fast path.
Frequency admission already rejects most one-off keys before storage, so this
optimization primarily removes signature, lookup and admission overhead for
the dynamic comparison. No measured latency benefit is claimed.

### Signature identity

Memoize the canonical signature input and its 128-bit hash once on each
immutable logical expression. Copies across segment-specific expressions and
backend keys share the immutable input instead of copying large strings.
Normalize IN values for signing only; preserve typed literal distinctions and
existing evaluation data.

The hash is only a lookup accelerator. `ExprCacheSignature::operator==` compares
the complete canonical input and the complete sequence of derivation domains
when hashes match. Domains distinguish raw null-rejection modes, whole-filter
results and dynamic TTL context. Derivation shares the original string; it must
not discard identity by hashing only the parent's hash. Thus equal hashes for
different inputs or domains still miss or occupy separate entries/slots.

Both EntryPool and DiskSlotFile's in-memory slot index use this exact equality.
Disk headers continue to carry a 128-bit checksum, checked only after the exact
slot lookup; temporary disk files are never reopened to reconstruct an index.
Frequency admission remains intentionally approximate: colliding hash counters
may affect admission, but cannot establish result identity.

EntryPool conservatively charges the retained canonical input, derivation
storage and estimated allocation metadata per entry, including the old key's
capacity when replacing an existing map node. Sharing saves actual string
copies, but this change does not claim to charge shared strings only once.
Charging once across entries is a separate accounting optimization. The disk
budget continues to measure file bytes, not all in-memory slot-index metadata.

The signature correction adds forced full-hash-collision regressions for memory
and disk lookup, replacement and sparse reads, as well as exact domain identity,
shared input storage and retained-key budget accounting. These tests have not
been executed; builds and tests remain deferred at the user's request.

### Serving hits

Transfer a private full-segment decoded pair to a single output batch without
copying. Preserve its materialization reservation for the output's lifetime.
This also applies to TextMatch: the result and validity pointers alias the same
lease owner, so their reference count does not imply shared bitmap data.
Sequential adapters release their cache read handle after full decoding;
offset adapters keep it for subsequent gathers.
Offset reads produce only the requested bits in request order, including
duplicates; raw, Roaring and inverted Roaring payloads must agree with full
decode. Disk gathers read only touched words while retaining file/slot lifetime.
Validate offsets before any read and charge only the produced bitmap pair.
Prefetch must not force full materialization before the offset input is known.

## Validation

Use Dev CLI with the master baseline for native tests and affected Go packages.
Use controlled barriers and injected read/write failures for disk lifetime,
cross-segment progress, replacement, retirement, clear and configuration races.
Check bounded eviction rejection and reader-held payloads. Exercise ordinary and
two-stage search, TTL expiry/NULL behavior, growing bypass, signature reuse and
IN normalization, full-batch ownership, sparse/duplicate offsets and each codec.
Regenerate protobuf bindings with the repository target and update the removed
configuration option in paramtable and the shipped YAML. Functional
checks do not establish production performance gains; report actual test task
IDs, scope, failures, skips and untested scenarios with the change.

### Whole-filter restoration and subsequent TTL policy adjustment

- Commit `aa7f1ed7d1` was pushed before starting validation.
- Go cloud task `go-ut-local-lu-zhang-fix-expr-c-aa7f1ed-bbc22e05197aa3a1-u59677`
  succeeded: all 10 selected packages, 46 top-level tests and their subtests.
- C++ cloud task `cpp-ut-local-lu-zhang-fix-expr-aa7f1ed-20d5dbd097749688-u59677`
  was cancelled at the user's request while compiling; no completed C++ result
  is claimed for that restoration.
- The subsequent change retaining the default IS NULL policy updates the TTL
  regression's logical/physical policy assertions. It has not been built or
  tested. Further validation is deferred until the user requests it.

### Earlier validation (before whole-filter restoration)

These are Dev CLI **cloud** runs with `-b master`, not local execution or PR
Jenkins results.

- Main C++ regression, commit `9bea9fe622`:
  `cpp-ut-local-lu-zhang-fix-expr-9bea9fe-2361e6afb1504afd-u59677` succeeded.
  194 passed and 3 skipped out of 197 selected tests. The skips are the memory
  parameterizations of three disk-only concurrency tests; their disk variants
  passed. One existing disabled test was not executed. The scope
  includes expression cache, EntryPool, DiskSlotFile, compression, TextMatch,
  TTL physical time, FilterBits tracing, JSON flat expressions and FM index.
- Go API/configuration validation, commit `14fbd61010`:
  `go-ut-local-lu-zhang-fix-expr-c-14fbd61-afc4de01ed27ef26-u59677` succeeded.
  All 10 selected packages passed, with 46 top-level tests plus subtests:
  delegator, search tasks, segments, QueryNode/StreamingNode ViewQuery,
  QueryView client/server, segcore, initcore and paramtable. The selection
  covers two-stage search, request merging, execution options, segment/search
  calls and configuration callbacks. These results precede restoration.
- TextMatch follow-up, commit `9f12e1714e`:
  `cpp-ut-local-lu-zhang-fix-expr-9f12e17-2961ab1e6f7209aa-u59677` passed all
  7 selected `*TextMatchCache*` tests, including the new whole-output lease
  lifetime regression. These results precede whole-filter restoration.

Earlier C++ runs were not green: the `2553b3a81d` run found two old slot tests
that expected inconsistent active counts to be accepted; those tests now
verify rejection without overwriting an existing slot and replacement at a
fixed row count. The `14fbd61010` run found that the new JSON offset fixture
left the default disk mode without a directory. The fixture now explicitly
selects memory mode and removes raw data before warming, exercising the
index-only offset path. Both corrections passed in the main C++ run above.

No deployment E2E, sanitizer run or performance benchmark was performed.


## Separately audited observations (outside these follow-ups)

- **Config refresh flushes entries: confirmed.** `init_core.go` registers the
  same `UpdateExprResCacheConfig` callback for thresholds, capacities and mode;
  the C API calls `SetConfig`, which rebuilds the backend. This behavior remains.
- **Disk-pressure integration: confirmed.** `diskUsageFetcher` measures the
  local storage root through `GetLocalUsedSize`/`LocalChunkManager::GetSizeOfDir`;
  expression-cache files are under that root. `segment_loader.go` rejects loads
  against `DiskCapacityLimit`. Expression eviction runs on its own put/budget
  path, with no callback from that load-pressure rejection. This is distinct
  from the cache's own logical used-slot budget and remains a follow-up.
- **ResourceLimit fallback: confirmed for GetOrCompute.** It consumes boolean
  `Get`, evaluates on any failed lookup and can then attempt `Put`. Raw adapters
  already distinguish ResourceLimit and suppress capture; these follow-ups
  preserve that distinction when materialization is deferred to Eval.
- **Metrics: confirmed with a qualification.** The dedicated event counter is
  successful cache reads; miss/admission/eviction counters are absent. Memory
  and disk byte gauges also exist, so the cache is not entirely unaccounted.

Optional asynchronous disk writes are not included. Writes remain synchronous
on their calling query thread, but no manager lock covers their I/O. No claim
of measured production hit-rate or latency improvement is made.
