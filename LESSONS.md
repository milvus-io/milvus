# Engineering Lessons from Milvus Issues

Read the sections relevant to a change before implementing or reviewing it.
These lessons describe recurring failure modes, the contracts they violate,
and regression tests that can detect them.

The recurring problem is a contract lost when data crosses representations,
execution paths, versions, or asynchronous stages. Fix the contract and its
consumers, not only the function that reports the error.

The linked issues are historical evidence, not a list of defects present in
every current release. Some describe runtime reproductions; others describe
source-level findings, proposals, or behavior later clarified by maintainers.
Read the follow-up discussion before applying a lesson. This document
supplements [CONTRIBUTING.md](CONTRIBUTING.md) and
[CODE_REVIEW.md](CODE_REVIEW.md); it does not replace subsystem contracts.

## Data representation and query semantics

### 01. Make the coordinate system part of every offset contract

**An offset API must identify its domain: logical row, compact non-null row,
array element, or position within the current batch. Convert explicitly.**

For compact nullable vectors, validity describes logical rows while the payload
contains non-null physical rows. ArrayOfVector has used different layouts.
An integer type alone does not make these identifiers interchangeable. Sorting,
requery, refinement, and field retrieval must move IDs, values, and validity
through the same mapping.

- [#49881](https://github.com/milvus-io/milvus/issues/49881) documented compact
  nullable-vector assumptions that broke sorting, import, serialization, and
  retrieval. It explicitly distinguished ordinary compact vectors from the
  ArrayOfVector representation at that revision; that historical distinction
  must not be assumed to describe every later format.
- [#51842](https://github.com/milvus-io/milvus/issues/51842) reported values written
  by reserved offset while validity was appended in completion order. It also
  limited the exposure: the normal ingestion path serialized those writes at
  the time.

**Regression check:** Use distinct values with asymmetric validity, such as
`[true, false, true]`. Cover unordered and repeated offsets, all-null input,
nulls at both ends, chunk boundaries, and element-to-row mapping. Counts alone
do not establish alignment.

### 02. Distinguish null, empty, missing, and default by the API contract

**Do not infer nullness from payload length or silently substitute another
database's default-value semantics.**

A valid empty array has zero elements; validity identifies a null array.
Physically absent fields, omitted input, and explicit null must follow the
contract of the specific API and version.

- [#53438](https://github.com/milvus-io/milvus/issues/53438) reported Parquet
  `[]` becoming NULL even though an independent PyArrow read preserved the
  distinction.
- The [clarification on #52051](https://github.com/milvus-io/milvus/issues/52051#issuecomment-5148318983)
  explained that the insert contract then treated explicit null and an omitted
  value alike when applying a default. The report's title was not sufficient
  evidence of an implementation defect.

**Regression check:** Write an expected matrix for missing, explicit null,
empty, and default values. Compare SDK, REST, Parquet, and post-compaction
results. Treat a change to that matrix as a compatibility change.

### 03. Preserve three-valued logic through rewrites and execution

**Only the final filtering boundary may exclude UNKNOWN like FALSE.
Intermediate boolean operations and NOT must preserve UNKNOWN.**

The absence of null rows in ordinary query output does not justify two-valued
evaluation internally. `NOT(UNKNOWN AND FALSE)` and `NOT(UNKNOWN)` differ,
and valid short-circuit conditions depend on the consumer.

- [#50698](https://github.com/milvus-io/milvus/issues/50698) described separate
  validity losses in conjunction short-circuiting, final consumers, and offset
  evaluation.
- [#49891](https://github.com/milvus-io/milvus/issues/49891) reported a nullable
  `IN OR !=` predicate rewritten to AlwaysTrue, admitting null rows.
- The [follow-up on #51143](https://github.com/milvus-io/milvus/issues/51143#issuecomment-4918383232)
  identified an existing fix in #50979. The lesson is parity among raw, stats,
  and index paths, not a claim that the historical gap remains unfixed.

**Regression check:** Compare optimized and unoptimized evaluation, raw/index/
stats paths, and growing/sealed segments on the same data. Include NOT, missing
paths, cast failures, and UNKNOWN-heavy AND/OR. Verify that different paths
actually executed.

### 04. Apply algebra only after accounting for execution precision

**Establish where literals and fields are converted before merging ranges,
forming IN lists, or simplifying arithmetic.**

Integer limits, float32 narrowing, JSON doubles, and inclusive boundaries can
invalidate an otherwise plausible equivalence.

- [#52852](https://github.com/milvus-io/milvus/issues/52852) reported loss of an
  inclusive lower bound when merging OR ranges, with before/after deployment
  comparisons in the discussion.
- [#54176](https://github.com/milvus-io/milvus/issues/54176) concerns FLOAT
  rewrites that compare float64 literals before float32 execution. It is
  distinct from JSON numeric-conversion fixes.
- [#52049](https://github.com/milvus-io/milvus/issues/52049) reported REST dynamic
  integer output losing precision above 2^53 even when filtering remained exact.

**Regression check:** Test adjacent representable values, equal boundaries with
different inclusivity, swapped clause order, and enclosing NOT. Check input,
storage, filtering, and output precision separately.

### 05. Evaluation and skipping must consume the same input positions

**Eval, skip, overflow shortcuts, and constant-result shortcuts must share one
advancement contract. Skipping a callback must not silently skip its bookkeeping.**

- [#51537](https://github.com/milvus-io/milvus/issues/51537) described offset
  skip-index branches that did not call the callback, shifting subsequent
  bitmap-input positions. The [later validation](https://github.com/milvus-io/milvus/issues/51537#issuecomment-6120877087)
  correctly remained partial because SDK result comparisons did not prove
  execution of those skip branches.
- [#52258](https://github.com/milvus-io/milvus/issues/52258) reported a missing
  loop exit that advanced MoveCursor to the segment end.
- [#53896](https://github.com/milvus-io/milvus/issues/53896) reported divergence
  between a private overflow cursor and the data/validity cursor.

**Regression check:** Use B-1, B, B+1, and 2B+3 rows. Skip the first or middle
batch; include zero-element batches and sparse unordered offsets. Check result
and validity lengths and final cursor positions.

### 06. Local pruning must preserve everything needed by the global merge

**Prove that a local LIMIT, ordering, or aggregate truncation preserves the
information required for the global answer before pushing it down.**

Locally leading groups are not sufficient input for globally complete
aggregates. Correct result counts do not establish complete group contributions.

- The [discussion on #54311](https://github.com/milvus-io/milvus/issues/54311#issuecomment-6251407685)
  explains why partial group contributions must be merged before final
  ORDER BY and OFFSET/LIMIT.
- [#54381](https://github.com/milvus-io/milvus/issues/54381) reported tie
  reordering that permuted IDs but not their corresponding distances.

**Regression check:** Spread the same groups across shards and segments.
Include close scores, ties, and empty partials. Compare partitioned execution
with a single-partition reference and check complete ID/score/field tuples.

### 07. Do not build an oracle from a guarantee the API never made

**Confirm insert, upsert, duplicate-PK, and ANN semantics before classifying an
unexpected winner or fewer-than-topK result as an implementation bug.**

- The [maintainer response on #52047](https://github.com/milvus-io/milvus/issues/52047#issuecomment-5146714789)
  corrected the premise that ordinary insert promised last-write-wins for
  same-batch duplicate PKs or physical deduplication during compaction.
- [#52659](https://github.com/milvus-io/milvus/issues/52659#issuecomment-5345318441)
  likewise discusses undefined duplicate-PK behavior and the cost of changing it.

**Regression check:** Resolve an unclear contract before fixing an incidental
output in a test. Do not weaken assertions to hide a real regression, but do not
turn unspecified behavior into an accidental compatibility promise either.

## Storage and schema evolution

### 08. Follow old data through both reads and rewrites after schema changes

**A query-side default is not enough. Compaction, import/restore, and backfill
readers must also know whether a field physically exists.**

- [#52771](https://github.com/milvus-io/milvus/issues/52771) reported compaction
  materializing a synthesized default as NULL. Once persisted, the
  missing-field read fallback no longer applied.
- [#54642](https://github.com/milvus-io/milvus/issues/54642) described adding a
  sub-field, then dropping the last stored sibling before backfill. This removed
  the evidence needed to reconstruct element counts and row validity; the
  discussion also traced persistence of the wrong values through compaction.

**Regression check:** Mix old sealed rows, old growing rows, and new writes.
Exercise add/drop/reload/compaction/snapshot/restore. Persist independent
structural metadata, or reject operations that destroy its only source.

### 09. Publish a usable state, not just a newer schema version

**Schema, columns, indexes, and readiness must become visible consistently.
Do not publish a version before preparing the data it promises.**

- [#50281](https://github.com/milvus-io/milvus/issues/50281) reported a new schema
  becoming queryable before old segments had their added columns. Discussion
  identified staged publication and asked whether the add-field case was
  explicitly covered.
- [#50271](https://github.com/milvus-io/milvus/issues/50271) described
  `State=Importing` with `IsImporting=false`, exposing incomplete restore
  targets to recovery and snapshots.
- [#49695](https://github.com/milvus-io/milvus/issues/49695) described the reverse
  ordering error: removing ownership metadata before physical release finished.

**Regression check:** Pause between prepare/publish, copy/visibility, and
release/metadata removal. Readers must observe a complete old or new state.

### 10. A newer version does not prove correct content ancestry

**In-place manifest adoption must validate the base it was built from, not only
that its version is greater than the current version.**

Two tasks based on v10 may produce v11 and v12 without v12 containing v11's
changes. Recovery must not erase the dispatch fence.

- [#51376](https://github.com/milvus-io/milvus/issues/51376) described schema-bump
  output overwriting a concurrently published index.
- [#51723](https://github.com/milvus-io/milvus/issues/51723) found related gaps in
  sibling refresh/stats paths. Its follow-up records a persisted
  dispatched-manifest token rather than trusting a worker echo.
- [#53323](https://github.com/milvus-io/milvus/issues/53323) was split into
  #53328 and #53329; its closure explicitly did not establish either fix.

**Regression check:** Force read-base, concurrent commit, and stale adoption in
that order. Repeat across task recovery, old workers, batch updates, and ACK
boundaries.

### 11. Publish only after the data and every required sidecar succeed

**Write, close, upload, and copy errors must affect the final task result.
Success of the main file is not enough.**

- [#52178](https://github.com/milvus-io/milvus/issues/52178) reported ignored
  RecordBuilder errors producing nonempty but incomplete compaction output.
- [#52201](https://github.com/milvus-io/milvus/issues/52201) described partial
  field writes followed by replay after a swallowed flush error. Row counts
  could remain plausible while fields became misaligned.
- [#54978](https://github.com/milvus-io/milvus/issues/54978) reported index-build
  success despite failed validity or empty-offset uploads.
- The [resolution of #52125](https://github.com/milvus-io/milvus/issues/52125#issuecomment-5273516404)
  distinguishes acceptance of an asynchronous Azure copy from completion.

**Regression check:** Fail each persistence step independently. Verify no false
success, no visible partial artifact, and no duplicated field writes on retry.

### 12. A read error is not EOF, and unknown is not absent

**Preserve the distinction between exhaustion, missing objects, corrupted data,
and transient I/O. Do not return empty data merely to keep a pipeline moving.**

- [#53255](https://github.com/milvus-io/milvus/issues/53255) reported Arrow
  `Next=false` being translated to EOF without checking Err, allowing compaction
  to omit unread rows after a decode failure.
- [#53916](https://github.com/milvus-io/milvus/issues/53916) identified a
  checkpoint-protection concern when channel metadata could not be read.
  Subsequent fault injection verified a master metadata-GC boundary, not every
  downstream data-deletion path.

**Regression check:** Corrupt a later batch after an initial successful read.
Inject both authoritative absence and failed lookup into GC decisions; preserve
protection when the required state is unknown.

### 13. GC liveness must match the writer's reference model

**Missing reference metadata is not sufficient proof that a referenced data
object is dead. Investigate who omitted the reference before accelerating GC.**

- [#50730](https://github.com/milvus-io/milvus/issues/50730) reported deletion of
  referenced TEXT LOB objects. A missing merge-sort LOB context was a static
  root-cause hypothesis; the final comment closed the issue after non-reproduction,
  not a verified proof of that hypothesis.
- [#49585](https://github.com/milvus-io/milvus/issues/49585) progressed from GC
  scheduling starvation to missing segment-index cleanup in the drop path.
  The discussion emphasized fixing the source of orphans as well as the safety net.

**Regression check:** Flush, compact, drop source segments, run GC, and read the
output. Treat uncertain liveness conservatively and track reconciliation work;
permanently disabling cleanup is not a complete fix.

### 14. Migrate metadata sources along with storage formats

**Replacing binlogs with manifests also changes where row counts, byte sizes,
delete references, field sets, encryption, and index metadata come from.**

- [#53102](https://github.com/milvus-io/milvus/issues/53102) is a source-level
  report that Iceberg logical row counts survived while per-file delete metadata
  was lost during manifest creation.
- [#54993](https://github.com/milvus-io/milvus/issues/54993) reproduced a zero
  ArrayOfVector size estimate in a specific 3.0 build with a cherry-pick:
  the estimate still summed legacy binlogs, driving the DiskANN budget to zero.

**Regression check:** Cover manifest-only, legacy-only, mixed, recovered,
external, snapshot, and import layouts. Missing legacy metadata must not silently
mean zero rows or zero bytes.

### 15. Rolling upgrades require both read directions and intermediate writers

**A new reader accepting old data proves only one direction. Gate incompatible
new writes while old consumers remain, or preserve the old contract.**

- [#49907](https://github.com/milvus-io/milvus/issues/49907) reported relative
  stats filenames from a 3.0 DataNode being interpreted as complete object keys
  by 2.6 components.
- [#52988](https://github.com/milvus-io/milvus/issues/52988) concerns WAL-format
  changes when embedding materialization moves before append.
- [#54722](https://github.com/milvus-io/milvus/issues/54722) describes relocating
  validity while old components still read and modify the legacy copy. The
  report explicitly limited the initial change to unreleased builds.

**Regression check:** Exercise old writer/new reader, new writer/old reader,
an old intermediary rewriting and forwarding data, restart/replay, and supported
rollback. Version gates must represent actual capabilities.

### 16. Encryption acceptance must outlive the writer

**Encryption and decryption operations succeeding separately do not prove
recoverability. Persist the key metadata that belongs to the actual ciphertext.**

- [#52514](https://github.com/milvus-io/milvus/issues/52514) reported fresh
  per-slice DEKs whose EDEKs were discarded, while the directory retained an
  EDEK unrelated to the encrypted slices.
- [#53250](https://github.com/milvus-io/milvus/issues/53250) showed how CI could
  announce CMEK subpackages while repeatedly executing the root package.

**Regression check:** Write real fixtures, destroy the writer or process, then
load with a new reader. Cover multiple slices and keys, rotation, old artifacts,
reload, and wrong-key failures. Verify which tests actually ran.

### 17. Define flush durability against the intended crash model

**Data-file persistence and publication of durable metadata references must
satisfy the promised crash model, not merely survive process exit.**

- [#54436](https://github.com/milvus-io/milvus/issues/54436) reported local files
  without fsync while etcd durably stored their references, with syscall
  observations and failure details in the report.

**Regression check:** Distinguish process kill, container restart, and power
loss. Where required, enforce file fsync, rename, directory fsync, then metadata
publication. Check both recovered data and the absence of durable references
to non-durable files.

## Concurrency, lifetimes, and recovery

### 18. Keep one query generation alive through its last consumer

**Columns, schema, derived caches, iterators, and native views need compatible
ownership. Independent getter snapshots do not establish operation-wide consistency.**

- [#51693](https://github.com/milvus-io/milvus/issues/51693) discusses ownership
  of geometry caches and TEXT readers. Its
  [correction](https://github.com/milvus-io/milvus/issues/51693#issuecomment-5075992885)
  withdrew the assumption that unchanged segment ID and row count imply unchanged
  values: refresh and rewrite can replace existing data.
- [#52469](https://github.com/milvus-io/milvus/issues/52469) reported hybrid
  sub-searches sharing a mutable sealed snapshot, so one path's pruning modified
  another path's input.

**Regression check:** Pause after pinning, then reopen, replace, or drop.
Old readers must complete safely while new readers see the new state.
Share immutable state across fan-out; keep per-request pruning views separate.

### 19. Draining futures and propagating errors are separate obligations

**Drain every task borrowing the current frame before exceptional exit, and
inspect task results rather than only waiting for readiness.**

- [#51179](https://github.com/milvus-io/milvus/issues/51179) describes
  packaged-task futures whose destruction does not wait for remaining tasks:
  the first throwing get can destroy stack variables still borrowed by workers.
- The [Folly probe in #51902](https://github.com/milvus-io/milvus/issues/51902#issuecomment-5094935259)
  distinguishes wait from exception retrieval. A
  [follow-up](https://github.com/milvus-io/milvus/issues/51902#issuecomment-5097598861)
  adds runtime crash evidence and a reachable pool-reconfiguration trigger.

**Regression check:** Fail the first task while a later borrower is blocked.
Assert that borrows end before unwinding. Cover accepted tasks, cancellation,
worker-expansion failure, and pool reconfiguration.

### 20. Do not wait under a lock needed by the work that must finish

**Map the wait-for relationships before shrinking a critical section. Moving
RPCs outside a lock must preserve the ordering invariants that lock protected.**

- [#50788](https://github.com/milvus-io/milvus/issues/50788) reported enqueue
  blocking on a full channel while holding the mutex needed by QuerySlot and
  completion.
- [#51723](https://github.com/milvus-io/milvus/issues/51723#issuecomment-5987657349)
  includes a production account of object-store manifest commits holding global
  segment metadata locks and delaying unrelated work.
- [#50811](https://github.com/milvus-io/milvus/issues/50811#issuecomment-4804345795)
  explains why simply removing deleteMut was unsafe: it also preserved delete
  ordering and a gap-free catch-up-to-publication boundary.

**Regression check:** Combine slow I/O, full queues, and completion callbacks
needing the same lock. A lock-scope change must preserve ordered deletion and
gap-free publication, not just improve latency.

### 21. Atomic state does not replace a correct waiting protocol

**Checking state, registering a waiter, notification, and cancellation must
form a protocol that cannot lose a wakeup.**

- [#54148](https://github.com/milvus-io/milvus/issues/54148) is a source-derived
  lost-wakeup sequence: an atomic status does not close the interval between
  checking it and entering Wait when the notifier does not coordinate on the
  condition lock. The report explicitly says an earlier stress test did not
  reproduce the race.
- [#51837](https://github.com/milvus-io/milvus/issues/51837) reported a premature
  completion marker: a failed analyzer update had already advanced the version,
  so a retry skipped the missing side effect.

**Regression check:** Use barriers to force check-before-wait and
side-effect-before-version-update interleavings. Sleeps and a clean race-detector
run do not prove protocol correctness.

### 22. Model recovery around logical operations, not individual attempts

**Separate intent, attempt identity, ownership, and commit point. Late results
must not revive obsolete attempts.**

- [#49985](https://github.com/milvus-io/milvus/issues/49985) reported import
  retries retaining progress from earlier attempts, leaving row counts and
  final binlogs inconsistent.
- [#50499](https://github.com/milvus-io/milvus/issues/50499#issuecomment-5437096140)
  records no-fix and fixed deployment comparisons: an early CommitImport must
  not lose its intent merely because the local job is not ready.
- [#53315](https://github.com/milvus-io/milvus/issues/53315) is a source-level
  report about ordinary timeout cleanup treating a partially committed import
  as though nothing had become visible.

**Regression check:** Exercise early commit, lost ACKs, late worker completion,
results arriving after drop, and restart. Check final visibility and state,
not only RPC return codes.

### 23. Do not derive ordered semantics from completion order

**Timestamps, reserved offsets, acknowledgment watermarks, and physical
completion order are different dimensions.**

- [#52784](https://github.com/milvus-io/milvus/issues/52784) describes
  first-applied delete timestamps discarding a later-arriving, earlier delete
  and violating historical MVCC visibility.
- [#51671](https://github.com/milvus-io/milvus/issues/51671) identifies
  unacknowledged offsets in OffsetMap; returning an empty array is not a
  substitute for enforcing snapshot visibility.
- [#51842](https://github.com/milvus-io/milvus/issues/51842) uses reverse
  completion order to expose validity appended outside its reserved range.

**Regression check:** Include multiple PK versions, different delete timestamps,
and out-of-order reserved ranges. Validate historical as well as latest reads.

### 24. Sharing computation must not share one caller's cancellation rights

**A shared operation cannot simply inherit the first request's context.
Each caller owns its own cancellation and result.**

- [#53508](https://github.com/milvus-io/milvus/issues/53508) reported grouped
  searches failing together when the owner's context was canceled.
- [#54180](https://github.com/milvus-io/milvus/issues/54180) reported model HTTP
  requests not inheriting parent cancellation; a separate timeout is insufficient.
- The micro-batching proposal in
  [#50121](https://github.com/milvus-io/milvus/issues/50121) enumerated schema,
  namespace, response, and cancellation constraints but was rejected. Its
  constraints are useful; the proposal is not an adopted design.

**Regression check:** Cancel the owner, a follower, and all participants;
give callers different deadlines. Shared work should follow the remaining
consumers, with results mapped back per request.

## Failure isolation and resource control

### 25. Preserve error classification and bound retry work

**Keep invalid input, permanent failures, transient failures, cancellation, and
ownership loss distinguishable across language and component boundaries.**

- [#50903](https://github.com/milvus-io/milvus/issues/50903) treats input-error
  classification and retriability as separate axes.
- [#53187](https://github.com/milvus-io/milvus/issues/53187) reported malformed
  JSON retrying indefinitely. Its discussion notes that an inspector can
  recreate some failed task families, so one terminal-state assignment is not
  sufficient.
- [#54512](https://github.com/milvus-io/milvus/issues/54512) describes uncoded
  decoder/storage errors treated as transient by Import V3.
- [#51019](https://github.com/milvus-io/milvus/issues/51019) reported frequent
  redispatch sustaining an object-store quota storm.

**Regression check:** Test recovery from transient errors, permanent bad input,
cancellation, fencing, and retry-budget exhaustion. Confirm that both the
scheduler and any recreating inspector stop futile work. Follow the
[error-handling guide](docs/dev/error_handling_guide.md) when changing mappings.

### 26. Background throughput must leave resources for online progress

**Budget total in-flight bytes and stage occupancy, not only the size of each
individual thread pool.**

- [#50811](https://github.com/milvus-io/milvus/issues/50811) connects shared pool
  saturation during historical delete replay with stalled online tSafe progress.
- [#52669](https://github.com/milvus-io/milvus/issues/52669) explains why
  per-cache-slot limits do not prevent concurrent segment warmups from evicting
  one another.
- [#54295](https://github.com/milvus-io/milvus/issues/54295) proposes overlapping
  preparation, construction, and upload while accounting for memory throughout
  the transitions and spilling when construction cannot proceed.

**Regression check:** Combine constrained memory, long-tail I/O, oversized
segments, and mixed priorities. Preserve cancellation and a forward-progress
path for work larger than an ordinary admission budget.

### 27. Prove balancer convergence and account for the cost of moving

**A measured imbalance does not imply that every immediate move improves the
system. Include work still in flight.**

- [#51244](https://github.com/milvus-io/milvus/issues/51244) reported repeated
  A-to-B-to-A movement and cancellation of lazy-load searches. The discussion
  explains that a longer interval can reduce decisions based on unsettled
  distribution state; it does not prove convergence.

**Regression check:** Include pending moves, cold caches, heterogeneous segment
sizes, rollback, and cancellation. Measure imbalance, net progress, query
failures, and extra I/O separately.

### 28. Cache keys and invalidation must cover semantic identity

**A name, row count, or parameter key may be only part of an identity.
List the changes that invalidate a value before caching it.**

- [#49510](https://github.com/milvus-io/milvus/issues/49510) reported alias-keyed
  lookups missing entries stored under canonical collection names.
- [#52877](https://github.com/milvus-io/milvus/issues/52877) reported old offsets
  reused after dropping and re-adding a struct with the same name but a new ID.
- [#53460](https://github.com/milvus-io/milvus/issues/53460) reported duration
  caching without the conversion unit in the identity.
- The [correction on #51693](https://github.com/milvus-io/milvus/issues/51693#issuecomment-5075992885)
  shows that an unchanged segment ID and row count can still hide replaced values.

**Regression check:** Use the same name with a new ID, the same ID with replaced
data, the same key with different units, and aliases with changed targets.
Check invalidation, in-flight readers, and eventual cleanup.

## Performance work with measurable value

### 29. Remove repeated work before increasing concurrency

**Move invariants to the scope where they are invariant: row, batch, request,
segment, or shared immutable state.**

- [#52264](https://github.com/milvus-io/milvus/issues/52264) reported rebuilding
  the same field map per row. Its follow-up corrected an allocation assumption:
  the map was on the stack, so this was CPU work, not per-row heap pressure.
- [#52273](https://github.com/milvus-io/milvus/issues/52273) describes quadratic
  repeated proto.Size scans of an accumulated result under a shared lock.
- [#51981](https://github.com/milvus-io/milvus/issues/51981) reduces a merge heap
  to one entry per reader and removes repeated column resolution from comparisons.

**Regression check:** Measure scaling as input doubles, CPU, and allocations.
Inspect sibling callers on both read and write paths rather than fixing only
one loop.

### 30. Answer from small metadata before materializing large payloads

**Offsets, validity, footers, and coarse candidates can avoid data access,
provided the shortcut preserves the full semantic contract.**

- [#51378](https://github.com/milvus-io/milvus/issues/51378) proposes computing
  array length from offsets while preserving null-versus-empty behavior.
- [#53408](https://github.com/milvus-io/milvus/issues/53408) asks for bounded
  shredding reads without copying whole-chunk views, while retaining full-result
  cache and outer-batch semantics.
- [#50674](https://github.com/milvus-io/milvus/issues/50674) separates GIS coarse
  filtering from exact refinement so cheap predicates reduce geometry work.

**Regression check:** Count materialized rows and bytes, not only calls. Check
that coarse candidates contain every exact match, and cover nulls, incompatible
types, sparse offsets, and cache hits.

### 31. Count actual remote requests, not high-level batches

**Concurrency, range coalescing, metadata reuse, and connection reuse solve
different problems. Measure request sizes, counts, and first-access costs.**

- [#49616](https://github.com/milvus-io/milvus/issues/49616) explains why batched
  loading still creates small reads when byte-range gaps exceed Arrow's
  coalescing threshold.
- [#54063](https://github.com/milvus-io/milvus/issues/54063) distinguishes
  within-task client reuse from cross-task reconstruction. Singleflight alone
  cannot eliminate repeated sequential initialization.
- [#54768](https://github.com/milvus-io/milvus/issues/54768) proposes reusing
  footer, directory, and metadata by exact file identity while keeping
  request-bound context and reader state separate.

**Regression check:** Cover cold/warm, sequential/concurrent first access,
file replacement, cancellation, and multiple backends. Reducing HEAD/GET calls
must not remove format or corruption validation.

### 32. Do not present a microbenchmark as an end-to-end speedup

**State the measured boundary, workload, and remaining costs. Check that an
optimization preserves results rather than changing the workload.**

- The [correction in #52264](https://github.com/milvus-io/milvus/issues/52264)
  explains that an isolated map-rebuild ratio was not the speedup of the entire
  AppendFieldData operation.
- [#51981](https://github.com/milvus-io/milvus/issues/51981) adds package-level
  wide-schema and VARCHAR measurements beyond standalone merge machinery.
- [#52263](https://github.com/milvus-io/milvus/issues/52263#issuecomment-5209197645)
  corrects compilation claims made without the required test build tag.

**Regression check:** Record before/after SHAs, valid build flags, identical
result sets, and representative data. Separate CPU, allocations, peak memory,
I/O, throughput, and tail latency.

## API boundaries, observability, and acceptance

### 33. Every entry point must preserve validation and authorization

**REST, gRPC, streaming, management endpoints, and SDKs do not acquire equivalent
protection merely by sharing a wrapper.**

- [#50458](https://github.com/milvus-io/milvus/issues/50458#issuecomment-4806212568)
  records verification of both permission-denied responses and unchanged import
  job state after unauthorized commit/abort calls.
- [#52236](https://github.com/milvus-io/milvus/issues/52236) proposes descriptor
  enumeration to fail CI when a non-allowlisted RPC lacks privilege annotations.
- [#53661](https://github.com/milvus-io/milvus/issues/53661) illustrates another
  boundary mismatch: C++ validated UTF-8 byte length while Rust asserted
  character count for NGRAM literals.

**Regression check:** Cover every route, defaults, and malformed input. Assert
that rejected requests leave state unchanged. Align length, encoding, error,
and panic contracts across FFI boundaries. Historical reports do not establish
exposure in every current release.

### 34. Observability has hot-path and lifecycle costs too

**Bound label values, clean up on drop/rename, and avoid expensive statistics
when tracing is not recording.**

- [#54806](https://github.com/milvus-io/milvus/issues/54806) includes live metric
  observations of request-derived labels and series retained across object churn.
- [#54893](https://github.com/milvus-io/milvus/issues/54893) proposes distinguishing
  prefetch wait, evaluation, cache hits, and passing rows in an existing filter
  span, with extra bitmap statistics computed only when recording.

**Regression check:** Send varied unknown URLs and repeat create/drop/rename.
Assert bounded series growth. Distinguish filter output from downstream MVCC
visibility instead of giving different counts the same name.

### 35. A test must prove it reached the original failure path

**Different switches, descriptive test names, and a green run do not establish
that the intended behavior was exercised.**

- [#52373](https://github.com/milvus-io/milvus/issues/52373#issuecomment-5341690689)
  explains that differential tests can pass vacuously if both runs take the
  same implementation path.
- [#53250](https://github.com/milvus-io/milvus/issues/53250) reported CI announcing
  different subpackages while repeatedly executing the root package.
- [#51537](https://github.com/milvus-io/milvus/issues/51537#issuecomment-6120877087)
  explicitly downgraded acceptance to partial when skip-branch execution was
  not established.
- The [follow-up on #53597](https://github.com/milvus-io/milvus/issues/53597#issuecomment-6571944613)
  questions a failpoint outside the backend retry loop. Creating duplicates
  under a changed error-propagation path does not by itself prove a reachable
  failure in the unmodified system.

**Regression check:** Show the old implementation failing and the fix passing.
Use counters, traces, or failpoints to establish branch execution. Inject at
the actual failure layer while preserving retry and recovery. Record the SHA,
configuration, image, and untested cases.

### 36. Isolate tests and record why an issue is closed

**Tests may clean up only their own resources. Closure should distinguish a fix,
duplicate, intended behavior, superseded tracking, and non-reproduction.**

- [#51971](https://github.com/milvus-io/milvus/issues/51971) reported unit-test
  cleanup using an empty etcd root or shared object-storage prefix.
- [#53323](https://github.com/milvus-io/milvus/issues/53323) closed to split
  tracking, not to certify a fix.
- [#50730](https://github.com/milvus-io/milvus/issues/50730#issuecomment-4808416307)
  closed after non-reproduction; [#52051](https://github.com/milvus-io/milvus/issues/52051#issuecomment-5148318983)
  contains a contract clarification. These are different outcomes.

**Regression check:** Use independent namespaces, preserve external sentinels,
and bound teardown. Link fixes, branches, validation environments, and remaining
gaps when closing work. A master merge does not establish release or deployment.

## Pre-review checklist

Choose the boundaries affected by the change; not every PR needs the full
Cartesian product.

- [ ] **Representation:** logical/physical/element/batch offsets; null, empty,
      missing, and default values; dimensions, lengths, and precision.
- [ ] **Execution parity:** raw/index/stats/cache, sequential/offset,
      growing/sealed, ordinary/iterative/scorer consumers.
- [ ] **Old data:** add/drop/re-add, cold reload, compaction, manifest-only
      layouts, import and restore.
- [ ] **Concurrency:** pin then replace, validate then concurrent commit,
      reverse completion, throwing futures, cancellation and notification races.
- [ ] **Persistence:** visible write/close/upload/sidecar errors; no partial
      publication; retry and recovery without duplication or loss.
- [ ] **Compatibility:** old and new readers/writers and intermediate nodes;
      supported rolling upgrades and rollback on the actual target branches.
- [ ] **Performance:** largest inputs, cold/warm caches, sparse/dense matches,
      peak in-flight memory, and evidence of actual paths and operations.
- [ ] **Acceptance:** old code fails, new code passes; correct build tags,
      packages, SHAs, and fault-injection boundaries.
- [ ] **Follow-through:** tracked sibling gaps and clear distinctions between
      issue closure, merge, release, and deployment.

## Maintaining these lessons

Extend an existing lesson with a concrete counterexample and regression test
before adding a synonymous rule. Cite the most specific follow-up conclusion,
and update the main text when an earlier premise is corrected.

Keep runtime incidents, source-level findings, proposals, and independent
reproductions distinct. Do not count duplicate issues or sibling fixes as
independent incidents. A rejected proposal can preserve useful constraints
without becoming an adopted design.

Update subsystem documentation when an accepted change alters a contract.
Keep this document focused on reusable engineering lessons rather than current
bug status or a release-by-release fix inventory.
