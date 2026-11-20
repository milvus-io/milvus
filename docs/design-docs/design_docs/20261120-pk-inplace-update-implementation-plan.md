# PK Inplace-update Implementation Plan

[中文版](20261120-pk-inplace-update-implementation-plan.zh-CN.md)

Implement the [design](20261109-pk-inplace-update-design.md) in **one Milvus PR**, using separate commits for the six stages below. The planning audit used master `c31070a6cf` on 2026-11-20. Production [PR #55742](https://github.com/milvus-io/milvus/pull/55742) extends its existing protocol foundation, based on master `20746425a1`. Use the feature patch from [Draft PR #55699](https://github.com/milvus-io/milvus/pull/55699), rebased at `ccce0c06fe`, as the Segcore reference; do not copy its unrelated master commits. That unmerged draft supports a sealed-segment INT64/VARCHAR POC, not the complete feature.

- Use only TS-vector + Offset Dictionary + updated-offset bitset; keep initial values in native base storage.
- Implement stages in the numbered order below. Keep new writes disabled until all stages pass acceptance.
- Include component tests with each stage and e2e tests in the same PR. Update external proto/SDK repositories only where required by the new API.
- Keep the design's existing schema/backfill exclusions, channel-level L0/Mix exclusion and ordinary Mix scheduling.
- Reuse master's transaction, materialization, TransformLog and QueryView mechanisms. Keep additional TS-vector performance experiments, bitmap checkpoint caches and alternative index structures outside this implementation; snapshot correctness remains mandatory.

## Stage review before proceeding

- Finish and review one stage before starting the next. Review Segcore subsections 4.1–4.4 separately; test 4.4 with controlled GC boundaries until Stage 5 provides runtime protection.
- Compare the actual diff with every implementation bullet in that stage. Record missing items and scope changes; fix unintended deviations and agree necessary scope changes before proceeding.
- Check each added data structure, configuration, cache, background task and state transition. Reuse existing code where possible and remove additions that are not needed for the listed behavior.
- Run the stage's tests and inspect results. Include applicable failure/retry, historical snapshot and boundary cases; compilation or success-path tests alone do not close those checks.
- Record a short review summary with completed plan items, changed files, test commands/results and remaining issues. Resolve blocking findings, then commit the stage separately in the same PR before moving on.

## 1. Proxy, request and WAL

Code entry points: Proxy request handlers, message types/builders, SN append validation, CDC replication, paramtable and milvus.yaml.

Master already supplies VChannel routing and transactions, but has no PK-update request/message. Continue the existing protocol foundation in [PR #55742](https://github.com/milvus-io/milvus/pull/55742) and its companion proto PR #700; keep subsequent Milvus stages in that same feature PR. Its read-dependent DML auto-load/retry path is not a prerequisite for literal PK-update writes.

### Code changes

- Add the PK-update RPC/request and WAL message. Include collection/schema identity, partition scope, `schemapb.IDs`, and assignments containing `field_id` plus a typed literal or explicit NULL. Generate bindings and add the SDK call.
- Disable automatic SDK retries for this RPC, including transport failures and schema mismatch. A lost response can follow a committed update; a replay with a fresh timestamp must not overwrite a later writer. Before release, replace temporary proto fork pins with the merged upstream version and verify SDK compilation from a downstream module.
- Register `proxy.pkUpdate.enabled=false` and `proxy.pkUpdate.maxPkCount` in paramtable and milvus.yaml; default the count cap to 10,000 submitted PKs. Invalid or nonpositive count configuration falls back to 10,000; the count and byte limits remain independent.
- Check the enable flag at public request admission. Turning it off rejects new requests while accepted updates continue through replay, materialization, CDC and reads.
- Validate permission, PK type, scope, assignment type/nullability and restricted fields. Reject empty or over-limit PK lists before deduplication, routing or WAL append; count the entire submitted list.
- Accept one assignment per field in a request; reject duplicate field IDs at admission. Literal updates must not fetch existing rows or require a loaded collection before WAL append. Preserve ordered writes from separate valid requests/transaction bodies.
- Enforce request byte limits separately. Bound both the incoming protobuf and the sum of prepared WAL envelopes by `proxy.grpc.serverMaxRecvSize`; repeated assignment payloads count once per fragment. Route admitted PKs to VChannels and split by complete encoded WAL message size, including assignments, using the selected WAL backend's limit minus the existing envelope reserve. RocksMQ uses the request budget because it has no per-record cap. Reject before any append if one PK plus the shared assignment set cannot fit or the prepared total exceeds the request budget.
- Put each VChannel's fragments in the existing transaction mechanism. Consumers use CommitTxn TimeTick and preserve body order; retain the existing cross-VChannel atomicity boundary.
- Add UPDATE to SN schema-version checks, message classification and CDC forwarding. Preserve assignments and source replay identity through every conversion.

### Tests

- Round-trip multi-PK/multi-field updates, explicit NULL, empty values and ARRAY; reject unsupported fields/types.
- Test PK counts 0, limit-1, limit and limit+1, duplicates and multiple VChannels; rejected requests must append nothing. Test long VARCHAR PKs and large assignments independently of count.
- Test different Kafka/Pulsar size limits, envelope headroom, incoming bytes, and prepared fanout at/over the total byte budget; every preflight rejection must append nothing.
- Test default-off, enable/disable, duplicate field IDs, schema mismatch, transaction rollback and CDC replay. Verify admission on an unloaded collection without an implicit pre-read. Disabling admission must leave accepted data readable.
- Test a committed update followed by a lost response and a later writer; the SDK must return the RPC error without issuing a second update.

## 2. Delta file codec and SN materialization

Code entry points: storage delta readers/writers and FFI, `vchannel/module.go`, `l0materializer.WALMaterializer.ObserveMessage()`, `walMaterializeTask.Execute()`, `SyncMaterializer` and durable-M publication.

Master constructs `WALMaterializer` in `VChannelRecoveryModule`; its active path retains Delete/Flush handles. Extend this running path, including its earlier-L1-publication gate, rather than wiring only the retained Summary consumer.

### Code changes

- Add the common typed Parquet writer: mutation timestamp, original transaction body ordinal, operation, PK list and per-field nullable assignment Structs. Store field IDs, logical types and collection/channel/scope/schema metadata.
- Persist `source_ordinal` as UInt32: standalone zero, otherwise the original transaction body position including skipped inserts. Reject ordinal overflow. Sort/deduplicate across files using VChannel, effective timestamp, ordinal and PK as appropriate; preserve the ordinal across scope splits and fragment re-cuts. Legacy equal-timestamp deletes may use ordinal zero.
- Encode absent assignment, SET NULL and SET value separately. Omit fields unused by the file; keep one shared assignment set for multiple PKs.
- Add projected reads of requested fields plus common columns. Adapt legacy L0, V1/V2 and packed V3 delete files to the same reader output.
- Extend `ClassifyTransformLogMessage()` and `BuildTransformLogEntry()` with UPDATE and mixed transaction bodies; an update must not become a payload-free barrier. Update `ObserveMessage()`, task extraction and the writer together to retain assignments and write the new format. Include updates in buffered-byte and operation counts, without counting them as deletes.
- Keep complete effective-TimeTick groups together. Register all required scope outputs before advancing materialization progress and notifying asynchronous durable-M persistence.
- Retain only the values needed after decoding; release input Arrow batches. Bound reader windows/concurrency and update the storage dependency if the required FFI is missing.
- Preserve legacy DELETE-only output during rolling upgrades. Do not downgrade already typed DELETE projections with nonzero source ordinals: keep the typed writer to retain their identity. Outputs containing UPDATE also use the typed writer independently of the current admission switch; accepted updates remain consumable when new writes are disabled.

### Tests

- Use the native FFI to round-trip mixed delete/update files, sparse fields, NULL and shared assignments; verify projected reads and legacy adapters.
- Repeat the wide sparse file case; record file size, decoded Arrow memory and peak RSS separately.
- Inject write/registration failure and retry. Unfinished work must not advance M; registered duplicates retain their original mutation ordering.
- Trace a real UPDATE message and a mixed transaction through classification, retained handles and native file decoding; assert the assignments survive and progress cannot advance after a payload is dropped or a scope registration fails.

## 3. L0 dispatch and Mix compaction

Code entry points: MixCoord L0 policy/task code, DN `l0_compactor` and `mix_compactor`.

Master groups L0 inputs by partition/channel. `L0CompactionTask.transformCursorUpdates()` already limits completed cursors by durable M and unselected-input holes; this does not implement fixed-M0 input selection. Reuse its publication safety while changing task selection and payload execution.

### Code changes

- Read persisted M once per scheduling round as M0. Select every registered input needed for that round's coverage through M0; exclude later inputs.
- Keep M0 and the selected scope/input set in the task across retries. Thresholds may trigger a round, but must not truncate its claimed coverage; an input owned by another task cannot be treated as already applied.
- If no eligible ALL_PARTITIONS input exists, create partition-scoped tasks, each with that partition's eligible L0 inputs and targets; retain channel-level execution exclusion.
- If any eligible ALL_PARTITIONS input exists, create one collection/VChannel task containing all eligible partition-specific and ALL_PARTITIONS inputs. Include targets in partitions with no private L0. For a target in partition P, deliver P's inputs plus ALL_PARTITIONS inputs in mutation order.
- Persist every applicable target's deltalogs and replay cursor, including zero-output targets, before completing the task or retiring shared inputs. Restore unfinished targets after retry/restart. An ALL_PARTITIONS input cannot retire after only one partition finishes. When catalog publication spans transactions, write each nonempty target's delta keys before its cursor; persist the all-target completion marker before input retirement.
- Process bounded input groups inside the same frozen DN task and collect all target outputs. Apply memory admission to the resident group, not the full prefix; retain original mutation identities across output files and certify M0 only after every group completes.
- Switch DN dispatch and Mix to the common delta reader. Merge records across files by effective mutation order, then apply PK-delete/update to eligible rows before writing compacted base data. Reuse this mutation application in ordinary sort/clustering rewrites; an excluded schema/backfill rewrite must explicitly reject unsupported update input before publication rather than discard it.
- Keep the minimum input cursor on Mix output. Include update files/bytes in backlog calculations while retaining existing admission and channel exclusion.

Example: with M0=200, inputs P1@100, P2@130 and ALL_PARTITIONS@160 belong to one task; P1@220 waits. P1 targets receive 100+160, P2 targets receive 130+160, and P3 targets receive 160 even without a P3 file. Retire shared inputs only after every applicable target's output/cursor is durable. A target with no BF hit still completes cursor coverage without writing an empty file.

### Tests

- Interleave partition-specific and ALL_PARTITIONS updates, including a target with no BF hit and a partition with no private L0.
- Verify files beyond durable M are excluded. Retry after partial publication and restart before retirement without losing any target's updates.
- Mix overlapping files, duplicate SETs and delete/update sequences; reload output and replay its suffix, then compare final values with independent replay.

## 4. Segcore

Code entry points: `UpdatesRecord`, `SealedUpdatesRecord`, `UpdatesRecordReaders`, Growing/Sealed segment apply, expression evaluators and output fill.

Master supplies native column Scan/Take, scalar indexes and Delete MVCC, but not this field-update store. Integrate the TS-vector draft with the complete write/load path below; its independent performance tuning is not a prerequisite for the feature stages.

### 4.1 Storage and apply

- Keep one UpdatesRecord per physical segment. Per field, store `vector<Delta<T>>` entries `{timestamp, offset, value}`, NULL flags, `offset -> vector<entry_position>`, membership bitset B and distinct updated-offset count.
- Index B by row offset, not timestamp; bit 1 means a retained delta exists and still requires Q_TS selection. Keep TS order in the vector and each dictionary history. Under the same lock/generation, B may include future-only updates, but cannot omit visible ones; derive U_Q during the query instead of storing timestamped copies of B.
- Extend the draft's INT64/VARCHAR storage to INT8/16/32/64, BOOL, TIMESTAMPTZ and ordinary scalar ARRAY. ARRAY updates replace the whole value.
- In `Apply()`, resolve all matching physical rows using scope, strict `row_ts < mutation_ts` and delete visibility. Same-timestamp newborn rows are not update targets. Prepare allocations, then append every field's entries and dictionary positions and set B before publishing completion.
- Preserve transaction body order and deduplicate replay by source identity. Replace the draft's strict per-cell timestamp rejection where valid transaction bodies share a commit timestamp.
- Add live apply to attached Growing and Sealed stores. Grow row-count/bitset capacity with Growing inserts; retain native base readers for unchanged cells.
- Keep fixed-width values inline and variable-length payloads owned/shared. Store no copied base versions and no history for unchanged offsets.

### 4.2 Expression evaluation

- For indexed predicates, retain the native index bitmap I. In `VisitUpdates()`, use `upper_bound(Q_TS)`, scan backward and evaluate only the first visible entry per offset; update result and validity together.
- Skip visited allocation when each retained offset appears once. Otherwise mark first-seen offsets even for NULL/non-matches, and stop at the prefix start or after selecting all distinct retained offsets.
- Correct only offsets with a visible delta: `(I - U_Q) union H_Q`. Exact Q_TS visibility is a correctness requirement: B must never be substituted for U_Q when clearing base hits, and future-only updates leave the base result unchanged. Derive U_Q during the visible-prefix traversal, or directly overwrite only selected offsets; no separate persistent U_Q bitmap is required.
- Do not add bitmap checkpoint caches in this version. If introduced separately, reconstruct U_Q from a protected checkpoint at or before Q_TS plus the complete retained gap; a latest bitmap containing future updates is not a valid substitute.
- For raw expressions, retain native Scan/Take. Use `ReadRangeUpdates()` to select updated candidates in the current batch, copy a private batch only when patches exist, and overlay values/validity before evaluation.
- Fix ColumnExpr and CompareExpr to use the same sparse overlay; unchanged rows must not call the update dictionary. Honor candidate masks before MVCC lookup.
- Return all-valid for non-nullable fields. Bypass stale index min/max, empty-result and all-null shortcuts, and disable unsafe expression caching on updated fields.
- Preserve native text matching on mutable VARCHAR. Evaluate selected visible delta values in bounded query-private Tantivy batches using the bound reader's registered analyzer and original MATCH/PHRASE/FUZZY options. Release each temporary batch after correction; do not add a segment-owned text delta index.

### 4.3 Lookup and output fill

- In `Gather()`/`Lookup()`, use B to bypass unchanged offsets. For updated offsets, check the dictionary's last entry, otherwise binary-search referenced timestamps at Q_TS.
- Use base when no visible delta exists; return NULL for visible SET NULL. Preserve requested order, duplicate offsets and search padding; use native bulk lookup when no requested offset needs an update.
- Pass the same Q_TS and protected physical store from filtering to output fill, deferred protobuf/Arrow output, sorting, grouping, aggregation and other updated-field readers.
- Reuse existing bulk reader/output interfaces and keep returned string/array storage alive until the consumer finishes. Further wrapper-allocation and conversion optimizations are a separate performance task.

### 4.4 GC

- Add `CSegment.GCUpdates(ctx, safeTS) error` in the Go segcore wrapper, `CStatus GCUpdates(CSegmentInterface segment, uint64_t safe_ts)` in the C ABI, and `SegmentInterface::GCUpdates(Timestamp safe_ts)` forwarding to `UpdatesRecord::GC(safe_ts)`; names are proposed. Pin the physical segment during the call and propagate failures through the existing status conversion. C++ never calls Go.
- Go selects safeTS from R_G/R_T according to the read paths protected for that physical store. Per offset, retain its last entry at or before safeTS and all newer entries; never erase an entire old timestamp prefix blindly.
- Reclaim vector entries and dictionary references together. If positions change, rebuild both in one publication and keep old storage alive for pinned readers. Keep B and counts consistent under the same lock/generation.
- Test with controlled boundaries first. Enable runtime reclamation only after Stage 5 prevents admission of reads requiring reclaimed versions.
- Test the Go/C/C++ call chain, repeated or older cutoff as a no-op, segment release racing GC, and an old reader retaining references while GC publishes replacement storage.

### Tests

- Cover change-away-and-back, repeated versions, multi-field updates, NULL transitions, old Q_TS, every supported type and repeated physical PKs.
- Compare reverse selection against independent replay, including uneven history depth and future-only updates. With first updates r10@100 and r20@200, verify B contains both but U_140 contains only r10.
- Apply two ordered transaction bodies that assign different values to the same cell at one commit timestamp; verify the later body wins, replay adds no duplicate mutation, and file/load ordering does not change the result.
- Check sparse allocation, failed-apply atomicity, append/GC reference safety and Growing insert/update interleaving.
- Verify indexed correction, raw overlay, ColumnExpr/CompareExpr, candidate-mask skipping and ordered output lookup at the same Q_TS. Count selected deltas and dictionary probes.
- Resolve or isolate the draft's reproduced executor teardown crash before reporting the full test process as passing; its current evidence is core-test and segment-assertion success only.

## 5. QueryView delivery, load and recovery

Code entry points: WALSummary/TransformLog, GrowingRuntime live/replay handlers, QN physical-segment loader, segment apply and QueryMVCC progress.

Master already has Growing bounded Transform replay followed by live WAL delivery, Sealed deltalog loading/Transform delivery, and QueryView streaming/requery. Their mutation handlers currently apply Delete, not field assignments. QueryView leases protect resources and Summary retention protects replay history; neither is an existing Segcore update-version GC service.

### Code changes

- Add UPDATE payload construction/classification to WALSummary and TransformLog; retain PKs, assignments, commit timestamps and complete transaction entries.
- Growing load/recovery: restore insert/base state, extend `drainDeleteReplay()` and `applyTransformLogEntry()` to replay PK-update TransformLog entries through BaseTransformTimeTick, then join existing QueryRuntime live events without gaps. Extend the current live WAL-message dispatch with UPDATE; do not add a second live subscription.
- Sealed load: load the base and read the segment's existing persisted deltalogs through the Stage 2 reader, including legacy deletes and typed update files. Merge by effective mutation order and dispatch Delete/Update to native apply; restore the update store even when no live updates follow. Release decoded Arrow batches after retaining the required values.
- Bind the loaded base/deltalog metadata version to `transform_start_after_timetick`; register from that cursor, then apply TransformLog history catchup and live entries using scope/BF filtering before exact PK matching. Never infer the replay cursor from the maximum timestamp found in a file. Retain the required history during load/registration and handle overlap by the agreed source identity/order.
- Keep ACK-prefix snapshot publication separate from consumer visibility. Advance the runtime's applied Growing/Transform frontier and readiness only after native apply succeeds; no later barrier may pass a failed apply.
- Preserve the preparation contract: exactly one `OnReady` or `OnUnrecoverable`. Report a later live-apply failure through a separate asynchronous runtime-failure callback. Reject new reads on that runtime, wake waiters, and allow a fresh generation to rebuild while old handles drain. A failed Ready View reports Unrecoverable; a failed Up View retains its durable Up recovery record and follows the existing query-triggered replacement path. Check runtime identity before acting on delayed callbacks.
- Fix each physical update store's tracked field set from its load scope: field ID, logical type, ARRAY element type and nullability. Compatible sets retain existing base/cursor sharing. A different set requires a new physical instance and Transform registration, using the shared buffer to replay from its own base cursor before readiness. Project persisted and live assignments with the same fixed set; scope expansion must not reuse a narrow instance's already-applied frontier.
- Pin the physical store and pass the successful query attempt's Q_TS through filtering, streaming/requery and deferred output. Reuse the existing worker/retained-plan path. Bypass stale SN segment pruning for updated fields.
- Register active read requirements and the minimum timestamp allowed for future admissions before deriving R_G/R_T. Coordinate registration with GC-boundary publication, select the safe cutoff for each physical store and call the new `CSegment.GCUpdates()` wrapper. Latest applied progress or resource reference counts alone are not safe cutoffs; also retain Transform history required by delayed loads/reconnects. Keep GC disabled until these read and recovery protections are tested.
- Keep JSONL injection opt-in for local tests. Production calls apply on the attached store rather than rebuilding a Begin/Apply/Commit POC session for each mutation.
- Report apply progress, update/version counts, delta memory and aggregated read-path counters; avoid per-cell logs/atomic statistics in hot loops.

### Tests

- Send real API updates to Growing, Sealed and unloaded collections; verify query/search visibility and output fields after the required apply frontier.
- Exercise initial load, partial persistence, restart, recovery/live handoff, release/reload and compaction replacement.
- Load a segment using persisted update files with no live tail; then test mixed legacy deletes/updates, NULL, historical Q_TS, overlapping persisted/replayed records and an update arriving during load registration. Compare with independent mutation replay.
- Test streaming on/off, historical requery, retries after partial output and deferred protobuf/Arrow fill. Discard failed-attempt metadata/results and retain the successful attempt's snapshot.
- Inject apply failure; verify queries cannot treat that segment as caught up. Check updated matches survive global pruning.
- Test live failure both before and after Ready becomes Up, waiter wakeup, replacement with old handles still pinned, and delayed callbacks from the failed generation. Load a narrow field scope, apply an update to an unloaded field without persisting it, then expand the scope and verify replay restores that update while the old read remains valid.
- Hold an old snapshot while applying updates and running GC; test delayed admission and registration races. Disable new writes, restart/replay and verify existing updates remain visible.

## 6. Local benchmark and e2e acceptance

- Generate base data, ordered dispersed updates and an independent final-value mirror. Compare returned offsets/PKs and projected values before timing.
- Run pure-memory `Segment::Retrieve` with one request thread, empty output fields and limit 1000. Match runtime raw/index residency, including STL_SORT-only loading where applicable.
- Use one ClickBench expression shape: `BinaryRange(ClientIP) AND Term(URL IN ...)`, with 100 deterministic bound/value parameter sets. Start with 1% and 5% single updates plus one separately labelled repeated-update case.
- Compare TS-vector/dictionary with the mirror under identical native index settings; measure raw and indexed configurations separately. Warm up, then run at least 1000 queries per configuration with expression cache disabled; exclude setup/apply/plan preparation and confirm execution counters advance.
- Measure output lookup separately with equality and empty versus selected INT64/VARCHAR output fields. Record latency, scanned/selected deltas, dictionary probes, bitmap/overlay work and RSS; measure load/apply separately.
- Run service e2e on equal resources and matched single-segment layouts. Include legacy files, CDC, materialization retry, durable-M lag, cross-scope dispatch, compaction/reload, apply failures and GC protection.
- Enable public writes only after these checks pass and supported schema/backfill combinations are documented. Keep the existing test instances and collections.
