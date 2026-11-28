# Cold Search validation — 2026-09-14 to 2026-09-15

> Historical fork validation. The results below apply only to the revisions
> named here, not to the consolidated master metrics PR. Current validation
> is reported in that PR; no historical performance number is a result for it.

Implementation branch: `codex/cold-search-observability`.
Initial validation base: `66bff8f29ad063494dc8d801483254e89a2d1103` (source tree
matches `86b1671569e7bafc28cd3803bf6d4c15c742f439`). Before PR submission, rebased
onto `877214740a9ba9609f1ebc2003b63b3131dc8306`, including the empty-segment
cleanup fix. Rebase preserved all implementation files byte-for-byte. All changes
are in an isolated worktree.

## Verified locally

Checks ran on macOS arm64 with local Milvus native libraries. The root module
selected Go 1.26.6; the `pkg` module selected Go 1.26.5. Go tests used
`-tags dynamic,test -gcflags='all=-N -l' -count=1`.

- Full package suites passed: coord balancer, coordview, syncer, QV observers,
  QueryNode qnview/qvresource/viewquery, shared queryclient/viewquery, SN snview,
  and SN BM25 idf.
- Targeted QueryCoord readiness/watch cases and SN GetQueryPlan cases passed.
- After rebase, full balancer, coordview, syncer, lifecycle-observer, SN snview,
  and SN BM25 idf suites passed again. The new base's targeted empty-segment
  cleanup and recovered-final-commit cases also passed.
- Proxy auto-load, independent cancellation, saturated queue bypass, early
  readiness failures, and same-cohort DQL timing cases passed.
- Search task/heap merge/marshal/reduce-layout unit cases passed.
- New pending-cohort and failed-catalog cases passed. Persistence failure did
  not dispatch sync or execute post-persist reference release callbacks.
- The independent-collection regression passed after the change. Temporarily
  restoring normal apply's Begin/Commit made it fail at the five-second guard
  with `B Ready->Up was held by unrelated A apply`; removing it passed again.
- Race checks passed for the stage recorder, syncer, and QV lifecycle observer.
- Changed-lines lint passed on all affected root packages (`0 issues`). Full
  lint of `pkg/util/stage` and `pkg/metrics` passed (`0 issues`).
- `make lint-fix` ran its formatters but full-repository type checking stopped
  at the pre-existing `internal/metastore/kv/querycoord/kv_catalog_test.go:372:
  undefined: mocks`. Targeted unfiltered lint also reports existing issues;
  changed-lines validation is not a claim that the whole repository is clean.
- `run_clang_format.sh` passed using clang-format 15. All unrelated formatter
  changes were restored. This patch contains no native source edits.
- Dashboard JSON parses and its 23 panel IDs are unique. It was not imported
  into or queried against a running Grafana/Prometheus instance.

After removing newly added tracing on 2026-09-15, all eleven full package suites
listed above passed again. Targeted Proxy, QueryCoord, SN query-plan, and SN
query-runtime cases also passed. The timer's race check verifies that each outcome
records a histogram observation exactly once and balances its inflight gauge,
without initializing a tracing provider. The final PR diff adds no OTel code and
removes no tracing that existed in the base revision. Syncer and lifecycle-observer
race checks also passed again after tracing removal.

The warmed metrics-only timer benchmark reported 96.46 ns/op, 0 B/op and
0 allocs/op on an Apple M5 with Go 1.26.5. This is a microbenchmark, not an
application overhead or latency improvement measurement.

## Baseline failures and unavailable integration coverage

The following failures were reproduced in the untouched base worktree:

- `TestSearchTask_PostExecute/Test_search_rerank`: `types.ProxyComponent is nil,
  not *proxy.Proxy`.
- `TestManagerRetriesViewBuildUntilSnapshotReady`: its one-second
  `attempts.Load() > 1` condition is not satisfied. Other runtime preparation
  behavior was inspected; no attempt was made to fix this unrelated test here.
- The additional full `pkg/metrics` race run failed in
  `TestCleanupProxyCollectionMetricsDropsEveryCollectionSeries` because an
  unchanged fixture passes six label values to an existing four-label counter
  (`metrics_test.go:295`). Running that same case on the untouched base reproduces
  the panic. This is separate from the new stage-metric timer tests, which passed.

A broader native Search-task run blocked in the test fixture's MinIO client
initialization (`setupTestSegments -> NewPersistentStorageChunkManager ->
NewMinioClient`), before the instrumented Search execution. The local test was
stopped and storage-independent unit cases were run separately. Native storage
integration and injected S3 faults are not verified by these passing unit tests.

No service was deployed, restarted, scaled, or subjected to generated Search
traffic. There is no post-change production/UAT result or verified catalog-write
amplification measurement. Hosted CI is tracked on the PR separately from these
local checks. The implementation and
remaining native/client attribution gaps are described in
[cold-search-observability.md](cold-search-observability.md).

## Rebase onto `990a0035bb` (2026-10-15)

The instrumentation was ported onto the `yz/qv` base `990a0035bb`, whose design
differs from the validation base above:

- QueryCoord owns automatic loading through `EnsureCollectionReady`. The Proxy
  auto-load stages became `coord/auto_load`, and QueryCoord returns the readiness
  path it observed in `Status.ExtraInfo["readiness_path"]`.
- Normal apply already avoids a registry-wide flush window, and `AddPreparing`
  acquires DataView references outside the manager lock. The independent-collection
  regression test was dropped in favor of the base's
  `TestBalancerApplyDoesNotHoldOtherShardEventsDuringReferenceWait`.
- The base no longer emits Coord persist and sync-accepted events, so flush feeds
  those view-lifecycle boundaries to the lifecycle observer directly.
- Segment load stages moved to `segmentResource.prepare`. StreamingNode BM25
  stats are read and decoded in one stream without a segment cache, so `read` and
  `decode` became `read_decode` and the cache hit/miss counters were removed.

Checks on the rebased tree: vet of all affected packages; full suites of
coordview, syncer, loadstatus, nodeview, viewquery, queryclient, QV observe,
SN adaptor/snview/idf/queryresource, QN qnview/qvresource, segcore and
storagev2/packed (except `TestMakePropertiesFromStorageConfig_Talon`, which
cannot create `/var/lib/milvus` here); targeted Proxy DQL/auto-load and
QueryCoord `EnsureCollectionReady` cases, including new readiness-path tests.
The balancer suite passes except tests that build a fresh paramtable (same
`/var/lib/milvus` failure) and `TestRecoveredLoadIdentityAvoidsRebuildButDetectsRealChanges`,
which fails identically on the untouched base. QueryNode Search-task tests abort
in the AWS SDK allocator during fixture setup, before instrumented code runs. The
`queryresource` suite crashed once with SIGILL inside a mockey-patched call and
passed on three reruns.
