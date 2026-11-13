# Paimon external collection implementation and validation

Date: 2026-11-13. This report distinguishes implemented cases from the full
reviewed plan; passing the cases below does not establish all plan coverage.

## Sources and deliverables

- Server PR #51897: `4f95cb61d849bd4ecb03fa1f641179825a58b05d`.
- Storage pin: `5c2ea329889e30d94b4e65209c0bc782256e7a95`.
- Python E2E base: `22341c46c91a3a573b794dd853c5739c62ba71cc`.
- Draft submission base: `b66a69d2d4a0e5756e577b1c3abf6b896898cb5b`; the five
  upstream commits since the validated Python base do not change `tests/python_client`.
- Worktree branch: `test/paimon-external-python-e2e`.
- Ordinary E2E: `milvus_client/test_milvus_client_external_table_paimon.py`.
- Independent GT/upload/search adapter: `common/paimon_fixture.py`.
- Offline writer/reader and physical Vortex converter: `scripts/paimon/`.
- Dedicated L3 tests, driver and recovery: `milvus_client/test_milvus_client_external_table_paimon_l3.py`, `k8s_phase_driver.py`.
- Optional test-image Go changes: `l3_test_hooks.patch`; no production Go changes
  are included in the ordinary test branch.

The fixture bundle has 11 tables, 28 versions, 257 unique blobs and 23,489,831
bytes. SHA256: `5e13b655273b0f8ee04e5a8b7452b86236e10b0b93f791c6c8794e094c93516a`.
The manifest records artifact hashes, Java generator/reader provenance and
conversion provenance. Java generator SHA256 is
`2e1d08fe791ea66e88883e772f9ae87a7fabc07aa56510116a89df51cd19ca72`.

## Jenkins source distribution regression

Actual PR #55507 [e2e-default build 1333](https://jenkins-milvus-ci.milvus.io/job/MILVUS-CI-V2-PR-PIPELINES/job/milvus-e2e-tcus-pipeline/1333/)
failed before starting pytest. At pipeline revision `86cc395434ba15ab3dfc36c1a70673e0a027ec7d`,
`tcus_source_bundle.py` rejects compressed source archives above 32 MiB.
Reproducing its exact pack operation on `8c6ba70f12` produced 35,511,194 bytes and
`Invalid source bundle size: compressed_bytes`. All ten shards were NOT COLLECTED;
this was a PR-induced packaging failure, not a failing Paimon assertion.

The fixture ZIP and offline builder now use standard-library LZMA compression.
All 258 decompressed archive entries, including the manifest, are byte-identical
to the original DEFLATE bundle; all blob hashes and generator provenance remain
unchanged. Initial runtime evidence in `validation-summary.json` refers to that
original 29,701,383-byte container (SHA256 `1972bb57e692e02b1173f97ee534b1f6bb6e4b3af5b6742b4f84517043f14496`).
The change does not remove fixtures, snapshots, vectors or independent GT.

A new offline regression validates the real archive and all 28 versions and
limits the fixture's compressed size to 26 MiB, reserving 6 MiB for other CI
sources. It failed on the old bundle and passes after recompression. The complete
helper suite now has 106 passing tests. CI entry/selection contracts remain
unchanged (10 passing contract tests). An additional four-case Cell rerun was
blocked in fixture setup by MinIO HTTP 503 responses; no new server result is
claimed for that run. A successful local source-pack validation is not an actual
Jenkins e2e pass; the next PR run must execute the ordinary CI matrix.

## Ordinary environment results

### Review regressions

The recovery regression executes the real driver `arm()` and `restore()` across
both apply/index phases, interrupting partial writes, failed renames, legacy
staging and delayed rename completion. It checks that both overrides are restored,
owned gates disappear and CAS is released exactly once. Separate ownership checks
retain foreign final/legacy gates while cleaning the current lease's partial write.

Switch scenarios now check persisted source, format, scan mode and snapshot after
each refresh and reload. The original same-S1 direct→split→auto sequence remains;
native-only MOR S2 followed by append S2 also checks different logical data and
exposes forced direct execution. Both format cases run pinned Paimon S1→plain
S2→pinned Paimon S1→S2, verifying complete rows/vectors/search before and after
reload, then validating failed-override preservation again after reload.

Offline mutation checks call the actual E2E scenario methods. The prior scenarios
missed all six injected faults: ignored Parquet/Lance switches, metadata-only
Parquet/Lance switches, retained direct-file configuration and forced direct-file
execution. The revised scenarios reject all six, with three correct-backend
controls. These are backend simulations, not mutated server execution results.
Fresh runtime and helper results are recorded in
`../../assets/paimon/review-regression-summary.json`.

| Review verification | Result |
|---|---|
| Offline fixture, recovery and switch regression | 105 passed |
| CI selection/entry contracts | 10 passed |
| Real mode switch and Parquet/Lance roundtrips, two workers | 3 passed, 101.03 seconds |
| Dedicated serial L3 shell entry, Cell backend | 3 passed, 270.04 seconds |

Neither runtime run skipped or reran cases. Both deployments were empty and
RPC-healthy afterward. L3 had no active or staging gate, control lease or config
override; effective wait=false and timeout=600 were independently checked. This
review rerun used the local Python runner against the same fixed Cell images;
it is separate from the earlier Linux CI-compatible evidence below.

### Initial full matrix

All levels below used one unmodified PR deployment, `paimon-51897-e2e`, in the
personal Milvus Cell vcluster. No wait/index threshold/global server configuration
was changed between L0, L1 and L2. Python 3.12 and existing E2E requirements were
used. Fixture writers, JVM, native storage tools and Kubernetes access were not
requirements of the ordinary runner.

Image: `harbor.tencent-stage.zilliz.cn/manta/milvus:pr-51897-4f95cb6`.
Runtime reported server commit `4f95cb61d84`.
Build: `build-pr-51897-4f95cb6-u3805`.

| Level | Actual result | Duration | Task ID suffix |
|---|---|---|---|
| L0 | 2 passed, no skips | 22.85 s | `fc6e8f13-u3805` |
| L1 | 24 passed, no skips | 132.61 s | `c5fb58c1-u3805` |
| L2 | 131 passed, no skips | 216.83 s | `b41fed80-u3805` |

Full task prefix:
`pysdk-e2e-local-yanliang-test-paimon-ext-22341c4-`.
Collection independently confirms 157 ordinary cases, with L1 below its 50-case
budget. L2 includes actual TEI direct-model GT and MinHash normal controls;
neither was silently counted from a skip.

The approved review corrections have explicit coverage: `-1` and `"-1"` read
latest, smaller negatives and missing/expired nonnegative IDs fail without fallback;
scan_mode's empty/null/default semantics have unique expectations; one-row and
empty exploration are separate. Mixed-schema auto routing and same-collection
mode switches compare full multiset/raw data. ANN refresh starts from an actually
built S1 HNSW/IVF index and verifies S2 before and after reload.

Initial failures exposed test setup issues, including a trailing table-root slash,
Int8 query encoding, MinHash parameter/raw-output requirements, URI quoting, and
assuming a multi-page iterator pinned a view across concurrent refresh. These were
corrected and the affected full levels rerun; no product failure was changed to an
unconditional skip/xfail or an accept-or-reject assertion.

## CI integration verification

The actual #51897 `ci-v2/e2e-default` was checked through Jenkins build
[1036](https://jenkins-milvus-ci.milvus.io/job/MILVUS-CI-V2-PR-PIPELINES/job/milvus-e2e-tcus-pipeline/1036/).
It used TCUS pipeline revision `c7d6c3e2d8b6b65a570427ad96c88d3ba3daf63f`:
source files are first selected by a `CaseLabel.L0/L1` expression, then pytest
runs assigned files with eight workers, `--tags L0 L1 --dist loadgroup`, two
reruns and a 1200-second case timeout. The original literal string tags passed
explicit-file runs but missed this outer file-selection phase. They now use the
standard `CaseLabel` constants. The actual selector/balancer includes the
ordinary file in exactly one Python shard; the L3 file is excluded.

All cases now live under `milvus_client/`. The separate `paimon_l3` directory and
its conftest were removed. Root conftest registers the profile and validates
serial/dedicated L3 selection. The dedicated entry is
`tests/scripts/ci_paimon_external_l3.sh`; see [L3 CI setup](L3.md).

A Linux Kubernetes runner used the existing CI Python environment helper and the
repository requirements (Python 3.12.11, PyMilvus 3.1.0rc106). Its registry-pinned
pytest base and recorded results are in `../../assets/paimon/ci-validation-summary.json`.
This is CI-compatible execution evidence; it is not a Jenkins e2e check result
for draft PR #55507.

| Verification | Result |
|---|---|
| Full Python-client root collection with L0/L1 | 9,488 selected, 9,293 deselected; no collection errors |
| Paimon within that root selection | 26 ordinary cases, zero L3 cases |
| TCUS-style eight-worker mixed execution | 26 Paimon cases + one existing external-table control: 27 passed, no reruns, 92.20 seconds |
| Dedicated serial CI shell entry | Three L3 controls passed, 149.59 seconds |
| L3 backend identity | Internal Service URLs plus projected service account; no personal kubeconfig or Cell CLI |
| Post-run checks | No collections, active gates, CAS lease or remaining config overrides; wait=false, timeout=600 |

The Linux full-root run avoided the workstation's unrelated XGBoost/libomp
collection limitation. Runner bootstrap required its venv to own pytest/py
entrypoints rather than fall through to older global packages. A temporary Nexus
NCCL-wheel timeout was handled by installing the same required version from
upstream PyPI; no repository dependency pins or test expectations were weakened.

The GT adapter now decodes equal `rows_blob` content only once per worker while
still validating every version's provenance, snapshot and blob hash. A local
single-process `FixtureBundle` measurement dropped peak RSS from about 792 MiB
to 390 MiB; this is not a CI memory benchmark. The cache has positive and negative
coverage for shared payloads, different data with the same row count and an
invalid snapshot bound to a shared payload.

L0/L1 need no new CI switches or deployment changes. L2 remains selected by L2
jobs. L3 is a separate deployment/job; this patch provides its standard test path,
profile option, serial entry and Kubernetes backend. An external Jenkins L3
schedule/stage is not registered by this repository change.

## Auxiliary verification

- Python helper tests: 86 passed, including literal protobuf wire evidence,
  actual SDK byte-vector output, hand-calculated rankings/ties, hash/path/snapshot
  validation, publication failures, repair, config confirmation and restoration.
- Ruff checks and formatting: 12 Python files passed.
- CI selection/shell contract tests: 10 passed.
- Submission-time `make verifiers` could not pass its native dependency setup:
  this workstation has no supported LLVM 14–18 installation. Python Ruff, helper
  tests and diff checks passed independently; the full verifier target is not
  claimed green. The server and L3 hook images were built remotely as recorded below.
- Paimon server Go UT: `externalspec`, `specutil`, `paramtable` passed in
  `go-ut-pr-51897-4f95cb6-3507e9ef-u3805`.
- Packed Paimon/range UT passed in `go-ut-pr-51897-4f95cb6-894ca9c6-u3805`.
- Test hook Go UT: six tests passed with `-tags dynamic,test`,
  `-gcflags=all=-N -l`, `-count=1`.
- An earlier broader Go UT run failed on the unrelated packed test
  `TestAsyncManifestRecordsAndPassesManifestSize` because its runner MinIO bucket
  was missing. This report does not claim that entire broad Go UT run passed.

## Dedicated L3 validation

PAI-L3-13 has three executable controls: post-publication index-wait timeout,
pre-publication timeout, and index delay with waiting disabled. Each verifies
phase-specific data/source/spec behavior, actual ANN completion, reload and retry.
The driver records authoritative job markers, actual index task identity and
live segment evidence, and restores runtime configuration and hook ownership.

Final hook build: `build-local-yanliang-qia-test-paimon-4f95cb6-ffe49186218d-u3805`
succeeded in 8m51s. Image:
`harbor.tencent-stage.zilliz.cn/manta/milvus:local-u3805-test-paimon-external-l3-hooks-4f95cb6-ffe49186218d`.
Image digest: `sha256:b7f17f78117fc119ddef8064b8307632d37d7b9fdd015bf5eddc41c8d56b5478`.
Final hook-image rerun: **3 passed, no skips, 312.72 seconds**. This run used
exactly the final supplied hook patch, including actual field/index selectors,
real request metadata and atomic event publication. The phase evidence is saved
in `../../assets/paimon/l3-phase-evidence.json`; exact 64-bit IDs are represented
as strings in that JSON artifact. JUnit and patch hashes are recorded in
`../../assets/paimon/validation-summary.json`.

After this run, independent checks confirmed both deployments Healthy and updated,
with no collections. The dedicated deployment has no active gate, no control lease,
and no remaining etcd wait/timeout overrides. Its actual runtime values are
`refreshWaitForIndex=false` and `externalCollectionJobTimeout=600`.

## Remaining plan coverage

The following are not implemented or validated by this change. They remain
explicit backlog rather than empty tests or unconditional skips:

- L2-02/04/09: same-file/same-cardinality alternate DV positions, row-group-boundary
  deletes, bad DV CRC, additional sparse physical encodings and invalid/null
  vector representations. Current truncation cases do not certify DV CRC handling.
- L2-06/10: BLOB and special merge-engine/sidecar/row-range/pre/post-compaction
  fixtures, and native bitmap64 discovery. Direct bitmap64 rejection is covered.
- L2-05: group-by/hybrid/search-iterator, extended index-parameter boundaries and
  collection/index mmap combinations beyond the current search/range matrix.
- L2-07/11/12: source evolution that removes a mapped column or changes vector
  dimension, invalid/nonexistent table-root paths, and further low-frequency
  function-parameter boundaries. Existing format-switch, bad-mapping-at-create,
  missing-snapshot, credential and function-control cases do not cover these.
- L2-13/14: concurrent schema changes observed through public APIs, drop/recreate
  against the same fixed snapshot, and dependency removal after a previously
  loaded view. Current repair coverage removes a snapshot before first read;
  it does not establish a cold read after dependency removal. Forced process
  cold start remains L3.
- L2-15/16: remainder of import/struct/recursive/element-nullable-array rejection,
  ArrayOfVector discovery, cross-DB/alias and async API coverage, invalid job
  parameters, and public cancel/timeout behavior where provided. Cancellation
  API availability still needs an explicit N/A-or-supported evidence record.
- L3-01–12: split/task-limit configuration, multi-node scheduling, component/storage
  chaos, real cloud IAM/STS, upgrade/rollback, performance, 24h stability, general
  stage hooks, special storage/hardware, auth and online source/service profiles.

These gaps prevent describing this delivery as full execution of every plan item
or comprehensive feature parity between Paimon, Parquet and Lance. The common
capabilities exercised by the representative parity cases matched; explicit
snapshot support, all-deleted refresh and bitmap64/BLOB limitations retain the
plan's distinctions.

### Implementation dependencies and blockers

There is no unresolved failure in the implemented 160 E2E cases. The default
Milvus Cell deployment and existing Python E2E dependencies are sufficient for
the implemented L0–L2 matrix. The L3-13 driver also ran successfully on its
separate test-hook image. This is scoped evidence, not proof of uncovered behavior.

The missing L2 cases above are principally fixture and test implementation work;
no hard environment blocker has been established. DV cases need real source
commits that preserve physical-file identity while changing deleted positions,
and a certified corrupt-DV variant. BLOB, special merge engines and alternate
vector encodings need a suitable writer plus independent reader GT before they
can enter ordinary E2E. Extended search/API cases need API and normal-collection
control checks rather than a deployment change.

| Pending L3 ID | Missing case scope | Dependency / current blocker status |
|---|---|---|
| L3-01 | split limits 1/2/64/65/0/-1, target rows, >=130 splits per bucket | Large-split fixture and scoped config/manifest driver are not implemented; Cell deployment is available. |
| L3-02 | files-per-task limits, >10000 splits, multi-node scheduling, scaling and competing collections | Large fixture and multi-node profile/task evidence are not implemented; multi-node capacity has not been validated. |
| L3-03 | DataCoord/DataNode/QueryNode/index-worker failure and restart/retry/cold read | Separate-component cluster and fault/recovery orchestration are needed; the phase driver supports one standalone pod. |
| L3-04 | MinIO/network/latency/429/5xx/short-read/disk/resource/403 faults | Fault proxy or chaos mechanism and recovery evidence are not implemented; injection capability has not been verified. |
| L3-05 | cloud IAM/STS expiration/rotation/audit, cross-bound references, OSS/GCS/Azure/TLS | No dedicated cloud credential/role/bucket or alternate-cloud test target is configured for this suite. |
| L3-06 | parent/new versions, v8/v9 fixtures, rolling upgrade/rollback and old SDK | Versioned fixtures and compatibility deployments/workflow are not implemented. |
| L3-07 | 1M/10M cold/hot performance, request/byte/CPU/RSS/scanning/latency/recall curves | Large fixture, fixed-resource benchmark and measurement pipeline are not implemented; capacity/budget has not been established. |
| L3-08 | >=24h, >=100 refreshes, online commits, competing collections and resource trends | Online writer and long-running runner/observers are not implemented; a real >=24h execution window is required. |
| L3-09 | general explore/build/load hooks, commit/schema races, routing/descriptors/functions/GC | Only L3-13's apply/index hooks exist; the remaining hooks and internal evidence checks are not implemented. |
| L3-10 | storage modes, global mmap/quota, DISKANN/GPU/special build/ORC | Per-capability profile/fixture and appropriate storage/hardware/build are not selected or validated. |
| L3-11 | auth/role/privilege/tenant/object-store restrictions and cross-DB isolation | Restricted-user and storage-permission fixtures plus dedicated auth profile are not implemented. |
| L3-12 | live Java/Flink writer/compactor, source retention/cold restart, custom TEI/mock/options | Online source/service lifecycle and version-pinned profiles are not implemented; an existing TEI was tested only in L2. |

The optional L3 hook patch is test-image-only and is not a proposed production
server change. Its driver supports Cell external access and namespace-scoped Kubernetes Service
access for dedicated standalone CI; multi-node layouts still need another backend. The 23.5 MB pre-generated fixture bundle
keeps Java/native writer dependencies out of ordinary E2E, but its distribution
and the placement of recorded validation evidence are review decisions.

The ordinary and dedicated deployments are retained for follow-up. Original user
changes in the parent Milvus checkout were not modified. The submission is a
draft review of this implemented subset, with the remaining scope listed above.
