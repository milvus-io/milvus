# Standalone Replaceability Stage 2 Design

**Date:** 2026-03-27

## Goal

Stage 2 exists to answer one question cleanly:

- can `knowhere-rs`, when wired through the Milvus shim, replace upstream Knowhere for the Milvus **standalone** test surface that actually exercises vector indexing and vector search?

This is not a generic Milvus standalone certification effort. It is a replaceability effort for the Knowhere-facing product surface.

## Updated User Intent

The target is no longer the narrow stage-1 smoke path.

The target is now:

- derive a subset of **Milvus original standalone test cases**
- keep only cases whose behavior materially depends on Knowhere / the shim
- run that subset against Milvus standalone backed by `knowhere-rs`
- fix shim, harness, and Milvus build/runtime gaps directly
- only file an issue when the root cause is a real `knowhere-rs` capability gap

## Hard Constraints

- do not modify `knowhere-rs` source code
- use the remote x86 authority host under `/data/work`
- Milvus deployment mode remains standalone
- do not pull `tests/integration` multi-process cluster testing into this stage
- do not expand the goal into unrelated standalone product validation

## What Counts as a Valid Test Target

A Milvus original test case belongs to the stage-2 target subset only if all of the following are true:

1. it can run against a standalone Milvus server without the `tests/integration` MiniCluster framework
2. it uses an official Milvus test suite already present in the repository
3. its core assertion passes through a Knowhere-shaped path:
   - index build
   - search
   - range search
   - iterator search
   - vector retrieval from index
   - vector-index serialization / deserialization / load path
   - growing or brute-force fallback paths
4. it is meaningful for replaceability rather than generic server behavior

## Source Suites for the Standalone Subset

The stage-2 subset is derived from Milvus original tests in these standalone-capable suites:

- `tests/go_client`
- `tests/python_client`
- `tests/restful_client`
- `tests/restful_client_v2`

The subset must not invent new product tests. It may add shim-local smoke tests, but those are support checks, not acceptance targets.

## Inclusion Rules

The subset should include official standalone cases that exercise:

- `create_index`
- `describe_index`
- `load_collection` / `release_collection`
- vector `search`
- vector `range search`
- `search iterator`
- `hybrid search` only when the vector branch depends on Knowhere
- `get_vector_by_ids`
- index load / serialize / deserialize behavior
- growing search paths
- brute-force fallback paths
- supported vector index families that the original tests request under standalone

## Exclusion Rules

The subset should explicitly exclude standalone tests whose assertions do not depend on Knowhere:

- auth / rbac / user / role
- database, alias, and most collection-management-only scenarios
- HTTP server routing behavior
- geometry-only behavior
- full-text, embedding, or reranker service behavior
- k8s, distributed, chaos, MiniCluster, or `tests/integration`
- any test whose outcome would be identical even if Knowhere were stubbed out

## Triage Rule

Every failing case in the subset must be classified into one of four buckets:

- `HARNESS_GAP`
  - remote layout, module path, dependency, or runner problem
- `SHIM_GAP`
  - the Milvus compatibility shim is missing API or runtime behavior
- `MILVUS_BUILD_GAP`
  - build-layer or standalone integration wiring is wrong
- `KNOWHERE_RS_GAP`
  - the needed algorithm, ABI, or persistent behavior is not available from `knowhere-rs`

Only the last bucket becomes a `knowhere-rs` issue by policy.

This corrects the earlier stage-1 habit of filing issues for every discovered gap. Stage 2 should fix non-`knowhere-rs` blockers directly.

## Acceptance Model

Stage 2 is complete when the subset reaches a stable replaceability verdict:

- every case in the standalone subset is enumerated in a manifest
- every case has a final status
- all remaining failures are explained by evidence
- no remaining unexplained red case is caused by shim, harness, or build-layer uncertainty

Allowed final statuses:

- `PASS`
- `PASS_AFTER_SHIM_FIX`
- `PASS_AFTER_HARNESS_FIX`
- `BLOCKED_BY_KNOWHERE_RS`
- `EXCLUDED_WITH_REASON`

## Replaceability Standard

`knowhere-rs` should be judged replaceable only against the accepted subset, not against all Milvus standalone tests.

The final question is:

- for all standalone tests that materially depend on Knowhere, how much of the official surface passes unchanged under the shim?

The answer must be quantitative and backed by the original test inventory, not by anecdotal smoke runs.

## Coverage Phases

### Phase 1: Build the authoritative subset

- identify standalone original cases that touch Knowhere
- encode them in a manifest with suite, test name, path kind, index family, and runner command

### Phase 2: Establish the baseline

- run the subset against remote standalone
- record passes, failures, and failure signatures

### Phase 3: Remove non-`knowhere-rs` noise

- fix harness problems
- fix build/layout problems
- fix shim API and runtime holes

### Phase 4: Decide true capability gaps

- only after the noise is removed, decide which remaining failures are real `knowhere-rs` gaps

### Phase 5: Publish the replaceability matrix

- produce a case-by-case matrix and a summary verdict

## Expected Runtime Gaps

Current evidence already shows that the next wave of work is not only `HNSW`.

The subset is expected to hit at least these runtime families:

- sealed vector index search
- growing search
- brute-force fallback
- binary-vector search
- range / iterator / vector retrieval paths
- index persistence and load semantics

Some official tests may request index families beyond stage 1:

- `FLAT`
- `BIN_FLAT`
- `BIN_IVF_FLAT`
- `IVF_FLAT`
- `IVF_SQ8`
- `IVF_PQ`
- `SCANN`
- `DISKANN`
- `SPARSE_*`

Those families should be treated pragmatically:

- if the path can be satisfied in the shim without lying about `knowhere-rs`, do so
- if the path requires genuine `knowhere-rs` functionality that does not exist, record a `knowhere-rs` issue and mark the case accordingly

## Deliverables

Stage 2 should produce:

- an authoritative standalone test subset manifest
- runner scripts for the subset
- a baseline result report
- incremental shim/runtime fixes
- `knowhere-rs` issues only for true `knowhere-rs` gaps
- a final replaceability matrix with verdict

## Non-Goals

Stage 2 does not attempt to:

- certify all standalone Milvus functionality
- support `tests/integration`
- re-implement every upstream Knowhere algorithm inside the shim
- hide `knowhere-rs` capability limits behind fake compatibility

## Decision

Stage 2 will be test-driven from a Milvus-original standalone subset.

The subset is the source of truth.
The shim is expanded only as required by that subset.
Only true `knowhere-rs` capability gaps become `knowhere-rs` issues.
