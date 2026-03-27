# Standalone Replaceability Stage 2 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run the Milvus-original standalone test subset that materially depends on Knowhere, then expand the shim until every remaining failure is either fixed or proven to be a real `knowhere-rs` gap.

**Architecture:** Treat the Milvus original test subset as the source of truth. Add repeatable runners, baseline the failures, fix harness and shim gaps directly, and only file `knowhere-rs` issues when the failing behavior truly requires missing `knowhere-rs` capability.

**Tech Stack:** Milvus source tree in the current worktree, remote x86 standalone under `/data/work/milvus-rs-integ`, C++ shim in `internal/core/thirdparty/knowhere-rs-shim`, Go client tests, Python client tests, RESTful client tests, shell/Python runner scripts, Markdown/JSON manifests.

---

## File Map

- Create: `docs/knowhere-rs-shim/standalone-test-subset.md`
- Create: `docs/knowhere-rs-shim/standalone-test-baseline.md`
- Create: `docs/knowhere-rs-shim/standalone-replaceability-matrix.md`
- Create: `scripts/knowhere-rs-shim/standalone_test_subset.json`
- Create: `scripts/knowhere-rs-shim/run_go_client_subset.sh`
- Create: `scripts/knowhere-rs-shim/run_python_client_subset.sh`
- Create: `scripts/knowhere-rs-shim/run_restful_subset.sh`
- Create: `scripts/knowhere-rs-shim/run_standalone_subset.sh`
- Create: `scripts/knowhere-rs-shim/collect_subset_results.py`
- Modify: `scripts/knowhere-rs-shim/remote_env.sh`
- Modify: `scripts/knowhere-rs-shim/build_remote.sh`
- Modify: `scripts/knowhere-rs-shim/start_standalone_remote.sh`
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/comp/brute_force.h`
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/dataset.h`
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/binaryset.h`
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/index/index_factory.h`
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/index/index_static.h`
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/index/index_node.h`
- Create: `internal/core/thirdparty/knowhere-rs-shim/tests/binary_bruteforce_smoke.cpp`
- Create: `internal/core/thirdparty/knowhere-rs-shim/tests/growing_search_smoke.cpp`
- Create: `internal/core/thirdparty/knowhere-rs-shim/tests/index_persistence_smoke.cpp`
- Create: `docs/knowhere-rs-shim/issues/0021-*.md` and later issue files only for confirmed `knowhere-rs` gaps

## Chunk 1: Build the Authoritative Standalone Subset

### Task 1: Derive the Milvus-original standalone subset manifest

**Files:**
- Create: `docs/knowhere-rs-shim/standalone-test-subset.md`
- Create: `scripts/knowhere-rs-shim/standalone_test_subset.json`

- [ ] **Step 1: Enumerate candidate standalone suites**

Read these sources and extract candidate cases:

```bash
cd /Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1
find tests/go_client/testcases -maxdepth 1 -type f | sort
find tests/python_client/testcases -maxdepth 2 -type f | sort
find tests/restful_client/testcases -maxdepth 1 -type f | sort
find tests/restful_client_v2/testcases -maxdepth 1 -type f | sort
```

Expected: a stable list of original standalone-capable test files.

- [ ] **Step 2: Filter by Knowhere-touching behavior**

Keep only cases whose core assertion touches:

- vector index build
- vector search
- range search
- iterator search
- load / release around vector index
- get-vector/index-persistence paths
- growing or brute-force fallback

Expected: a reduced candidate list with excluded classes removed.

- [ ] **Step 3: Classify every selected case**

For each selected case, record:

- suite
- test file
- test name
- path kind
- index family
- runner type

Expected: all selected cases fit into a small fixed set of path kinds.

- [ ] **Step 4: Write the JSON manifest**

Write `scripts/knowhere-rs-shim/standalone_test_subset.json` with one object per case.

Minimum keys:

```json
{
  "suite": "go_client",
  "file": "tests/go_client/testcases/search_test.go",
  "case": "TestSearchDefault",
  "path_kind": "sealed_search",
  "index_family": "HNSW",
  "runner": "go_test"
}
```

- [ ] **Step 5: Write the human-readable subset doc**

Summarize the same inventory in `docs/knowhere-rs-shim/standalone-test-subset.md`, including explicit exclusions and rationale.

- [ ] **Step 6: Verify manifest validity**

Run:

```bash
cd /Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1
python3 -m json.tool scripts/knowhere-rs-shim/standalone_test_subset.json >/dev/null
```

Expected: exit `0`.

- [ ] **Step 7: Commit the subset inventory**

```bash
cd /Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1
git add docs/knowhere-rs-shim/standalone-test-subset.md scripts/knowhere-rs-shim/standalone_test_subset.json
git commit -m "docs(shim): define standalone replaceability test subset"
```

## Chunk 2: Make the Subset Runnable

### Task 2: Add repeatable runners for the standalone subset

**Files:**
- Create: `scripts/knowhere-rs-shim/run_go_client_subset.sh`
- Create: `scripts/knowhere-rs-shim/run_python_client_subset.sh`
- Create: `scripts/knowhere-rs-shim/run_restful_subset.sh`
- Create: `scripts/knowhere-rs-shim/run_standalone_subset.sh`
- Create: `scripts/knowhere-rs-shim/collect_subset_results.py`
- Modify: `scripts/knowhere-rs-shim/remote_env.sh`

- [ ] **Step 1: Encode a stable result layout**

Choose a remote artifact layout under:

```bash
/data/work/milvus-rs-integ/artifacts/standalone-subset/
```

Expected: one directory per suite and one result file per test case.

- [ ] **Step 2: Write the Go client runner**

`run_go_client_subset.sh` should:

- source `remote_env.sh`
- read the JSON manifest
- execute only `go_client` cases
- preserve the required `tests/go_client` module layout assumptions
- write per-case logs and exit codes

- [ ] **Step 3: Write the Python client runner**

`run_python_client_subset.sh` should:

- source `remote_env.sh`
- pass `--uri`, `--token`, or host/port args consistently
- run only selected `python_client` cases
- write per-case logs and exit codes

- [ ] **Step 4: Write the RESTful runner**

`run_restful_subset.sh` should do the same for selected `restful_client` and `restful_client_v2` cases.

- [ ] **Step 5: Write the top-level subset runner**

`run_standalone_subset.sh` should:

- start from a healthy standalone
- call suite runners in sequence
- never stop on first failure
- always emit a summary path

- [ ] **Step 6: Write the collector**

`collect_subset_results.py` should fold per-case logs into one machine-readable summary and one Markdown summary.

- [ ] **Step 7: Verify the runners on one known pass and one known fail**

Run at least:

```bash
ssh hannsdb-x86 'bash -lc "
cd /data/work/milvus-rs-integ/milvus-src &&
source scripts/knowhere-rs-shim/remote_env.sh &&
cd tests/go_client &&
go test ./testcases/search_test.go ./testcases/main_test.go -run TestSearchDefault\\$ -count=1 -timeout=20m -args -addr http://localhost:19530
"'
```

and:

```bash
ssh hannsdb-x86 'bash -lc "
cd /data/work/milvus-rs-integ/milvus-src &&
source scripts/knowhere-rs-shim/remote_env.sh &&
cd tests/go_client &&
go test ./testcases -run ^TestSearchDefaultGrowing$ -count=1 -timeout=20m -args -addr http://localhost:19530
"'
```

Expected: the first passes, the second reproduces the known brute-force gap until fixed.

- [ ] **Step 8: Commit the runner layer**

```bash
cd /Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1
git add scripts/knowhere-rs-shim
git commit -m "feat(shim): add standalone subset test runners"
```

## Chunk 3: Establish the Baseline and Remove Harness Noise

### Task 3: Run the full subset and classify every failure

**Files:**
- Create: `docs/knowhere-rs-shim/standalone-test-baseline.md`

- [ ] **Step 1: Run the complete subset**

Run:

```bash
ssh hannsdb-x86 'bash -lc "
cd /data/work/milvus-rs-integ/milvus-src &&
bash scripts/knowhere-rs-shim/run_standalone_subset.sh
"'
```

Expected: a full result set under `/data/work/milvus-rs-integ/artifacts/standalone-subset/`.

- [ ] **Step 2: Classify each failing case**

Each red case must be assigned exactly one bucket:

- `HARNESS_GAP`
- `SHIM_GAP`
- `MILVUS_BUILD_GAP`
- `KNOWHERE_RS_GAP`

Expected: no `UNKNOWN` case remains after the baseline pass.

- [ ] **Step 3: Write the baseline report**

Record:

- total cases
- passes
- failures by bucket
- first failing signature for each bucket

- [ ] **Step 4: Fix harness-only blockers immediately**

Examples:

- broken module `replace` paths
- missing private Python site-packages
- runner argument mismatches
- remote path assumptions

Expected: these fixes do not require new issues.

- [ ] **Step 5: Re-run the subset after harness fixes**

Expected: all remaining failures are either `SHIM_GAP`, `MILVUS_BUILD_GAP`, or `KNOWHERE_RS_GAP`.

- [ ] **Step 6: Commit the baseline checkpoint**

```bash
cd /Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1
git add docs/knowhere-rs-shim/standalone-test-baseline.md scripts/knowhere-rs-shim
git commit -m "docs(shim): baseline standalone replaceability subset"
```

## Chunk 4: Close Runtime Surface Gaps Before Chasing Index Families

### Task 4: Expand brute-force and growing-search coverage

**Files:**
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/comp/brute_force.h`
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/dataset.h`
- Create: `internal/core/thirdparty/knowhere-rs-shim/tests/binary_bruteforce_smoke.cpp`
- Create: `internal/core/thirdparty/knowhere-rs-shim/tests/growing_search_smoke.cpp`

- [ ] **Step 1: Write a binary brute-force smoke**

Cover:

- binary vectors
- at least one binary metric used by official tests
- top-k result buffer behavior

- [ ] **Step 2: Run it and verify red**

Run:

```bash
cd /Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1
cmake --build cmake_build --target knowhere_rs_binary_bruteforce_smoke -j8
```

Expected: the smoke fails or does not compile until the runtime gap is fixed.

- [ ] **Step 3: Implement the minimal binary brute-force runtime**

Add only the behavior required by the official subset.

- [ ] **Step 4: Write a growing-search smoke**

Model the shape of `load -> insert -> search` against the fallback path.

- [ ] **Step 5: Run official regression cases**

At minimum rerun:

```bash
go test ./testcases -run ^TestSearchDefaultGrowing$ -count=1 -timeout=20m -args -addr http://localhost:19530
```

Expected: move from `Brute force search fail: not_implemented` to pass or to a narrower next blocker.

- [ ] **Step 6: Commit the brute-force/growing fixes**

```bash
cd /Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1
git add internal/core/thirdparty/knowhere-rs-shim
git commit -m "feat(shim): expand brute-force and growing search coverage"
```

### Task 5: Expand index persistence and retrieval surface

**Files:**
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/binaryset.h`
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/index/index_static.h`
- Modify: `internal/core/thirdparty/knowhere-rs-shim/include/knowhere/index/index_node.h`
- Create: `internal/core/thirdparty/knowhere-rs-shim/tests/index_persistence_smoke.cpp`

- [ ] **Step 1: Write a persistence smoke**

Cover:

- serialize
- deserialize
- load path metadata
- minimal `BinarySet` contract

- [ ] **Step 2: Verify red**

Expected: the smoke fails until missing behavior is implemented.

- [ ] **Step 3: Implement only the subset-required persistence semantics**

Do not generalize beyond what original standalone cases need.

- [ ] **Step 4: Re-run official cases that depend on describe/load/search after index build**

Expected: persistence-related failures either pass or shrink to a clear next gap.

- [ ] **Step 5: Commit the persistence checkpoint**

```bash
cd /Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1
git add internal/core/thirdparty/knowhere-rs-shim
git commit -m "feat(shim): improve index persistence compatibility"
```

## Chunk 5: Expand by Index Family Only When the Subset Demands It

### Task 6: Walk the subset by index family and decide fix vs. issue

**Files:**
- Modify: `docs/knowhere-rs-shim/standalone-test-baseline.md`
- Create: `docs/knowhere-rs-shim/issues/0021-*.md` and later issue files as needed
- Modify: shim headers and smoke tests as demanded by each family

- [ ] **Step 1: Group remaining failures by index family**

At minimum track:

- `HNSW`
- `FLAT` / brute-force sealed
- `BIN_FLAT` / `BIN_IVF_FLAT`
- `IVF_FLAT`
- `IVF_SQ8`
- `IVF_PQ`
- `SCANN`
- `DISKANN`
- `SPARSE_*`

- [ ] **Step 2: For one family at a time, verify whether `knowhere-rs` actually has the needed capability**

Use primary evidence:

- current FFI exports
- existing `knowhere-rs` runtime behavior already reachable from the shim

Expected: no family is marked as a `knowhere-rs` gap without direct evidence.

- [ ] **Step 3: If the blocker is shim-side, fix it directly**

Do not open an issue for shim, harness, or build-layer gaps.

- [ ] **Step 4: If the blocker is truly in `knowhere-rs`, file a `knowhere-rs` issue**

Each issue must include:

- original Milvus test case
- exact failing command
- why shim cannot truthfully bridge it
- the missing `knowhere-rs` capability

- [ ] **Step 5: Re-run only the affected family cases**

Expected: each family iteration reduces open red cases.

- [ ] **Step 6: Commit family-by-family checkpoints**

Use focused messages such as:

```bash
git commit -m "feat(shim): support standalone binary search subset"
git commit -m "docs(shim): record knowhere-rs gap for standalone IVF subset"
```

## Chunk 6: Publish the Replaceability Verdict

### Task 7: Produce the final standalone replaceability matrix

**Files:**
- Create: `docs/knowhere-rs-shim/standalone-replaceability-matrix.md`

- [ ] **Step 1: Re-run the entire subset from the top-level runner**

Run:

```bash
ssh hannsdb-x86 'bash -lc "
cd /data/work/milvus-rs-integ/milvus-src &&
bash scripts/knowhere-rs-shim/run_standalone_subset.sh
"'
```

Expected: a fresh full result set.

- [ ] **Step 2: Write one row per original test case**

Each row must contain:

- suite
- test name
- path kind
- index family
- final status
- evidence link or artifact path

- [ ] **Step 3: Summarize the verdict**

Include:

- total case count
- pass count
- pass-after-fix count
- confirmed `knowhere-rs` gap count
- excluded count

- [ ] **Step 4: State the replaceability judgment**

The conclusion must be evidence-based, for example:

- replaceable for this subset
- partially replaceable with named gaps
- not yet replaceable because of named `knowhere-rs` blockers

- [ ] **Step 5: Commit the final matrix**

```bash
cd /Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1
git add docs/knowhere-rs-shim/standalone-replaceability-matrix.md docs/knowhere-rs-shim/issues
git commit -m "docs(shim): publish standalone replaceability matrix"
```
