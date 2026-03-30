# 0025 Python Client HNSW Build Param Parity Gap

## Summary

This issue started as the first authoritative `python_client` standalone probe
that reached real `create_index` execution but did not match upstream `HNSW`
build-param behavior under the shim.

Current status: resolved for the tracked probe.

This is not a remote environment problem:

- `CI_LOG_PATH` was redirected off `/tmp`
- `pytest` report output was redirected off `/tmp`
- the remote private Python site was corrected back to `numpy==1.26.4`
- collection now succeeds and the probe executes the real test body

## Initial Authoritative Reproduction

Remote host: `hannsdb-x86`  
Artifact root: `/data/work/milvus-rs-integ/artifacts/python-client-probe-20260328-1`

Command path:

- `scripts/knowhere-rs-shim/standalone_subset_lib.py run-suite`
- suite: `python_client`
- case pattern: `^TestHnswBuildParams::test_hnsw_build_params$`

Live log:

- `/data/work/milvus-rs-integ/artifacts/python-client-probe-20260328-1/python_client/python_client__test_hnsw__TestHnswBuildParams_test_hnsw_build_params/output.log`

Observed failures from the official test:

- `param 'M': invalid parameter`
- `param 'efConstruction': invalid parameter`
- one valid-looking `create_index` path later times out in `wait_for_creating_index` after `120s`

## Boundary

This bucket mixes two different root-cause classes and they must not be
conflated:

1. `knowhere-rs` capability / ABI gaps
   - `HNSW` does not expose `M` as a first-class FFI config field
   - tracked already in `0002-abi-gap.md`

2. shim-side validation / parity gaps
   - official negative/type-error cases expect specific upstream-style messages
   - the shim currently emits generic `Out of range in json` / `invalid parameter`
     errors instead of the expected standalone-compatible messages
   - the timeout path may also be shim behavior rather than a `knowhere-rs`
     kernel limitation

## Resolution

Root cause was in shim-side validation and message parity, not in the
`knowhere-rs` kernel:

- `HNSW` integer validation in
  [config.h](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/internal/core/thirdparty/knowhere-rs-shim/include/knowhere/config.h)
  treated decimal-string cases such as `"16.0"` and `"100.0"` as generic
  invalid integers instead of the upstream-style `wrong data type in json`
  expectation.
- regression coverage was added in
  [config_check_smoke.cpp](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/internal/core/thirdparty/knowhere-rs-shim/tests/config_check_smoke.cpp)
  before re-running the remote probe.

## Authoritative Verification

Remote host: `hannsdb-x86`  
Artifact root: `/data/work/milvus-rs-integ/artifacts/python-client-probe-20260328-3`

Result:

- summary: `1 / 1 PASS`
- case: `TestHnswBuildParams::test_hnsw_build_params`
- result json:
  `/data/work/milvus-rs-integ/artifacts/python-client-probe-20260328-3/python_client/python_client__test_hnsw__TestHnswBuildParams_test_hnsw_build_params/result.json`

This issue should no longer be treated as the current blocker for the
`python_client` standalone lane.
