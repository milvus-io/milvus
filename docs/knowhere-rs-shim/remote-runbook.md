# Remote Runbook

## Scope

- Stage-1 only
- Remote x86 host only
- Milvus standalone only
- `FLOAT_VECTOR + HNSW + create_index/load/search` only
- Do not modify `knowhere-rs`

## Fixed Paths

- Remote integration root: `/data/work/milvus-rs-integ`
- Remote Milvus source tree: `/data/work/milvus-rs-integ/milvus-src`
- Remote `knowhere-rs` FFI library: `/data/work/milvus-rs-integ/knowhere-rs-target/release/libknowhere_rs.so`
- Remote runtime state: `/data/work/milvus-rs-integ/milvus-var`
- Remote artifact directory: `/data/work/milvus-rs-integ/artifacts`
- Standalone log: `/data/work/milvus-rs-integ/milvus-var/logs/standalone-stage1.log`

## Host Assumption

- Reference host: HannsDB x86 VM `189.1.218.159`
- SSH alias used during stage-1 work: `hannsdb-x86`

## Environment Bootstrap

Every remote build or runtime command should run from:

```bash
cd /data/work/milvus-rs-integ/milvus-src
source scripts/knowhere-rs-shim/remote_env.sh
```

`remote_env.sh` is not optional. It pins writable state under `/data/work`, enables the shim CMake switch, sets library paths for `libknowhere_rs.so`, and avoids the full root filesystem.

## Build Flow

### Thirdparty only

Use this when remote thirdparty dependencies are not present yet:

```bash
cd /data/work/milvus-rs-integ/milvus-src
bash scripts/knowhere-rs-shim/build_remote.sh thirdparty
```

### Proto only

Use this when generated protobuf or Go-side prereqs are missing:

```bash
cd /data/work/milvus-rs-integ/milvus-src
bash scripts/knowhere-rs-shim/build_remote.sh proto
```

### Full core lane

Use this for the normal stage-1 rebuild path:

```bash
cd /data/work/milvus-rs-integ/milvus-src
bash scripts/knowhere-rs-shim/build_remote.sh core
```

This wrapper handles:

- `TMPDIR` under `/data/work`
- local Go toolchain under `/data/work/milvus-rs-integ/go`
- local OpenBLAS under `/data/work/milvus-rs-integ/openblas`
- shim CMake args
- GCC 13 RocksDB workaround via `-include cstdint`

### Incremental core rebuild

For a faster rebuild after small shim changes:

```bash
cd /data/work/milvus-rs-integ/milvus-src
source scripts/knowhere-rs-shim/remote_env.sh
cmake --build cmake_build --target milvus_core milvus_query knowhere_rs_brute_force_smoke -j16
```

## Standalone Start

```bash
cd /data/work/milvus-rs-integ/milvus-src
bash scripts/knowhere-rs-shim/start_standalone_remote.sh
```

Expected result:

- prints `PID=...`
- prints `LOG=...`
- exits with `HEALTHY`

Health check:

```bash
curl -sf http://127.0.0.1:9091/healthz && echo
```

The script keeps all runtime state under `/data/work/milvus-rs-integ/milvus-var` and uses embedded etcd so stage-1 does not depend on Docker.

## Python Client Prerequisite

The remote smoke uses `pymilvus`. Install it into the stage-1 private site-packages path, not the system Python:

```bash
cd /data/work/milvus-rs-integ/milvus-src
source scripts/knowhere-rs-shim/remote_env.sh
python3 -m pip install --target /data/work/milvus-rs-integ/python-site pymilvus
```

## Smoke Verification

### Run the stage-1 smoke

```bash
cd /data/work/milvus-rs-integ/milvus-src
bash scripts/knowhere-rs-shim/run_remote_smoke.sh
```

### Capture an artifact explicitly

```bash
cd /data/work/milvus-rs-integ/milvus-src
bash scripts/knowhere-rs-shim/run_remote_smoke.sh \
  > /data/work/milvus-rs-integ/artifacts/smoke_hnsw_manual.json
```

Expected smoke result:

- `index_type == "HNSW"`
- `top_hit_id == 0`
- `top_hit_distance == 1.0`
- `topk == 2`

## Known Good Artifacts

- `/data/work/milvus-rs-integ/artifacts/smoke_hnsw_.json`
- `/data/work/milvus-rs-integ/artifacts/smoke_hnsw_run1.json`
- `/data/work/milvus-rs-integ/artifacts/smoke_hnsw_run2.json`
- `/data/work/milvus-rs-integ/artifacts/smoke_hnsw_run3.json`

## Troubleshooting

### Root filesystem is full

- `/` is full on the reference host
- `/data` has the free space
- Do not move caches, temp files, Python installs, or runtime state back to default paths under `/root` or `/tmp`

Related issues:

- `0007-remote-prerequisites.md`
- `0008-remote-rootfs-pressure.md`
- `0009-rocksdb-gcc13-build-blocker.md`

### Standalone still shows old behavior after rebuild

Rebuilding `milvus_core` is not enough by itself. Restart standalone so the process loads the new shared library:

```bash
cd /data/work/milvus-rs-integ/milvus-src
bash scripts/knowhere-rs-shim/start_standalone_remote.sh
```

### Smoke cannot import `pymilvus`

Re-source `remote_env.sh` and reinstall into the private Python site path:

```bash
cd /data/work/milvus-rs-integ/milvus-src
source scripts/knowhere-rs-shim/remote_env.sh
python3 -m pip install --target /data/work/milvus-rs-integ/python-site pymilvus
```

### Standalone fails health check

Inspect the standalone log:

```bash
tail -n 120 /data/work/milvus-rs-integ/milvus-var/logs/standalone-stage1.log
```

## Stage-1 Exit Criteria

Stage-1 is considered green only when all of the following are true on the remote host:

- `knowhere_rs_brute_force_smoke` passes
- standalone `/healthz` returns `OK`
- `run_remote_smoke.sh` completes successfully
- smoke output shows `top_hit_id = 0`
- smoke output shows `top_hit_distance = 1.0`
