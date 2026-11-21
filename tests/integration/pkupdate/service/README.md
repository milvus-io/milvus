# PK update service acceptance

Run from a clean Milvus checkout. The exercised site uses macOS, Go 1.26.10, an ABI-compatible Release native core with benchmark observation OFF, LLVM 19, and Python with PyMilvus 3.0.2 and gRPC. Load the Go/cgo, pkg-config and dynamic loader environment for that core. Set `PKUPDATE_NATIVE_BUILD_DIR` to its CMake build directory. The cluster launcher needs local etcd on port 2379, local MinIO with the configured port and credentials, unused loopback ports, and writable storage for Woodpecker, role logs and data. Worker replacement uses ps and lsof. Disable HTTP proxies for loopback.

## Public reads and recovery

Set `build` and `site` to unused absolute paths outside the checkout and `py` to the Python interpreter. Run each command after the preceding one succeeds.

```bash
suite=tests/integration/pkupdate/service
bash "$suite/build.sh" "$build" normal
"$py" "$suite/cluster_site.py" "$site" "$build/bin/milvus" configs
"$py" "$suite/run.py" "$site"
```

The normal service uses the production Go sources directly. The runner creates retained candidate and mirror collections, toggles the public admission switch, and checks Query, Search, schema validation, sealed output and recovery. Clients bound each RPC, load and flush. Configuration refresh probes allow 45 attempts and worker health checks allow 120 seconds. Check the role log listed in `processes.jsonl` on timeout. Keep the site directory and processes for inspection. Use fresh output directories and collection names for independent runs.

## Isolated fault and CDC cases

Build the service with private failpoint rewrites in a separate unused directory. Use a dedicated test site for these commands. The fixtures replace owned worker processes and retain files, process ledgers and collections.

```bash
bash "$suite/build.sh" "$fault_build" faults
bin="$fault_build/bin/milvus"
"$py" "$suite/materialization_faults.py" "$site" "$bin"
"$py" "$suite/owner_faults.py" "$site" "$bin" streamingnode
"$py" "$suite/owner_faults.py" "$site" "$bin" querynode
"$py" "$suite/replay_gap.py" "$site" "$bin"
"$py" "$suite/read_gc.py" "$site" "$bin"
"$py" "$suite/cdc_site.py" "$site" "$cdc_root" "$bin"
"$py" "$suite/cdc_topology.py" "$site" "$cdc_root/secondary"
"$py" "$suite/cdc_updates.py" "$site" "$cdc_root"
```

Use a fresh CDC root and an unpaired primary for topology creation. The topology uses a separate CDC role and a secondary cluster. Repeat the workload on a retained topology by running only cdc_updates.py. The build directories record the source revision, native cache and core digests, any rewritten inputs, Go build information, linked libraries and executable hashes. For a running site, compare the library mapped by the role PID with the recorded native digest.

Before merging the product branch, merge [milvus-proto PR 700](https://github.com/milvus-io/milvus-proto/pull/700), pin the accepted upstream revision, and remove the four temporary fork replacements from the root, pkg, client and tests/go_client modules.
