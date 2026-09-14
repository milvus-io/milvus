# CMEK integration tests

The suite uses repository-owned encryption fixtures and isolated MiniCluster processes. Build the fixtures with `make build-cmek-fixtures` and the server with `make build-go CMEK_TEST_OBSERVER=1` after building the C++ libraries. Set `MILVUS_WORK_DIR` to the Milvus checkout when running the tests. The test runner uses the `dynamic,test` build tags.

`CMEK_TEST_OBSERVER=1` adds the `cmektest` server build tag. It records actual `SyncTask` completion and `SaveBinlogPaths` acknowledgements in a per-suite temporary directory. The runner checks the originating node, child PID, run token, record sequence, positive insert rows and exact committed manifest. The observer composes with the existing completion callback and is disabled in ordinary server builds.

The Storage V3 campaign currently covers canonical DataNode Parquet output with growing-source flush disabled. `TestRawDataV3Suite` runs basic, scalar, vector and StructArray cases. Each case checks the original committed manifest, all referenced raw-data objects and a complete release/reload with exact query results. Reading evidence is accepted only while the authoritative segments and actual QueryNode manifest, node and load version stay consistent. Each round shares a three-minute deadline, with at most three rounds for observed identity changes.

An independent Arrow Go reader also checks the same nonempty Parquet object with a correct fixture-derived key, no decryption configuration and a legal-length incorrect key. Manifest parsing, observer evidence and read-round rejection rules have focused component tests. Vortex, TEXT/LOB and growing-source campaigns remain blocked and are outside this suite's current coverage.

Use the repository's resource-guarded build and test entrypoints for local execution. Run the basic V3 case first, then the remaining campaigns; investigate an unexpected failure before expanding the run. The GitHub integration workflow builds the observer-enabled server before running the CMEK package.
