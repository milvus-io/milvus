# CMEK integration tests

The suite uses repository-owned encryption fixtures and isolated MiniCluster processes. Build the C++ libraries and ordinary Milvus server, then run `make build-cmek-fixtures`. Set `MILVUS_WORK_DIR` to the Milvus checkout and `MILVUS_CMEK_FIXTURE_DIR` to its `bin/cmek-fixtures` directory. The test runner uses the `dynamic,test` build tags and matching race fixtures when enabled.

Raw Data suites disable compaction before starting the cluster and keep growing-source flush disabled. V2 and V3 share collection preparation, accepted-schema field IDs, deterministic data and query/search assertions. After `Flush` and `WaitForFlush`, they inspect every object referenced by the returned segments, then perform one complete release/reload and verify the actual values. Vector tests also verify that no physical vector index exists and keep interim indexes disabled.

`TestRawDataV3Suite` covers non-TEXT Parquet scalar, vector and StructArray campaigns. It independently parses each exact manifest and checks every raw-data object. Existing QueryNode distribution RPCs confirm that the inspected segments and manifests are loaded and serviceable, with no growing data serving the query. An unexpected change of segment, manifest, node or load version fails the test directly.

The scalar campaign supplies one nonempty Parquet object for the format's key baseline. An independent Arrow Go reader fully reads its known payload with the correct fixture-derived key, rejects missing decryption configuration and rejects a legal-length wrong key. All three modes use the same bytes and a fresh reader. Separate packed-writer regressions cover binary keys containing NUL bytes, valid-length truncated keys and key-allocation failures.

Focused tests cover manifest completeness, structural metadata rejection, Parquet envelopes, reader failure modes and accepted-schema field IDs. Vortex, TEXT/LOB and growing-source campaigns remain blocked and are outside current coverage. Start with the V3 scalar campaign, then run the V2/V3 matrix; investigate an unexpected failure before expanding the run.
