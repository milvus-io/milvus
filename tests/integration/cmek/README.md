# CMEK integration tests

The suites verify persisted raw data and index artifacts under CMEK using repository-owned cipher fixtures and isolated MiniCluster processes. Each IT writes through Milvus, locates the resulting objects from authoritative metadata, checks their encryption, then releases/reloads the collection and verifies query or search results.

## Running the suites

Build the C++ libraries and ordinary Milvus server, then run `make build-cmek-fixtures`. Set `MILVUS_WORK_DIR` to the Milvus checkout and `MILVUS_CMEK_FIXTURE_DIR` to its `bin/cmek-fixtures` directory. The test runner uses the `dynamic,test` build tags and matching race fixtures when enabled.

## IT entry points

| File | Test entry point | What it verifies |
| --- | --- | --- |
| `raw_data_v2_test.go` | `TestRawDataV2Suite` | Scalar, vector and StructArray raw data in Storage V2: inspect every binlog-referenced Parquet object, then release/reload and check actual values. |
| `raw_data_v3_test.go` | `TestRawDataV3Suite` | Non-TEXT scalar, vector and StructArray raw data in Storage V3: inspect every manifest-referenced Parquet object, verify loaded manifest identities, then check actual values after release/reload. The scalar case also verifies correct, missing and wrong keys on one real object. |
| `raw_data_v3_growing_test.go` | `TestRawDataV3GrowingSuite` | One-shard V3 growing batches, Stop/restart recovery, append after recovery, and sealed cold reads. Each snapshot checks actual DataCoord/WAL metadata and every referenced encrypted Parquet object. |
| `raw_data_v3_consumer_test.go` | `TestRawDataV3ConsumerSuite`, growing-suite index case | Canonical and growing-enabled scenarios use encrypted V3 manifests as real HNSW build inputs, physical index files, loaded build identity, and query/search results. |
| `scalar_index_v2_test.go` | `TestScalarIndexV2Campaign` | STL_SORT, Trie, BITMAP, HYBRID, INVERTED, NGRAM, RTREE and TextMatch artifacts under scalar engine version 2 and legacy IndexData format V2. |
| `scalar_index_v3_test.go` | `TestScalarIndexV3Campaign` | The same scalar families under scalar engine version 3 and packed index artifact format V3, including object checks and query results after reload. |
| `scalar_index_fmindex_v3_test.go` | `TestScalarIndexFMINDEXCampaign` | FMINDEX under scalar engine version 5 and packed artifact format V3. Its cost-ratio setting makes the small fixture's LIKE query use FMINDEX. |
| `vector_index_test.go` | `TestVectorIndexV2Suite` | HNSW index artifacts produced from Storage V2 raw data, using vector engine version 8 and IndexData format V2. Verify all index objects, actual loaded index identities and the deterministic search result. |

Segment Storage Version, index engine version and index artifact format are separate choices. The scalar-index suites currently all use Storage V2 source segments, including the packed V3 and FMINDEX suites. They do not provide Storage V3 raw-data coverage. The canonical V2/V3 raw-data campaigns and Growing batch/recovery cases keep physical indexes absent; the V3 consumer cases build physical HNSW indexes from encrypted V3 source. `vector_index_test.go` covers physical HNSW from V2 source.

## Shared scenario code

| File | Responsibility |
| --- | --- |
| `raw_data_suite_test.go` | Shared V2/V3 MiniCluster setup, flushed-segment metadata, loaded fields and absence of physical vector indexes. |
| `raw_data_campaign_test.go` | Shared scalar/vector/StructArray schemas, deterministic data, collection preparation, server-assigned field IDs, flush and exact query/search assertions. Its vector-data helper is also used by the vector-index suite. |
| `scalar_index_fixture_test.go` | Shared scalar-index cluster lifecycle and `runCell` flow: create, insert, flush, build/locate/inspect index objects, release/reload and query. It also currently owns package-wide `TestMain`, temporary cipher configuration and fixture plugin-path resolution used by all CMEK suites. |
| `scalar_index_oracle_test.go` | Scalar-index test inputs, index parameters, query predicates and expected IDs/counts for each family. `assertOracle` compares Milvus query results with those expectations. |
| `fixture_keys_test.go` | Independently authenticate fixture EDEKs and derive object DEKs for the real Parquet object's three key modes. |

All raw-data suites disable compaction before starting the cluster. Growing-source flush is enabled in the V3 Growing suite; canonical V2/V3 and canonical HNSW keep it disabled. The Growing cases inspect each background commit before final `Flush`, require the Segment to remain Growing, and compare earlier object digests across revisions. Recovery stops the channel owner once through the existing Stop interface, reads the actual metadata after exit, creates a replacement, checks every expected row, appends another batch, and checks new encrypted objects. The single-StreamingNode fixture sets the existing QueryNode shutdown timeout to five seconds because no replacement is available for channel migration yet. `Stop` must reap the process before replacement starts; the test accepts a successful exit or code 1 from the existing shutdown-deadline path, and fails on other exit errors. It does not assert that graceful migration completed. In-flight sync may finish during shutdown; the test does not assume a frozen manifest or claim an unobserved WAL replay boundary. Sealed cases inspect every object, release/reload, and confirm the loaded Segment identities.

The V3 scalar scenario uses one nonempty Parquet object for the format's key baseline. An independent Arrow Go reader fully reads its known payload with the correct fixture-derived key, rejects missing decryption configuration and rejects a legal-length wrong key. All three modes use the same bytes and a fresh reader.

## Object discovery and encryption checks

| File | Responsibility |
| --- | --- |
| `inspector/storage_v2_binlogs.go` | `LocateRawDataV2` finds every raw-data object through `FieldBinlog/Binlog`. |
| `inspector/storage_v3_manifest.go` | `LocateManifestsV3` selects each segment's exact manifest revision; `ParseParquetObjectsV3` reads the fields needed to enumerate every referenced data object, ignoring unrelated metadata extensions. |
| `inspector/parquet_encryption.go` | `InspectEncryptedParquet` checks encrypted footer metadata, algorithm, EZ and collection identity. `ReadEncryptedParquet` independently reads all row groups. These are physical-format tools shared by V2/V3 where applicable. |
| `inspector/locator.go` | `LocateScalarIndex` resolves scalar-index paths from segment/index metadata; `LocateTextLog` resolves TextMatch objects from `TextStatsLogs`. |
| `inspector/scalar_v2.go` | `InspectV2` checks the legacy scalar IndexData envelope's EDEK and EZ identity. |
| `inspector/scalar_v3.go` | `InspectV3` independently validates the packed scalar-index header, footer, directory, entry/slice ranges and encryption metadata. |
| `inspector/vector_index.go` | `LocateVectorIndex` resolves all vector-index objects and build identities; `InspectIndexDataV2` checks descriptor identity/encryption metadata and rejects a complete plaintext index event. |
| `inspector/object_reader.go` | `ObjectReader.Read` fetches object bytes through the cluster's ChunkManager and rejects empty objects. |

The current V2 and testable V3 raw-data scenarios both store Parquet files. Their object discovery differs: V2 follows binlogs; V3 follows manifests. Scalar packed index format V3 is a different format from these Parquet objects.

## Cipher fixtures and tool checks

| Files | Responsibility |
| --- | --- |
| `pluginmock/go/main.go`, `pluginmock/cpp/cipher_plugin.cpp` | Go/C++ cipher plugins implementing the same test key derivation and authenticated EDEK protocol. Built into shared libraries consumed by the Milvus processes. |
| `plugin_mode_test.go`, `plugin_mode_race_test.go` | Select the Go fixture library matching the test binary's non-race/race build mode. |
| `fixture_plugins_test.go`, fixture checks in `scalar_index_fixture_test.go` | Check fixture artifact paths, Go plugin ABI and cipher configuration. |
| `raw_data_schema_test.go`, `raw_data_metadata_test.go` | Check field-ID binding and metadata-selection helpers used by the raw-data scenarios. |
| `inspector/*_test.go` | Check the object locators and encryption validators, including incomplete object sets, malformed metadata and independent-reader failure handling. |
| `pluginmock/go/main_test.go`, `pluginmock/cpp/cipher_plugin_test.cpp` | Check the fixture protocol implementations. |

Tool and fixture checks verify the reliability of the IT's supporting code; they are not additional product scenarios. Product acceptance runs through collection, flush/build and release/reload. `growing_loader_test.go` additionally calls the real Go FlushData and loader with the same cipher fixture, checks the physical objects and exact loaded fields, and has no WAL replay. Native growing tests directly verify the interim-index source after raw chunks are cleared. Internal paths are not inferred from production log messages.

Deterministic binary-key regressions live in the existing unit tests: `internal/core/unittest/CipherPluginContextTest.cpp` checks `GetEncParams` for both imported and registered contexts, and `internal/storagev2/packed/packed_writer_ffi_test.go` verifies Parquet readback with Arrow Go. Both use 32-byte keys whose first NUL is at byte 0, 16, 24, or 31, including the two truncation lengths that remain valid AES keys.

The C++ fixture requires a registered collection context before creating an encryptor and rejects it after unref. Growing-source acceptance therefore depends on QueryNode's real Collection Ref path, including when the writer supplies only lookup IDs. The Growing and HNSW consumer suites enable strict fixture mode, which rejects decryption until that process registers the matching EZ and collection context. This tests registration on a replacement node and during index build. The fixture still derives key bytes deterministically; it does not model an external KMS outage or delayed key revocation during overlapping handoff.

V3 raw-data Vortex and TEXT/LOB remain outside current coverage. Growing-source coverage is limited to non-TEXT Parquet groups. Start raw-data verification with the V3 scalar scenario, then run the V2/V3 matrix; investigate an unexpected failure before expanding the run.
