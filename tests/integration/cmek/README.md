# CMEK integration tests

The suites verify persisted raw data and index artifacts under CMEK using repository-owned cipher fixtures and isolated MiniCluster processes. Each IT writes through Milvus, locates the resulting objects from authoritative metadata, checks their encryption, then releases/reloads the collection and verifies query or search results.

## Running the suites

Build the C++ libraries and ordinary Milvus server, then run `make build-cmek-fixtures`. Set `MILVUS_WORK_DIR` to the Milvus checkout and `MILVUS_CMEK_FIXTURE_DIR` to its `bin/cmek-fixtures` directory. The test runner uses the `dynamic,test` build tags and matching race fixtures when enabled.

## IT entry points

| File | Test entry point | What it verifies |
| --- | --- | --- |
| `raw_data_v2_test.go` | `TestRawDataV2Suite` | Scalar, vector and StructArray raw data in Storage V2: inspect every binlog-referenced Parquet object, then release/reload and check actual values. |
| `raw_data_v3_test.go` | `TestRawDataV3Suite` | Non-TEXT scalar, vector and StructArray raw data in Storage V3: inspect every manifest-referenced Parquet object, verify loaded manifest identities, then check actual values after release/reload. The scalar case also verifies correct, missing and wrong keys on one real object. |
| `raw_data_v3_growing_test.go` | `TestRawDataV3GrowingSuite` | The V3 Parquet campaigns with growing-source flush enabled and the collection loaded before insert. Existing completion logs tie every inspected manifest to a nonempty growing-source flush. Compaction stays disabled. |
| `scalar_index_v2_test.go` | `TestScalarIndexV2Campaign` | STL_SORT, Trie, BITMAP, HYBRID, INVERTED, NGRAM, RTREE and TextMatch artifacts under scalar engine version 2 and legacy IndexData format V2. |
| `scalar_index_v3_test.go` | `TestScalarIndexV3Campaign` | The same scalar families under scalar engine version 3 and packed index artifact format V3, including object checks and query results after reload. |
| `scalar_index_fmindex_v3_test.go` | `TestScalarIndexFMINDEXCampaign` | FMINDEX under scalar engine version 5 and packed artifact format V3. Its cost-ratio setting makes the small fixture's LIKE query use FMINDEX. |
| `vector_index_test.go` | `TestVectorIndexV2Suite` | HNSW index artifacts produced from Storage V2 raw data, using vector engine version 8 and IndexData format V2. Verify all index objects, actual loaded index identities and the deterministic search result. |

Segment Storage Version, index engine version and index artifact format are separate choices. The scalar-index suites currently all use Storage V2 source segments, including the packed V3 and FMINDEX suites. They do not provide Storage V3 raw-data coverage. Raw Vector scenarios in the raw-data suites keep physical and interim vector indexes absent; `vector_index_test.go` explicitly builds and reads a physical HNSW index.

## Shared scenario code

| File | Responsibility |
| --- | --- |
| `raw_data_suite_test.go` | Shared V2/V3 MiniCluster setup, flushed-segment metadata, loaded fields and absence of physical vector indexes. |
| `raw_data_campaign_test.go` | Shared scalar/vector/StructArray schemas, deterministic data, collection preparation, server-assigned field IDs, flush and exact query/search assertions. Its vector-data helper is also used by the vector-index suite. |
| `scalar_index_fixture_test.go` | Shared scalar-index cluster lifecycle and `runCell` flow: create, insert, flush, build/locate/inspect index objects, release/reload and query. It also currently owns package-wide `TestMain`, temporary cipher configuration and fixture plugin-path resolution used by all CMEK suites. |
| `scalar_index_oracle_test.go` | Scalar-index test inputs, index parameters, query predicates and expected IDs/counts for each family. `assertOracle` compares Milvus query results with those expectations. |
| `fixture_keys_test.go` | Independently authenticate fixture EDEKs and derive object DEKs for the real Parquet object's three key modes. |

All raw-data suites disable compaction before starting the cluster. Growing-source flush is enabled only in `TestRawDataV3GrowingSuite`; the canonical suites keep it disabled. After `Flush` and `WaitForFlush`, they inspect every object referenced by the returned segments, then perform one release/reload. V3 also uses existing QueryNode distribution RPCs to confirm that the inspected segments and manifests are loaded and serviceable, without growing data serving the query. Unexpected segment, manifest, node or load-version changes fail the test.

The V3 scalar scenario uses one nonempty Parquet object for the format's key baseline. An independent Arrow Go reader fully reads its known payload with the correct fixture-derived key, rejects missing decryption configuration and rejects a legal-length wrong key. All three modes use the same bytes and a fresh reader.

## Object discovery and encryption checks

| File | Responsibility |
| --- | --- |
| `inspector/storage_v2_binlogs.go` | `LocateRawDataV2` finds every raw-data object through `FieldBinlog/Binlog`. |
| `inspector/storage_v3_manifest.go` | `LocateManifestsV3` selects each segment's exact manifest revision; `ParseParquetObjectsV3` validates its contents and enumerates every data object. |
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

Tool and fixture checks verify the reliability of the IT's supporting code; they are not additional product scenarios. Product acceptance runs through collection, flush/build and release/reload. There is no separate packed-writer regression in this IT suite.

The C++ fixture requires a registered collection context before creating an encryptor and rejects it after unref. Growing-source acceptance therefore depends on QueryNode's real Collection Ref path, including when the writer supplies only lookup IDs.

V3 raw-data Vortex and TEXT/LOB remain outside current coverage. Growing-source coverage is limited to non-TEXT Parquet groups. Start raw-data verification with the V3 scalar scenario, then run the V2/V3 matrix; investigate an unexpected failure before expanding the run.
