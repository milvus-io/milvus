# CDC Sync Test Cases

## Overview

This test suite validates CDC (Change Data Capture) synchronization between upstream and downstream Milvus clusters. It verifies that operations performed on the upstream cluster are correctly replicated to the downstream cluster through CDC.

**What it tests:**
- Database, collection, partition, and index operations
- Data manipulation (insert, delete, upsert, bulk insert)
- RBAC operations (users, roles, privileges)
- Collection management (load, release, flush, compact)
- Alias operations

**Key Features:**
- Automatic CDC topology setup at test start
- Query-based verification to ensure data consistency
- Configurable sync timeout with progress logging
- Explicit checks for replicated state, local operations, and rejected interfaces

## Prerequisites

Before running the tests, ensure you have:

1. **Two running Milvus instances:**
   - Upstream cluster (source)
   - Downstream cluster (target)

2. **Python dependencies:**
   ```bash
   # From the repository root
   uv venv -p 3.12
   source .venv/bin/activate
   uv pip install -r tests/python_client/requirements.txt
   ```

3. **Network connectivity:**
   - Both clusters should be accessible from the test environment
   - Authentication credentials for both clusters

## Quick Start

The simplest way to run tests with default configuration:

```bash
cd /path/to/milvus/tests/python_client/cdc

# Run all tests with default settings
pytest testcases/
```

**Note:** The test framework automatically configures CDC topology at startup. Default configuration uses:
- Upstream URI: `http://10.104.17.154:19530`
- Downstream URI: `http://10.104.17.156:19530`
- Authentication: `root:Milvus`

To customize connections, see the [Configuration](#configuration) section.

### How CDC Topology Works

The test framework automatically sets up CDC replication between clusters at session start:

1. Creates cluster configurations with specified IDs and physical channels
2. Establishes one-way replication: upstream → downstream
3. Initializes the CDC connection (5-second wait period)

This setup is transparent - tests will automatically use the configured topology.

## Test Categories

The test suite is organized into the following categories:

### 1. Database Operations
**File:** `test_database.py`
- CREATE_DATABASE
- DROP_DATABASE
- ALTER_DATABASE_PROPERTIES
- DROP_DATABASE_PROPERTIES

### 2. Resource Group Operations
**File:** `test_resource_group.py`
- CREATE_RESOURCE_GROUP
- DROP_RESOURCE_GROUP
- UPDATE_RESOURCE_GROUP (local on both roles)
- TRANSFER_REPLICA (primary assignment and default secondary-local override)

### 3. RBAC Operations
**File:** `test_rbac.py`
- CREATE_ROLE / DROP_ROLE
- CREATE_USER / DROP_USER
- GRANT_ROLE / REVOKE_ROLE
- GRANT_PRIVILEGE / REVOKE_PRIVILEGE

### 4. Collection DDL Operations
**File:** `test_collection.py`
- CREATE_COLLECTION
- DROP_COLLECTION
- RENAME_COLLECTION

### 5. Index Operations
**File:** `test_index.py`
- CREATE_INDEX
- DROP_INDEX

### 6. Data Manipulation Operations
**File:** `test_dml.py`
- INSERT
- DELETE
- UPSERT
- BULK_INSERT

### 7. Collection Management Operations
**File:** `test_collection_management.py`
- LOAD_COLLECTION
- RELEASE_COLLECTION
- FLUSH
- COMPACT

### 8. Alias Operations
**File:** `test_alias.py`
- CREATE_ALIAS
- DROP_ALIAS
- ALTER_ALIAS

### 9. Partition Operations
**File:** `test_partition.py`
- CREATE_PARTITION / DROP_PARTITION
- LOAD_PARTITION / RELEASE_PARTITION
- Partition data operations (INSERT, DELETE)

## Configuration

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--upstream-uri` | Upstream Milvus URI | `http://10.104.17.154:19530` |
| `--upstream-token` | Upstream authentication token | `root:Milvus` |
| `--downstream-uri` | Downstream Milvus URI | `http://10.104.17.156:19530` |
| `--downstream-token` | Downstream authentication token | `root:Milvus` |

### CDC Topology Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--source-cluster-id` | Source cluster identifier | `cdc-test-source-0930` |
| `--target-cluster-id` | Target cluster identifier | `cdc-test-target-0930` |
| `--pchannel-num` | Number of physical channels | `16` |

### Test Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--sync-timeout` | Sync timeout in seconds | `30` |

## Usage Examples

### Run Specific Test Category

```bash
# Database operation tests
pytest testcases/test_database.py

# RBAC operation tests
pytest testcases/test_rbac.py

# Data manipulation tests
pytest testcases/test_dml.py
```

### Custom Connection Configuration

```bash
pytest testcases/ \
  --upstream-uri http://localhost:19530 \
  --upstream-token root:Milvus \
  --downstream-uri http://localhost:19531 \
  --downstream-token root:Milvus
```

### Custom Sync Timeout

For slower networks or larger data volumes:

```bash
pytest testcases/test_dml.py --sync-timeout 180
```

### Custom CDC Topology

Configure custom cluster IDs and channel count:

```bash
pytest testcases/ \
  --source-cluster-id my-source \
  --target-cluster-id my-target \
  --pchannel-num 32
```

### Full Custom Configuration

Combine all parameters:

```bash
pytest testcases/test_database.py \
  --upstream-uri http://10.100.1.10:19530 \
  --upstream-token root:Milvus \
  --downstream-uri http://10.100.1.20:19530 \
  --downstream-token root:Milvus \
  --source-cluster-id prod-source \
  --target-cluster-id prod-target \
  --pchannel-num 32 \
  --sync-timeout 180
```

### Run Specific Test Method

```bash
pytest testcases/test_database.py::TestCDCSyncDatabase::test_create_database \
  --upstream-uri http://localhost:19530 \
  --downstream-uri http://localhost:19531
```

## Project Structure

For developers who need to understand the test organization:

```
testcases/
├── base.py                     # Base test class and utility functions
├── test_database.py            # Database operation tests
├── test_rbac.py                # RBAC operation tests
├── test_collection.py          # Collection DDL operation tests
├── test_index.py               # Index operation tests
├── test_dml.py                 # Data manipulation tests
├── test_collection_management.py # Collection management tests
├── test_alias.py               # Alias operation tests
└── test_partition.py           # Partition operation tests
```

## Interface contract regression

The supplemental cases use the repository-pinned PyMilvus version and require a
CDC pair, so they are tagged `CDC,L3`. Run them serially: topology changes and
Import 2PC use session-wide state. Both clusters must use
`common.storage.useLoonFFI=true`,
`dataCoord.compaction.bumpSchemaVersion.enabled=true`, and
`dataCoord.compaction.storageVersion.enabled=true` for the function-field lifecycle case. Replica
assignment cases assume the default
`streaming.replication.useLocalReplicaConfig=true` and one secondary replica.
The multi-replica case additionally needs two primary query nodes and, when
StreamingNodes host replica delegators, two primary StreamingNodes. Missing
capacity is reported as a skip, not a pass.

CDC test operations and state checks use `MilvusClient`, including replica and
file-resource inspection. CDC fixtures, stability workloads and the comparison
script do not register ORM connections or use `MilvusSys`. Stability runs must
pass `--upstream-milvus-name <Milvus-CR-or-release-name>` for Kubernetes Pod
readiness checks; the CDC cluster ID is not assumed to be the release name.

Protocol-only coverage uses the authenticated client channel when PyMilvus
3.1.0rc98 has no public wrapper: unsupported namespace/prewarm RPCs, manifest
updates, RBAC backup/restore, phrase slop, telemetry commands, snapshot
export/external restore, and schema requests the public wrappers cannot express.
Import jobs use the SDK bulk-import helpers and the import REST endpoints;
collection and row checks use `MilvusClient`.

| File | Contracts checked |
| --- | --- |
| `test_api_mutations.py` | Add/drop scalar and struct fields; function/output-index lifecycle; rejected legacy Function calls; truncate; partial update; field/index properties; secondary write rejection; FlushAll |
| `test_api_boundaries.py` | Eight unimplemented RPCs on both roles; snapshot local lifecycle; primary-only snapshot/manifest calls; complete snapshot restore data |
| `test_api_control.py` | RestoreRBAC, AlterRole, restored credentials and effective permissions; phrase slop; local heartbeat/telemetry/client commands |
| `test_api_storage.py` | Local file-resource registration/removal; external refresh/list/progress; snapshot export/state and external restore |
| `test_import_2pc.py` | Manual import commit/abort, rejected auto-commit without visible rows, and subsequent successful imports |
| `test_resource_group.py` | Actual create/update/drop/transfer calls, with local/replicated-state assertions |

Run from `tests/python_client` with the repository virtual environment active:

```bash
pytest cdc/testcases --tags L3 -n 0 \
  -k 'TestCDCAPIChanges or TestCDCUnsupportedAPI or TestCDCSnapshotBoundary or TestCDCControlAPI or TestCDCStorageAPI or TestCDCImport2PC or TestCDCSyncResourceGroup' \
  --upstream-uri "$CDC_SOURCE_URI" --downstream-uri "$CDC_TARGET_URI" \
  --source-cluster-id "$CDC_SOURCE_ID" --target-cluster-id "$CDC_TARGET_ID" \
  --import-2pc-minio-host "$CDC_SOURCE_MINIO" \
  --import-2pc-minio-bucket "$CDC_SOURCE_BUCKET" \
  --import-2pc-downstream-minio-host "$CDC_TARGET_MINIO" \
  --import-2pc-downstream-minio-bucket "$CDC_TARGET_BUCKET" \
  --sync-timeout 120 --timeout 600
```

Import cases require `dataCoord.import.enableInReplicatingCluster=true` on both
clusters. They verify manual commit/abort and rejection of `auto_commit=true`.
Keep the effective `dataCoord.import.enableIDRangeMsg` value consistent across
both clusters. For a homogeneous build supporting the UpdateImport WAL message,
set it to `true` on both Milvus CRs for these tests. Its default `auto` gate
resolves independently in each cluster; a secondary that enables it while the
primary keeps the legacy path can wait indefinitely for ID ranges. Do not force
the new WAL message on older nodes that cannot replay it.
MinIO endpoints must be reachable by the runner and Milvus servers. Use the
actual bucket names from each deployment's runtime configuration. Objects are
created under unique per-test prefixes. Storage API cases remove their objects
in finalizers; the shared Import2PCChecker retains its staged import files.

Rejected requests must return the expected error and preserve collection schema,
index definitions, partitions, and exact rows on both clusters. Unimplemented
RPCs require gRPC `UNIMPLEMENTED`; transport errors do not count as success.
Local control-plane tests use a subsequent replicated database operation as a
CChannel progress barrier. This barrier does not prove progress on every data
channel; data cases additionally wait for exact row contents.

Snapshot restore and external refresh cases deliberately require complete
business data on the replica after a successful source operation. If the server
accepts an operation but only replicates the collection metadata, these tests
fail. Such a failure must not be converted to a skip or relaxed to metadata-only
checks. Collection/format checks do not establish an E2E pass, and adding these
files does not by itself verify a recurring Jenkins selection.
