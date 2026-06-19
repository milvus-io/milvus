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
- Comprehensive test coverage for all CDC-supported operations

## Prerequisites

Before running the tests, ensure you have:

1. **Two running Milvus instances:**
   - Upstream cluster (source)
   - Downstream cluster (target)

2. **Python dependencies:**
   ```bash
   pip install pymilvus>=2.6.0 pytest numpy
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
- TRANSFER_NODE
- TRANSFER_REPLICA

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
