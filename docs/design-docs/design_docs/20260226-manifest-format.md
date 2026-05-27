# Milvus Storage Manifest Format

- **Created:** 2026-02-26
- **Author(s):** @tedxu
- **Status:** Implemented
- **Component:** Storage
- **Related Issues:** milvus-io/milvus-storage#406

## Summary

The manifest is the core metadata format in milvus-storage that tracks the state of a dataset. Each manifest version is an immutable snapshot describing which column groups, delta logs, statistics, and indexes comprise the dataset at a given point in time. Manifests are serialized using Apache Avro binary encoding and stored as versioned files on the filesystem.

## Motivation

Milvus Storage is a multi-format columnar storage engine that supports concurrent reads and writes across multiple storage backends (local, S3, GCS, Azure, etc.). The system needs a lightweight, versioned metadata layer that:

1. **Tracks dataset composition**: which data files, column groups, and formats are in use
2. **Supports transactional updates**: enables atomic commits with conflict resolution
3. **Records auxiliary metadata**: delta logs for deletes, statistics for query optimization, and index references for accelerated search
4. **Works across storage backends**: serializes compactly and uses relative paths for portability
5. **Evolves without breaking readers**: older manifest versions remain readable

---

## 1. Directory Layout

All files for a dataset are organized under a base directory:

```
base_dir/
├── _metadata/                         # Manifest metadata directory
│   └── manifest-{version}.avro       # Manifest files (e.g., manifest-1.avro)
├── _data/                             # Column group data files
│   └── {group_id}_{uuid}.{format}    # Data files (Parquet, Vortex, etc.)
├── _delta/                            # Delta log files
│   └── {delta_log_files}             # Delete operation logs
├── _stats/                            # Statistics files
│   └── {stats_files}                 # Bloom filters, BM25, etc.
└── _index/                            # Index files
    └── {index_files}                  # HNSW, IVF, bitmap, etc.
```

Manifest file naming follows the pattern `manifest-{version}.avro` where version is an integer starting from 1 and incrementing with each commit. The latest version is determined by scanning the `_metadata/` directory for the highest numbered file.

---

## 2. Binary Format

Manifests use Apache Avro binary encoding. The on-disk byte layout is:

```
+-------------------+-------------------+
| MAGIC (int32)     | VERSION (int32)   |
| 0x4D494C56        | 1 or 2            |
+-------------------+-------------------+
| COLUMN_GROUPS     | DELTA_LOGS        |
| (avro array)      | (avro array)      |
+-------------------+-------------------+
| STATS             | INDEXES           |
| (avro map)        | (avro array,      |
|                   |  version 2+ only) |
+-------------------+-------------------+
```

### 2.1 Header

| Field | Type | Value | Description |
|-------|------|-------|-------------|
| Magic | int32 | `0x4D494C56` | ASCII "MILV", identifies valid manifest files |
| Version | int32 | 1 or 2 | Manifest format version |

### 2.2 Version History

| Version | Description |
|---------|-------------|
| 1 | Initial format: column_groups, delta_logs, stats |
| 2 | Added indexes field for index metadata (current) |

Backward compatibility: version 1 manifests are readable by version 2 code; the indexes field is treated as empty. Forward compatibility is not supported; versions greater than `MANIFEST_VERSION` fail with an error.

---

## 3. Data Structures

### 3.1 ColumnGroupFile

Represents a single physical file within a column group.

```cpp
struct ColumnGroupFile {
  std::string path;               // File path (relative within _data/)
  int64_t start_index;            // Start row index in the file (inclusive)
  int64_t end_index;              // End row index in the file (exclusive)
  std::vector<uint8_t> metadata;  // Optional metadata (e.g., external table info)
};
```

Avro encoding order: `path`, `start_index`, `end_index`, `metadata`.

### 3.2 ColumnGroup

A set of related columns stored together in the same physical file(s).

```cpp
struct ColumnGroup {
  std::vector<std::string> columns;    // Column names in this group
  std::string format;                  // Storage format
  std::vector<ColumnGroupFile> files;  // Physical file references
};
```

Avro encoding order: `columns`, `files`, `format`.

Supported formats:

| Format | Description |
|--------|-------------|
| `parquet` | Apache Parquet columnar format |
| `vortex` | Vortex compressed format |
| `lance` | Lance format (read-only) |
| `binary` | Raw binary format |

### 3.3 DeltaLog

Records delete operations against the dataset.

```cpp
struct DeltaLog {
  std::string path;     // File path (relative within _delta/)
  DeltaLogType type;    // Delete type (encoded as int32)
  int64_t num_entries;  // Number of delete entries
};
```

Avro encoding order: `path`, `type` (as int32), `num_entries`.

| DeltaLogType | Value | Description |
|--------------|-------|-------------|
| PRIMARY_KEY | 0 | Delete by primary key |
| POSITIONAL | 1 | Delete by row position |
| EQUALITY | 2 | Delete by equality predicate |

### 3.4 Stats

A map from stat name to a list of file paths. Each key identifies a type of statistic (e.g., bloom filter, BM25) and the values are relative paths within `_stats/`.

```
std::map<std::string, std::vector<std::string>> stats;
```

### 3.5 Index (Version 2+)

Metadata for a column index.

```cpp
struct Index {
  std::string column_name;                         // Column this index is on
  std::string index_type;                          // Index algorithm
  std::string path;                                // File path (relative within _index/)
  std::map<std::string, std::string> properties;   // Index-specific parameters
};
```

Avro encoding order: `column_name`, `index_type`, `path`, `properties`.

Supported index types:

| Index Type | Description |
|------------|-------------|
| `hnsw` | Hierarchical Navigable Small World graph |
| `ivf-sq` | IVF with scalar quantization |
| `ivf-pq` | IVF with product quantization |
| `inverted` | Inverted index for scalar fields |
| `bitmap` | Bitmap index for low-cardinality fields |
| `ordered` | Ordered index for range queries |

The `properties` map is intentionally flexible. Common keys include:

| Key | Example | Description |
|-----|---------|-------------|
| `index_id` | `"42"` | Unique index identifier |
| `version` | `"1"` | Index build version |
| `M` | `"16"` | HNSW max connections per layer |
| `efConstruction` | `"128"` | HNSW construction quality parameter |
| `metric_type` | `"L2"` | Distance metric |
| `num_rows` | `"1000000"` | Number of indexed rows |
| `index_size` | `"134217728"` | Index file size in bytes |

---

## 4. Path Handling

All paths in the manifest are stored as **relative paths** within their respective subdirectories. During serialization, absolute paths are stripped of the base directory prefix. During deserialization, relative paths are reconstructed as absolute paths.

```
Serialization:   /data/base/_data/0_abc.parquet  →  0_abc.parquet
Deserialization:  0_abc.parquet  →  /data/base/_data/0_abc.parquet
```

**External table exception**: paths that contain a URI scheme (e.g., `s3://bucket/file.parquet`, `gs://bucket/file.parquet`) are kept as absolute paths. This allows external table column groups to reference files in external storage systems.

---

## 5. Transaction System

Manifests are updated through a transaction system that provides atomic commits and conflict resolution.

### 5.1 Transaction Lifecycle

```
Open(fs, base_path, version)
        |
        v
  Read manifest at version
  (or empty manifest if version=0)
        |
        v
  Record updates via fluent API:
    .AddColumnGroup(cg)
    .AppendFiles(cgs)
    .AddDeltaLog(delta)
    .UpdateStat(key, files)
    .AddIndex(idx)
    .DropIndex(col, type)
        |
        v
  Commit()
    1. Read latest manifest (seen_manifest)
    2. Invoke resolver(read, read_ver, seen, seen_ver, updates)
    3. Write resolved manifest as manifest-{seen_ver+1}.avro
    4. On conflict, retry up to retry_limit times
        |
        v
  Return committed version
```

### 5.2 Updates

The `Updates` class tracks all changes made during a transaction:

| Operation | Description |
|-----------|-------------|
| `AddColumnGroup` | Add a new column group |
| `AppendFiles` | Append files to existing column groups |
| `AddDeltaLog` | Add a delta log entry |
| `UpdateStat` | Add or replace a stat entry |
| `AddIndex` | Add or replace an index (keyed by column_name + index_type) |
| `DropIndex` | Remove an index by column_name + index_type |

### 5.3 Conflict Resolution

Three built-in resolvers handle concurrent commits:

| Resolver | Behavior |
|----------|----------|
| `FailResolver` | Fails if `read_version != seen_version` (strict serialization) |
| `MergeResolver` | Applies updates to `seen_manifest` (merges concurrent changes) |
| `OverwriteResolver` | Applies updates to `read_manifest` (ignores concurrent changes) |

### 5.4 Index Deprecation

When files are appended to existing column groups via `AppendFiles`, indexes on affected columns are **automatically dropped**. This is because appending new data invalidates existing index structures. Other operations (delta logs, stats updates) do not affect indexes.

```
Transaction: AppendFiles to column group containing "embedding"
  → Any index with column_name="embedding" is silently removed
```

---

## 6. Manifest API

### 6.1 Manifest Class

```cpp
class Manifest final {
  // Construction
  explicit Manifest(ColumnGroups column_groups = {},
                    const std::vector<DeltaLog>& delta_logs = {},
                    const std::map<std::string, std::vector<std::string>>& stats = {},
                    const std::vector<Index>& indexes = {},
                    uint32_t version = MANIFEST_VERSION);

  // Serialization
  arrow::Status serialize(std::ostream& output,
                          const std::optional<std::string>& base_path = std::nullopt) const;
  arrow::Status deserialize(std::istream& input,
                            const std::optional<std::string>& base_path = std::nullopt);

  // Accessors
  ColumnGroups& columnGroups();
  std::shared_ptr<ColumnGroup> getColumnGroup(const std::string& column_name) const;
  std::vector<DeltaLog>& deltaLogs();
  std::map<std::string, std::vector<std::string>>& stats();
  std::vector<Index>& indexes();
  const Index* getIndex(const std::string& column_name,
                        const std::string& index_type) const;
  int32_t version() const;
};
```

### 6.2 Transaction Class

```cpp
class Transaction {
  static arrow::Result<std::unique_ptr<Transaction>> Open(
      const ArrowFileSystemPtr& fs,
      const std::string& base_path,
      int64_t version = LATEST,
      const Resolver& resolver = FailResolver,
      uint32_t retry_limit = 1);

  arrow::Result<int64_t> Commit();
  arrow::Result<std::shared_ptr<Manifest>> GetManifest();
  int64_t GetReadVersion() const;

  // Fluent builder
  Transaction& AddColumnGroup(const std::shared_ptr<ColumnGroup>& cg);
  Transaction& AppendFiles(const std::vector<std::shared_ptr<ColumnGroup>>& cgs);
  Transaction& AddDeltaLog(const DeltaLog& delta_log);
  Transaction& UpdateStat(const std::string& key,
                           const std::vector<std::string>& files);
  Transaction& AddIndex(const Index& index);
  Transaction& DropIndex(const std::string& column_name,
                          const std::string& index_type);
};
```

---

## 7. Example

A typical manifest for a dataset with two column groups, one delta log, and one HNSW index:

```
manifest-3.avro
├── Magic: 0x4D494C56
├── Version: 2
├── ColumnGroups:
│   ├── [0] columns=["id", "text"], format="parquet"
│   │   └── files:
│   │       ├── path="0_a1b2c3.parquet", start=0, end=50000
│   │       └── path="0_d4e5f6.parquet", start=0, end=30000
│   └── [1] columns=["embedding"], format="parquet"
│       └── files:
│           ├── path="1_g7h8i9.parquet", start=0, end=50000
│           └── path="1_j0k1l2.parquet", start=0, end=30000
├── DeltaLogs:
│   └── path="delete_001.del", type=PRIMARY_KEY, num_entries=128
├── Stats:
│   └── "bloomfilter" → ["bf_001.bin"]
└── Indexes:
    └── column_name="embedding", index_type="hnsw",
        path="embedding_hnsw.idx",
        properties={"M": "16", "efConstruction": "128", "metric_type": "L2"}
```

---

## 8. Source Files

| File | Description |
|------|-------------|
| `cpp/include/milvus-storage/manifest.h` | Manifest, DeltaLog, Index struct definitions |
| `cpp/include/milvus-storage/column_groups.h` | ColumnGroup and ColumnGroupFile definitions |
| `cpp/include/milvus-storage/common/layout.h` | Directory layout constants and path utilities |
| `cpp/include/milvus-storage/transaction/transaction.h` | Transaction API and resolver definitions |
| `cpp/src/manifest.cpp` | Avro serialization/deserialization implementation |
| `cpp/src/transaction/transaction.cpp` | Transaction, Updates, and resolver logic |
| `cpp/test/column_groups_test.cpp` | Manifest serialization round-trip tests |
| `cpp/test/api_transaction_test.cpp` | Transaction and index operation tests |

---

## References

- [Apache Avro Specification](https://avro.apache.org/docs/current/specification/)
- [Apache Parquet Format](https://parquet.apache.org/)
- [Milvus Storage Repository](https://github.com/milvus-io/milvus-storage)
- [Apache Iceberg Manifest Design](https://iceberg.apache.org/spec/) (inspiration for versioned manifest approach)
