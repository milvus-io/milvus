# HDFS Storage Backend Design Document

Current state: "Proposed"

ISSUE: https://github.com/milvus-io/milvus/issues/2745

Keywords: storage, HDFS, Hadoop, ChunkManager, object storage

## 1. Overview

### 1.1 Background

Milvus currently supports two storage backends for persistent data:

- **Remote** (MinIO / S3 / GCS / Azure Blob) — the default for production deployments.
- **Local** — for embedded or development use only.

Many enterprise users run Hadoop-based data infrastructure where HDFS is the authoritative storage layer. Their existing pipelines (Spark, Hive, Flink) read and write directly to HDFS. Forcing these users to operate a separate MinIO/S3 cluster solely for Milvus creates unnecessary operational cost, doubles storage footprint, and introduces a data hand-off layer between their existing pipeline and Milvus.

This document describes the design for adding HDFS as a first-class storage backend.

### 1.2 Goals

1. Implement `HDFSChunkManager` — a full implementation of the `ChunkManager` interface backed by HDFS.
2. Wire HDFS into the existing `ChunkManagerFactory` so it is activated via a single config change.
3. Add HDFS configuration parameters (`hdfs.address`, `hdfs.user`, `hdfs.rootPath`) following established param conventions.
4. Support both simple authentication and Kerberos authentication (provided by the underlying Go HDFS client).
5. Leave all existing storage backends (MinIO, local) entirely unchanged.

### 1.3 Non-Goals

1. HDFS is not a replacement for MinIO/S3 — it targets a specific enterprise use case.
2. Mmap support over HDFS — HDFS does not expose a memory-mappable interface; callers relying on `Mmap` must use a different backend.
3. HDFS HA (High Availability / multiple NameNodes) — not in scope for the initial implementation. The `colinmarc/hdfs` client supports this and can be added in a follow-up.
4. Automatic HDFS cluster provisioning or management.

---

## 2. Design

### 2.1 Architecture

Milvus storage is abstracted through the `ChunkManager` interface defined in `internal/storage/types.go`. All components (DataNode, QueryNode, IndexNode, etc.) interact with storage exclusively through this interface. Adding a new backend requires only:

1. A new struct implementing `ChunkManager`.
2. A case in the factory switch.
3. Config params and YAML entries.

No coordinator or node logic needs to change.

```
ChunkManager (interface, internal/storage/types.go)
    ├── LocalChunkManager        ← local filesystem
    ├── RemoteChunkManager       ← MinIO / S3 / GCS / Azure
    └── HDFSChunkManager  [NEW]  ← HDFS via NameNode RPC
```

### 2.2 HDFS Client Library

We use [`github.com/colinmarc/hdfs/v2`](https://github.com/colinmarc/hdfs), a pure-Go HDFS client that speaks the native Hadoop RPC protocol. It was chosen over alternatives because:

- Pure Go — no CGo, no `libhdfs` JNI dependency.
- Supports both simple auth and Kerberos (GSSAPI).
- Actively maintained; used in production at scale.
- `*hdfs.FileReader` already implements `io.Reader`, `io.ReaderAt`, `io.Seeker`, and `io.Closer`, making it trivially wrappable into `FileReader`.

**New transitive dependencies added to `go.mod`:**

| Package | Why |
|---|---|
| `github.com/colinmarc/hdfs/v2` | HDFS client |
| `github.com/jcmturner/gokrb5/v8` | Kerberos auth (bundled by hdfs client) |
| `github.com/jcmturner/aescts/v2` | AES cipher suites for Kerberos |
| `github.com/jcmturner/dnsutils/v2` | DNS SRV lookup for Kerberos |
| `github.com/jcmturner/gofork` | Kerberos crypto |
| `github.com/jcmturner/goidentity/v6` | Kerberos identity |
| `github.com/jcmturner/rpc/v2` | Kerberos RPC |
| `github.com/hashicorp/go-uuid` | UUID generation for Kerberos sessions |

These are all indirect dependencies pulled in by `gokrb5`. They add Kerberos support, which is required in many enterprise Hadoop clusters and adds real value.

### 2.3 `HDFSChunkManager` Implementation

**File:** `internal/storage/hdfs_chunk_manager.go`

The struct holds an `*hdfs.Client` (connected to the NameNode) and a `rootPath`:

```go
type HDFSChunkManager struct {
    client   *hdfs.Client
    rootPath string
}
```

All paths passed to the interface methods are made absolute by prepending `rootPath` when not already absolute. This matches the behavior of `LocalChunkManager`.

Key implementation decisions:

- **Write**: removes any existing file before calling `hdfs.Create`, since the HDFS client returns an error if the file already exists (unlike `os.Create`).
- **WalkWithPrefix**: uses `hdfs.Client.ReadDir` on the parent directory and filters by prefix, recursing into subdirectories when `recursive=true`. This avoids a full tree walk for non-recursive calls.
- **Mmap**: returns an explicit error — HDFS does not support memory-mapped I/O. This matches the behavior of `RemoteChunkManager`.
- **RemoveWithPrefix**: guards against empty prefix (same guard as `LocalChunkManager`) to prevent accidental full-tree deletion.

### 2.4 Factory Wiring

**File:** `internal/storage/factory.go`

`NewChunkManagerFactoryWithParam` gains an HDFS branch before the existing MinIO path:

```go
if params.CommonCfg.StorageType.GetValue() == "hdfs" {
    return NewChunkManagerFactory("hdfs",
        objectstorage.RootPath(params.HdfsCfg.RootPath.GetValue()),
        objectstorage.HDFSAddress(params.HdfsCfg.Address.GetValue()),
        objectstorage.HDFSUser(params.HdfsCfg.User.GetValue()),
    )
}
```

`newChunkManager` (the internal switch) gains:

```go
case "hdfs":
    return NewHDFSChunkManager(f.config.HDFSAddress, f.config.HDFSUser, f.config.RootPath)
```

### 2.5 Configuration

**New config params** (`pkg/util/paramtable/service_param.go`):

| Key | Default | Description |
|---|---|---|
| `hdfs.address` | `localhost:9000` | NameNode address in `host:port` format |
| `hdfs.user` | `hdfs` | HDFS user for simple authentication |
| `hdfs.rootPath` | `milvus` | Root path inside HDFS for all Milvus data |

**Activation**: set `common.storageType: hdfs` in `milvus.yaml`. The existing `minio.*` params are ignored when this is set.

**YAML section added to `configs/milvus.yaml`:**

```yaml
hdfs:
  address: localhost:9000   # NameNode host:port
  user: hdfs                # HDFS user (simple auth)
  rootPath: milvus          # Root path for all Milvus data in HDFS
```

**`pkg/objectstorage/options.go`** gains two new fields on `Config` and two new `Option` functions (`HDFSAddress`, `HDFSUser`) following the existing pattern.

---

## 3. Testing

Integration tests are in `internal/storage/hdfs_chunk_manager_test.go`. They are skipped unless the environment variable `HDFS_ADDRESS` is set, so they do not block CI runs without an HDFS cluster.

To run against a real cluster:

```bash
HDFS_ADDRESS=localhost:9000 HDFS_USER=hdfs \
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/storage/... -run TestHDFSChunkManagerSuite
```

Tests cover: `Write`, `Read`, `Exist`, `Size`, `MultiWrite`, `MultiRead`, `ReadAt`, `WalkWithPrefix` (recursive), `Remove`, `MultiRemove`, `RemoveWithPrefix`, `Copy`, and the expected error from `Mmap`.

---

## 4. Compatibility and Upgrade

- This change adds a new storage type; all existing storage types (`remote`, `local`, `minio`, `opendal`) are unaffected.
- No schema or metadata format changes.
- No rolling-upgrade concerns — the HDFS backend is only activated by explicit config opt-in.
- Existing deployments that do not set `common.storageType: hdfs` are entirely unaffected.

---

## 5. Future Work

- **HDFS HA (multiple NameNodes)**: the `colinmarc/hdfs` client supports passing multiple NameNode addresses; a follow-up can expose `hdfs.addresses` as a list.
- **Kerberos config**: expose `hdfs.kerberosRealm`, `hdfs.kerberosKDC`, and keytab path as config params for clusters using Kerberos auth.
- **Connection pooling**: the `hdfs.Client` is already goroutine-safe and reuses connections internally; no additional work needed.
