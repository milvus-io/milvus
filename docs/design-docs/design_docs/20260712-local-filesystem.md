# Rooted Local File System

- **Created:** 2026-07-12
- **Updated:** 2026-07-17
- **Status:** Draft
- **Component:** Local filesystem / Segcore / Index / Caching layer
- **Related code:** `internal/core/src/local`,
  `internal/core/src/storage`, `internal/core/src/index`,
  `internal/core/src/segcore`

## 1. Summary

Milvus needs an explicit abstraction for files that live on the node running a
component. These files include downloaded index artifacts, mmap backing files,
and operation-scoped scratch data. They are different from persisted objects
in S3-compatible storage even when both happen to be accessed through file-like
APIs.

This design introduces `milvus::local`:

```text
milvus::storage
    persisted object/key storage and remote transfer

milvus::local::FileSystem
    a rooted capability for one local filesystem namespace

milvus::local::io
    RAII handles for opened files and mapped regions

milvus::cachinglayer
    cache loading, pinning, accounting, and eviction policy
```

`FileSystem` is a small, copyable value handle. `FileSystem::Open()` binds
an absolute physical root once. All operations below that root use validated
relative `local::Path` values. `Subtree()` derives a narrower capability
without introducing another configured service or global registry.

```cpp
auto node_cache = local::FileSystem::Open(node_cache_root);
auto local_chunk = node_cache.Subtree(local::Path("local_chunk"));
auto growing_mmap = node_cache.Subtree(local::Path("growing_mmap"));
```

Directories that need writer-versus-cleanup coordination explicitly use
`ManagedSubtree`. Ordinary filesystem operations remain immediate and do not
hide cache or lifecycle policy.

## 2. Motivation

### 2.1 Local files are not object storage

`ChunkManager` models objects addressed by keys. Its implementations cover
remote object storage and persistent `storageType=local` compatibility.
Node-local artifacts have different requirements:

- directory creation and recursive removal;
- validated paths below a configured root;
- POSIX file descriptors and positioned I/O;
- mmap lifetime ownership;
- third-party libraries that require native paths;
- coordination between active writers and cleanup.

Putting these operations under `milvus::storage` blurs persistent storage,
remote transfer, node-local cache, and scratch lifetimes.

Persistent `storageType=local` remains under `milvus::storage`. The new
namespace is for local filesystem mechanisms used by node-local artifacts.

### 2.2 Absolute path strings spread authority

Passing strings such as
`/var/lib/milvus/data/cache/<node-id>/local_chunk/...` through business code
duplicates root construction and lets every consumer address sibling
directories. It also makes tests depend on process-global setup.

A rooted handle centralizes the absolute root and gives leaf components only
the subtree they need.

### 2.3 One physical root contains several logical namespaces

The node cache currently contains logical children such as:

```text
localStorage.path/cache/<node-id>/
├── local_chunk/
├── growing_mmap/
├── bm25/
├── file_resource/
└── expr_cache/
```

These are subtrees, not five independent filesystem services. A configured
mmap directory may still be opened as a separate root when it points at a
different disk or mount.

## 3. Goals and Non-goals

### 3.1 Goals

- represent local filesystem access as an explicit dependency;
- bind absolute paths at composition boundaries;
- use relative paths inside rooted namespaces;
- reject absolute paths and `..` root escape;
- support multiple roots in one process and in tests;
- provide move-only RAII file and mmap handles;
- preserve existing writer behavior and native-library compatibility;
- make concurrent writer/cleanup coordination explicit;
- preserve current on-disk paths and formats;
- allow incremental migration from legacy local file access.

### 3.2 Non-goals

- replace `milvus::cachinglayer`;
- introduce another cache namespace or eviction policy;
- move S3 APIs into `milvus::local`;
- remove persistent `storageType=local`;
- redesign index layouts or remote object names;
- migrate every direct filesystem call in one change;
- introduce io_uring, a buffer pool, or a virtual filesystem plugin system.

## 4. Namespace and Ownership Model

```text
internal/core/src/local/
├── Path.h
├── FileSystem.h
├── FileSystem.cpp
├── ManagedSubtree.h
├── ManagedSubtree.cpp
└── io/
    ├── File.h
    ├── File.cpp
    ├── MappedRegion.h
    └── MappedRegion.cpp
```

```cpp
namespace milvus::local {
class Path;
class FileSystem;
class ManagedSubtree;
}

namespace milvus::local::io {
class RandomAccessFile;
class WritableFile;
class MappedRegion;
}
```

The dependency direction is:

```text
storage transfer -----> local
index / segcore ------> storage + local + cachinglayer
cachinglayer policy --> local-backed values
local ----------------X storage
local ----------------X cachinglayer
```

`milvus::local` supplies mechanisms. It does not decide whether a file is a
cache entry, scratch data, or persistent data.

## 5. Relative Paths

`local::Path` always represents a path relative to a `FileSystem` handle:

```cpp
class Path {
 public:
    explicit Path(std::string value);
    std::string_view String() const noexcept;
};
```

Construction rejects:

- absolute paths;
- embedded NUL bytes;
- any normalized path containing `..`;
- values that would escape the handle root.

The absolute root appears only at `FileSystem::Open()`. Code below the
composition boundary carries `Path` rather than absolute strings.

## 6. FileSystem Value Handles

```cpp
class FileSystem final {
 public:
    static FileSystem Open(std::filesystem::path absolute_root);

    FileSystem Subtree(const Path& path) const;
    std::shared_ptr<ManagedSubtree> ManageSubtree(const Path& path) const;

    bool Exists(const Path& path) const;
    uint64_t FileSize(const Path& path) const;
    std::vector<Path> List(const Path& directory, bool recursive) const;

    void CreateDirectories(const Path& path) const;
    void RemoveFile(const Path& path) const;
    void RemoveAll(const Path& path) const;
    void Rename(const Path& from, const Path& to) const;

    io::RandomAccessFile OpenForRead(const Path& path) const;
    io::WritableFile OpenForWrite(const Path& path,
                                  const WriteOptions& options) const;
    io::MappedRegion OpenMappedRegion(const Path& path,
                                      const MapOptions& options) const;

    std::filesystem::path ResolveNativePath(const Path& path) const;
    Path PathFromNativePath(std::filesystem::path native_path) const;
};
```

The handle contains immutable shared root state plus a subtree prefix. Copies
are cheap and may be used concurrently. Sharing implementation state does not
make the handle a process-wide service.

There is no default filesystem, global registry, or `GetInstance()`.
Consumers receive a handle through construction context.

### 6.1 Subtree narrows authority

```cpp
auto node_cache = FileSystem::Open("/var/lib/milvus/data/cache/1001");
auto local_chunk = node_cache.Subtree(Path("local_chunk"));

local_chunk.OpenForRead(Path("index_files/10/index"));
```

The resolved native path is:

```text
/var/lib/milvus/data/cache/1001
    + local_chunk
    + index_files/10/index
```

The holder of `local_chunk` cannot address `expr_cache` through the
relative API.

### 6.2 Native path compatibility

Knowhere, Tantivy, Arrow, and other third-party libraries sometimes require
native paths. `ResolveNativePath()` is the explicit escape hatch. It accepts
only a validated relative `Path` and produces a path below the scoped root.

Milvus-owned I/O should prefer opened handles. Native paths should remain at
third-party boundaries.

## 7. Opened File I/O

`RandomAccessFile` and `WritableFile` are move-only RAII wrappers. Their
destructors close the native descriptor.

```cpp
class RandomAccessFile final {
 public:
    size_t ReadAt(uint64_t offset, std::span<std::byte> output) const;
    uint64_t Size() const;
};

class WritableFile final {
 public:
    size_t Write(std::span<const std::byte> data);
    size_t WriteAt(uint64_t offset, std::span<const std::byte> data);
    uint64_t Size() const;
    void Truncate(uint64_t size);
    void Sync();
};
```

A read-only handle does not expose writes. `ReadAt()` does not mutate shared
file position and can be used concurrently while the handle remains alive.
`WritableFile` does not serialize writes; callers own ordering and must not
issue overlapping writes.

Higher-level writer policy such as buffering, direct I/O alignment, priority,
rate limiting, and completion remains above `WritableFile`.

## 8. Mapped Regions

`FileSystem::OpenMappedRegion()` returns a move-only
`local::io::MappedRegion`. It stores both the page-aligned OS mapping and
the caller-visible byte range. Destruction calls `munmap`.

```cpp
auto region = files.OpenMappedRegion(
    Path("field.bin"),
    MapOptions{.offset = offset, .length = length, .populate = true});
auto bytes = region.Data();
```

Mapping is a filesystem operation rather than a method on
`RandomAccessFile`. This keeps positional reads and OS mapping as separate
capabilities. Higher-level chunk and segment layout remains outside
`milvus::local::io`.

Unlinking a mapped file is valid on supported POSIX systems: the mapping keeps
the inode alive until `munmap`. Components that require path-level cleanup
coordination use `ManagedSubtree`; they do not retain artificial reader
leases solely for mmap lifetime.

## 9. ManagedSubtree

Ordinary deletion stays immediate:

```cpp
files.RemoveAll(path);
```

Only directories with a real writer-versus-cleanup race use
`ManagedSubtree`.

```cpp
auto directory = files.ManageSubtree(Path("index_files/10"));

{
    auto lease = directory->AcquireWriter();
    auto output = directory->Files().OpenForWrite(
        Path("index"), WriteOptions{.create = true});
    // Materialize the artifact.
}

directory->RemoveWhenIdle();
```

The state machine is:

```text
OPEN
  |-- AcquireWriter -------------> OPEN, writers + 1
  |-- RemoveWhenIdle -----------> CLOSING

CLOSING
  |-- AcquireWriter -------------> rejected
  |-- final writer released -----> remove subtree -> REMOVED

REMOVED
  |-- AcquireWriter -------------> rejected
  |-- RemoveWhenIdle ------------> no-op
```

`RemoveWhenIdle()` is destructor-safe and reports cleanup failures through
the existing error/logging path. `RemoveAndWait()` waits and rethrows cleanup
failure to callers that can handle it.

## 10. Composition and Lifetimes

Runtime code opens roots from configuration and derives scoped handles:

```cpp
auto node_cache = local::FileSystem::Open(
    local_storage_path / "cache" / std::to_string(node_id));

NodeLocalFiles files{
    .local_chunk = node_cache.Subtree(Path("local_chunk")),
    .growing_mmap = node_cache.Subtree(Path("growing_mmap")),
    .bm25 = node_cache.Subtree(Path("bm25")),
    .file_resource = node_cache.Subtree(Path("file_resource")),
    .expr_cache = node_cache.Subtree(Path("expr_cache")),
};
```

`NodeLocalFiles` is a construction value, not a global registry. A leaf
object receives only the handle it needs.

Across CGo, Go owns an opaque C++ `FileSystem` value and passes it explicitly
to collections, segments, index tasks, or file-manager contexts. Leaf code
does not fetch a global fallback.

The intended lifetime order is:

```text
consumer stops using bytes
    -> MappedRegion unmaps
    -> file handles close
    -> operation/cache owner requests subtree cleanup
    -> directory is removed when active writers reach zero
```

Cache policy remains in `milvus::cachinglayer`. Scratch operations may use
the same filesystem mechanisms without becoming cache entries.

## 11. Error and Concurrency Semantics

Local I/O must preserve distinct failures including:

- path not found;
- permission denied;
- disk full or quota exhaustion;
- read, write, truncate, sync, or mmap failure;
- invalid internal relative path;
- writer acquisition after cleanup begins;
- cleanup failure.

For valid internal load/build requests these are system failures unless the
request content itself directly forces the error. Adding context must not
erase an existing error category at the CGo boundary.

Thread-safety contracts are explicit:

- copied `FileSystem` handles are safe for concurrent use;
- `RandomAccessFile::ReadAt()` supports concurrent positional reads;
- `WritableFile` does not serialize overlapping writes;
- `MappedRegion` and write leases are move-only;
- `ManagedSubtree` serializes lifecycle transitions.

## 12. Compatibility

The migration preserves existing physical paths:

```text
localStorage.path/cache/<node-id>/local_chunk
localStorage.path/cache/<node-id>/growing_mmap
localStorage.path/cache/<node-id>/bm25
localStorage.path/cache/<node-id>/file_resource
localStorage.path/cache/<node-id>/expr_cache
```

Index contents, mmap layouts, remote object names, and persistent
`storageType=local` behavior do not change. No required configuration is
added.

## 13. Migration

1. Introduce `Path`, `FileSystem`, opened-file handles,
   `MappedRegion`, and `ManagedSubtree` without changing production
   callers.
2. Move local writers, readers, and file managers onto rooted handles.
3. Pass scoped handles through QueryNode/segcore construction.
4. Pass scoped handles through DataNode/index construction.
5. Remove legacy adapters and process-global local filesystem access after
   every production consumer has an explicit handle.

Removing `LocalChunkManagerSingleton` is the final migration step, not the
identity of this design. `LocalChunkManager` remains for persistent
`storageType=local` object-storage compatibility.

## 14. Verification

### 14.1 Path and filesystem tests

- reject absolute paths and root escape;
- open multiple roots in one process;
- verify subtree composition and sibling isolation;
- create, list, rename, and remove files and directories;
- round-trip validated native paths;
- prove cwd changes do not affect rooted operations.

### 14.2 File and mmap tests

- positional reads and writes;
- create, truncate, sync, and file-size behavior;
- descriptor closure on normal and exceptional paths;
- aligned and unaligned mmap offsets;
- mappings remain valid after source descriptor closure;
- mmap failure categories.

### 14.3 ManagedSubtree tests

- multiple concurrent writers;
- cleanup with active writers;
- rejection of writers after closing begins;
- removal on final writer release;
- repeated removal requests;
- synchronous cleanup error propagation;
- independent managed subtrees under one root.

### 14.4 Integration and repository audit

- scalar/vector index build, upload, and load;
- mmap and non-mmap segment load;
- concurrent load, release, and cleanup;
- QueryNode and DataNode shutdown;
- persistent local storage beside node-local handles;
- no production dependency on a global/default local filesystem.

## 15. Rejected Alternatives

### Keep node-local I/O under `milvus::storage`

Rejected because object storage and node-local POSIX filesystem mechanisms
have different ownership and lifetime semantics.

### Replace the old singleton with a global `FileSystem`

Rejected because a global getter or registry preserves hidden dependencies,
test interference, and implicit shutdown order.

### Create one service per logical directory

Rejected because logical cache directories are subtrees below a small number
of physical roots. `Subtree()` expresses the boundary more directly.

### Put lifecycle tracking in every filesystem operation

Rejected because ordinary removal would gain surprising deferred behavior.
Only directories with an actual writer/cleanup race use `ManagedSubtree`.

### Add another cache namespace

Rejected because `milvus::cachinglayer` already owns cache policy. The local
filesystem layer provides mechanisms and composes with it.

### Require `RandomAccessFile::Map()`

Rejected because positional reads and path-rooted OS mapping are distinct
capabilities.

## 16. Review Invariants

1. `FileSystem` is a normal value handle with no global getter or registry.
2. Business code uses validated relative paths below injected roots.
3. `Subtree()` narrows authority without changing physical path layout.
4. Ordinary filesystem operations do not hide lease or cache policy.
5. `ManagedSubtree` is used only for real writer/cleanup coordination.
6. Open files and mappings use move-only RAII ownership.
7. Native paths are limited to compatibility boundaries.
8. Cache policy remains in `milvus::cachinglayer`.
9. Persistent `storageType=local` remains under `milvus::storage`.
10. Existing paths, formats, and error categories remain compatible.
