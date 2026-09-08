# MEP: Rooted Local File System

- **Created:** 2026-07-12
- **Updated:** 2026-09-08
- **Author(s):** @sparknack
- **Status:** Draft
- **Component:** Local filesystem / Segcore / Index / Caching layer
- **Related Issues:** #51507
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

milvus::local::FileHandle
    move-only ownership of one opened native file

milvus::local::io::MappedRegion
    RAII ownership of one mapped region

milvus::cachinglayer
    cache loading, pinning, accounting, and eviction policy
```

`FileSystem` is a small, copyable value handle. `FileSystem::Open()` binds
a canonical absolute root path once; it does not pin a directory inode.
All operations below that root use validated
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
├── FileHandle.h
├── FileHandle.cpp
├── FileSystem.h
├── FileSystem.cpp
├── ManagedSubtree.h
├── ManagedSubtree.cpp
└── io/
    ├── FileWriter.h
    ├── FileWriter.cpp
    ├── MappedRegion.h
    └── MappedRegion.cpp
```

```cpp
namespace milvus::local {
class Path;
class FileHandle;
class FileSystem;
class ManagedSubtree;
}

namespace milvus::local::io {
class FileWriter;
class PositionedFileWriter;
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

    FileHandle Open(const Path& path, const OpenOptions& options) const;
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

auto file = local_chunk.Open(
    Path("index_files/10/index"),
    OpenOptions{.mode = OpenMode::ReadOnly});
```

The resolved native path is:

```text
/var/lib/milvus/data/cache/1001
    + local_chunk
    + index_files/10/index
```

The holder of `local_chunk` cannot construct a relative path that traverses
to `expr_cache`. Owners must also prevent symlink aliases between independently
owned artifact directories; a lexical subtree prefix does not bind a directory
inode or establish an OS security boundary.

### 6.2 Isolation contract

This API operates on trusted node-local artifact directories. It prevents
accidental absolute paths and parent traversal, and checks current symlink
targets before native I/O. It is not a filesystem sandbox: canonicalization
and the subsequent native operation are separate, so owners must prevent
concurrent replacement of path components or symlink targets during an
operation. The same requirement applies to roots and subtree ancestors.

The configured root may resolve through a symlink at `Open()`. Below it,
Milvus-owned layouts should use ordinary directories and avoid directory
aliases. Existing file symlinks are supported when their targets stay within
the handle's resolved scope. Native-library adapters must obey the same
ownership and path-stability contract. `ResolveNativePath()` validates the
current path only; it cannot protect future operations using the returned
string. Protection against untrusted concurrent path replacement would require
a separate directory-fd-based design, including deletion and rename semantics.

### 6.3 Native path compatibility

Knowhere, Tantivy, Arrow, and other third-party libraries sometimes require
native paths. `ResolveNativePath()` is the explicit escape hatch. It accepts
only a validated relative `Path` and produces a path below the scoped root.

Milvus-owned I/O should prefer opened handles. Native paths should remain at
third-party boundaries.

### 6.4 Directory enumeration

`List(directory, recursive)` returns sorted regular-file entry paths relative
to the handle's scope. File symlinks retain their entry names, not the names of
their targets, and their targets must stay inside the scope. Directory symlinks
are neither returned nor recursively traversed. Deleting a listed file symlink
therefore deletes the link, not its target.

Relative entry paths are computed lexically using a scope calculated once per
listing. Ordinary entries do not each trigger canonicalization of their path
and root; file symlinks receive a separate boundary check. Failures while
constructing or advancing either iterator are converted to `FileReadFailed`.
Boundary-check failures retain `FileOpenFailed`. Listings are not snapshots;
callers needing consistency must coordinate concurrent mutations.

## 7. Opened File Handles

`FileHandle` is a small move-only RAII owner for one native descriptor. Its
destructor closes the descriptor. It deliberately does not define read, write,
truncate, sync, buffering, rate limiting, or completion policy.

```cpp
struct FileHandle final {
 public:
    int Get() const noexcept;
    int Release() noexcept;
    const std::filesystem::path& DebugPath() const noexcept;
    bool DirectIOEnabled() const noexcept;
};
```

`FileSystem::Open()` accepts typed options rather than exposing POSIX flags:

```cpp
enum class OpenMode { ReadOnly, ReadWrite };

struct OpenOptions {
    OpenMode mode{OpenMode::ReadOnly};
    bool create{false};
    bool truncate{false};
    bool create_parent{false};
    bool direct_io{false};
};
```

The handle records ownership and open metadata. The receiving reader, writer,
or native-library adapter performs I/O appropriate to its own contract. Small
internal helpers may centralize partial-transfer and `EINTR` handling, but they
are not methods on `FileHandle`.

Higher-level writer policy such as buffering, direct I/O alignment, priority,
rate limiting, and completion remains in `milvus::local::io::FileWriter`.

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

Mapping is a filesystem operation rather than a method on `FileHandle`. This
keeps descriptor ownership and OS mapping lifetime as separate mechanisms.
Higher-level chunk and segment layout remains outside `milvus::local::io`.

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

`ManageSubtree(path)` creates a new lifecycle owner; it does not find or
intern an owner by path. The composition/cache owner must call it once for a
directory and distribute the same `shared_ptr<ManagedSubtree>` to every writer
and cleanup caller. Independently managed directories must not be identical,
aliases, or ancestor/descendant pairs. A parent owner must drain its child
owners before removing their containing directory.

`Files()` exposes ordinary filesystem operations and does not automatically
acquire leases. All writers, including native-library operations, must retain
a lease until writing finishes. Owners must prevent direct ancestor deletion
and path reuse until managed cleanup succeeds, including any retries. The
`ManagedSubtree` destructor itself does not request removal; cleanup remains
an explicit owner responsibility.

```cpp
auto directory = files.ManageSubtree(Path("index_files/10"));

{
    auto lease = directory->AcquireWriter();
    auto output = directory->Files().Open(
        Path("index"),
        OpenOptions{.mode = OpenMode::ReadWrite,
                    .create = true,
                    .create_parent = true});
    // Materialize the artifact.
}

directory->RemoveWhenIdle();
```

The state machine is:

```text
OPEN
  |-- AcquireWriter -------------> OPEN, writers + 1
  |-- removal requested ---------> CLOSING

CLOSING
  |-- AcquireWriter -------------> rejected
  |-- no active writers ---------> REMOVING

REMOVING
  |-- removal succeeds ----------> REMOVED
  |-- removal throws ------------> FAILED, original exception retained

FAILED
  |-- AcquireWriter -------------> rejected
  |-- RemoveWhenIdle ------------> no-op
  |-- RemoveAndWait -------------> REMOVING, new attempt

REMOVED
  |-- AcquireWriter -------------> rejected
  |-- removal requested ---------> no-op
```

`RemoveWhenIdle()` is `noexcept` and initiates only the first attempt. It can
perform synchronous deletion on the caller or final writer's thread; it is
not an asynchronous executor and does not automatically retry failures. An
owner requiring recovery must retain the object and call `RemoveAndWait()`.

`RemoveAndWait()` starts the first attempt, joins an outstanding attempt, or
retries a previously failed attempt once. It waits for that attempt and
rethrows the original exception, including its code and path/cause details.
Each attempt has its own shared completion result: a later retry cannot
overwrite the result observed by earlier waiters. Removal runs outside the
lifecycle mutex; completion is published under that mutex before notifying
waiters. Failure leaves the object closed to new writers and may leave a
partially deleted directory, which the next explicit retry removes.

The first attempt is allocated when the owner is constructed. Failure to
allocate a retry leaves the previous failed attempt intact. Successful cleanup
is terminal and subsequent requests do not delete a newly created directory at
the same path. Callers must never wait for removal while holding one of the
owner's writer leases, since removal cannot complete until those leases exit.

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
- `FileHandle` is move-only and does not serialize operations on its fd;
- receiving readers and writers define their own concurrency contracts;
- `MappedRegion` and write leases are move-only;
- `ManagedSubtree` serializes lifecycle transitions for one shared owner;
  path identity and overlapping directories are coordinated by its owner;
- copied handles do not serialize filesystem mutations or protect against
  concurrent symlink/path replacement.

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

1. Introduce `Path`, `FileSystem`, `FileHandle`,
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
- preserve listed file-symlink entry names and reject out-of-scope targets;
- preserve typed errors when recursive iteration encounters permission denial;
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
- original cleanup exception context and explicit retry after failure;
- multiple waiters on a shared owner observe their own attempt's result;
- destructor-safe cleanup does not automatically retry failures;
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

### Require `FileHandle::Map()`

Rejected because positional reads and path-rooted OS mapping are distinct
capabilities.

## 16. Review Invariants

1. `FileSystem` is a normal value handle with no global getter or registry.
2. Business code uses validated relative paths below injected roots.
3. `Subtree()` narrows lexical scope under the trusted-directory and
   path-stability contract; it does not provide OS sandbox isolation.
4. Ordinary filesystem operations do not hide lease or cache policy.
5. `ManagedSubtree` is created once per owned directory and shared by all
   writers/cleanup callers; independently managed scopes never overlap.
6. Open files and mappings use move-only RAII ownership.
7. Native paths are limited to compatibility boundaries.
8. Cache policy remains in `milvus::cachinglayer`.
9. Persistent `storageType=local` remains under `milvus::storage`.
10. Existing paths, formats, and error categories remain compatible.

## 17. References

- [Rooted local filesystem proposal](https://github.com/milvus-io/milvus/issues/51507)
- [Initial implementation](https://github.com/milvus-io/milvus/pull/51509)
- [Local storage I/O migration](https://github.com/milvus-io/milvus/pull/51518)
- [Query segment dependency injection](https://github.com/milvus-io/milvus/pull/51519)
- [Local chunk manager singleton removal](https://github.com/milvus-io/milvus/pull/51520)
