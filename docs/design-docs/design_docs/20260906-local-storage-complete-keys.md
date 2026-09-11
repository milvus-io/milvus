# MEP: Complete Storage Keys for Local Storage

- **Feature DRI:** @xiaofanluan
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** TBD
- **Created:** 2026-09-06
- **Author(s):** @xiaofanluan
- **Status:** Under Review
- **Component:** DataCoord | DataNode | QueryNode | Storage | Segcore
- **Related Issue:** [milvus-io/milvus#53051](https://github.com/milvus-io/milvus/issues/53051), [milvus-io/milvus#53052](https://github.com/milvus-io/milvus/issues/53052)
- **Target Release:** TBD

## Summary

On `common.storageType=local`, three layers disagreed about what a storage path
means. The result is that in Milvus 3.0.0 and 3.0.1 everything written through
the Arrow / loon filesystem landed under `<root>/<root>/...` while the paths
recorded in etcd said something else, so Go-side readers, GC, clone and snapshot
export all addressed files that were not there.

This document defines one rule for every layer — a storage key is always
complete — and describes the migration for instances already affected.

## Motivation

### The three layers and where they disagreed

| Layer | remote | local (3.0.0 / 3.0.1) |
| --- | --- | --- |
| ChunkManager key (Go and C++) | `files/insert_log/...`, includes `minio.rootPath` | `/var/lib/milvus/data/insert_log/...`, includes `localStorage.path` |
| loon / Arrow filesystem root | the bucket; `root_path` is never applied | `localStorage.path` (`SubTreeFileSystem`, milvus-storage #351) |
| StorageV3 manifest base | `files/insert_log/<c>/<p>/<s>` | flush: `files/insert_log/<c>/<p>/<s>` (#53052); compaction: `/var/lib/milvus/data/insert_log/<c>/<p>/<s>` |

Remote has exactly one namespace, so nothing ever converts anything and the
disagreement is invisible. Local had a filesystem rooted at `localStorage.path`
receiving keys that already start with `localStorage.path`.

### What broke

Everything written through the Arrow / loon filesystem was displaced to
`<root>/<root>/...` (#53051): StorageV2 packed insert logs, every StorageV3
segment, unified text indexes, and JSON stats shredding data. C++ reads went
back through the same filesystem and stayed self-consistent, so a StorageV2
collection still answered queries — which is why this survived to a release.
The Go side uses the recorded key directly, so:

- GC never deleted those files; dropping a collection leaked them permanently.
- Clone and snapshot export failed to find the files they were told to copy.
- StorageV3 segment load failed on the first bloom-filter read.

2.6 is unaffected: its pinned milvus-storage predates #351, so `root_path` was
inert there and the keys happened to be correct. The one 2.6 residue is a late
build that wrote unified indexes into the process working directory.

### Why not simply re-root the local filesystem

Rooting the local filesystem at `localStorage.path` and making local keys
*relative* is the other self-consistent choice. It was rejected because it makes
local and remote structurally different: every Go call site that touches a key
would need to know which backend it is on, and every boundary (manifest,
ChunkManager, GC, copy-segment, snapshot) would need a conversion. The bug class
this PR fixes is exactly "a boundary forgot to convert".

## Design

### The rule

> A storage key is `<storage prefix>/<layout>`. The prefix is `minio.rootPath`
> for remote and `localStorage.path` for local. The StorageV3 manifest stores
> exactly that key. The loon / Arrow filesystem is rooted at the **namespace
> root**: the bucket for remote, `/` for local.

Three consequences:

1. A key never needs conversion between the manifest, the ChunkManager and the
   disk — they are one string.
2. `localStorage.path` must be an absolute filesystem path. A relative value
   would silently depend on the process working directory, so it is rejected
   during configuration initialization.
3. Arrow's `SubTreeFileSystem` strips the stem's leading slash before joining,
   so with a root of `/` an absolute key passes through unchanged.

`StorageConfig.root_path` keeps its meaning (the key prefix, and the C++
`LocalChunkManager` root). Only the *filesystem* root changes.

### Owners of the invariant

The rule is expressed in exactly two places per language, and every other site
derives from them:

| Invariant | Go | C++ |
| --- | --- | --- |
| loon filesystem root | `storagev2.LoonFSRootPath` | `LoonFSRootPath` in `storage/loon_ffi/util.h` |
| manifest base formula | `storage.SegmentManifestBasePath` / `SegmentPartitionBasePath` | — (derived from the manifest) |

Go sites that turn a `StorageConfig` into loon properties:
`packed.MakePropertiesFromStorageConfig` and the filesystem-metrics lookup.
C++ sites: `StorageV2FSCache::Get`,
`MakeInternalPropertiesFromStorageConfig`, `MakeInternalLocalProperies`.

Every primary-storage StorageV3 writer — ordinary flush, growing-source flush,
compaction, import, DataCoord's initial manifest, the vchannel write path —
derives its base through `SegmentManifestBasePath`. Building it from
`minio.rootPath` under local storage was #53052.

### No boundary conversions

The stats resolver, GC, rejected-stats cleanup, copy-segment, snapshot export
and the C++ `FileManager` pass manifest paths through unchanged. The C++
unit-test hook `index::kOverrideRootPathForUT`, which existed only to paper over
the double join in tests, is removed.

Two read paths that previously hard-coded `minio.rootPath` now use the
storage-type-aware `binlog.GetRootPath()`; this incidentally recovers 3.0.x
local text indexes and JSON key stats that were unreadable before.

## Migration

Two displacement classes exist and are handled by two different mechanisms,
because only one of them can be moved safely.

### Class 1 — displaced root, moved on startup

Keys that already carried `localStorage.path` were written to
`<root>/<root without leading slash>/...`. `internal/storage/localmigrate` moves
`{insert_log, text_log, json_stats, index_files, index_v1}` from there up into
`<root>/...`, and also moves recognized late-2.6 unified index directories out
of the process working directory.

Properties:

- Per-entry rename within one filesystem; copy-then-remove across filesystems.
- Directories are merged, never overwritten. A same-name different file is a
  conflict: startup aborts and reports the list.
- Crash-safe and resumable without a journal: a cross-filesystem copy publishes
  through a hard-linked marker that a retry recognizes as its own, and target
  ancestors are fsynced before the source is removed.
- Guarded by a lock file under the storage root.
- Runs before etcd and every component, for standalone with
  `common.storageType=local`.
- Bounded by `localStorage.layoutMigrationTimeout` (default 1h, `<=0`
  disables). Exceeding it aborts startup rather than serving a half-migrated
  root; the work resumes on the next start.
- Reports the whole plan before moving anything, then each directory as it
  starts and finishes.

### Class 2 — legacy minio prefix, read in place

Keys built from `minio.rootPath` (`files/insert_log/...`) were written to
`<root>/files/insert_log/...`. These are **not** moved: `DataCoord` resolves such
a manifest base to its existing absolute location at metadata load, preserving
the legacy prefix. The migration explicitly refuses to move anything overlapping
that namespace.

The resolution is read-only — it rewrites the loaded view, not the catalog — so
a later ordinary segment update persists the absolute path and every reload
re-derives it until then.

### Scope and limits

- remote: byte-identical keys, filesystem root and manifest bases.
- local 2.6 with an absolute `localStorage.path`: layout unchanged.
- local 3.0.0 / 3.0.1 with an absolute `localStorage.path`: migrated as above.
- Migration of data written with a **relative** `localStorage.path` is not
  supported.
- Migration runs for standalone only. There is no offline migration command.
- **The upgrade is one-way.** There is no reverse migration, journal or backup;
  files are physically renamed or copied and unlinked. Downgrading to 3.0.0 /
  3.0.1 after migration is not supported.
- Because a Class 2 manifest base is validated against the current
  `minio.rootPath`, that value must not change on a local instance that was
  upgraded from 3.0.x. Changing it is already documented as causing failures to
  read legacy data.
- Unreferenced LOB files that remain in the legacy namespace are not reclaimed
  by LOB GC.

## Compatibility and safety notes

`localStorage.path` is now required to be an absolute path on every deployment,
including remote ones where it is only a cache directory.

Segment GC gained checks that are independent of this layout change and fix
pre-existing hazards: a deletion prefix is segment-boundary aware (so segment
`2001` no longer matches `20010`), the prefix must be inside the configured
storage root, and it must carry the canonical `insert_log/<c>/<p>/<s>` suffix
matching the segment being dropped. `RemoveWithPrefix` refuses an empty prefix
on both backends, and local prefix walking treats a missing directory as an
empty listing so removal is idempotent.

## Test Plan

- Unit: the loon root rule in Go and C++; the manifest base formula at every
  writer; the migration (merge, cross-filesystem copy, interrupted copy, same
  name conflicts, symlink refusal, lock contention, empty-directory cleanup,
  deadline and cancellation, plan and per-directory reporting); metadata-load
  resolution including identity mismatch and foreign namespaces; GC prefix and
  identity validation; rejected-stats cleanup for both the manifest and the
  legacy layouts.
- C++: writing an absolute key through `StorageV2FSCache` lands at exactly that
  path and not at the double-joined one.
- End-to-end upgrade: a 3.0.1 local instance with StorageV2 and StorageV3
  collections, upgraded in place; old collections remain readable, migration
  moves the displaced files, GC reclaims them after a drop, and new writes land
  under `<root>/insert_log/...`.
