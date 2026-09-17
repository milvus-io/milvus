# Storage path contract

Read this before changing storage paths, manifests, local migration, or GC.
Update it with the code when a path or ownership rule changes.

## Complete keys

ChunkManager receives complete keys; it does not prepend its root.

| Backend | Key | Example |
| --- | --- | --- |
| Local | Absolute filesystem path including `localStorage.path` | `/var/lib/milvus/data/insert_log/1/2/3/...` |
| Remote | Bucket-relative object key, normally including `minio.rootPath` | `files/insert_log/1/2/3/...` |

**New local segment/index namespaces never use minio.rootPath.** Updates to an
existing legacy manifest retain its base path, including the historical prefix;
do not rebase that manifest while appending data or stats. A remote key does not include
the bucket name or a URI scheme. Local loon/Arrow filesystems use `/` as their
filesystem root so a complete key is not joined to the storage root twice.

Explore is a separate layout: local temporary manifests live under
`localStorage.path/__explore_temp__/`; remote manifests live under
`__explore_temp__/` at the bucket root, without `minio.rootPath`.

## Path ownership

- Writers use shared layout helpers: `storage.SegmentManifestBasePath`,
  `SegmentPartitionBasePath`, and the binlog/index helpers in `metautil`.
- Readers consume the complete key recorded in metadata or the manifest.
  They must not prepend another root.
- Some API fields describe relative file names rather than complete keys.
  TEXT stats results contain complete keys; JSON stats results contain names
  relative to their field's stats directory. Resolve those names once at the
  consuming boundary.
- Cleanup uses the same layout as the producer. Preserve remote keys exactly:
  `.`, `..`, and repeated separators are literal object-key components.

## Local upgrade compatibility

Let `R = localStorage.path` and `M = the legacy minio.rootPath`.

| Existing layout | Upgrade behavior |
| --- | --- |
| `R/<layout>/...` | Use directly |
| Manifest base `M/insert_log/...`, files at `R/M/insert_log/...` | Resolve the loaded manifest to the existing absolute path; read in place |
| `R/<R without leading slash>/<layout>/...` | Move displaced layouts into the standard root at standalone startup |
| Unified indexes under the old process CWD | Move recognized index directories if they remain accessible |

Migration uses rename, merges overlapping directories, and falls back to copy
on cross-filesystem moves. It does not overwrite conflicting files. Restart
resumes from remaining sources. Shutdown cancellation is checked while walking
and between file-copy reads; partial temporary copies are never published. There
is no configured migration timeout, reverse migration, or backup.
Old relative `localStorage.path` configurations are not supported.

`normalizeLocalManifestPath` applies only to locally owned segments, not
remote data or foreign/self-contained snapshots. It updates the loaded view,
without writing the catalog; a later ordinary update may persist the path.
Keep the old `minio.rootPath` unchanged while relative legacy manifests remain.
Compatibility rules must not become new-write rules.

TEXT LOB references contain a file ID and row offset, not their source storage
prefix. Compaction may reuse those encoded references only when every source
manifest and the output manifest have the same partition base. When a legacy
local source under `R/M/insert_log/<c>/<p>` is compacted into a canonical output
under `R/insert_log/<c>/<p>`, the source reader must decode TEXT through the
legacy LOB directory and the output writer must create new LOB files under the
canonical directory. Copying references or merely registering the old LOB file
metadata in the output manifest is not sufficient: readers reconstruct the LOB
directory from the output manifest base.

## Necessary checks, at clear boundaries

- Configuration initialization requires an absolute local root and freezes it
  against runtime configuration updates.
- Metadata loading resolves recognized legacy paths and checks their identity.
- Deletion validates its segment/field ownership and applicable root boundary.
  Recursive segment prefixes end in `/`, so `2001` cannot match `20010`.
- Avoid repeated URI or path-spelling blacklists. Check the actual range or
  ownership constraint, and share a helper when the semantics are identical.
- Migration recognizes old CWD indexes by layout and
  `milvus_packed_<type>_index.v3` names, rather than moving arbitrary CWD data.
  Packed files move last during a merge so interrupted leaves stay recognizable.
  Symlinks inside migration paths are refused; automatic alias repair is not
  supported. Root confinement alone does not protect an in-root live alias.

## GC coverage and limits

- V1/V2: delete recorded files and periodically scan standard layouts for
  orphans, including independent `bm25_stats` files.
- Dropped V3 segments: remove the manifest base directory, including data and
  stats, subject to retention and snapshot protection.
- V3 orphan scan: enumerate `insert_log` and reclaim files without segment
  metadata after the protection period. Files belonging to registered V3
  segments are skipped, even if an individual file is absent from the manifest.
- Therefore an uncommitted stats file inside a live V3 segment is not guaranteed
  to be reclaimed by DataCoord's orphan scan.
- Legacy `R/M/insert_log` orphan/LOB layouts and old Explore temporary paths
  have separate cleanup gaps. Do not claim this PR repairs all such leftovers.
  Startup cannot recover indexes hidden with an old container's CWD.

## Implementation and regression tests

- Roots and writers: `internal/storagev2/loon_root.go`,
  `internal/core/src/storage/loon_ffi/util.cpp`,
  `internal/storage/binlog_record_writer.go`.
- Upgrade: `cmd/roles/local_layout.go`, `internal/storage/localmigrate/*_test.go`.
- Compatibility: `internal/datacoord/meta_local_manifest*.go` and
  `snapshot_local_manifest_test.go`.
- Cleanup: `internal/datacoord/garbage_collector*.go` and `task_stats*.go`.

Source review and unit tests do not replace a real version-to-version upgrade
test. Report which versions, layouts, and failure cases were actually exercised.
