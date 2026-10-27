# Storage path contract

Read this before changing storage paths, manifests, local migration, or GC.
Update it with the code when a path or ownership rule changes.

## Complete keys

ChunkManager receives complete keys; it does not prepend its root.

| Backend | Key | Example |
| --- | --- | --- |
| Local | Absolute filesystem path including `localStorage.path` | `/var/lib/milvus/data/insert_log/1/2/3/...` |
| Remote | Bucket-relative object key, normally including `minio.rootPath` | `files/insert_log/1/2/3/...` |

**New local segment namespaces never use minio.rootPath.** Updates to an
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

## Manifest index artifacts

When `dataCoord.index.writeSegmentIndexToManifest` is enabled, new StorageV3
vector/scalar index builds select `IndexStorePathVersion=2` and write to
`<manifest base>/_index/<build>/<version>/<file>`. This includes new indexes on
legacy local segments: they follow that existing manifest base. Disabling the
switch preserves the existing version-0/1 layout selection for new tasks.
New or updated index entries must stay inside that `_index` directory; parent
traversal and external namespaces are errors. Existing entries are not migrated.
Builds retain their creation-time publication mode across switch changes and
restarts: version 2 finishes in the manifest, while version 0/1 finishes in etcd.
Readers and cleanup use each artifact's persisted version.
Resolving v2 index files requires the owning segment's manifest base; load and
snapshot consumers report missing or invalid metadata instead of guessing the
canonical base. Snapshot generation uses its selected segment metadata.
See [design and compatibility](../../design-docs/design_docs/20261019-segment-rooted-index-artifacts.md).

## Local upgrade compatibility

The official Docker Compose and embedded standalone launchers run Milvus as the
image's `milvus` user. Before starting Milvus, their one-shot volume initializer
changes an uninitialized bind mount to that image user's numeric UID/GID. A
versioned `.milvus-volume-owner-<uid>-<gid>` marker makes the recursive ownership
migration resumable and prevents repeating it on every start. The marker is
written only after the full ownership change succeeds; changing the image UID or
GID causes a new migration. Restoring files with another owner after the marker
was created requires removing the marker before the next start.

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
- A dedicated build-aware scan handles canonical `insert_log/.../_index`
  orphans, including those in live V3 segments; snapshot and import protections
  still apply.
- Malformed `_index` paths are skipped by the orphan scan because their build
  ownership cannot be established. Dropped-segment directory cleanup still
  covers them; malformed objects without a segment row can remain orphaned.
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


## Snapshot export and Restore paths

The [distributed execution design](../../design-docs/design_docs/20260923-snapshot-export-datanode.md)
owns scheduling, inventory, concurrency, statistics, publication and recovery.
The [external snapshot design](../../design-docs/design_docs/20260609-external-snapshot-export-restore.md)
owns bundle layouts, credentials and relocation.

- All Export ranges share their UUID-namespaced `<job-root>`.
  Lost ranges retry with fresh task IDs in the same root. Pinned immutable
  inputs and equivalent decoded Avro records make late/repeated writes safe
  under complete-object writes. V6 OCF sync markers may differ across retries;
  worker cleanup never deletes these target objects.
- Source keys are complete keys. Preserve native relative data/LOB layout when
  copying pinned native manifests unchanged. Reject mappings that change it.
  V6 Avro OCF records explicit target paths; top-level metadata relocates native
  entry points. Relocating self-contained bundles rebases their root; referenced
  snapshots retain original paths.
- Ordinary V3 physical inventory comes from the pinned manifest plus nonempty
  PB delta/stats/BM25 supplements, never a segment-directory walk. Avro IndexFiles
  alone selects vector/scalar index artifacts, including an empty inventory.
  Missing references/read failures fail closed; they do not enable LIST fallback.
- Validate raw external URI ownership before key deduplication. Destination
  keys remain literal. Per-segment source/destination conflicts fail before that
  segment writes; there is no task/job-wide preparation barrier.
- Shared immutable LOBs may be copied repeatedly. Restore task cleanup excludes
  the actual target partition's `lobs/` namespace; coordinator cleanup prefixes
  stay segment/index-owned. Reference-aware LOB GC owns orphan reclamation under
  its safety window. Disabling it can retain orphan LOBs.
- Export worker removal never deletes copied objects. Failed/abandoned attempt
  prefixes are not reclaimed by generic orphan GC.
