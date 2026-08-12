# StorageV3 Manifest Index Publication

- **Created:** 2026-08-11
- **Status:** Implemented
- **Component:** DataNode, DataCoord, StorageV3
- **Related upstream change:** [milvus-storage#620](https://github.com/milvus-io/milvus-storage/pull/620/)

## Summary

Completed StorageV3 index builds publish their artifacts through a
milvus-storage transaction.  The committed manifest revision is returned with
the index-task result and DataCoord persists that revision on the segment
before it marks the index task complete.

## Motivation

An index built from a StorageV3 segment is valid only for the manifest revision
that supplied the source data.  Storing index-file keys only in index-task
metadata makes the artifact unavailable to manifest-aware consumers and risks
associating a completed build with newer segment data.

## Design

```
DataNode upload index files
        |
        v
publish IndexInfo against source manifest revision
        |
        v
commit new manifest revision
        |
        v
return manifest_path in IndexTaskInfo
        |
        v
DataCoord atomically persists the exact segment manifest path
        |
        v
QueryNode handoff continues to use SegmentIndex metadata
```

`ManifestIndexInfo` uses the typed `LoonIndexInfo` fields introduced by
milvus-storage#620: index name/type, field/index/build IDs, artifact and engine
versions, row count, serialized and memory sizes, path-layout version, and the
relative index-file keys. `properties` is reserved for index-specific
parameters such as metric type and Knowhere options. This preserves Milvus's
multi-file index layout without encoding required load metadata as strings.

Publication uses `LOON_TRANSACTION_RESOLVE_FAIL` with one attempt.  If the
source manifest revision has advanced, publication fails rather than merging an
index built from stale data into a newer revision.  The DataNode task becomes
retryable, so the next build reads the current manifest.

The transaction runs only after index bytes are uploaded.  A publication
failure can therefore leave unreferenced uploaded files, but cannot expose an
index artifact through an incorrect manifest revision; normal index-file GC
can reclaim such files.

## Compatibility and Scope

The new `manifest_path` field on `workerpb.IndexTaskInfo` is additive. It is
used only to atomically advance the segment to the exact revision returned by
the index-publication transaction. No local `minor_version` is introduced:
milvus-storage removed that field from its manifest model.

DataCoord normally passes the completed `SegmentIndex` metadata through
QueryCoord to QueryNode without reading object storage. If a finished
StorageV3 `SegmentIndex` no longer has `index_file_keys`, DataCoord lazily reads
the segment manifest once and uses the matching typed `LoonIndexInfo` entry as
a fallback. The exact manifest base path is validated before task completion,
preventing a concurrent compaction from combining an index revision with a
different segment manifest base.

## Verification

- StorageV3 manifest round-trip covers every typed index load field,
  properties, version increment, and stale-manifest rejection.
- DataNode task-result projection preserves `manifest_path`.
- DataCoord uses SegmentIndex metadata without manifest I/O and falls back to
  a matching typed manifest entry only when index_file_keys are absent.
- The milvus-storage C FFI library is rebuilt at the merged upstream PR #620
  commit `dac5781e5cd298e07d2b5822ba8a2b879e99cd45`.
