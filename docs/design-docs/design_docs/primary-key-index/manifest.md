# Primary Key Index Manifest Service

- **Created:** 2026-10-12
- **Author:** bigsheeper
- **Status:** Draft
- **Component:** DataCoord, `internal/pkindex/coordinator`, `internal/pkindex/core/manifest`
- **Related work:** Primary Key Index Core Library (#53910), issue #52305

## Summary

The primary key index of a collection keeps one LSM tree for each vchannel. The
streaming node writes the tree as SST files in object storage. A reader must
know which SST files make up the tree and up to which WAL position the files
are complete. The manifest records this state.

This document describes the manifest service. The service runs in DataCoord.
It is the only durable writer of the manifests. It does these tasks:

- It applies edits (claim, flush, compaction) to the manifest of one vchannel.
- It stores each manifest version as an object and points to it from the meta
  kv (etcd or TiKV).
- It fences an old owner of a vchannel by its pchannel term.
- It deletes the manifest bodies and SST files that no manifest needs (GC).

## Scope

In scope:

- The manifest proto, the object key layout, and the edit rules.
- The commit steps in DataCoord, with the crash windows.
- The gRPC service `PKIndexCoordService` and its client.
- The GC of manifest bodies and SST files.
- A new meta kv predicate, `KeyNotExist`.

Out of scope (future work):

- The streaming node side that uploads SSTs and commits flushes (snapshot).
- The streaming node recovery that claims a vchannel and replays the WAL.
- Compaction of SSTs. The edit rule and the GC input exist, but no task uses
  them.
- Cleanup of all primary key index objects after a collection drop.
- Snapshot protection of SST files.

## Data Model

The proto is in `pkg/proto/pkindex.proto`.

```protobuf
message Manifest {
  int64 collection_id = 1;
  string vchannel = 2;
  int64 version = 3;               // increases by 1 on every committed edit
  int64 pchannel_term = 4;         // highest pchannel term seen
  schema.DataType pk_data_type = 5;
  uint64 checkpoint_time_tick = 6; // SSTs cover all WAL messages at or below it
  repeated SSTRef ssts = 7;        // newest first
  string sst_base_path = 8;        // object key prefix of the SST files
}

message SSTRef {
  int64 id = 1;            // Milvus global ID of the SST
  uint32 table_format = 2; // Pebble sstable table format
  int64 size = 3;
  bytes min_key = 4;
  bytes max_key = 5;
  int64 num_entries = 6;
}

message ManifestPointer {
  int64 version = 1;
  string body_key = 2; // complete object key of the manifest body
}
```

Rules of the model:

- **Order.** The SST list is newest first. For one key, the first SST that holds
  the key has the current value. A tombstone is a value.
- **Version, not generation.** The engine already uses "generation" for a local
  write container that the streaming node allocates. One manifest version can
  hold the SSTs of several generations. A claim or a compaction changes the
  version and no generation. The name follows the `manifest_version` of the
  segment manifest.
- **Checkpoint is only a time tick.** Recovery replays the WAL from the global
  checkpoint of the pchannel. The primary key index only needs to skip the
  messages that its SSTs already cover. A time tick is enough for that. A WAL
  message ID has no reader here, and its format depends on the WAL backend.
- **SST path.** The manifest records the SST prefix once in `sst_base_path`. The
  object key of an SST is `<sst_base_path>/<id>.sst`. A reader uses the recorded
  prefix and does not build it from the root path. This keeps each `SSTRef`
  small, and DataCoord keeps the current version of every vchannel in memory.
- **No file checksum.** The block checksums and the footer of the SST protect
  its content.
- **`pk_data_type`.** The first version sets it. Each later claim must send the
  same type.

## Storage Layout

| Store | Key | Value |
|---|---|---|
| Meta kv | `<metaRoot>/pkindex-meta/manifest/<collectionID>/<vchannel>` | `ManifestPointer` |
| Object storage | `<rootPath>/pk_index/<collectionID>/<vchannel>/manifest/<version>-<uuid>.pb` | `Manifest` body |
| Object storage | `<rootPath>/pk_index/<collectionID>/<vchannel>/sst/<id>.sst` | SST file |

- `pk_index` is `common.PKIndexPath`. No other storage prefix starts with it, so
  the existing orphan scans do not see these objects.
- `sst_base_path` is `<rootPath>/pk_index/<collectionID>/<vchannel>/sst`. The
  first version sets it from the DataCoord layout.
- The writer never overwrites a body. Each body key has a new UUID. Two writers
  that commit the same version at the same time write two different objects.
  The conditional pointer write selects one of them. The body of the other
  writer stays unreferenced, and GC deletes it.
- An SST ID is a Milvus global ID and is used only once. A retry of an upload
  writes the same key with the same content.

## Edits and Validation

A caller sends an edit, not a complete manifest. The manager applies the edit
to the current version. The edit functions are pure functions in
`internal/pkindex/core/manifest/edit.go`. They do not change the current
version.

### Claim

Recovery claims a vchannel when it opens the vchannel. The claim carries the
pchannel term and the primary key data type.

| Condition | Result |
|---|---|
| No manifest | Write version 1: empty SST list, checkpoint 0, claim term |
| Claim term > manifest term | Write a new version with only the term changed |
| Claim term = manifest term | No write. Return the current version |
| Claim term < manifest term | `ErrFenced` |

A claim with a different primary key data type returns `ErrSchemaMismatch`. The
term check comes first.

The claim happens at open, not at the first flush. This closes a window: after
a new owner takes the vchannel and before its first flush, a late flush from
the old owner would otherwise succeed.

### Flush

The snapshot side commits a flush. A flush carries the term, the new SSTs
(newest first), and the new checkpoint.

| Condition | Result |
|---|---|
| No manifest | `ErrNotClaimed` |
| Flush term < manifest term | `ErrFenced` |
| Checkpoint does not move forward, same term | No write. Return the current version |
| Checkpoint does not move forward, higher term | `ErrCheckpointRegressed` |
| An SST ID is in the manifest, or is twice in the flush | `ErrDuplicateSST` |
| Otherwise | New version: SSTs go to the front, checkpoint moves forward, term goes up to the flush term |

Notes:

- A higher term means a newer owner. The manager accepts it and raises the
  recorded term. It does not fence the real owner.
- A same-term flush with an old checkpoint can only be a retry of an applied
  flush. One term has one owner, and its checkpoint only moves forward. The
  manager does not look at the SSTs of such a flush. A compaction can replace
  them before the retry arrives.
- A flush with no SSTs only moves the checkpoint.
- The duplicate check runs only when the checkpoint moves forward.

### Compaction

A compaction replaces a run of SSTs (inputs) with merged SSTs (outputs).

| Condition | Result |
|---|---|
| No manifest | `ErrNotClaimed` |
| Inputs are not empty, no input is in the list, and all outputs are in the list | No write. Return the current version |
| Inputs are empty, or are not a contiguous run of the list in the same order | `ErrStaleEdit` |
| An output ID is in the manifest, or is twice in the outputs | `ErrDuplicateSST` |
| Otherwise | New version: outputs take the place of the inputs. Checkpoint and term do not change |

The second row makes a compaction commit idempotent. If the pointer write of
a compaction took effect but the caller got an error, the caller sends the same
edit again. The outputs have new global IDs, and no other edit can add them to
the list. Thus "no input in the list and all outputs in the list" occurs only
when this compaction took effect. Every other mismatch, for example some inputs
in the list or only some outputs in the list, gets `ErrStaleEdit`.

Compaction is in-process only. `Manager.CommitCompact` exists, but the gRPC
service has no compaction method yet.

### Flush and compaction commute

A flush only adds SSTs at the front of the list. A compaction only replaces a
run that is already in the list. The two edits give the same result in either
order. Thus a flush and a compaction do not make each other retry.

This is true only when compaction follows this contract:

1. The inputs are a contiguous run of the current SST list.
2. The outputs take the position of the inputs.
3. The outputs have new SST IDs.

## Commit Steps and Crash Windows

`internal/pkindex/coordinator/manifestmanager` holds the only writer. It keeps
one lock for each vchannel, so it applies the edits of one vchannel one at a
time. Different vchannels commit at the same time.

```
Manager (holds the vchannel lock)
  1. Load the current version: from the cache, or from the pointer and its body.
  2. Apply the edit. If nothing changes, return the current version.
  3. Write the new body to <manifest dir>/<version>-<uuid>.pb.
  4. Write the pointer:
       first version: on the condition KeyNotExist(pointer key)
       later version: on the condition ValueEqual(pointer key, loaded value)
  5. On an error in step 4, read the pointer again, with a new context
     (no cancel from the caller, 5 s timeout).
       It names the new body: the commit took effect.
       Otherwise: forget the cache entry and return a retryable error.
  6. Put the new version in the cache and return it.
```

The read-back in step 5 does not use the context of the caller. A timeout is
the most frequent cause of an unknown result. At that time the context of the
caller is often expired, and a read with it always fails. The manager uses
`context.WithoutCancel` on the caller context, with a timeout of 5 seconds.

The pointer value is the deterministic encoding of `ManifestPointer`. The body
key has a UUID, so each version has a unique pointer value. `ValueEqual` thus
fails when any other writer committed in between.

Crash windows:

| Crash point | State after the crash | Effect |
|---|---|---|
| Before step 3 completes | No new object, or a partial upload | Caller retries |
| After step 3, before step 4 | An unreferenced body | GC deletes it after `grace` while its version is above the current version. After a later commit reaches its version, GC deletes it by the body retention rules |
| During step 4, result unknown | Pointer is old or new | Step 5 reads the pointer and decides |
| After step 4, before the reply | New version is committed | A flush retry with the same term is idempotent |

Cache rules:

- The cache entry of a vchannel is filled on its first use. DataCoord does not
  load all pointers at start.
- The manager forgets the entry after a failed pointer write. The next edit
  reads the pointer again.
- Until a failed write, `GetManifest` does not see commits from another
  DataCoord. The conditional pointer write keeps the commits correct when the
  cache is stale.
- GC never uses the cache. It reads the pointer from the meta kv.

Each commit RPC returns the complete manifest after the commit. A caller that
installs the result can compare its version with the version that it has.

## Fencing by Pchannel Term

The term is `PChannelInfo.term`. The streaming coordinator increases it each
time it assigns the pchannel to a streaming node. The term does not depend on
the WAL backend.

- A claim or a flush with a term below the manifest term gets `ErrFenced`.
- A fenced caller is not the owner of the vchannel. It must stop the writes on
  that vchannel and must not retry.
- The checkpoint rule alone rejects most late flushes. The term also rejects a
  late flush from an old owner whose checkpoint is higher. This can occur when
  two streaming nodes write the same pchannel for a short time.

## Errors at the Service Boundary

Code under `internal/pkindex` does not return merr. It uses two kinds of
errors:

- Sentinels that a caller can test with `errors.Is`. The manifest sentinels are
  in `core/manifest/errors.go`.
- Categories from `internal/pkindex/pkerr`, attached with `errors.Mark`.

The service translates each error once, before it returns. `ErrFenced` maps to
`ChannelNotAvailable`. All other errors go through `pkerr.ToMerr`.

`pkerr.ToMerr` checks the category marks before a merr in the error chain. This
order matters here: the meta kv and the chunk manager already return merr
errors (for example `IoFailed`, which is not retryable). The manifest code
marks those failures as `ErrUnavailable`, and the mark must win.

| Error | Source | merr code | Retryable |
|---|---|---|---|
| Service has no manager yet | Service start | `ServiceNotReady` | Yes |
| `collection_id` is not positive, or `vchannel` is empty, `.`, `..`, or contains `/` | Request check, all three RPCs | `ParameterInvalid` | No (caller bug) |
| `pk_data_type` is not `Int64` or `VarChar` | Request check, `ClaimManifest` | `ParameterInvalid` | No (caller bug) |
| `checkpoint_time_tick` is 0 | Request check, `CommitFlush` | `ParameterInvalid` | No (caller bug) |
| `ErrFenced` | Claim, flush | `ChannelNotAvailable` | No. Stop the writes on the vchannel |
| Meta kv or object storage failure (`ErrUnavailable`) | Load, write, read-back, walk | `ServiceUnavailable` | Yes |
| Pointer write failed and the read-back does not show the new body | Commit step 5 | `ServiceUnavailable` | Yes |
| Missing body object (`ErrCorrupted`) | Load | `DataIntegrity` | No |
| Body or pointer does not decode, or versions differ (`ErrCorrupted`) | Load | `DataIntegrity` | No |
| `ErrNotClaimed`, `ErrCheckpointRegressed`, `ErrDuplicateSST`, `ErrStaleEdit`, `ErrSchemaMismatch` | Edits | `ServiceInternal` | No (caller bug) |
| Error with no category | Any | `ServiceInternal` | No |

Notes:

- A remote store failure is transient, so it is retryable. `pkerr.ErrIO` (code
  `IoFailed`) is only for local disk, and the manifest service does not use it.
- A missing object has both marks: `ErrCorrupted` from the object store adapter
  and `ErrUnavailable` from the caller. `ToMerr` checks `ErrCorrupted` first.
- The edit errors are caller bugs. A correct caller does not send them.
- The client uses `merr.CheckRPCCall`, so the caller receives the merr code.
  The client translates a connection failure or a gRPC transport error that has
  no merr code to `ServiceUnavailable` and keeps the cause. It returns a
  context cancel or deadline error unchanged. It also returns a gRPC status
  error with code `DeadlineExceeded` or `Canceled` unchanged. gRPC does not
  wrap the context error in these status errors, so the client checks
  `status.Code(err)`. The client retries only gRPC `UNAVAILABLE` transport
  failures.
- The caller decides the retry with `merr.IsRetryableErr`. `ServiceUnavailable`
  and `ServiceNotReady` are retryable.
- Code 503 (`ChannelNotAvailable`) means "fenced" only in an error that this
  client returns. Other components use the same code for a retryable state, for
  example `GetChannelRecoveryInfo` in DataCoord. The caller must classify the
  client error at the call site. It must not use `errors.Is` on an error chain
  that has other errors in it. A caller that uses `retry.Do` must pass
  `retry.RetryErr(merr.IsRetryableErr)`. Without this option, `retry.Do`
  retries a fenced call.
- The service rejects an invalid request before it calls the manager. The
  `ErrFenced` translation keeps the original error as a secondary error, so
  logs that print the stack show the recorded and requested terms.

## Garbage Collection

`internal/pkindex/coordinator/gc` runs in DataCoord. It is a separate loop from
the segment garbage collector. Each round lists all manifest pointers and
handles each vchannel separately. An error on one vchannel is logged, and the
round continues.

### Read order

For each vchannel, GC reads in this order:

1. The in-flight compaction outputs.
2. The pointer and the current body, from the meta kv. Never from the cache.
3. The other manifest bodies.
4. The SST files.

The in-flight outputs come before the pointer. If GC read the pointer first, a
compaction that commits between the two reads would be in neither set, and GC
would delete its outputs.

### What GC keeps and deletes

Manifest bodies:

| Body | Kept when |
|---|---|
| Current body (the pointer names it) | Always |
| Version ≤ current | Version is in the newest `manifestRetainVersions` versions (current included), or age ≤ `manifestRetainDuration` |
| Version > current (not committed yet, or never) | Age ≤ `grace` |

GC deletes a body at or below the current version and outside the newest
`manifestRetainVersions` versions when the body is older than
`manifestRetainDuration`. `grace` does not apply to it. The pointer only moves
up, so such a body never becomes the current version again.

A body in the retained version range is kept even when it lost the pointer
write. Bodies do not record their parent, so GC cannot tell the winner from the
loser in that range.

SST files: the live set is the SSTs of all retained bodies plus the in-flight
compaction outputs. GC deletes an SST that is not in the live set and is older
than `grace`. If GC cannot read a retained body, it deletes no SST of that
vchannel in that round. It still deletes the bodies outside the retention.

### Grace window

`grace` must be longer than the longest time from an object write to its
commit:

- a manifest body write to its pointer write,
- an SST upload to its flush commit,
- a compaction output upload to its compaction commit.

GC deletes an SST, or a body above the current version, that stays
uncommitted longer than `grace`. GC rejects a `grace` below `MinGrace`
(10 minutes). DataCoord then logs an error and does not start GC. The unit of
the parameter is seconds. A duration string such as `"1h"` reads as 0, so GC
does not start. Compaction outputs do not depend on `grace`, because they are in
the in-flight set until their commit takes effect.

### Layout mismatch

GC skips a vchannel when the current body key is not directly under the layout
manifest directory, or when `sst_base_path` differs from the layout. It logs a
warning and deletes nothing for that vchannel. This prevents deletes in a
directory that GC does not own, for example after a root path change.

### Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `dataCoord.pkindex.gc.interval` | 3600 s | Time between two rounds. Must be positive |
| `dataCoord.pkindex.gc.grace` | 86400 s | Minimum age of an unreferenced SST, or of a body above the current version, before GC deletes it. Must be at least 600 s (`MinGrace`) |
| `dataCoord.pkindex.gc.manifestRetainVersions` | 8 | Number of newest versions kept, current included |
| `dataCoord.pkindex.gc.manifestRetainDuration` | 3600 s | Minimum age of a body at or below the current version, outside the newest versions, before GC deletes it |

The duration parameters are in seconds. If one value is not valid, DataCoord
logs an error and does not start GC. The manifest service still runs.

## Meta KV Predicate `KeyNotExist`

The first version has no pointer to compare with. `ValueEqual` cannot express
"create only if absent", so this work adds `predicates.KeyNotExist(key)` in
`pkg/kv/predicates/predicate.go`.

- **etcd.** The predicate becomes `CreateRevision(key) = 0` in the transaction.
- **TiKV.** The transaction reads the key. A missing key meets the predicate.
  The same call must also write the key. TiKV detects a concurrent creator only
  as a write-write conflict on that key.
- Both backends reject an unknown predicate type before they read.

## Known Limits and Follow-ups

- **Streaming node side.** The snapshot side (SST upload, flush commit, handoff)
  and recovery (claim, WAL replay from the checkpoint) are not in this work.
- **Compaction.** No compaction task exists. DataCoord passes no in-flight
  output source to GC yet. A compaction task must add one before it uploads
  outputs.
- **Collection drop.** Nothing deletes the `pk_index/<collectionID>` prefix after
  a collection drop.
- **Snapshot protection.** Collection snapshots protect segments and index
  builds, but not primary key index SSTs.
- **Tables in use by streaming nodes.** Lazy reads from object storage will let
  a streaming node read an SST after a compaction replaced it in the manifest.
  GC has no input for these tables yet.
- **Compaction commit retry.** If the pointer write of a compaction commit took
  effect but the read-back failed, the service reports the commit as
  unavailable. A retry of the same edit returns the current version, because
  the compaction rule accepts an applied retry.
- **Pointer listing cost.** Each GC round lists all pointers in one call. The
  cost with very many vchannels is not measured.
- **Stalled commits.** A commit that stalls longer than `grace` loses its
  objects. The committed manifest can then refer to deleted files.
- **Stale cache.** After a DataCoord failover, an old DataCoord can keep a stale
  cache entry. It refreshes the entry only after a failed pointer write.
- **Two live coordinators.** The manager does not read the meta kv again for an
  edit that changes nothing. When two DataCoords are live at the same time, a
  claim or a flush that changes nothing can succeed on a stale cache for a
  fenced owner. The first edit from that owner that changes the manifest fails
  the conditional pointer write. The manager then forgets the cache entry and
  returns a retryable error. The retry loads the new version and gets
  `ErrFenced`. The fenced owner does not write bad data.
