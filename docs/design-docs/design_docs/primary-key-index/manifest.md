# Primary Key Index Manifest Service

- **Created:** 2026-10-12
- **Author:** bigsheeper
- **Status:** Draft
- **Component:** DataCoord, `internal/datacoord/pkindex/manifest`, `internal/pkindex/objpath.go`
- **Related work:** Primary Key Index Core Library (#53910), issue #52305

## Summary

The primary key index of a collection keeps one LSM tree for each vchannel. The
streaming node writes the tree as SST files in object storage. A reader must
know which SST files make up the tree and up to which WAL position the files
are complete. The manifest records this state.

This document describes the manifest service. The service runs in DataCoord.
It is the only durable writer of the manifests. It does these tasks:

- It applies edits (flush, compaction) to the manifest of one vchannel.
- It stores each manifest version as an object and points to it from the meta
  kv (etcd or TiKV).
- It fences an old owner of a pchannel by the pchannel term that the
  streaming coordinator records.
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
- The streaming node recovery that reads the manifests and replays the WAL.
- The compaction task that plans, runs and commits a compaction. See
  [compaction.md](compaction.md).
- Cleanup of all primary key index objects after a collection drop.
- Snapshot protection of SST files.

## Data Model

The proto is in `pkg/proto/pkindex.proto`.

```protobuf
message Manifest {
  int64 collection_id = 1;
  string vchannel = 2;
  int64 version = 3;               // increases by 1 on every committed edit
  int64 last_flush_term = 4;       // pchannel term of the last flush
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
  int64 run_id = 7;        // 0: a flushed table; otherwise the run of a compaction
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
  hold the SSTs of several generations. A compaction changes the version and no
  generation. The name follows the `manifest_version` of the
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
- **`pk_data_type`.** The first flush of a vchannel sets it. Each later flush
  must send the same type.
- **`last_flush_term`.** Only a flush writes it. It tells a retry from a
  regression when the checkpoint of a flush does not move forward. Fencing does
  not use it.

## Storage Layout

| Store | Key | Value |
|---|---|---|
| Meta kv | `<metaRoot>/pkindex-meta/manifest/<collectionID>/<vchannel>` | `ManifestPointer` |
| Object storage | `<rootPath>/pk_index/<collectionID>/<vchannel>/manifest/<version>-<uuid>.pb` | `Manifest` body |
| Object storage | `<rootPath>/pk_index/<collectionID>/<vchannel>/sst/<id>.sst` | SST file |

- The pointer keys come from `PointerKey` in
  `internal/datacoord/pkindex/manifest`.
- A flush also reads `<metaRoot>/streamingcoord-meta/pchannel/<pchannel>`, the
  `PChannelMeta` of the streaming coordinator. The manifest service does not
  write this key. Its name comes from `BuildPChannelInfoPath` in
  `internal/metastore/kv/streamingcoord`, which the streaming coordinator
  catalog also uses. In MixCoord, DataCoord and the streaming coordinator use
  the same meta kv and the same meta root path.
- `pk_index` is `common.PKIndexPath`. No other storage prefix starts with it, so
  the existing orphan scans do not see these objects.
- `sst_base_path` is `<rootPath>/pk_index/<collectionID>/<vchannel>/sst`. The
  function `pkindex.SSTBasePath(rootPath, collectionID, vchannel)` in
  `internal/pkindex/objpath.go` builds it. That file builds all object
  keys of the primary key index. The writer of the SSTs, the manifest writer
  and GC all use it, so they cannot disagree. The first version records the
  value, and readers and GC use the recorded value.
- The writer never overwrites a body. Each body key has a new UUID. Two writers
  that commit the same version at the same time write two different objects.
  The conditional pointer write selects one of them. The body of the other
  writer stays unreferenced, and GC deletes it.
- An SST ID is a Milvus global ID and is used only once. A retry of an upload
  writes the same key with the same content.

## Edits and Validation

A caller sends an edit, not a complete manifest. The manager applies the edit
to the current version. The edit rules are private functions of the manifest
writer in `internal/datacoord/pkindex/manifest`. They do not change the
current version.

### Flush

The snapshot side commits a flush with `CommitFlush`. A flush carries the
vchannel, the term, the primary key data type, the new SSTs (newest first), and
the new checkpoint. The pchannel of the flush is the pchannel part of the
vchannel name.

The flush term is first checked against `channel.term` of the pchannel meta.
The manager reads the meta from the meta kv on every flush. It keeps the
stored bytes for the condition of the pointer write, and it compares the
decoded term:

| Condition | Result |
|---|---|
| No pchannel meta | `ErrTermNotAssigned` |
| Flush term < meta term | `ErrFenced` |
| Flush term > meta term, also after one more read | `ErrTermNotAssigned` |
| Flush term = meta term | Continue with the edit |

The streaming coordinator increases the term when it assigns the pchannel. It
stores the new term before the streaming node gets the assignment, so a
correct streaming node never sends a term above the stored term.

Then the flush is applied to the manifest:

| Condition | Result |
|---|---|
| No manifest | Apply the flush to an empty manifest. The result is version 1 with the `sst_base_path` of the layout and the `pk_data_type` of the flush |
| `pk_data_type` differs from the manifest | `ErrSchemaMismatch` |
| Checkpoint does not move forward, flush term = `last_flush_term` | No write. Return the current version |
| Checkpoint does not move forward, flush term ≠ `last_flush_term` | `ErrCheckpointRegressed` |
| An SST ID is in the manifest, or is twice in the flush | `ErrDuplicateSST` |
| Otherwise | New version: SSTs go to the front, checkpoint moves forward, `last_flush_term` becomes the flush term |

Notes:

- A flush with an old checkpoint and the last flush term can only be a retry
  of an applied flush. One term has one owner, and its checkpoint only moves
  forward. The manager does not look at the SSTs of such a flush. A compaction
  can replace them before the retry arrives.
- A flush with an old checkpoint and another term is a regression. If it were
  accepted as a retry, a new owner with a bug would get a success for SSTs that
  are not in the manifest.
- A flush with no SSTs only moves the checkpoint.
- The duplicate check runs only when the checkpoint moves forward.
- The pointer write of a flush is in the same meta kv transaction as a
  condition on the pchannel meta. See "Commit Steps and Crash Windows".

### Compaction

A compaction replaces a contiguous range of SSTs (inputs) with merged SSTs
(outputs). A **run** is a set of SSTs whose key ranges do not overlap. A
flushed SST has `run_id` 0 and is a run of its own. All outputs of one
compaction share one nonzero `run_id`. The flushed SSTs are always a prefix of
the list. See [compaction.md](compaction.md).

| Condition | Result |
|---|---|
| No manifest | `ErrNoManifest` |
| Inputs are not empty, no input is in the list, and all outputs are in the list | No write. Return the current version |
| Inputs are empty, or are not a contiguous range of the list in the same order | `ErrStaleEdit` |
| An output ID is in the manifest, or is twice in the outputs | `ErrDuplicateSST` |
| The inputs split a run: the SST before the first input or after the last input has the same nonzero `run_id` as that input | `ErrStaleEdit` |
| The outputs are not one new run: an output has `run_id` 0, the outputs have different run ids, or the run id is already in the manifest | `ErrStaleEdit` |
| An output has a min key above its max key | `ErrStaleEdit` |
| The key ranges of the outputs overlap or are not in ascending order | `ErrStaleEdit` |
| After the edit, a flushed SST (`run_id` 0) is behind a run | `ErrStaleEdit` |
| Otherwise | New version: outputs take the place of the inputs. Checkpoint and `last_flush_term` do not change |

The second row makes a compaction commit idempotent. If the pointer write of
a compaction took effect but the caller got an error, the caller sends the same
edit again. The outputs have new global IDs, and no other edit can add them to
the list. Thus, for non-empty outputs, "no input in the list and all outputs in
the list" occurs only when this compaction took effect. For empty outputs, the
check reduces to "no input in the list", which another compaction of the same
inputs also satisfies. Every other mismatch, for example some inputs in the
list or only some outputs in the list, gets `ErrStaleEdit`.

The duplicate check runs before the run checks. When `applyCompaction` returns
`ErrStaleEdit`, the manager deletes the cache entry of the vchannel.

Compaction is in-process only. The compaction task in DataCoord calls
`Manager.CommitCompaction`. The gRPC service has no compaction method.

### Flush and compaction commute

A flush only adds SSTs at the front of the list. A compaction only replaces a
contiguous range that is already in the list. The two edits give the same result in either
order. Thus a flush and a compaction do not make each other retry.

This is true only when compaction follows this contract:

1. The inputs are a contiguous range of the current SST list. The range starts
   and ends at run boundaries, so it does not split a run.
2. The outputs take the position of the inputs.
3. The outputs have new SST IDs.
4. The flushed SSTs stay a prefix of the list.

## Commit Steps and Crash Windows

`internal/datacoord/pkindex/manifest` holds the only writer. It keeps
one lock for each vchannel, so it applies the edits of one vchannel one at a
time. Different vchannels commit at the same time.

```
Manager (holds the vchannel lock)
  1. Load the current version: from the cache, or from the pointer and its body.
  2. Apply the edit. If nothing changes, return the current version.
     If a compaction gets ErrStaleEdit, forget the cache entry of the
     vchannel and return the error.
  3. Write the new body to <manifest dir>/<version>-<uuid>.pb.
  4. Write the pointer in one meta kv transaction:
       first version: on the condition KeyNotExist(pointer key)
       later version: on the condition ValueEqual(pointer key, loaded value)
       a flush also: on the condition ValueEqual(pchannel meta key, bytes
       that the flush term was checked against); the transaction does not
       write the pchannel meta key
  5. On an error in step 4, read the pointer again, with a new context
     (no cancel from the caller, 5 s timeout).
       It names the new body: the commit took effect.
       Otherwise: forget the cache entry of the vchannel, and return a
       retryable error.
  6. Put the new version in the cache, call the observer
     (ManifestChanged), and return the new version.
```

The read-back in step 5 does not use the context of the caller. A timeout is
the most frequent cause of an unknown result. At that time the context of the
caller is often expired, and a read with it always fails. The manager uses
`context.WithoutCancel` on the caller context, with a timeout of 5 seconds.

The pointer value is the deterministic encoding of `ManifestPointer`. The body
key has a UUID, so each version has a unique pointer value. `ValueEqual` thus
fails when any other writer committed in between.

The pchannel meta condition makes the term check and the pointer write one
atomic step. In one term, the streaming coordinator changes the value of the
pchannel meta only when it marks the pchannel assigned or unavailable. A new
term changes the value too. If the value changes between the check and the
write, the condition fails and the flush gets a retryable error. The retry
reads the pchannel meta again and checks the term again. A failed condition
of the pointer or of the pchannel meta takes the same path: the caller
retries. The manager has no retry loop of its own.

- **etcd.** The condition is a compare in the transaction. etcd evaluates it
  atomically with the writes.
- **TiKV.** TiKV transactions are optimistic and check conflicts only on the
  keys that they write or lock. A read of the pchannel meta key at the start
  timestamp does not see an assignment that commits later, and an optimistic
  lock of the key makes every two flushes of the pchannel conflict. When a value predicate
  targets a key that the call does not write, the TiKV meta kv makes the
  transaction pessimistic. It takes a pessimistic lock (`LockKeys`) on every
  predicate key at a fresh timestamp and evaluates the predicates on the
  locked values. A pchannel meta write that commits before the lock is seen
  by the predicate. A pchannel meta write that starts while the lock is held
  waits for the flush.
  Flushes of different vchannels of one pchannel wait for each other on the
  lock instead of failing. A waiter that wakes after another writer committed
  takes the lock again at a newer timestamp, until the request times out. A
  failed pessimistic transaction is always rolled back, so it does not keep
  its locks. The same rule applies to every caller of the TiKV meta kv with
  this predicate shape. One existing caller has it: the recovery snapshot of
  the streaming node guards on an unchanged checkpoint key.

Crash windows:

| Crash point | State after the crash | Effect |
|---|---|---|
| Before step 3 completes | No new object, or a partial upload | Caller retries |
| After step 3, before step 4 | An unreferenced body | GC deletes it after `grace` while its version is above the current version. After a later commit reaches its version, GC deletes it by the body retention rules |
| During step 4, result unknown | Pointer is old or new | Step 5 reads the pointer and decides |
| After step 4, before the reply | New version is committed | A flush retry with the same term is idempotent |

Cache rules:

- The cache entry of a vchannel is filled on its first use. DataCoord does
  not load all pointers at start.
- The manager forgets the entry after a failed write. The next edit reads it
  again.
- The manager forgets the entry of a vchannel after a compaction that gets
  `ErrStaleEdit`. The next edit reads the pointer and the body again.
- The manager does not cache the pchannel meta. Each flush reads it, because
  the condition of the write needs its current bytes. A flush occurs once for
  each vchannel handover, so the read is rare.
- `GetRecoveryManifests` never uses the cache. It reads the pointers from the
  meta kv.
- The conditional writes keep the commits correct when the cache is stale.
- GC never uses the cache. It reads the pointer from the meta kv.

Each commit RPC returns the complete manifest after the commit. A caller that
installs the result can compare its version with the version that it has. The
client sets its gRPC receive limit from `rootCoord.grpc.clientMaxRecvSize`, the
limit of the mix coordinator client (512 MB by default), because the manifest
of a large vchannel can be bigger than the 4 MB default of gRPC.

## Recovery Read

Recovery calls `GetRecoveryManifests` once with the vchannels of the pchannel
that it opens. It returns the manifest pointer of each vchannel; the caller
reads the body from object storage.

- The response has one `ManifestPointerEntry` per target, in the order of the
  targets: a status, `found`, and the pointer.
- Each entry has its own status. The failure of one target, for example a
  pointer that does not decode, does not fail the others.
- The service reads the pointers from the meta kv in batches of 128 keys, the
  default limit of operations in one etcd transaction. A failed batch read
  fails the entries of that batch only.
- A request has 1 to 10000 targets without duplicates. Other requests get
  `ServiceInternal`. A caller with more vchannels sends more requests.
- The coordinator reads only the meta kv, not object storage. The body is
  immutable: its key has a UUID and it is never overwritten. GC keeps the
  current body, the newest versions and the bodies of the last
  `manifestRetainDuration`, so a body that a caller reads a few seconds after
  this call still exists. If the read fails, the caller asks again.
- The SST base path of a vchannel without a manifest comes from
  `pkindex.SSTBasePath`, not from this call.

## Fencing by Pchannel Term

The term is `PChannelInfo.term`. The streaming coordinator increases it each
time it assigns the pchannel to a streaming node. The term does not depend on
the WAL backend.

- The streaming coordinator records the term once for each pchannel, in the
  pchannel meta. The manifest service reads it there and keeps no term record
  of its own. A recovery of a pchannel with many vchannels writes nothing to
  the manifest service before its first flush.
- A flush with a term below the meta term gets `ErrFenced`.
- A flush with a term above the meta term gets `ErrTermNotAssigned`.
- A fenced caller is not the owner of the pchannel. It must stop the writes on
  that pchannel and must not retry.
- The checkpoint rule alone rejects most late flushes. The term also rejects a
  late flush from an old owner whose checkpoint is higher. This can occur when
  two streaming nodes write the same pchannel for a short time.

## Errors at the Service Boundary

Code under `internal/pkindex` does not return merr. It uses two kinds of
errors:

- Sentinels that a caller can test with `errors.Is`. The manifest sentinels are
  in `internal/datacoord/pkindex/manifest`.
- Categories from `internal/pkindex`, attached with `errors.Mark`.

The service translates each error once, before it returns. `ErrFenced` maps to
`ChannelNotAvailable`. All other errors go through `pkindex.ToMerr`.

`pkindex.ToMerr` checks the category marks before a merr in the error chain. This
order matters here: the meta kv and the chunk manager already return merr
errors (for example `IoFailed`, which is not retryable). The manifest code
marks those failures as `ErrUnavailable`, and the mark must win.

| Error | Source | merr code | Retryable |
|---|---|---|---|
| Service has no manager yet | Service start | `ServiceNotReady` | Yes |
| `collection_id` is not positive, or `vchannel` is empty, `.`, `..`, or contains `/` | Request check, `CommitFlush`, each target of `GetRecoveryManifests` | `ServiceInternal` | No (caller bug) |
| No targets, more than 10000 targets, or a repeated target | Request check, `GetRecoveryManifests` | `ServiceInternal` | No (caller bug) |
| `pchannel_term` is not positive | Request check, `CommitFlush` | `ServiceInternal` | No (caller bug) |
| The `vchannel` name has no pchannel part | Request check, `CommitFlush` | `ServiceInternal` | No (caller bug) |
| `pk_data_type` is not `Int64` or `VarChar` | Request check, `CommitFlush` | `ServiceInternal` | No (caller bug) |
| `checkpoint_time_tick` is 0 | Request check, `CommitFlush` | `ServiceInternal` | No (caller bug) |
| `ErrFenced` | Flush | `ChannelNotAvailable` | No. Stop the writes on the pchannel |
| Meta kv or object storage failure (`ErrUnavailable`) | Load, write, read-back, walk | `ServiceUnavailable` | Yes |
| Object storage failure that a retry does not fix: permission denied, bucket not found, invalid credentials, entity too large | Load, write | Its own code (`IoPermissionDenied`, `IoBucketNotFound`, `IoInvalidCredentials`, `IoEntityTooLarge`) | No |
| Pointer write failed, for example on a failed condition, and the read-back does not show the new value | Commit step 5 | `ServiceUnavailable` | Yes |
| Missing body object (`ErrCorrupted`) | Load | `DataIntegrity` | No |
| Body, pointer or pchannel meta does not decode, or has no body key, version 0 or term 0, or versions differ (`ErrCorrupted`) | Load | `DataIntegrity` | No |
| `ErrTermNotAssigned`, `ErrNoManifest`, `ErrCheckpointRegressed`, `ErrDuplicateSST`, `ErrStaleEdit`, `ErrSchemaMismatch` | Edits | `ServiceInternal` | No (caller bug) |
| Error with no category | Any | `ServiceInternal` | No |

Notes:

- A remote store failure is transient, so it is retryable. The exceptions are
  the failures of a wrong store configuration and of a too large object: a
  retry does not fix them, so they keep their own code. `IoFailed` is not one
  of them, because the object storage client also returns it for unknown and
  transient failures. `pkindex.ErrIO` (code `IoFailed`) is only for local disk,
  and the manifest service does not use it.
- A missing object has both marks: `ErrCorrupted` from the object store adapter
  and `ErrUnavailable` from the caller. `ToMerr` checks `ErrCorrupted` first.
- `GetRecoveryManifests` returns a load error of one target in the status of
  its entry, with the same codes. The status of the response covers only the
  request check and the service state.
- The requests come from streaming nodes, not from users. A request that
  fails a request check is a caller bug or a version mismatch, so it gets
  `ServiceInternal`, not `ParameterInvalid`, which marks a user input error.
- The edit errors are caller bugs. A correct caller does not send them.
  `ErrTermNotAssigned` from a flush means that the streaming coordinator has no
  pchannel meta at the flush term.
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

`internal/datacoord/pkindex/gc` runs in DataCoord. It is a separate loop from
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
does not start. `Inspector.InFlightOutputs` of the compaction task is the
in-flight source. A task registers the output range of its current attempt
when the dispatch succeeds. After that, the range is protected until the task
ends. `grace` covers the moment before the registration. The ranges of
earlier attempts are not protected.

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
  as a write-write conflict on that key. A `ValueEqual` on a key that the call
  does not write makes the transaction pessimistic; see "Commit Steps and
  Crash Windows".
- Both backends reject an unknown predicate type before they read.

## Known Limits and Follow-ups

- **Streaming node side.** The snapshot side (SST upload, flush commit, handover)
  and recovery (`GetRecoveryManifests`, WAL replay from the checkpoint)
  are not in this work.
- **Compaction.** [compaction.md](compaction.md) describes the compaction
  task. `Inspector.InFlightOutputs` is the in-flight output source of GC.
- **Collection drop.** Nothing deletes the `pk_index/<collectionID>` prefix after
  a collection drop.
- **Vchannel without a pointer.** GC finds a vchannel only through its manifest
  pointer. If the first commit of a vchannel writes the body (and the streaming
  node uploads SSTs) but the pointer write never succeeds, GC does not see
  these objects. A later successful flush of the vchannel creates the pointer,
  and GC then deletes them after `grace`. If no flush of the vchannel succeeds
  before it is dropped, they stay, like the prefix of a dropped collection.
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
- **Fenced retry that changes nothing.** A flush that changes nothing does not
  write, so it has no condition on the pchannel meta. The term check before it
  reads the meta kv, so a fenced owner gets `ErrFenced`. The only gap is a
  term change between that read and the return. The fenced owner does not
  write bad data: its next flush that changes the manifest fails the term
  check or the condition of the write.
