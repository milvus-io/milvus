# Primary Key Index Core Library (codec, SST, engine)

- **Created:** 2026-09-17
- **Status:** Draft
- **Component:** `internal/pkindex/core`
- **Related work:** LSM-based Primary Key Index, issue #52305

## Summary

The LSM-based primary key index (#52305) keeps, per vchannel, a map from
primary key to the segment holding the live row, and consults it on the write
path to deduplicate inserts. This document covers only the storage library
underneath that feature: three packages with no dependency on any node, RPC, or
WAL code.

| Package | Responsibility |
|---|---|
| `core/codec` | Order-preserving primary key encoding, value encoding, the tombstone value |
| `core/sst` | SST write, read, iteration, content-addressed naming, checksum verification |
| `core/engine` | Per-vchannel engine: generations of writes plus a read-only committed SST set, the cycle that hands one over to the other, node-level shared caches |

The library provides mechanism only. When to rotate, flush, upload, install or
drop is decided by its callers, which are not part of this change. Nothing in
Milvus calls the library yet.

## Context

The feature design in #52305 fixes the constraints this library works under:

- One index instance per vchannel, so that dropping a channel is a directory
  delete and instances have independent lifecycles.
- The storage engine's own WAL is off. Durability comes from the Milvus WAL;
  the index is rebuilt by replaying it from a checkpoint.
- The storage engine's automatic compaction is off. Merging is done elsewhere,
  so the streaming node never pays for it on the write path.
- Committed data is a set of immutable SST files whose authoritative copy is in
  object storage. It is only ever replaced as a whole, by a new set, and only
  after the manifest that lists it is committed.
- The streaming node keeps writing at all times. No step of handing data over
  to the committed set may pause writes.

## Goals

- A point lookup that sees, for any key, the most recent write or delete across
  the generations and the committed set.
- Replacing the committed set atomically with respect to concurrent lookups.
- Handing a generation's data over to the committed set without pausing writes
  and without a window in which acknowledged state is unreachable.
- SST files that are interchangeable between the producer on the streaming node
  (memtable flush) and any other producer using `core/sst`.
- Resuming an interrupted hand-over after a crash without leaking directories.

## Non-goals

- The manifest that records which SST files form the committed set.
- Merging SST files.
- Uploading, downloading, or garbage-collecting files in object storage.
- The write-path decision logic and its integration into the WAL.
- Configuration parameters. The engine takes a `Config` struct; wiring it to
  `paramtable` belongs with the first caller.

## Encoding

A key is the order-preserving encoding of one primary key and carries no
prefix: an engine instance belongs to one vchannel, whose collection has one
primary key field of a fixed type.

- `int64`: 8 bytes big-endian with the sign bit flipped, so bytewise order
  equals numeric order.
- `varchar`: the raw bytes.

Order preservation is what lets an SST's min/max key prune point lookups
without decoding.

A value starts with a kind byte that selects its layout: `0x00` is a tombstone
and carries nothing else, `0x01` is a PK entry and carries the 8-byte
big-endian segment ID.

Later optional fields, redundant field data and a custom timestamp, go into a
protobuf message `PKEntryExt` appended after the fixed part, as a new kind
`0x02` = `[SegmentID 8B BE][PKEntryExt]`. The message runs to the end of the
value, so it needs no length prefix, and a node that does not know a field
skips it instead of failing. SegmentID keeps the same offset as in `0x01`, so
the hot path that only reads SegmentID never unmarshals protobuf; only a
caller that wants the optional fields pays for decoding. This follows
CockroachDB's `MVCCValue`, which likewise pairs a hand-encoded simple form
with an extended one.

The key and value kinds `0x00` and `0x01` are hand-encoded. Protobuf cannot
encode the key at all, because varints and field tags do not preserve order,
and writing a protobuf definition that merely describes a format the code does
not actually use would put a second, diverging source of truth next to the
real one. The layouts live in the codec package's comments, and golden tests
pin the exact bytes.

A delete is stored as the tombstone value, not as the storage engine's native
tombstone. A delete on the writable side must mask an older entry in the
committed set, and must survive a memtable flush into an SST that later joins
that set; a native tombstone does neither once it leaves its DB.
Physically dropping tombstoned keys is the job of whatever merges SSTs.

The tombstone encoding is reserved. `Engine.Apply` takes a nil value to mean
delete and rejects a non-nil value equal to the tombstone, so the rule is
enforced at the single write entry rather than left to callers.

## SST files

SSTs use Pebble's sstable format with `pebble.DefaultComparer` and a whole-key
bloom filter of 10 bits per key. The format major version is pinned to a named
version rather than to whatever Pebble considers newest, and the table format
is derived from it; raising it is a rolling upgrade, readers first, because the
files are read by other nodes. One Pebble options template in `core/sst`
carries the version, the comparer and the filter, and the Writer, the Reader
and the generations all build their options from it, so a memtable flush
output and a Writer output cannot drift apart and a flush output can serve as a
committed table as is.

### Identity and description

Which table it is, and what its bytes are, are kept apart from where it is.

- **`sst.ID`** is which table it is: a Milvus global ID, from the same
  allocator that hands out segment and log IDs, passed in by the caller. The
  engine takes it from `Config.AllocID`. It names the file and the object.
- **`sst.Info`** is what the bytes are: the ID, the key range, the entry count,
  the size and the table format. It is identical on every node and is what a
  manifest records. A local path is not part of it, and travels alongside as
  an argument instead.

This package reads and writes local files only. Reading a table straight from
object storage, and caching its blocks on local disk, is a later piece of work;
keeping the abstraction out until then leaves the change to it mechanical.

There is no content addressing and no whole-file checksum. An ID is used once,
so an upload that retries under the same ID is still idempotent, and the
manifest records the object key and the size. Integrity is Pebble's per-block
checksum, verified on every block read, plus a footer that makes a truncated
file fail to open; reading the wrong file is caught by the unique ID together
with the size check `sst.ExpectSize` performs. Hashing the whole file would add
a full read on the flush path for no gain. This follows what comparable systems
do: SlateDB names tables by ULID and records only per-block CRCs, RocksDB-Cloud
and CockroachDB backups name them by a unique number.

### Bloom filter

Deduplicating an insert mostly probes keys that do not exist, and an absent key
is the most expensive point lookup: every committed table whose range covers the
key must be consulted. Range pruning handles monotonically increasing keys, but
not user-supplied random or varchar keys, where every table spans the whole key
space.

Pebble's built-in filter works here without a comparer that defines `Split`.
The sstable writer adds the whole user key when there is no `Split`; the
sstable-level `SeekPrefixGE(prefix, key)` only uses `prefix` to query the
filter; `DB.Get` passes the whole key as the prefix when there is no `Split`.
Only the DB-level `Iterator.SeekPrefixGE`, which the library does not use,
requires `Split`.

Measured once, with a warm cache: 20,000 in-range misses against a table of
200,000 keys went from 60,111 block accesses and 37 ms to 41,707 and 14 ms, for
a 14% larger file. Cold-read benefit has not been measured.

Cost to be aware of: Pebble's table filter is one block per table, about 1.25
bytes per key, and only helps while it stays in the block cache. A table of 2.5
million keys has a filter block of about 3 MB; once evicted, each probe reads it
back whole, which is worse than having no filter. Callers sizing the node-level
cache must budget for it. A memory-resident filter outside the block cache
remains possible later; the filter is not part of any interface.

## Engine

### Two parts

- **Generations**: containers for writes, numbered in time order. Exactly one
  is active and takes writes; the rest are frozen and waiting to hand over.
  Today a generation is a Pebble DB with WAL and automatic compaction disabled
  and `L0StopWritesThreshold` raised out of reach, because a stall here would
  block the Milvus WAL append path. Generations exist because Pebble can delete
  by key range but not by write time: one container per batch makes handing a
  batch over a matter of deleting that container.
- **Committed tables**: read-only SST readers over tables that are already
  uploaded and already listed by a committed manifest. The name states the
  invariant: nothing enters this set before the manifest that lists it is
  committed. They are not ingested into any Pebble DB.

A lookup walks the active generation, then the frozen ones newest first, then
the committed tables in the order they were installed, skipping tables whose
key range excludes the key. The first layer holding the key decides; a
tombstone resolves to "absent" and stops the walk.

**Rejected: one Pebble DB holding both parts, with `IngestAndExcise` swapping in
merge results.** Excise removes the key span from every level, including writes
that arrived after the merge's input was fixed; and a single ingest batch must
be non-overlapping, so "merge output plus newer L0 tables" cannot go in
atomically. With a writer that never stops, this either loses new writes or
requires pausing them.

### Draining cycle

```
gen := RotateIncrement()               // freeze the active generation
flushed := FlushDraining(gen)          // write it out, staged by sst.ID
... upload, commit the manifest ...
InstallCommitted(tables, retire: gen)  // swap in and retire, in one step
```

The engine supplies the steps; driving them is the caller's, because the two
slow parts, uploading and committing, belong to the component that owns object
storage and the manifest. One handover is:

1. `RotateIncrement` freezes the active generation.
2. `FlushDraining` writes each frozen generation out and stages its tables,
   newest generation first. This includes generations an earlier failed or
   interrupted handover left behind.
3. Upload the staged tables, then commit a manifest listing the complete
   committed set.
4. `InstallCommitted` installs that set and retires the generations it covers,
   in one call.

Three things follow, and all three are the caller's to honour because the
engine cannot see them.

- **Retrying is safe.** If step 3 fails the generations stay frozen, and the
  next handover flushes them again to the same tables under the same IDs, so
  re-uploading writes the same objects.
- **Committing must be idempotent.** If step 3 succeeds and step 4 fails, the
  next handover commits those same tables into a manifest that already lists
  them. The manifest's own compare-and-set is where that is settled.
- **One handover at a time per engine**, with no `DropDraining` alongside it.
  A second would freeze and publish the generations the first is still
  uploading and then retire them, leaving the first reading staging that has
  been deleted. A single background task per vchannel satisfies this.

A handover with nothing to publish retires its empty generations with
`DropDraining`.

None of this runs under an engine lock. Uploading and committing take as long
as the network does and must not block writes or another component's install,
which reaching the engine only through `Lifecycle` is what guarantees.

Rotating first is the fence: once it returns, writes land in the new generation
and can never reach the one being handed over, so its output is stable. The
frozen generation stays in the read path until it retires, so the cycle needs
neither a write pause nor atomicity across steps.

Installing and retiring are one step, not two calls, so coverage cannot go
backwards: there is no window in which the frozen generation is already gone
and the tables that replace it are not yet in. This mirrors Pebble's own flush,
which adds the new L0 files and removes the flushed memtable in one update.

**Rejected: rotate as "open a new DB and delete the old one".** Writes that
arrive after the flush and before the rotation sit in the old memtable, neither
uploaded nor committed; deleting the container loses them.

Invariants:

1. Only a frozen generation is ever written out for upload. There is no
   in-place flush of the active one.
2. Coverage is added before it is removed, which `InstallCommitted` enforces by
   taking the generations to retire as a parameter.
3. `Open` reopens every generation left on disk: the newest becomes active and
   the rest are restored as frozen. A cycle interrupted by a crash can be
   resumed, and no directory leaks. Writes that were only in a memtable are
   lost by design and come back through Milvus WAL replay.

### Staging and idempotent retries

Pebble numbers its files per DB, so two generations produce the same file
names, and a generation's directory disappears when it retires. `FlushDraining`
therefore hard-links every output into a staging directory of its own, under
the ID allocated for it, and records the order in a manifest written last.

That manifest is what makes a retry safe. A second call for the same generation
finds it and returns exactly the same tables under exactly the same IDs, so an
upload that already put some of them in object storage simply writes them
again. A staging directory without the manifest is the debris of an interrupted
attempt and is discarded and redone. Staging left by a generation that no
longer exists is deleted at `Open`.

Staging belongs to the engine and is deleted when the generation retires, so a
publisher that wants to keep serving a committed table from local disk must
make its own link or copy and hand that back as the table's `Path`.

### Recency order of SSTs

A memtable flushes whenever it fills, so one frozen generation commonly holds
several L0 tables with overlapping key ranges. If they reach the committed set
in the wrong order, a deleted key comes back to life.

`FlushDraining` returns the tables newest first. Flushes of one generation are
serial and nothing compacts or ingests, so the tables' sequence number ranges
are disjoint and descending `LargestSeqNum` is recency order. `sst.Info`
carries no ordering field: list order is the contract. Whoever records the
committed set must keep a newer generation's tables ahead of an older
generation's, and keep the order `FlushDraining` returned within a generation.

### Locking

Structural operations (`RotateIncrement`, `FlushDraining`, `InstallCommitted`,
`DropDraining`, `Close`) serialize on one mutex and do their disk IO (opening a
DB, flushing, staging, opening readers, closing a DB, deleting a directory)
while holding only that mutex. `MultiGet` and `Write` never take it. The
read-write locks guarding the generations and the committed set are
write-locked only to swap pointers, and `InstallCommitted` takes both at once,
which is what makes installing and retiring one step as far as a reader is
concerned.

`FlushDraining` and `InstallCommitted` deliberately do not do IO under a read
lock either: Go's `RWMutex` blocks new readers once a writer is waiting, so a
long read-locked flush would stall every `MultiGet` and `Write` as soon as a
rotation queued behind it. The price is that structural operations wait for one
another; writes are unaffected. Publishing, the slowest step of all, happens
outside every one of these locks.

### Shared caches

`SharedResources` holds one block cache and one table cache per node.
Generations and committed readers all attach to it. `sst.ReadInfo`, which only
reads metadata, reads without caching. `SharedConfig` sizes it: the block cache
budget in bytes, the number of sstables the table cache keeps open (default
4096), and the number of table cache shards (default `GOMAXPROCS`, as in
pebble). The table cache serves only the generations' DBs; a committed reader
holds its own file handle. Turning these sizes into Milvus configuration waits
until a component uses the engine.

A lookup on the write path runs while the caller holds a lock, so it must not
be the thing that faults an index or a filter in from disk. `InstallCommitted`
therefore calls `Reader.Preload` on every table it opens, before it swaps
anything in, and its contract is that on return the index and filter of every
committed table are in the block cache.

Replacing the committed set usually repeats most of its tables, so a table
already open under the same ID keeps its reader; only tables new to the set are
opened, and only tables dropped from it are closed. That matters because each
reader has its own cache ID in the shared block cache: reopening a table would
strand everything its blocks had cached, and Pebble offers no per-file eviction
for a standalone reader, so those stranded blocks linger until evicted by use.

## Interface

One implementation, `*Engine`, is split into two interfaces cut to how it is
used rather than to how it is built. The deduplication decision and WAL replay
need only the hot path; moving data between the layers, and shutting the engine
down, belong to whoever owns it.

```go
// the hot path
type RW interface {
    MultiGet(ctx, keys [][]byte) ([][]byte, error)  // nil for absent or deleted
    Write(ctx, muts []Mutation) error               // applied in order, atomically
}
type Mutation struct {
    Key    []byte
    Value  []byte
    Delete bool                                     // true ignores Value
}

// data moving between layers, and shutdown
type Lifecycle interface {
    RotateIncrement(ctx) (Generation, error)
    FlushDraining(ctx, gen) ([]FlushedTable, error)  // newest first, staged by ID
    DrainingGenerations() []Generation               // newest first
    InstallCommitted(ctx, tables []CommittedTable, retire ...Generation) error
    DropDraining(ctx, gen) error
    Stats() Stats
    Close() error                                    // keeps disk state
    Destroy() error                                  // deletes the data directory
}

type CommittedTable struct{ Info sst.Info; Path string }
type FlushedTable   struct{ Info sst.Info; Path string }  // in the engine's staging

func Open(ctx, cfg Config) (*Engine, error)
```

Both sides work on the same state under the same locks, so there is one
implementation and not two. `Open` returns the concrete `*Engine`, following
the Go convention of accepting interfaces and returning structs. The method
names come from the RocksDB family, `MultiGet` and `Write`, which is also what
the write-path component above this one already calls them.

The engine moves opaque bytes; encoding belongs to the caller. Its one piece of
value knowledge is the tombstone. Errors are returned as they are; what to do
about a failing engine is the caller's policy. Mocks are generated for `RW` and
`Lifecycle`.

## Failure semantics

- `InstallCommitted` does not re-read the content of the tables it installs: it
  opens each new one and checks its size against the `Info`. Beyond that,
  integrity is Pebble's per-block checksum, verified on every read.
- If opening or preloading a new table fails, nothing changes: the committed
  set and the generations are exactly as they were.
- `InstallCommitted` fails before changing anything if a generation named in
  `retire` is not draining.
- A failed `RotateIncrement` leaves the engine unchanged.
- A failed `DropDraining` has already removed the generation from the read
  path and must not be retried; whatever is left on disk is restored as
  draining by the next `Open` and can be dropped then.
- A failed publish leaves the frozen generations in place, and the next
  handover re-flushes them to the same IDs and publishes them again.
- A corrupt file is not a missing key. Pebble reports a block it cannot read by
  ending iteration, the same way it reports the end of the data, and carries
  the reason on the iterator instead. A read path that ignores it answers
  "absent" for every key in that block, which for this index means silently
  skipping deduplication, so both the point lookup and the scan check the
  iterator before concluding that a key is not there.

### Errors

The library does not return `merr`. A caller inside the primary key index
branches on what happened, and `merr.Is` matches on the numeric code alone, so
distinct situations that share a code become indistinguishable: an engine that
is closed and a generation that is not draining would both arrive as
`ServiceInternal`, although one is worth retrying against a reopened engine and
the other is a caller mistake. Errors are therefore shaped in two layers:

- A signal a caller branches on is a sentinel declared by the package that
  returns it: `engine.ErrClosed`, `engine.ErrNotDraining`.
- How a failure should be reported outward is a category from the leaf package
  `pkerr`, attached with `errors.Mark` so the original error and the context
  each call site added both survive: `ErrIO` for a local disk failure,
  `ErrCorrupted` for stored bytes that do not match the expected layout, and
  `ErrUnavailable` for a condition a retry may clear. A closed engine carries
  both `ErrClosed` and `ErrUnavailable`.
- A failure that is a bug in Milvus rather than a condition to act on — no
  shared resources, a value equal to the reserved tombstone — carries no
  category at all, so nothing downstream retries it or reads it as bad data.

Translation happens once, at the boundary that leaves the primary key index
tree: `pkerr.ToMerr` for a coordinator's gRPC service or a worker's task
result, and the streamingnode interceptor's own mapping to streaming status
errors. `ToMerr` maps `ErrCorrupted` to `ErrDataIntegrity`, `ErrIO` to
`ErrIoFailed`, `ErrUnavailable` to `ErrServiceUnavailable`, and anything
uncategorized to `ErrServiceInternal`; a `merr` passes through untouched. None
of these is caused by request content, so none is an input error. This mirrors
how the streaming package keeps `walimpls.ErrFenced` internal and translates at
its interceptor boundary. An error that escapes a boundary untranslated crosses
gRPC as code 65535, so each boundary carries a test covering every sentinel and
category it can produce.

A mark is only visible to `github.com/cockroachdb/errors.Is`, which is what
`errors` resolves to across this repository; the standard library's `errors.Is`
does not see it and reports false.

## Feasibility: many DBs per node

One Pebble DB per vchannel was checked before building on it, with 1 to 1024
DBs on one node: idle memory 87 to 125 KB per DB, 1024 DBs opened in parallel in
2.42 s, and no order-of-magnitude loss of aggregate write throughput from 64 to
1024 DBs (single run, variance not estimated). Steady state is about 200 KB and
7 file descriptors per DB. The read path was not part of that measurement.

## Verification

Unit tests, run with `-race`:

- codec: order preservation over 1e5 random pairs for both key types plus the
  boundary values; round trips; the value kind byte; golden bytes for both key
  types and both value kinds.
- pkerr: every category and an uncategorized error translate to the expected
  merr code with the call site's context still in the message; a merr passes
  through.
- sst: round trip; the ID names the file; a flipped byte inside a data block
  fails the point lookup and the scan rather than reading as a missing key;
  flush output and Writer output share the pinned table format; min/max keys;
  `ReadInfo` matches what the Writer reported and does not scan the table; a
  size that does not match the manifest fails the open; a reader uses the cache
  it is given; in-range misses are answered by the filter, not by data blocks;
  after `Preload` a miss costs no cache miss.
- engine: write then read; delete masks; the active generation overrides the
  committed set; committed recency order; the full handover cycle; concurrent
  writes during a cycle are all retained; several overlapping L0 tables in one
  generation keep deletes and latest values after the cycle; flush outputs
  carry the filter; committed lookups hit the node-level cache; a lookup right
  after an install faults in neither index nor filter; a table that stays in
  the set keeps its reader and its cached blocks, and a failed open leaves the
  set untouched; `MultiGet` and
  `Write` proceed while a rotation's or a drop's disk IO is held up; a
  concurrent read never sees a half-applied install; re-flushing a generation
  returns the same tables under the same IDs, and an interrupted staging is
  redone; a flushed table stays readable until its generation retires, and its
  staging is gone afterwards; retiring a generation that is not draining
  changes nothing; warm restart restores every generation; unflushed writes do
  not survive a restart; reserved value rejected with no partial write; every
  method rejects work after `Close` with both `ErrClosed` and `ErrUnavailable`;
  an unknown generation reports `ErrNotDraining`; opening without an ID
  allocator fails; `Destroy`.
- staging: re-flushing a generation returns the same tables under the same IDs,
  and an interrupted staging is redone under fresh ones.

`make static-check` passes.

## Follow-ups

- A caller-driven iterator on `sst.Reader`. The callback `Iter(fn)` cannot
  drive a k-way merge.
- Reporting which Milvus WAL position a frozen generation covers. Writes are
  applied concurrently, so apply order is not timetick order; whether the engine
  or its caller tracks this is undecided.
- Read-path measurements: committed lookup cost, cache hit benefit, cold-read
  benefit of the filter, and the cache budget the filter needs.
- `paramtable` wiring for the data directory, cache size and memtable size.
