# Collection-batched Balancer Reconciliation

The extracted QueryView coordinator supports background reconciliation through
the cache's `Scanner`. This describes the component implementation; production
QueryCoord activation remains separate integration work.

## Scanner contract

```go
type Scanner interface {
    Scan(maxSlots int, visit func(*CollectionEntry) bool) error
}

func (c *Cache) NewScanner() Scanner
```

Calls on one scanner are serialized. Cache publications may run concurrently.
Each call examines at most `maxSlots` directory positions, including tombstones;
the caller must supply a positive budget. The callback runs outside cache locks.
Returning false consumes the current collection and stops the call. `io.EOF`
means the round has ended, possibly after delivering a nonempty final batch.
The next call starts a new round. An empty cache returns EOF immediately.

The cache retains its collection lookup map and maintains an additional array
of pointers to small scan slots, ordered by registration. Each slot contains an
atomic pointer to the existing collection slot. Updates publish immutable
collection entries through that collection slot. Actual directory removal clears
the scan slot atomically; re-creating the same ID appends a new slot.

On the first call of a round, the scanner copies only the directory's slice
header under a short read lock. It retains that fixed-length slice and an offset
until EOF. Existing array elements are never rewritten. Appends can extend the
cache's directory without extending an active round. No collection-map copy,
binary search, or compaction-generation handling is needed on the scan path.

This is weakly consistent enumeration. Existing collections can expose newer
publications during a round. A deletion racing with a slot read may still be
delivered; balancing re-reads current cache state through its planning context.
New registrations are covered by normal publication events and the next scan
round. The scanner does not retain a consistent collection-content snapshot.

## Lazy compaction

Removal only writes a tombstone. Once there are at least 1,024 tombstones and
roughly half the directory is deleted, the cache starts one maintenance job:

1. Capture the directory slice header and revision under a short read lock.
2. Allocate and filter a replacement array outside the lock, sharing live slots.
3. Take a short write lock and publish only if the revision still matches.

Appending a collection invalidates the result. The job discards it and a later
deletion can request another attempt; it does not retry in a tight loop.
Concurrent deletion does not invalidate the result: the shared atomic tombstone
prevents resurrection even if it was copied just before retirement. Tombstones
left in the replacement count toward subsequent compaction.

Active scanners continue reading their original arrays without resetting or
skipping positions. EOF releases the scanner's reference, allowing GC to reclaim
the old array. There is no array pool or in-place compaction. `Cache.Close`
detaches publishers and waits for its maintenance job after the controller stops.

The O(C) allocation/filtering runs outside the directory lock; capture and
publication still use locks. Continuous additions can defer reclamation, and a
paused scanner may retain an old pointer array until its round finishes.

## Balancer scheduling

Cache publications record shard, collection, node, or resource group scopes.
Local collection/shard notifications, node/RG notifications and full scans have
independent producers. Each advances its own immutable cursors in slices of at
most 100 shard IDs and can suspend inside a collection or replica. Directory
scans also yield after 256 slots, including tombstones. New scopes alternate
with unfinished cursors within each source. Overlapping requests for one active scope schedule
one following walk against the latest inputs. A periodic tick wakes the full-scan producer. Explicit bounded shard lists can
submit directly when the cache is ready.

Each slice uses the QueryView preparation priority to upsert the selected
shard IDs. The heap has one pending entry per (replica ID, vchannel). Scores
can rise or fall. Equal scores retain the original submission order. Each
scoring call takes a revision so slower older reads cannot replace a newer
queued score.

Each serial worker pass takes at most `queryView.balancer.shardBatchSize` shards
(default 100). The worker reads current inputs, skips ineligible candidates,
computes placements against the batch's predicted loads, and submits prepares
in dequeue order. A layout change can discover more work, which enters the
same queue. The worker releases its execution lock between batches. New work
can enter the queue while placement or AddPreparing is running. The next batch
reads current inputs. While node target accounting is in progress it refreshes
the selected shards before pinning the batch and defers optional optimization.
After a shard is dequeued, overlapping notifications can cause another policy
evaluation. A valid Preparing or Ready for the current schema suppresses a new
prepare; an invalid Preparing placement or obsolete schema permits replacement.

Explicit collection release withdraws load intent and directly requests teardown
from that collection's resident managers. Its callback waits for final registry
removal using collection-specific notifications, request cancellation and runtime
shutdown. Planning and apply errors for other shards remain local to their pass.
The scheduler validates current load intent before admitting a Preparing view.

Input and topology changes submit fresh work. The periodic scan revisits
shards after transient metadata or apply failures.

Layout cleanup is scoped to changed/visited collections. Configuration removal
discards that collection's layout even after its cache entry disappears, without
enumerating all retained layouts at each batch boundary.

## Complexity and limits

Starting a scan round is O(1) in the number of collections. Directory traversal
is bounded by the physical-slot budget per call. Registration is amortized O(1),
and tombstoning is O(1); append growth can still allocate/copy a pointer array.

A discovery slice visits a bounded part of a large collection. Enqueue and
priority updates take O(log N) per shard for N queued shards. Placement and apply
are bounded by the worker batch size. Time within a batch also depends on
metadata reads, layout updates, and exact-version DataView reference acquisition.

## Configuration boundaries

`queryView.logicalSize` belongs to DataView: `source` (`rowCount` or
`insertBytes`) and `bytesPerUnit` (default `64KB`, parsed by `GetAsSize`) are
fixed at manager initialization. `segmentBaseCost` defaults to 16 and is sampled
on the next normal DataView update. None of these changes directly requests a
balance pass or rewrites existing snapshots. See the [DataView cost model](data_view.md#native-scheduling-statistics).

`queryView.balancer.reconcileInterval` controls periodic scans.
`queryView.balancer.shardBatchSize` is a positive integer applied on the next dequeue. The
`optimization` group contains `enabled`, `minScoreGain` and
`maxCandidateEvaluationsPerShard`; these affect optional placement improvements,
not mandatory recovery and release. `minScoreGain` is the net weighted score
gain after movement and loading costs.

The `scoring` group separates `resourceGroup` load-band weights/tolerances,
`concentration` shard/collection weights and relative tolerance, `fanout` weight
and hysteresis, and `movement` per-unit move/load costs. Its shared
`targetLogicalSizePerNode` determines preferred shard fanout and the collection
concentration floor; it is not a node capacity limit. All size thresholds and
costs consume published LogicalSize, independent of the DataView source.

Balancer configuration remains refreshable as one validated profile pinned per
batch. A changed scoring or optimization profile requests a scan; an
interval-only change refreshes the timer. Changing only the batch size takes effect
on the next dequeue. Malformed
profiles retain the last valid profile. Legacy `queryCoord.autoBalance` remains
independent of these settings.

## Key packages and validation

- `internal/views/coord/balancer/cache/scanner.go`: directory, cursor, compaction.
- `internal/views/coord/balancer/cache/scanner_test.go`: EOF/checkpoints, concurrent
  publication, deletion/reload, compaction, and bounded traversal benchmark.
- `internal/views/coord/balancer/{balancer,discovery,shard_queue,planning_context}.go`: priority batch
  selection and resumable scope expansion.
- `internal/views/coord/balancer/scan_test.go`: collection boundaries, residual
  shards, event interleaving, layout removal and discovery during node loss.
- `internal/views/coord/balancer/shard_queue_test.go` exercises reprioritization,
  a 10,000-shard backlog, and periodic scoring during placement.
- Existing coordinator component integration and schema-recovery tests cover
  the retained load-config, registry, persistence, and recovery boundaries.
