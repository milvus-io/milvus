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

Startup, the periodic timer, and unscoped triggers request a scan. The controller
retains one scanner and coalesces requests received during a round into at most
one following round. A new request never resets an active cursor.

Each `Reconcile` invocation selects one unit of work:

1. Fresh scoped events, coalesced by collection, shard, or node.
2. Failed scoped work whose retry deadline has elapsed.
3. The next background scan batch.

A scan batch adds a complete collection's desired shards (replicas × DataView
vchannels) and resident shards to a deduplicated set. Once the unique ShardID
count **exceeds 100**, it stops at that collection checkpoint. Collections are
not split. Independently, each call examines at most **256 directory slots**, so
empty collections and tombstones cannot cause an unbounded scan-only pass.
The final batch is planned even when `Scan` returns EOF.

Each batch gets a fresh planning context, runs layout selection and policy
planning, applies its plan, and performs published-schema recovery. The existing
Must-first and size ordering, candidate predictions, and score budget apply
within the batch. There is no global ordering across the whole scan round.
Accepted Preparing publications update the cache synchronously; the next batch
samples that updated target load rather than reusing the previous baseline.

`reconcileMu` covers only the current pass. Scoped events are checked before the
next scan batch, and management APIs can acquire the same mutex between passes.
There is no cancellation or rollback of an in-flight batch and no strict FIFO
guarantee for management callers. Sustained scoped traffic may defer background
scanning. Scoped events themselves are not split by this scan budget.

Failed schema reads and apply operations keep their affected keys in a separate
retry set. A timer implements exponential backoff from 100 ms, capped at 5 s;
waiting does not block fresh events or scan continuation. Successful work resets
the delay when no failed keys remain. Scan continuation wakes the loop only while
a round or another request is pending. EOF alone never starts an endless loop.

Layout cleanup is scoped to changed/visited collections. Configuration removal
discards that collection's layout even after its cache entry disappears, without
enumerating all retained layouts at each batch boundary.

## Complexity and limits

Starting a scan round is O(1) in the number of collections. Directory traversal
is bounded by the physical-slot budget per call. Registration is amortized O(1),
and tombstoning is O(1); append growth can still allocate/copy a pointer array.

An entire Reconcile is **not** O(1): it captures node baselines and processes the
included collections' replicas, vchannels, segments, schema reads and view
application. One collection can exceed the shard threshold. Metadata callbacks,
DataView reference acquisition, or a large collection can therefore delay the
next interleaving point. No fixed millisecond latency is promised by this design.
Published schema retention, load-partition filtering, and view lifecycle rules
remain unchanged by batching.

## Configuration boundaries

`queryView.logicalSize` belongs to DataView: `source` (`rowCount` or
`insertBytes`) and `bytesPerUnit` (default `64KB`, parsed by `GetAsSize`) are
fixed at manager initialization. `segmentBaseCost` defaults to 16 and is sampled
on the next normal DataView update. None of these changes directly requests a
balance pass or rewrites existing snapshots. See the [DataView cost model](data_view.md#native-scheduling-statistics).

`queryView.balancer.reconcileInterval` controls periodic scans. The
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
interval-only change refreshes the timer without requesting a scan. Malformed
profiles retain the last valid profile. Legacy `queryCoord.autoBalance` remains
independent of these settings.

## Key packages and validation

- `internal/views/coord/balancer/cache/scanner.go`: directory, cursor, compaction.
- `internal/views/coord/balancer/cache/scanner_test.go`: EOF/checkpoints, concurrent
  publication, deletion/reload, compaction, and bounded traversal benchmark.
- `internal/views/coord/balancer/{balancer,trigger,planning_context}.go`: batch
  selection, retry deadlines, and local scope expansion.
- `internal/views/coord/balancer/scan_test.go`: collection boundaries, residual
  shards, event interleaving, layout removal, and retry responsiveness.
- Existing coordinator component integration and schema-recovery tests cover
  the retained load-config, registry, persistence, and recovery boundaries.
