// Package partial_update implements optimistic concurrency control (OCC/CAS)
// for pk-state CAS upsert at the streaming-node WAL append layer.
package partial_update

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
)

// numShards is the number of shards for the PK lock map.
const numShards = 256

// pkKey scopes a primary key to its collection. Identical PK values in
// different collections must NOT share a version-cache entry, otherwise a
// CAS Check for one collection would observe (and conflict with) another
// collection's version. The cache is keyed by this composite, never by the
// bare PK.
type pkKey struct {
	collectionID int64
	pk           interface{}
}

// ErrCASConflict indicates the expected version of a PK does not match the
// observed last-appended version, so the pk-state must be retried.
var ErrCASConflict = errors.New("pk-state CAS conflict")

// ErrUnavailable indicates the version cache is not yet ready to serve
// (e.g. still recovering from history after a leader switch).
var ErrUnavailable = errors.New("pk-state PK version cache unavailable")

// pkEntry is a single cached PK version together with the last time it was
// touched by a CAS Check. lastCheck drives the timeout-based LRU eviction.
type pkEntry struct {
	ts        uint64
	lastCheck time.Time
}

// PKVersionCache keeps a hot in-memory cache of the last successfully
// appended Timestamp for each primary key, sharded by hash(pk) mod numShards
// to reduce lock contention. Entries that have not been touched by a CAS
// Check within timeout are evicted by a background sweeper (LRU by last
// Check time).
type PKVersionCache struct {
	shards  [numShards]pkShard
	timeout time.Duration
	stop    chan struct{}
	stopped sync.Once
}

type pkShard struct {
	mu    sync.Mutex
	cache map[pkKey]*pkEntry
}

// NewPKVersionCache creates a new PKVersionCache. timeout is the LRU idle
// window: PK entries not touched by a CAS Check within timeout are evicted by
// a background sweeper. A non-positive timeout disables eviction (and the
// sweeper goroutine), which is intended for tests.
func NewPKVersionCache(timeout time.Duration) *PKVersionCache {
	c := &PKVersionCache{timeout: timeout, stop: make(chan struct{})}
	for i := 0; i < numShards; i++ {
		c.shards[i].cache = make(map[pkKey]*pkEntry)
	}
	if timeout > 0 {
		go c.sweepLoop()
	}
	return c
}

// Close stops the background sweeper. Safe to call multiple times.
func (c *PKVersionCache) Close() {
	c.stopped.Do(func() { close(c.stop) })
}

// sweepLoop periodically evicts entries idle for longer than timeout.
func (c *PKVersionCache) sweepLoop() {
	interval := c.timeout / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.sweep(time.Now())
		}
	}
}

// sweep evicts entries whose lastCheck is older than now-timeout.
func (c *PKVersionCache) sweep(now time.Time) {
	deadline := now.Add(-c.timeout)
	for i := 0; i < numShards; i++ {
		s := &c.shards[i]
		s.mu.Lock()
		for pk, e := range s.cache {
			if e.lastCheck.Before(deadline) {
				delete(s.cache, pk)
			}
		}
		s.mu.Unlock()
	}
}

// shardOf returns the shard index for a composite (collectionID, pk) key.
func shardOf(key pkKey) int {
	var cb [8]byte
	binary.LittleEndian.PutUint64(cb[:], uint64(key.collectionID))
	switch v := key.pk.(type) {
	case int64:
		// Mix bits of pk, then fold in the collection id.
		u := uint64(v)
		u ^= u >> 33
		u *= 0xff51afd7ed558ccd
		u ^= u >> 33
		u ^= uint64(key.collectionID) * 0x9e3779b97f4a7c15
		return int(u % numShards)
	case string:
		h := fnv.New64a()
		_, _ = h.Write(cb[:])
		_, _ = h.Write([]byte(v))
		return int(h.Sum64() % numShards)
	default:
		return int(uint64(key.collectionID) % numShards)
	}
}

// Lock acquires per-PK locks for the given composite keys. Keys are
// deduplicated and sorted (by shard index) before acquiring locks to avoid
// deadlocks across concurrent callers. The returned function releases the
// locks.
func (c *PKVersionCache) Lock(keys []pkKey) func() {
	if len(keys) == 0 {
		return func() {}
	}
	// Compute shard set (dedup).
	seen := make(map[int]struct{}, len(keys))
	shardIdx := make([]int, 0, len(keys))
	for _, key := range keys {
		s := shardOf(key)
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		shardIdx = append(shardIdx, s)
	}
	sort.Ints(shardIdx)
	for _, s := range shardIdx {
		c.shards[s].mu.Lock()
	}
	return func() {
		// Reverse-order unlock for symmetry.
		for i := len(shardIdx) - 1; i >= 0; i-- {
			c.shards[shardIdx[i]].mu.Unlock()
		}
	}
}

// Check verifies the expected version for a key is still the latest one
// observed by the cache. OCC semantics here are monotonic: the caller asserts
// "no one has written this key after the version I read"; the cache only
// rejects when it has already seen a strictly newer version. Caller MUST hold
// the shard lock for key (acquired via Lock).
//
//   - expectedExists=false: always accepted. A non-existent-row assertion
//     that still finds a cache entry can only mean the row was deleted after
//     the version was cached (a stale entry), since cache entries are created
//     solely by successful OCC writes and the caller's read ran at Strong
//     consistency. Treat it as a legitimate re-insert and let the subsequent
//     Update advance the version, restoring full protection for the new row.
//     This gives up concurrent duplicate-insert detection (not a lost update),
//     but keeps the concurrent-update (expectedExists=true) protection intact.
//   - expectedExists=true:
//   - cache miss: accepted (cold-start / post-recovery historical match).
//   - cached > expected: ErrCASConflict (a later write has happened).
//   - cached <= expected: accepted; cache is lazily advanced to expectedTs
//     so subsequent checks see the latest known version even when a
//     non-OCC path (regular insert / regular upsert) advanced the segment
//     hidden timestamp without going through this interceptor.
func (c *PKVersionCache) Check(key pkKey, expectedTs uint64, expectedExists bool) error {
	s := &c.shards[shardOf(key)]
	cur, ok := s.cache[key]
	if ok {
		// Refresh the idle timer: a touched PK is kept hot (LRU by last Check).
		cur.lastCheck = time.Now()
	}
	if !expectedExists {
		// First-write / re-insert after delete: always accept. The caller
		// observed the row as non-existent; any lingering cache entry is a
		// stale post-delete remnant and must not block the re-insert.
		return nil
	}
	if !ok {
		// Cold cache, accept and rely on storage-side recovery.
		// TODO(partial_update recovery): this assumed-historical match is unsafe
		// right after a leader switch when the cache is cold but the PK actually
		// has a newer version in WAL/storage history. Phase 2 recovery must warm
		// the cache before serving CAS, so a genuine miss here means "truly first
		// write" rather than "not yet recovered". See builder.go ready-gating.
		return nil
	}
	if cur.ts > expectedTs {
		return errors.Wrapf(ErrCASConflict, "pk version mismatch: cached=%d > expected=%d", cur.ts, expectedTs)
	}
	if cur.ts < expectedTs {
		cur.ts = expectedTs
	}
	return nil
}

// Update advances the cached version for key to max(cache[key], ts).
// Caller MUST hold the shard lock for key.
func (c *PKVersionCache) Update(key pkKey, ts uint64) {
	s := &c.shards[shardOf(key)]
	if cur, ok := s.cache[key]; ok {
		if cur.ts < ts {
			cur.ts = ts
		}
		cur.lastCheck = time.Now()
		return
	}
	s.cache[key] = &pkEntry{ts: ts, lastCheck: time.Now()}
}
