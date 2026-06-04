package partial_update

import (
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestPKVersionCache_FirstWrite(t *testing.T) {
	c := NewPKVersionCache(0)
	k := pkKey{collectionID: 1, pk: int64(1)}
	unlock := c.Lock([]pkKey{k})
	defer unlock()

	// expectedExists=false on a cold cache must succeed (first-write CAS).
	assert.NoError(t, c.Check(k, 0, false))
	c.Update(k, 100)

	// A second first-write attempt (re-insert after delete) must also be
	// accepted: a non-existent-row assertion that still finds a cache entry
	// can only be a stale post-delete remnant, never a real conflict.
	assert.NoError(t, c.Check(k, 0, false))
}

func TestPKVersionCache_UpdateMatch(t *testing.T) {
	c := NewPKVersionCache(0)
	k := pkKey{collectionID: 1, pk: int64(2)}
	unlock := c.Lock([]pkKey{k})
	defer unlock()

	c.Update(k, 200)
	// Matching version on update path passes.
	assert.NoError(t, c.Check(k, 200, true))

	// Mismatching version must fail with ErrCASConflict.
	err := c.Check(k, 199, true)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrCASConflict))
}

func TestPKVersionCache_ColdCacheUpdateAccepted(t *testing.T) {
	c := NewPKVersionCache(0)
	k := pkKey{collectionID: 1, pk: int64(3)}
	unlock := c.Lock([]pkKey{k})
	defer unlock()

	// expectedExists=true with empty cache: accepted (recovery hot-path).
	assert.NoError(t, c.Check(k, 999, true))
}

// TestPKVersionCache_CachedBehindExpectedAccepted verifies the monotonic OCC
// semantics: when the cache is lagging the version observed by the caller
// (e.g. a non-OCC path such as regular insert/upsert advanced the segment
// hidden timestamp without going through this interceptor), Check must accept
// and lazily advance the cache. Only cached > expected is a real conflict.
func TestPKVersionCache_CachedBehindExpectedAccepted(t *testing.T) {
	c := NewPKVersionCache(0)
	k := pkKey{collectionID: 1, pk: int64(5)}
	unlock := c.Lock([]pkKey{k})
	defer unlock()

	c.Update(k, 100)
	// Caller observed a newer version (200) than the cache (100): the cache
	// is behind because a non-OCC path advanced the segment hidden ts. The
	// CAS must accept and advance the cache to expected.
	assert.NoError(t, c.Check(k, 200, true))
	// A subsequent matching check at the new version must also pass.
	assert.NoError(t, c.Check(k, 200, true))
	// And going back to the stale version must now be a conflict.
	err := c.Check(k, 100, true)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrCASConflict))
}

func TestPKVersionCache_UpdateMonotonic(t *testing.T) {
	c := NewPKVersionCache(0)
	k := pkKey{collectionID: 1, pk: int64(4)}
	unlock := c.Lock([]pkKey{k})
	defer unlock()

	c.Update(k, 100)
	c.Update(k, 50) // older ts must not regress the cache.
	assert.NoError(t, c.Check(k, 100, true))
}

func TestPKVersionCache_StringPK(t *testing.T) {
	c := NewPKVersionCache(0)
	k := pkKey{collectionID: 1, pk: "abc"}
	unlock := c.Lock([]pkKey{k})
	defer unlock()

	assert.NoError(t, c.Check(k, 0, false))
	c.Update(k, 42)
	assert.NoError(t, c.Check(k, 42, true))
	assert.Error(t, c.Check(k, 41, true))
}

// TestPKVersionCache_CollectionIsolation verifies that identical PK values in
// different collections keep independent versions: a CAS Check for one
// collection must not observe (and conflict with) another collection's
// version.
func TestPKVersionCache_CollectionIsolation(t *testing.T) {
	c := NewPKVersionCache(0)
	kA := pkKey{collectionID: 100, pk: int64(7)}
	kB := pkKey{collectionID: 200, pk: int64(7)}
	unlock := c.Lock([]pkKey{kA, kB})
	defer unlock()

	c.Update(kA, 500)
	// Collection B has never seen pk 7: a first-write CAS must succeed even
	// though collection A already holds pk 7.
	assert.NoError(t, c.Check(kB, 0, false))
	// Collection A still observes its own version.
	assert.NoError(t, c.Check(kA, 500, true))
}

func TestPKVersionCache_ConcurrentSafe(t *testing.T) {
	// Verify that the sharded lock scheme actually serializes per-PK CAS
	// even under contention.
	c := NewPKVersionCache(0)
	const goroutines = 32
	const iterations = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				k7 := pkKey{collectionID: 1, pk: int64(7)}
				k8 := pkKey{collectionID: 1, pk: int64(8)}
				unlock := c.Lock([]pkKey{k7, k8})
				c.Update(k7, uint64(i))
				c.Update(k8, uint64(i))
				unlock()
			}
		}()
	}
	wg.Wait()
}
