package decider

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/authority"
)

func TestStripeGuardReleaseIsIdempotentAndNilSafe(t *testing.T) {
	locks := newStripeLocks(4)
	guard := locks.acquire([]authority.PK{authority.Int64PK(1), authority.Int64PK(1), authority.Int64PK(2)})
	guard.Release()
	guard.Release()

	var nilGuard *StripeGuard
	nilGuard.Release()

	// released stripes can be acquired again.
	again := locks.acquire([]authority.PK{authority.Int64PK(1)})
	again.Release()
}

func TestStripeGuardEmptyKeys(t *testing.T) {
	locks := newStripeLocks(4)
	guard := locks.acquire(nil)
	require.NotNil(t, guard)
	guard.Release()
}

func TestStripeGuardBlocksSameKey(t *testing.T) {
	locks := newStripeLocks(16)
	first := locks.acquire([]authority.PK{authority.VarCharPK("a")})

	acquired := make(chan struct{})
	go func() {
		second := locks.acquire([]authority.PK{authority.VarCharPK("a")})
		close(acquired)
		second.Release()
	}()

	select {
	case <-acquired:
		t.Fatal("the same key must not be acquired twice")
	case <-time.After(100 * time.Millisecond):
	}
	first.Release()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("the key was not acquired after release")
	}
}

// Opposite key orders must not deadlock: stripes are always locked in ascending order.
func TestStripeGuardNoDeadlockOnOppositeOrder(t *testing.T) {
	locks := newStripeLocks(8)
	keys := make([]authority.PK, 0, 64)
	for i := int64(0); i < 64; i++ {
		keys = append(keys, authority.Int64PK(i))
	}
	reversed := make([]authority.PK, len(keys))
	for i, k := range keys {
		reversed[len(keys)-1-i] = k
	}

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 200; i++ {
					if w%2 == 0 {
						locks.acquire(keys).Release()
					} else {
						locks.acquire(reversed).Release()
					}
				}
			}(w)
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock")
	}
}
