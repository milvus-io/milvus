package lock

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKeyLock(t *testing.T) {
	keys := []string{"Milvus", "Blazing", "Fast"}

	keyLock := NewKeyLock[string]()

	keyLock.Lock(keys[0])
	keyLock.Lock(keys[1])
	keyLock.Lock(keys[2])

	// should work
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		keyLock.Lock(keys[0])
		keyLock.Unlock(keys[0])
		wg.Done()
	}()

	go func() {
		keyLock.Lock(keys[0])
		keyLock.Unlock(keys[0])
		wg.Done()
	}()

	assert.Equal(t, keyLock.size(), 3)

	time.Sleep(10 * time.Millisecond)
	keyLock.Unlock(keys[0])
	keyLock.Unlock(keys[1])
	keyLock.Unlock(keys[2])
	wg.Wait()

	assert.Equal(t, keyLock.size(), 0)
}

func TestKeyRLock(t *testing.T) {
	keys := []string{"Milvus", "Blazing", "Fast"}

	keyLock := NewKeyLock[string]()

	keyLock.RLock(keys[0])
	keyLock.RLock(keys[0])

	// should work
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		keyLock.Lock(keys[0])
		keyLock.Unlock(keys[0])
		wg.Done()
	}()

	time.Sleep(10 * time.Millisecond)
	keyLock.RUnlock(keys[0])
	keyLock.RUnlock(keys[0])

	wg.Wait()
	assert.Equal(t, keyLock.size(), 0)
}

func TestNewKeyLock(t *testing.T) {
	keyLock := NewKeyLock[string]()
	keyLock.Lock("a")
	keyLock.Lock("b")

	keyLock.Unlock("a")
	keyLock.Unlock("b")

	assert.Equal(t, 0, keyLock.size())
	keyLock.keyLocksMutex.Lock()
	keyLen := len(keyLock.refLocks)
	keyLock.keyLocksMutex.Unlock()
	assert.Equal(t, 0, keyLen)
}

func TestKeyLockTryLock(t *testing.T) {
	keyLock := NewKeyLock[string]()
	ok := keyLock.TryLock("a")
	assert.True(t, ok)
	ok = keyLock.TryLock("b")
	assert.True(t, ok)

	ok = keyLock.TryLock("a")
	assert.False(t, ok)
	ok = keyLock.TryLock("b")
	assert.False(t, ok)

	ok = keyLock.TryRLock("a")
	assert.False(t, ok)
	ok = keyLock.TryRLock("b")
	assert.False(t, ok)

	assert.Equal(t, 2, keyLock.size())
	keyLock.Unlock("a")
	keyLock.Unlock("b")
	assert.Zero(t, keyLock.size())

	ok = keyLock.TryRLock("a")
	assert.True(t, ok)
	ok = keyLock.TryRLock("b")
	assert.True(t, ok)

	ok = keyLock.TryLock("a")
	assert.False(t, ok)
	ok = keyLock.TryLock("b")
	assert.False(t, ok)

	ok = keyLock.TryRLock("a")
	assert.True(t, ok)
	ok = keyLock.TryRLock("b")
	assert.True(t, ok)

	assert.Equal(t, 2, keyLock.size())
	keyLock.RUnlock("a")
	keyLock.RUnlock("b")
	assert.Equal(t, 2, keyLock.size())

	keyLock.RUnlock("a")
	keyLock.RUnlock("b")
	assert.Equal(t, 0, keyLock.size())
}

func TestKeyLockCtxUncontended(t *testing.T) {
	keyLock := NewKeyLock[string]()

	assert.NoError(t, keyLock.LockCtx(context.Background(), "a"))
	assert.Equal(t, 1, keyLock.size())
	// the lock is really held
	assert.False(t, keyLock.TryLock("a"))

	keyLock.Unlock("a")
	assert.Equal(t, 0, keyLock.size())

	// and the key is reusable afterwards
	assert.True(t, keyLock.TryLock("a"))
	keyLock.Unlock("a")
	assert.Equal(t, 0, keyLock.size())
}

// A waiter whose context expires while the key is held must return ctx.Err()
// and must NOT acquire the lock afterwards.
func TestKeyLockCtxExpiresWhileHeld(t *testing.T) {
	keyLock := NewKeyLock[string]()
	keyLock.Lock("a")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := keyLock.LockCtx(ctx, "a")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)

	// The abandoned waiter must not have taken the lock: the original holder is
	// still the only owner, and releasing it leaves the key free.
	keyLock.Unlock("a")
	assert.Eventually(t, func() bool {
		return keyLock.size() == 0
	}, 5*time.Second, 5*time.Millisecond, "abandoned waiter leaked a key entry")

	assert.True(t, keyLock.TryLock("a"), "abandoned waiter stole the lock")
	keyLock.Unlock("a")
	assert.Equal(t, 0, keyLock.size())
}

// An already-cancelled context never acquires and never registers a key.
func TestKeyLockCtxAlreadyCancelled(t *testing.T) {
	keyLock := NewKeyLock[string]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, keyLock.LockCtx(ctx, "a"), context.Canceled)
	assert.Equal(t, 0, keyLock.size())
	assert.True(t, keyLock.TryLock("a"))
	keyLock.Unlock("a")
	assert.Equal(t, 0, keyLock.size())
}

// Many abandoned waiters on a held key must leave no residual key state once
// the holder releases.
func TestKeyLockCtxAbandonedWaitersNoLeak(t *testing.T) {
	keyLock := NewKeyLock[string]()
	keyLock.Lock("a")

	wg := sync.WaitGroup{}
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			assert.Error(t, keyLock.LockCtx(ctx, "a"))
		}()
	}
	wg.Wait()
	assert.Equal(t, 1, keyLock.size())

	keyLock.Unlock("a")
	assert.Eventually(t, func() bool {
		return keyLock.size() == 0
	}, 5*time.Second, 5*time.Millisecond, "abandoned waiters leaked key entries")
}

// Exactly-one-holder invariant under a mix of successful and cancelled waiters.
func TestKeyLockCtxConcurrentExclusive(t *testing.T) {
	keyLock := NewKeyLock[string]()

	var holders atomic.Int32
	var succeeded atomic.Int32
	wg := sync.WaitGroup{}
	for i := 0; i < 64; i++ {
		wg.Add(1)
		cancelled := i%2 == 0
		go func() {
			defer wg.Done()
			timeout := 5 * time.Second
			if cancelled {
				timeout = time.Duration(1+i%5) * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if err := keyLock.LockCtx(ctx, "a"); err != nil {
				return
			}
			succeeded.Add(1)
			assert.Equal(t, int32(1), holders.Add(1))
			time.Sleep(time.Millisecond)
			holders.Add(-1)
			keyLock.Unlock("a")
		}()
	}
	wg.Wait()

	assert.Greater(t, succeeded.Load(), int32(0))
	assert.Eventually(t, func() bool {
		return keyLock.size() == 0
	}, 5*time.Second, 5*time.Millisecond)
	assert.True(t, keyLock.TryLock("a"))
	keyLock.Unlock("a")
}

// Different keys never block each other, even for the cancellable acquire.
func TestKeyLockCtxIndependentKeys(t *testing.T) {
	keyLock := NewKeyLock[string]()
	keyLock.Lock("a")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, keyLock.LockCtx(ctx, "b"))
	keyLock.Unlock("b")
	keyLock.Unlock("a")
	assert.Equal(t, 0, keyLock.size())
}
