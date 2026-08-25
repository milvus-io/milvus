package lock

import (
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

func TestTryLockMany(t *testing.T) {
	keyLock := NewKeyLock[int]()

	// All keys free: acquire the whole set atomically.
	assert.True(t, keyLock.TryLockMany([]int{1, 2, 3}))
	assert.Equal(t, 3, keyLock.size())

	// Any overlapping key held makes the whole attempt fail and leave nothing new
	// held: size is unchanged and the disjoint keys stay free afterwards.
	assert.False(t, keyLock.TryLockMany([]int{3, 4, 5}))
	assert.Equal(t, 3, keyLock.size())
	assert.True(t, keyLock.TryLock(4))
	assert.True(t, keyLock.TryLock(5))
	keyLock.Unlock(4)
	keyLock.Unlock(5)

	keyLock.UnlockMany([]int{1, 2, 3})
	assert.Zero(t, keyLock.size())

	// A fully disjoint attempt after release succeeds again.
	assert.True(t, keyLock.TryLockMany([]int{7, 8}))
	keyLock.UnlockMany([]int{8, 7})
	assert.Zero(t, keyLock.size())
}

// TestTryLockManyContendedSingleKey pins the rollback path: when a single-key
// holder owns exactly one member of the requested set, the batch attempt fails
// and releases every other member it briefly touched, so a plain Lock on those
// members proceeds without blocking.
func TestTryLockManyContendedSingleKey(t *testing.T) {
	keyLock := NewKeyLock[int]()
	keyLock.Lock(2)

	assert.False(t, keyLock.TryLockMany([]int{1, 2, 3}))
	// 1 and 3 must be free (rolled back), only 2 remains held.
	assert.True(t, keyLock.TryLock(1))
	assert.True(t, keyLock.TryLock(3))
	keyLock.Unlock(1)
	keyLock.Unlock(3)
	keyLock.Unlock(2)
	assert.Zero(t, keyLock.size())
}

// TestTryLockManyAtomicity asserts the mutual-exclusion guarantee under
// contention: two workers repeatedly grabbing overlapping sets must never both
// hold the set at once. A non-atomic ordered acquire could interleave partial
// holds; TryLockMany cannot.
func TestTryLockManyAtomicity(t *testing.T) {
	keyLock := NewKeyLock[int]()
	setA := []int{1, 2, 3}
	setB := []int{3, 4, 5} // overlaps A on key 3

	var inside int32
	worker := func(keys []int, wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			if !keyLock.TryLockMany(keys) {
				continue
			}
			// Only one holder of an overlapping set may be here at a time.
			if atomic.AddInt32(&inside, 1) != 1 {
				atomic.AddInt32(&inside, -1)
				keyLock.UnlockMany(keys)
				t.Errorf("two batches held overlapping sets simultaneously")
				return
			}
			atomic.AddInt32(&inside, -1)
			keyLock.UnlockMany(keys)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	go worker(setA, &wg)
	go worker(setB, &wg)
	wg.Wait()
	assert.Zero(t, keyLock.size())
}
