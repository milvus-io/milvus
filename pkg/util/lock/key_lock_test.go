package lock

import (
	"sync"
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
