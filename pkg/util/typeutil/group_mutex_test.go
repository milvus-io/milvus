package typeutil

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGroupMutex(t *testing.T) {
	groupMutex := NewGroupMutex[int]()
	opCount := 250
	dataSize := 10
	values := make([]string, dataSize)

	wg := sync.WaitGroup{}
	testLock := func(i int) {
		defer wg.Done()
		guard := groupMutex.Lock(i)
		defer guard.Unlock()
		values[i] = "testLock"
	}

	testLockIfExist := func(i int) {
		defer wg.Done()
		guard := groupMutex.LockIfExist(i)
		if guard == nil {
			return
		}
		defer guard.Unlock()
		values[i] = "testLockIfExist"
	}

	wg.Add(opCount * 2)
	for i := 0; i < opCount; i++ {
		j := i % 10
		go testLock(j)
		go testLockIfExist(j)
	}
	wg.Wait()

	for _, s := range values {
		assert.True(t, s == "testLock" || s == "testLockIfExist")
	}

	assert.True(t, groupMutex.Exists(0))
	assert.False(t, groupMutex.Exists(dataSize))
}

func TestRemovableGroupMutex(t *testing.T) {
	groupMutex := NewRemovableGroupMutex[int]()
	opCount := 250
	dataSize := 10
	values := make([]string, dataSize)

	wg := sync.WaitGroup{}
	testLock := func(i int) {
		defer wg.Done()
		guard := groupMutex.Lock(i)
		defer guard.Unlock()
		values[i] = "testLock"
	}

	testLockIfExist := func(i int) {
		defer wg.Done()
		guard := groupMutex.LockIfExist(i)
		if guard == nil {
			return
		}
		defer guard.Unlock()
		values[i] = "testLockIfExist"
	}

	wg.Add(opCount * 2)
	for i := 0; i < opCount; i++ {
		j := i % 10
		go testLock(j)
		go testLockIfExist(j)
	}
	wg.Wait()

	for _, s := range values {
		assert.True(t, s == "testLock" || s == "testLockIfExist")
	}

	assert.True(t, groupMutex.Exists(0))
	assert.False(t, groupMutex.Exists(dataSize))

	groupMutex.LockIfExist(0).UnlockAndRemove()
	assert.False(t, groupMutex.Exists(0))
	assert.Nil(t, groupMutex.LockIfExist(0))

	groupMutex.Lock(dataSize).UnlockAndRemove()
	assert.False(t, groupMutex.Exists(dataSize))
	assert.Nil(t, groupMutex.LockIfExist(dataSize))

	// Do a data race operation to invalid a mutex
	assert.True(t, groupMutex.Exists(1))
	l, loaded := groupMutex.mutexes.Get(1)
	_ = loaded
	l.valid = false
	assert.False(t, groupMutex.Exists(1))
	assert.Nil(t, groupMutex.LockIfExist(1))

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		groupMutex.Lock(1).Unlock()
	}()

	select {
	case <-ch:
		t.Errorf("should not be closed, goroutine should be blocked")
	case <-time.After(time.Millisecond * 20):
		lockGuard := &RemovableGroupMutexGuard[int]{
			locker:  l,
			key:     1,
			mutexes: groupMutex.mutexes,
		}
		lockGuard.locker.mutex.Lock()
		lockGuard.UnlockAndRemove()
	}
	// goroutine should be acquired the lock and return after remove the old lock.
	<-ch
}
