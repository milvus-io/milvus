// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lock

import (
	"context"
	"sync"

	pool "github.com/jolestar/go-commons-pool/v2"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

var (
	ctx             = context.Background()
	lockPoolFactory = pool.NewPooledObjectFactorySimple(func(ctx2 context.Context) (interface{}, error) {
		return newRefLock(), nil
	})
	lockerPoolConfig = &pool.ObjectPoolConfig{
		LIFO:                     pool.DefaultLIFO,
		MaxTotal:                 -1,
		MaxIdle:                  64,
		MinIdle:                  pool.DefaultMinIdle,
		MinEvictableIdleTime:     pool.DefaultMinEvictableIdleTime,
		SoftMinEvictableIdleTime: pool.DefaultSoftMinEvictableIdleTime,
		NumTestsPerEvictionRun:   pool.DefaultNumTestsPerEvictionRun,
		EvictionPolicyName:       pool.DefaultEvictionPolicyName,
		EvictionContext:          ctx,
		BlockWhenExhausted:       false,
	}
	refLockPoolPool = pool.NewObjectPool(ctx, lockPoolFactory, lockerPoolConfig)
)

type RefLock struct {
	mutex      sync.RWMutex
	refCounter int
}

func (m *RefLock) ref() {
	m.refCounter++
}

func (m *RefLock) unref() bool {
	if m.refCounter > 0 {
		m.refCounter--
		return true
	}
	return false
}

func newRefLock() *RefLock {
	c := RefLock{
		sync.RWMutex{},
		0,
	}
	return &c
}

type KeyLock[K comparable] struct {
	keyLocksMutex sync.Mutex
	refLocks      map[K]*RefLock
}

func NewKeyLock[K comparable]() *KeyLock[K] {
	keyLock := KeyLock[K]{
		refLocks: make(map[K]*RefLock),
	}
	return &keyLock
}

// Lock acquires a write lock for a given key.
func (k *KeyLock[K]) Lock(key K) {
	_ = k.tryLockInternal(key, func(mutex *sync.RWMutex) bool {
		mutex.Lock()
		return true
	})
}

// LockCtx acquires a write lock for a given key, giving up if ctx is done
// before the lock is acquired. It returns nil once the lock is held (the caller
// must Unlock exactly as with Lock), or ctx.Err() if the context ended first —
// in which case the lock is NOT held and must NOT be unlocked.
//
// Implementation note: the per-key primitive is a sync.RWMutex, shared with
// Lock/RLock/TryLock, and sync.RWMutex has no cancellable acquire. Replacing it
// with a channel- or semaphore-based mutex would have to reimplement RWMutex
// read/write semantics for every existing caller and would slow down the hot
// uncontended path, so instead:
//
//   - uncontended callers take the plain TryLock fast path and never allocate a
//     goroutine or a channel;
//   - a contended caller hands the blocking acquire to a goroutine and races it
//     against ctx.Done(). The handoff channel is unbuffered, so the acquisition
//     is transferred to the caller only if the caller is still waiting; if the
//     caller gave up, the goroutine unlocks immediately.
//
// That keeps the refcount bookkeeping exactly the one of Lock/Unlock: the
// abandoned waiter's goroutine still owns a reference until it releases, so the
// key entry is never deleted while its mutex is held, and it never "steals" the
// lock — it releases as soon as it gets it. The goroutine outlives the
// cancelled call only for as long as the current holder keeps the lock.
func (k *KeyLock[K]) LockCtx(ctx context.Context, key K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Fast path: no contention, no goroutine.
	if k.TryLock(key) {
		return nil
	}

	acquired := make(chan struct{})
	abandoned := make(chan struct{})
	go func() {
		k.Lock(key)
		select {
		case acquired <- struct{}{}:
			// handed over to the caller, which now owns the unlock
		case <-abandoned:
			// the caller gave up; release right away so the lock is not held
			// by nobody and the key entry can be reclaimed
			k.Unlock(key)
		}
	}()

	select {
	case <-acquired:
		return nil
	case <-ctx.Done():
		close(abandoned)
		return ctx.Err()
	}
}

// TryLock attempts to acquire a write lock for a given key without blocking.
func (k *KeyLock[K]) TryLock(key K) bool {
	return k.tryLockInternal(key, func(mutex *sync.RWMutex) bool {
		return mutex.TryLock()
	})
}

// Unlock releases a lock for a given key.
func (k *KeyLock[K]) Unlock(lockedKey K) {
	k.keyLocksMutex.Lock()
	defer k.keyLocksMutex.Unlock()
	keyLock, ok := k.refLocks[lockedKey]
	if !ok {
		mlog.Warn(context.TODO(), "Unlocking non-existing key", mlog.Any("key", lockedKey))
		return
	}
	keyLock.unref()
	if keyLock.refCounter == 0 {
		_ = refLockPoolPool.ReturnObject(ctx, keyLock)
		delete(k.refLocks, lockedKey)
	}
	keyLock.mutex.Unlock()
}

// RLock acquires a read lock for a given key.
func (k *KeyLock[K]) RLock(key K) {
	_ = k.tryLockInternal(key, func(mutex *sync.RWMutex) bool {
		mutex.RLock()
		return true
	})
}

// TryRLock attempts to acquire a read lock for a given key without blocking.
func (k *KeyLock[K]) TryRLock(key K) bool {
	return k.tryLockInternal(key, func(mutex *sync.RWMutex) bool {
		return mutex.TryRLock()
	})
}

// tryLockInternal is the internal function to try lock the key.
func (k *KeyLock[K]) tryLockInternal(key K, tryLocker func(mutex *sync.RWMutex) bool) bool {
	k.keyLocksMutex.Lock()
	// update the key map
	if keyLock, ok := k.refLocks[key]; ok {
		keyLock.ref()

		k.keyLocksMutex.Unlock()
		locked := tryLocker(&keyLock.mutex)
		if !locked {
			k.keyLocksMutex.Lock()
			keyLock.unref()
			if keyLock.refCounter == 0 {
				_ = refLockPoolPool.ReturnObject(ctx, keyLock)
				delete(k.refLocks, key)
			}
			k.keyLocksMutex.Unlock()
		}
		return locked
	} else {
		obj, err := refLockPoolPool.BorrowObject(ctx)
		if err != nil {
			mlog.Error(ctx, "BorrowObject failed", mlog.Err(err))
			k.keyLocksMutex.Unlock()
			return false
		}
		newKLock := obj.(*RefLock)
		locked := tryLocker(&newKLock.mutex)
		if !locked {
			_ = refLockPoolPool.ReturnObject(ctx, newKLock)
			k.keyLocksMutex.Unlock()
			return false
		}
		k.refLocks[key] = newKLock
		newKLock.ref()

		k.keyLocksMutex.Unlock()
		return true
	}
}

func (k *KeyLock[K]) RUnlock(lockedKey K) {
	k.keyLocksMutex.Lock()
	defer k.keyLocksMutex.Unlock()
	keyLock, ok := k.refLocks[lockedKey]
	if !ok {
		mlog.Warn(context.TODO(), "Unlocking non-existing key", mlog.Any("key", lockedKey))
		return
	}
	keyLock.unref()
	if keyLock.refCounter == 0 {
		_ = refLockPoolPool.ReturnObject(ctx, keyLock)
		delete(k.refLocks, lockedKey)
	}
	keyLock.mutex.RUnlock()
}

func (k *KeyLock[K]) size() int {
	k.keyLocksMutex.Lock()
	defer k.keyLocksMutex.Unlock()
	return len(k.refLocks)
}
