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

// TryLockMany atomically acquires write locks for every key in keys, or acquires
// none. It is the multi-key form of TryLock: the whole attempt runs under the
// internal map mutex using the non-blocking mutex.TryLock on each key, so
// competing lockers never observe a partially-held set and this call never blocks
// while holding a subset. That is what lets a caller take an arbitrary set of keys
// without the hold-and-wait convoy (or deadlock) that ordered blocking Lock()
// calls create: on the first key already held elsewhere it rolls back every key
// it just took and returns false, leaving nothing held for the caller to retry.
//
// Holding keyLocksMutex across the attempt is safe precisely because TryLock never
// blocks (unlike Lock, which releases keyLocksMutex before parking). keys must be
// de-duplicated; a repeated key makes the second TryLock on the same mutex fail,
// after which the call can never succeed.
//
// A separate "check every key first, then lock them all" pass is not cheaper. A
// sync.RWMutex exposes no non-acquiring "is-lockable" test — TryLock is itself the
// check and commits on success — and refCounter cannot stand in for one (readers,
// blocked writers, and lockers that ref before their TryLock all inflate it, so a
// count-based pre-check would reject lockable keys). Because the whole scan already
// runs under keyLocksMutex, this per-key TryLock-then-rollback is exactly that atomic
// check-and-commit with no TOCTOU window: on the conflict-free common path it does
// len(keys) TryLocks and never rolls back, and the rollback cost is paid only when a
// key is genuinely held elsewhere.
func (k *KeyLock[K]) TryLockMany(keys []K) bool {
	k.keyLocksMutex.Lock()
	defer k.keyLocksMutex.Unlock()

	// unlockLocked releases a key this attempt already acquired, mirroring Unlock's
	// ref-count/pool bookkeeping but assuming keyLocksMutex is already held.
	unlockLocked := func(key K) {
		keyLock := k.refLocks[key]
		keyLock.unref()
		if keyLock.refCounter == 0 {
			_ = refLockPoolPool.ReturnObject(ctx, keyLock)
			delete(k.refLocks, key)
		}
		keyLock.mutex.Unlock()
	}
	rollback := func(upto int) {
		for j := upto - 1; j >= 0; j-- {
			unlockLocked(keys[j])
		}
	}

	for i, key := range keys {
		if keyLock, ok := k.refLocks[key]; ok {
			keyLock.ref()
			if keyLock.mutex.TryLock() {
				continue
			}
			// Undo the ref taken for this contended key, then release the prefix.
			keyLock.unref()
			if keyLock.refCounter == 0 {
				_ = refLockPoolPool.ReturnObject(ctx, keyLock)
				delete(k.refLocks, key)
			}
			rollback(i)
			return false
		}
		obj, err := refLockPoolPool.BorrowObject(ctx)
		if err != nil {
			mlog.Error(ctx, "BorrowObject failed", mlog.Err(err))
			rollback(i)
			return false
		}
		newKLock := obj.(*RefLock)
		if !newKLock.mutex.TryLock() {
			_ = refLockPoolPool.ReturnObject(ctx, newKLock)
			rollback(i)
			return false
		}
		k.refLocks[key] = newKLock
		newKLock.ref()
	}
	return true
}

// UnlockMany releases write locks previously acquired together via TryLockMany.
// The same slice need not be passed; only that every key is currently held by the
// caller. Releasing under a single map-mutex acquisition keeps batch release
// symmetric with the batch acquire.
func (k *KeyLock[K]) UnlockMany(keys []K) {
	k.keyLocksMutex.Lock()
	defer k.keyLocksMutex.Unlock()
	for _, key := range keys {
		keyLock, ok := k.refLocks[key]
		if !ok {
			mlog.Warn(context.TODO(), "Unlocking non-existing key", mlog.Any("key", key))
			continue
		}
		keyLock.unref()
		if keyLock.refCounter == 0 {
			_ = refLockPoolPool.ReturnObject(ctx, keyLock)
			delete(k.refLocks, key)
		}
		keyLock.mutex.Unlock()
	}
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
