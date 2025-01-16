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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
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

func (k *KeyLock[K]) Lock(key K) {
	k.keyLocksMutex.Lock()
	// update the key map
	if keyLock, ok := k.refLocks[key]; ok {
		keyLock.ref()

		k.keyLocksMutex.Unlock()
		keyLock.mutex.Lock()
	} else {
		obj, err := refLockPoolPool.BorrowObject(ctx)
		if err != nil {
			log.Ctx(ctx).Error("BorrowObject failed", zap.Error(err))
			k.keyLocksMutex.Unlock()
			return
		}
		newKLock := obj.(*RefLock)
		// newKLock := newRefLock()
		newKLock.mutex.Lock()
		k.refLocks[key] = newKLock
		newKLock.ref()

		k.keyLocksMutex.Unlock()
		return
	}
}

func (k *KeyLock[K]) Unlock(lockedKey K) {
	k.keyLocksMutex.Lock()
	defer k.keyLocksMutex.Unlock()
	keyLock, ok := k.refLocks[lockedKey]
	if !ok {
		log.Warn("Unlocking non-existing key", zap.Any("key", lockedKey))
		return
	}
	keyLock.unref()
	if keyLock.refCounter == 0 {
		_ = refLockPoolPool.ReturnObject(ctx, keyLock)
		delete(k.refLocks, lockedKey)
	}
	keyLock.mutex.Unlock()
}

func (k *KeyLock[K]) RLock(key K) {
	k.keyLocksMutex.Lock()
	// update the key map
	if keyLock, ok := k.refLocks[key]; ok {
		keyLock.ref()

		k.keyLocksMutex.Unlock()
		keyLock.mutex.RLock()
	} else {
		obj, err := refLockPoolPool.BorrowObject(ctx)
		if err != nil {
			log.Ctx(ctx).Error("BorrowObject failed", zap.Error(err))
			k.keyLocksMutex.Unlock()
			return
		}
		newKLock := obj.(*RefLock)
		// newKLock := newRefLock()
		newKLock.mutex.RLock()
		k.refLocks[key] = newKLock
		newKLock.ref()

		k.keyLocksMutex.Unlock()
		return
	}
}

func (k *KeyLock[K]) RUnlock(lockedKey K) {
	k.keyLocksMutex.Lock()
	defer k.keyLocksMutex.Unlock()
	keyLock, ok := k.refLocks[lockedKey]
	if !ok {
		log.Warn("Unlocking non-existing key", zap.Any("key", lockedKey))
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
