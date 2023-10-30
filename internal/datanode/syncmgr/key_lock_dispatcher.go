package syncmgr

import (
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lock"
)

type Task interface {
	Run() error
}

type keyLockDispatcher[K comparable] struct {
	keyLock    *lock.KeyLock[K]
	workerPool *conc.Pool[struct{}]
}

func newKeyLockDispatcher[K comparable](maxParallel int) *keyLockDispatcher[K] {
	return &keyLockDispatcher[K]{
		workerPool: conc.NewPool[struct{}](maxParallel, conc.WithPreAlloc(true)),
		keyLock:    lock.NewKeyLock[K](),
	}
}

func (d *keyLockDispatcher[K]) Submit(key K, t Task) *conc.Future[struct{}] {
	d.keyLock.Lock(key)
	defer d.keyLock.Unlock(key)

	return d.workerPool.Submit(func() (struct{}, error) {
		err := t.Run()
		return struct{}{}, err
	})
}
