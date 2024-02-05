package syncmgr

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lock"
)

type Task interface {
	SegmentID() int64
	CalcTargetSegment() (int64, error)
	Checkpoint() *msgpb.MsgPosition
	StartPosition() *msgpb.MsgPosition
	ChannelName() string
	Run() error
}

type keyLockDispatcher[K comparable] struct {
	keyLock    *lock.KeyLock[K]
	workerPool *conc.Pool[error]
}

func newKeyLockDispatcher[K comparable](maxParallel int) *keyLockDispatcher[K] {
	dispatcher := &keyLockDispatcher[K]{
		workerPool: conc.NewPool[error](maxParallel, conc.WithPreAlloc(false)),
		keyLock:    lock.NewKeyLock[K](),
	}
	return dispatcher
}

func (d *keyLockDispatcher[K]) Submit(key K, t Task, callbacks ...func(error)) *conc.Future[error] {
	d.keyLock.Lock(key)

	return d.workerPool.Submit(func() (error, error) {
		defer d.keyLock.Unlock(key)
		err := t.Run()

		for _, callback := range callbacks {
			callback(err)
		}

		return err, nil
	})
}
