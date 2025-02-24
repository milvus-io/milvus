package syncmgr

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
)

type Task interface {
	SegmentID() int64
	Checkpoint() *msgpb.MsgPosition
	StartPosition() *msgpb.MsgPosition
	ChannelName() string
	Run(context.Context) error
	HandleError(error)
	IsFlush() bool
}

type keyLockDispatcher[K comparable] struct {
	keyLock    *lock.KeyLock[K]
	workerPool *conc.Pool[struct{}]
}

func newKeyLockDispatcher[K comparable](maxParallel int) *keyLockDispatcher[K] {
	dispatcher := &keyLockDispatcher[K]{
		workerPool: conc.NewPool[struct{}](maxParallel, conc.WithPreAlloc(false)),
		keyLock:    lock.NewKeyLock[K](),
	}
	return dispatcher
}

func (d *keyLockDispatcher[K]) Submit(ctx context.Context, key K, t Task, callbacks ...func(error) error) *conc.Future[struct{}] {
	d.keyLock.Lock(key)

	return d.workerPool.Submit(func() (struct{}, error) {
		defer d.keyLock.Unlock(key)
		err := t.Run(ctx)

		for _, callback := range callbacks {
			err = callback(err)
		}

		return struct{}{}, err
	})
}
