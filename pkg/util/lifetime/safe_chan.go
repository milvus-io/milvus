package lifetime

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// SafeChan is the utility type combining chan struct{} & sync.Once.
// It provides double close protection internally.
type SafeChan interface {
	IsClosed() bool
	CloseCh() <-chan struct{}
	Close()
}

type safeChan struct {
	closed chan struct{}
	once   sync.Once
}

// NewSafeChan returns a SafeChan with internal channel initialized
func NewSafeChan() SafeChan {
	return newSafeChan()
}

func newSafeChan() *safeChan {
	return &safeChan{
		closed: make(chan struct{}),
	}
}

func (sc *safeChan) CloseCh() <-chan struct{} {
	return sc.closed
}

func (sc *safeChan) IsClosed() bool {
	return typeutil.IsChanClosed(sc.closed)
}

func (sc *safeChan) Close() {
	sc.once.Do(func() {
		close(sc.closed)
	})
}
