package inspector

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// newSyncNotifier creates a new sync notifier.
func newSyncNotifier() *syncNotifier {
	return &syncNotifier{
		cond:   syncutil.NewContextCond(&sync.Mutex{}),
		signal: typeutil.NewSet[types.PChannelInfo](),
	}
}

// syncNotifier is a notifier for sync signal.
type syncNotifier struct {
	cond   *syncutil.ContextCond
	signal typeutil.Set[types.PChannelInfo]
}

// AddAndNotify adds a signal and notifies the waiter.
func (n *syncNotifier) AddAndNotify(pChannelInfo types.PChannelInfo) {
	n.cond.LockAndBroadcast()
	n.signal.Insert(pChannelInfo)
	n.cond.L.Unlock()
}

// WaitChan returns the wait channel.
func (n *syncNotifier) WaitChan() <-chan struct{} {
	n.cond.L.Lock()
	if n.signal.Len() > 0 {
		n.cond.L.Unlock()
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return n.cond.WaitChan()
}

// Get gets the signal.
func (n *syncNotifier) Get() typeutil.Set[types.PChannelInfo] {
	n.cond.L.Lock()
	signal := n.signal
	n.signal = typeutil.NewSet[types.PChannelInfo]()
	n.cond.L.Unlock()
	return signal
}
