package inspector

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// newSyncNotifier creates a new sync notifier.
func newSyncNotifier() *syncNotifier {
	return &syncNotifier{
		cond:   syncutil.NewContextCond(&sync.Mutex{}),
		signal: map[types.PChannelInfo]bool{},
	}
}

// syncNotifier is a notifier for sync signal.
type syncNotifier struct {
	cond   *syncutil.ContextCond
	signal map[types.PChannelInfo]bool
}

// AddAndNotify adds a signal and notifies the waiter.
func (n *syncNotifier) AddAndNotify(pChannelInfo types.PChannelInfo, forcePersisted bool) {
	n.cond.LockAndBroadcast()
	defer n.cond.L.Unlock()
	if _, ok := n.signal[pChannelInfo]; !ok {
		n.signal[pChannelInfo] = forcePersisted
		return
	}
	if forcePersisted {
		// A force-persisted signal can upgrade a pending regular sync.
		n.signal[pChannelInfo] = forcePersisted
	}
}

// WaitChan returns the wait channel.
func (n *syncNotifier) WaitChan() <-chan struct{} {
	n.cond.L.Lock()
	if len(n.signal) > 0 {
		n.cond.L.Unlock()
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return n.cond.WaitChan()
}

// Get gets the signal.
func (n *syncNotifier) Get() map[types.PChannelInfo]bool {
	n.cond.L.Lock()
	signal := n.signal
	n.signal = map[types.PChannelInfo]bool{}
	n.cond.L.Unlock()
	return signal
}
