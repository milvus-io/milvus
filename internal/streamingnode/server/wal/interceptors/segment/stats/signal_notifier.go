package stats

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// NewSealSignalNotifier creates a new seal signal notifier.
func NewSealSignalNotifier() *SealSignalNotifier {
	return &SealSignalNotifier{
		cond:   syncutil.NewContextCond(&sync.Mutex{}),
		signal: typeutil.NewSet[SegmentBelongs](),
	}
}

// SealSignalNotifier is a notifier for seal signal.
type SealSignalNotifier struct {
	cond   *syncutil.ContextCond
	signal typeutil.Set[SegmentBelongs]
}

// AddAndNotify adds a signal and notifies the waiter.
func (n *SealSignalNotifier) AddAndNotify(belongs SegmentBelongs) {
	n.cond.LockAndBroadcast()
	n.signal.Insert(belongs)
	n.cond.L.Unlock()
}

func (n *SealSignalNotifier) WaitChan() <-chan struct{} {
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
func (n *SealSignalNotifier) Get() typeutil.Set[SegmentBelongs] {
	n.cond.L.Lock()
	signal := n.signal
	n.signal = typeutil.NewSet[SegmentBelongs]()
	n.cond.L.Unlock()
	return signal
}
