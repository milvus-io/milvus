package inspector

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

// TimeTickInfo records the information of time tick.
type TimeTickInfo struct {
	MessageID              message.MessageID // the message id.
	TimeTick               uint64            // the time tick.
	LastConfirmedMessageID message.MessageID // the last confirmed message id.
	// The time tick may be updated, without last timetickMessage
}

// IsZero returns true if the time tick info is zero.
func (t *TimeTickInfo) IsZero() bool {
	return t.TimeTick == 0
}

// NewTimeTickNotifier creates a new time tick info listener.
func NewTimeTickNotifier() *TimeTickNotifier {
	return &TimeTickNotifier{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
		info: TimeTickInfo{},
	}
}

// TimeTickNotifier is a listener for time tick info.
type TimeTickNotifier struct {
	cond *syncutil.ContextCond
	info TimeTickInfo
}

// Update only update the time tick info, but not notify the waiter.
func (l *TimeTickNotifier) Update(info TimeTickInfo) {
	l.cond.L.Lock()
	if l.info.IsZero() || l.info.MessageID.LT(info.MessageID) {
		l.info = info
	}
	l.cond.L.Unlock()
}

// OnlyUpdateTs only updates the time tick, and notify the waiter.
func (l *TimeTickNotifier) OnlyUpdateTs(timetick uint64) {
	l.cond.LockAndBroadcast()
	if !l.info.IsZero() && l.info.TimeTick < timetick {
		l.info.TimeTick = timetick
	}
	l.cond.L.Unlock()
}

// WatchAtMessageID watch the message id.
// If the message id is not equal to the last message id, return nil channel.
// Or if the time tick is less than the last time tick, return channel.
func (l *TimeTickNotifier) WatchAtMessageID(messageID message.MessageID, ts uint64) <-chan struct{} {
	l.cond.L.Lock()
	// If incoming messageID is less than the producer messageID,
	// the consumer can read the new greater messageID from wal,
	// so the watch operation is not necessary.
	if l.info.IsZero() || messageID.LT(l.info.MessageID) {
		l.cond.L.Unlock()
		return nil
	}

	// messageID may be greater than MessageID in notifier.
	// because consuming operation is fast than produce operation.
	// so doing a listening here.
	if ts < l.info.TimeTick {
		ch := make(chan struct{})
		close(ch)
		l.cond.L.Unlock()
		return ch
	}
	return l.cond.WaitChan()
}

// Get gets the time tick info.
func (l *TimeTickNotifier) Get() TimeTickInfo {
	l.cond.L.Lock()
	info := l.info
	l.cond.L.Unlock()
	return info
}
