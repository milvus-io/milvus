package ack

import (
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ typeutil.HeapInterface = (*timestampWithAckArray)(nil)

// newAcker creates a new acker.
func newAcker(ts uint64, lastConfirmedMessageID message.MessageID) *Acker {
	return &Acker{
		acknowledged: atomic.NewBool(false),
		detail:       newAckDetail(ts, lastConfirmedMessageID),
	}
}

// Acker records the timestamp and last confirmed message id that has not been acknowledged.
type Acker struct {
	acknowledged *atomic.Bool // is acknowledged.
	detail       *AckDetail   // info is available after acknowledged.
}

// LastConfirmedMessageID returns the last confirmed message id.
func (ta *Acker) LastConfirmedMessageID() message.MessageID {
	return ta.detail.LastConfirmedMessageID
}

// Timestamp returns the timestamp.
func (ta *Acker) Timestamp() uint64 {
	return ta.detail.Timestamp
}

// Ack marks the timestamp as acknowledged.
func (ta *Acker) Ack(opts ...AckOption) {
	for _, opt := range opts {
		opt(ta.detail)
	}
	ta.acknowledged.Store(true)
}

// ackDetail returns the ack info, only can be called after acknowledged.
func (ta *Acker) ackDetail() *AckDetail {
	if !ta.acknowledged.Load() {
		panic("unreachable: ackDetail can only be called after acknowledged")
	}
	return ta.detail
}

// timestampWithAckArray is a heap underlying represent of timestampAck.
type timestampWithAckArray []*Acker

// Len returns the length of the heap.
func (h timestampWithAckArray) Len() int {
	return len(h)
}

// Less returns true if the element at index i is less than the element at index j.
func (h timestampWithAckArray) Less(i, j int) bool {
	return h[i].detail.Timestamp < h[j].detail.Timestamp
}

// Swap swaps the elements at indexes i and j.
func (h timestampWithAckArray) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes the last one at len.
func (h *timestampWithAckArray) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*Acker))
}

// Pop pop the last one at len.
func (h *timestampWithAckArray) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
func (h *timestampWithAckArray) Peek() interface{} {
	return (*h)[0]
}
