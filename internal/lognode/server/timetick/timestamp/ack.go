package timestamp

import (
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/atomic"
)

var _ typeutil.HeapInterface = (*timestampWithAckArray)(nil)

// newTimestampAck creates a new timestampAck.
func newTimestampAck(ts uint64) *Timestamp {
	return &Timestamp{
		detail:       newDefaultAckDetail(ts),
		acknowledged: atomic.NewBool(false),
	}
}

// Timestamp records the timestamp and if it has been acknowledged.
type Timestamp struct {
	acknowledged *atomic.Bool // is acknowledged.
	detail       *AckDetail   // info is available after acknowledged.
}

// Timestamp returns the timestamp.
func (ta *Timestamp) Timestamp() uint64 {
	return ta.detail.Timestamp
}

// Ack marks the timestamp as acknowledged.
func (ta *Timestamp) Ack(opts ...AckOption) {
	for _, opt := range opts {
		opt(ta.detail)
	}
	ta.acknowledged.Store(true)
}

// ackDetail returns the ack info, only can be called after acknowledged.
func (ta *Timestamp) ackDetail() *AckDetail {
	if !ta.acknowledged.Load() {
		panic("unreachable: ackDetail can only be called after acknowledged")
	}
	return ta.detail
}

// timestampWithAckArray is a heap underlying represent of timestampAck.
type timestampWithAckArray []*Timestamp

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
	*h = append(*h, x.(*Timestamp))
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
