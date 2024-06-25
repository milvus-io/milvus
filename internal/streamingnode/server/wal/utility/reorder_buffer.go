package utility

import (
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ReOrderByTimeTickBuffer is a buffer that stores messages and pops them in order of time tick.
type ReOrderByTimeTickBuffer struct {
	messageHeap typeutil.Heap[message.ImmutableMessage]
}

// NewReOrderBuffer creates a new ReOrderBuffer.
func NewReOrderBuffer() *ReOrderByTimeTickBuffer {
	return &ReOrderByTimeTickBuffer{
		messageHeap: typeutil.NewHeap[message.ImmutableMessage](&immutableMessageHeap{}),
	}
}

// Push pushes a message into the buffer.
func (r *ReOrderByTimeTickBuffer) Push(msg message.ImmutableMessage) {
	r.messageHeap.Push(msg)
}

// PopUtilTimeTick pops all messages whose time tick is less than or equal to the given time tick.
// The result is sorted by time tick in ascending order.
func (r *ReOrderByTimeTickBuffer) PopUtilTimeTick(timetick uint64) []message.ImmutableMessage {
	var res []message.ImmutableMessage
	for r.messageHeap.Len() > 0 && r.messageHeap.Peek().TimeTick() <= timetick {
		res = append(res, r.messageHeap.Pop())
	}
	return res
}

// Len returns the number of messages in the buffer.
func (r *ReOrderByTimeTickBuffer) Len() int {
	return r.messageHeap.Len()
}
