package utility

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ReOrderByTimeTickBuffer is a buffer that stores messages and pops them in order of time tick.
type ReOrderByTimeTickBuffer struct {
	messageHeap     typeutil.Heap[message.ImmutableMessage]
	lastPopTimeTick uint64
	bytes           int
}

// NewReOrderBuffer creates a new ReOrderBuffer.
func NewReOrderBuffer() *ReOrderByTimeTickBuffer {
	return &ReOrderByTimeTickBuffer{
		messageHeap: typeutil.NewHeap[message.ImmutableMessage](&immutableMessageHeap{}),
	}
}

// Push pushes a message into the buffer.
func (r *ReOrderByTimeTickBuffer) Push(msg message.ImmutableMessage) error {
	// !!! Drop the unexpected broken timetick rule message.
	// It will be enabled until the first timetick coming.
	if msg.TimeTick() < r.lastPopTimeTick {
		return errors.Errorf("message time tick is less than last pop time tick: %d", r.lastPopTimeTick)
	}
	r.messageHeap.Push(msg)
	r.bytes += msg.EstimateSize()
	return nil
}

// PopUtilTimeTick pops all messages whose time tick is less than or equal to the given time tick.
// The result is sorted by time tick in ascending order.
func (r *ReOrderByTimeTickBuffer) PopUtilTimeTick(timetick uint64) []message.ImmutableMessage {
	var res []message.ImmutableMessage
	for r.messageHeap.Len() > 0 && r.messageHeap.Peek().TimeTick() <= timetick {
		r.bytes -= r.messageHeap.Peek().EstimateSize()
		res = append(res, r.messageHeap.Pop())
	}
	r.lastPopTimeTick = timetick
	return res
}

// Len returns the number of messages in the buffer.
func (r *ReOrderByTimeTickBuffer) Len() int {
	return r.messageHeap.Len()
}

func (r *ReOrderByTimeTickBuffer) Bytes() int {
	return r.bytes
}
