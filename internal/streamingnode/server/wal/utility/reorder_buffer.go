package utility

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var ErrTimeTickVoilation = errors.New("time tick violation")

// ReOrderByTimeTickBuffer is a buffer that stores messages and pops them in order of time tick.
type ReOrderByTimeTickBuffer struct {
	messageIDs typeutil.Set[string] // After enabling write ahead buffer, we has two stream to consume,
	// write ahead buffer works with the timetick order, but the walscannerimpl works with the message order.
	// so repeated message may generate when the swithing between the two stream.
	// The deduplicate is used to avoid the repeated message.
	messageHeap     typeutil.Heap[message.ImmutableMessage]
	lastPopTimeTick uint64
	bytes           int
}

// NewReOrderBuffer creates a new ReOrderBuffer.
func NewReOrderBuffer() *ReOrderByTimeTickBuffer {
	return &ReOrderByTimeTickBuffer{
		messageIDs:  typeutil.NewSet[string](),
		messageHeap: typeutil.NewHeap[message.ImmutableMessage](&immutableMessageHeap{}),
	}
}

// Push pushes a message into the buffer.
func (r *ReOrderByTimeTickBuffer) Push(msg message.ImmutableMessage) error {
	// !!! Drop the unexpected broken timetick rule message.
	// It will be enabled until the first timetick coming.
	if msg.TimeTick() < r.lastPopTimeTick {
		return errors.Wrapf(ErrTimeTickVoilation, "message time tick is less than last pop time tick: %d", r.lastPopTimeTick)
	}
	msgID := msg.MessageID().Marshal()
	if r.messageIDs.Contain(msgID) {
		return errors.Errorf("message is duplicated: %s", msgID)
	}
	r.messageHeap.Push(msg)
	r.messageIDs.Insert(msgID)
	r.bytes += msg.EstimateSize()
	return nil
}

// PopUtilTimeTick pops all messages whose time tick is less than or equal to the given time tick.
// The result is sorted by time tick in ascending order.
func (r *ReOrderByTimeTickBuffer) PopUtilTimeTick(timetick uint64) []message.ImmutableMessage {
	var res []message.ImmutableMessage
	for r.messageHeap.Len() > 0 && r.messageHeap.Peek().TimeTick() <= timetick {
		r.bytes -= r.messageHeap.Peek().EstimateSize()
		r.messageIDs.Remove(r.messageHeap.Peek().MessageID().Marshal())
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
