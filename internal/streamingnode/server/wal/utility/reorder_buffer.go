package utility

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var ErrTimeTickVoilation = errors.New("time tick violation")

// ReOrderByTimeTickBufferDropReason describes why a message was intentionally dropped by the reorder buffer.
type ReOrderByTimeTickBufferDropReason string

const (
	ReOrderByTimeTickBufferDropReasonDuplicateTimeTick ReOrderByTimeTickBufferDropReason = "duplicate_timetick"
)

// ReOrderByTimeTickBufferPushResult reports whether Push accepted or intentionally dropped a message.
type ReOrderByTimeTickBufferPushResult struct {
	Dropped    bool
	DropReason ReOrderByTimeTickBufferDropReason
}

// ReOrderByTimeTickBuffer is a buffer that stores messages and pops them in order of time tick.
type ReOrderByTimeTickBuffer struct {
	// messageIDs deduplicates repeated messages while switching between the write
	// ahead buffer stream and the WAL scanner stream (same message ID seen twice).
	messageIDs typeutil.Set[string]
	// seenTimeTicks deduplicates a non-TimeTick message that repeats across the
	// write-ahead-buffer / WAL-scanner stream switch with a *different* message ID,
	// which the messageIDs set above cannot catch. It is nil when physical
	// dedup is disabled (streaming.idempotency.enabled=false): the drop rule
	// only exists for the idempotency feature, and gating it there makes the
	// feature flag a real kill switch that restores the pre-idempotency scanner
	// behavior.
	//
	// INVARIANT: the timetick interceptor assigns a unique timetick to every
	// appended message, so two genuinely distinct non-TimeTick messages never
	// share a timetick while both are retained here. A repeated timetick can
	// therefore only be a physical replay of the same logical message, and
	// dropping it is safe. If a future code path ever lets two genuinely distinct
	// messages reach this buffer with the same timetick, the second would be
	// silently dropped (data loss) -- this invariant MUST be preserved. Drops are
	// surfaced by the scanner via a Warn log and the
	// idempotency_reader_physical_dedup_drop_total metric, so an unexpected rate
	// of drops is observable.
	seenTimeTicks   typeutil.Set[uint64]
	messageHeap     typeutil.Heap[message.ImmutableMessage]
	lastPopTimeTick uint64
	bytes           int
}

// NewReOrderBuffer creates a new ReOrderBuffer. physicalDedup enables the
// timetick-based duplicate drop; pass the idempotency feature flag so the drop
// rule never applies on deployments that run without the feature.
func NewReOrderBuffer(physicalDedup bool) *ReOrderByTimeTickBuffer {
	buffer := &ReOrderByTimeTickBuffer{
		messageIDs:  typeutil.NewSet[string](),
		messageHeap: typeutil.NewHeap[message.ImmutableMessage](&immutableMessageHeap{}),
	}
	if physicalDedup {
		buffer.seenTimeTicks = typeutil.NewSet[uint64]()
	}
	return buffer
}

// Push pushes a message into the buffer.
func (r *ReOrderByTimeTickBuffer) Push(msg message.ImmutableMessage) (ReOrderByTimeTickBufferPushResult, error) {
	// !!! Drop the unexpected broken timetick rule message.
	// It will be enabled until the first timetick coming.
	if msg.TimeTick() < r.lastPopTimeTick {
		return ReOrderByTimeTickBufferPushResult{}, errors.Wrapf(ErrTimeTickVoilation, "message time tick is less than last pop time tick: %d", r.lastPopTimeTick)
	}
	msgID := msg.MessageID().Marshal()
	if r.messageIDs.Contain(msgID) {
		return ReOrderByTimeTickBufferPushResult{}, status.NewInner("message is duplicated: %s", msgID)
	}
	if r.seenTimeTicks != nil && msg.MessageType() != message.MessageTypeTimeTick {
		timetick := msg.TimeTick()
		if r.seenTimeTicks.Contain(timetick) {
			return ReOrderByTimeTickBufferPushResult{
				Dropped:    true,
				DropReason: ReOrderByTimeTickBufferDropReasonDuplicateTimeTick,
			}, nil
		}
		r.seenTimeTicks.Insert(timetick)
	}
	r.messageHeap.Push(msg)
	r.messageIDs.Insert(msgID)
	r.bytes += msg.EstimateSize()
	return ReOrderByTimeTickBufferPushResult{}, nil
}

// PopUtilTimeTick pops all messages whose time tick is less than or equal to the given time tick.
// The result is sorted by time tick in ascending order.
func (r *ReOrderByTimeTickBuffer) PopUtilTimeTick(timetick uint64) []message.ImmutableMessage {
	var res []message.ImmutableMessage
	for r.messageHeap.Len() > 0 && r.messageHeap.Peek().TimeTick() <= timetick {
		msg := r.messageHeap.Pop()
		r.bytes -= msg.EstimateSize()
		r.messageIDs.Remove(msg.MessageID().Marshal())
		if msg.MessageType() != message.MessageTypeTimeTick {
			r.seenTimeTicks.Remove(msg.TimeTick())
		}
		res = append(res, msg)
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
