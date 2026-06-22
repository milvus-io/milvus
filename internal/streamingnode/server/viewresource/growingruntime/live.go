package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (r *Runtime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	if r == nil {
		return
	}
	if event.Message != nil {
		r.applyLiveMessage(ctx, event.Message)
		return
	}
	if event.SegmentSealed != nil {
		r.markSegmentSealed(event.SegmentSealed.SegmentID, event.SegmentSealed.SealedAtDataVersion)
	}
}

func (r *Runtime) applyLiveMessage(ctx context.Context, msg message.ImmutableMessage) {
	if r == nil || msg == nil {
		return
	}
	if err := r.dispatchMessage(ctx, msg); err != nil {
		panic(errors.Wrap(err, "failed to apply live message to growing runtime"))
	}
	timeTick := msg.TimeTick()
	advanceTimeTick(&r.appliedGrowingTimeTick, timeTick)
	if messageAdvancesTransformFrontier(msg) {
		advanceTimeTick(&r.appliedTransformTimeTick, timeTick)
	}
}

func messageAdvancesTransformFrontier(msg message.ImmutableMessage) bool {
	if msg == nil {
		return false
	}
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		return true
	case message.MessageTypeTxn:
		txn := message.AsImmutableTxnMessage(msg)
		if txn == nil {
			return false
		}
		containsDelete := false
		_ = txn.RangeOver(func(inner message.ImmutableMessage) error {
			if inner.MessageType() == message.MessageTypeDelete {
				containsDelete = true
			}
			return nil
		})
		return containsDelete
	default:
		return false
	}
}

func advanceTimeTick(value interface {
	Load() uint64
	CompareAndSwap(old uint64, new uint64) bool
}, next uint64,
) {
	for {
		current := value.Load()
		if next <= current {
			return
		}
		if value.CompareAndSwap(current, next) {
			return
		}
	}
}
