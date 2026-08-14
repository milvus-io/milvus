package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
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
	timeTick := msg.TimeTick()
	applyGrowing := timeTick > r.appliedGrowingTimeTick.Load()
	applyTransform := messageAdvancesTransformFrontier(msg) && timeTick > r.appliedTransformTimeTick.Load()
	if err := r.dispatchMessage(ctx, msg, applyGrowing, applyTransform); err != nil {
		panic(errors.Wrap(err, "failed to apply live message to growing runtime"))
	}
	if applyGrowing {
		r.markGrowingTimeTick(timeTick)
	}
	if applyTransform {
		r.markTransformTimeTick(timeTick)
	}
	mlog.Debug(ctx, "applied live message to growing runtime",
		mlog.FieldVChannel(msg.VChannel()),
		mlog.String("messageType", msg.MessageType().String()),
		mlog.Uint64("timeTick", timeTick),
		mlog.Bool("applyGrowing", applyGrowing),
		mlog.Bool("applyTransform", applyTransform),
		mlog.Uint64("appliedGrowingTimeTick", r.appliedGrowingTimeTick.Load()),
		mlog.Uint64("appliedTransformTimeTick", r.appliedTransformTimeTick.Load()),
	)
}
func messageAdvancesTransformFrontier(msg message.ImmutableMessage) bool {
	return messageutil.ClassifyTransformLogMessage(msg) != messageutil.TransformLogKindNone
}

func advanceTimeTick(value interface {
	Load() uint64
	CompareAndSwap(old uint64, new uint64) bool
}, next uint64,
) bool {
	for {
		current := value.Load()
		if next <= current {
			return false
		}
		if value.CompareAndSwap(current, next) {
			return true
		}
	}
}
