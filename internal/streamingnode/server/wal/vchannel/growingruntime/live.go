package growingruntime

import (
	"context"

	"github.com/pingcap/failpoint"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/pkupdatepoint"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (r *Runtime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) error {
	if r == nil {
		return nil
	}
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	for r.refresh != nil && r.refresh.err == nil && len(r.refresh.pending) >= 1024 {
		r.eventCond.Wait()
	}
	if stage := r.refresh; stage != nil && stage.err == nil && stage.includes(event) {
		stage.pending = append(stage.pending, event)
	}
	return r.applyEvent(ctx, event)
}

func (r *Runtime) applyEvent(ctx context.Context, event walview.VChannelResourceEvent) error {
	r.mu.RLock()
	failed := r.applyErr
	r.mu.RUnlock()
	if failed != nil {
		return failed
	}
	if event.QueryBarrier != nil {
		r.markGrowingTimeTick(event.QueryBarrier.TimeTick)
		r.markTransformTimeTick(event.QueryBarrier.TimeTick)
		return nil
	}
	if event.Message != nil {
		return r.applyLiveMessage(ctx, event.Message)
	}
	if event.SegmentSealed != nil {
		r.markSegmentSealed(event.SegmentSealed.SegmentID, event.SegmentSealed.SealedAtDataVersion)
	}
	return nil
}

func (r *Runtime) applyLiveMessage(ctx context.Context, msg message.ImmutableMessage) error {
	if r == nil || msg == nil {
		return nil
	}
	timeTick := msg.TimeTick()
	applyGrowing := timeTick > r.appliedGrowingTimeTick.Load()
	applyTransform := messageAdvancesTransformFrontier(msg) && timeTick > r.appliedTransformTimeTick.Load()
	if err := r.dispatchMessage(ctx, msg, applyGrowing, applyTransform); err != nil {
		r.mu.Lock()
		r.applyErr = merr.Wrap(err, "failed to apply live message to growing runtime")
		r.mvccCond.Broadcast()
		r.mu.Unlock()
		failpoint.Inject("pkGrowingFailureFrontier", func(value failpoint.Value) {
			pkupdatepoint.Hold(ctx, value, msg.VChannel(), timeTick, 0, map[string]uint64{"growing": r.appliedGrowingTimeTick.Load(), "transform": r.appliedTransformTimeTick.Load()})
		})
		return err
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
	return nil
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
