package growingruntime

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type partitionRefresh struct {
	timeTick uint64
	version  qviews.DataVersion
	pending  []walview.VChannelResourceEvent
	err      error
}

func (s *partitionRefresh) includes(event walview.VChannelResourceEvent) bool {
	if event.QueryBarrier != nil {
		return event.QueryBarrier.TimeTick > s.timeTick
	}
	if event.Message != nil {
		return event.Message.TimeTick() > s.timeTick
	}
	return event.SegmentSealed != nil && event.SegmentSealed.SealedAtDataVersion.GT(s.version)
}

// PreparePartitions expands physical coverage from a no-gap WAL snapshot. Existing
// segments remain in place for retained views and pinned queries; each view's
// logical partition scope is enforced separately by QueryRuntime.
func (r *Runtime) PreparePartitions(ctx context.Context, meta *viewpb.QueryViewMeta, partitionIDs []int64) error {
	if !r.prepareMu.TryLock() {
		return merr.WrapErrServiceNotReadyMsg("growing partition preparation is in progress")
	}
	defer r.prepareMu.Unlock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return context.Canceled
	}
	missing := make(map[int64]struct{})
	for _, id := range partitionIDs {
		if !r.partitionLoaded(id) {
			missing[id] = struct{}{}
		}
	}
	if len(missing) == 0 {
		r.mu.Unlock()
		return nil
	}
	base := r.view
	target := proto.Clone(meta).(*viewpb.QueryViewMeta)
	if r.hasTruncateDataVersion && r.truncateDataVersion.GT(qviews.FromProtoDataVersion(target.GetVersion().GetDataVersion())) {
		target.Version.DataVersion = r.truncateDataVersion.IntoProto()
	}
	// Keep the shared CCollection alive throughout snapshot I/O, even on Close.
	r.pinQueryLocked()
	collection := r.collection
	r.mu.Unlock()
	defer r.unpinQuery()
	if base.CaptureQueryView == nil {
		return merr.WrapErrServiceInternalMsg("growing partition refresh has no WAL snapshot provider")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if base.RuntimeContext != nil {
		stop := context.AfterFunc(base.RuntimeContext, cancel)
		defer stop()
	}
	stage := &partitionRefresh{}
	defer func() { r.eventMu.Lock(); r.refresh = nil; r.eventCond.Broadcast(); r.eventMu.Unlock() }()
	stop := context.AfterFunc(ctx, func() { r.eventMu.Lock(); stage.err = ctx.Err(); r.eventCond.Broadcast(); r.eventMu.Unlock() })
	defer stop()
	var view walview.VChannelWALView
	if !base.CaptureQueryView(target, func(captured walview.VChannelWALView) {
		view = captured
		r.eventMu.Lock()
		stage.timeTick, stage.version = view.BaseGrowingTimeTick, view.SegmentSnapshot.DataVersion
		r.refresh = stage
		r.eventMu.Unlock()
	}) {
		return merr.WrapErrServiceNotReadyMsg("growing partition snapshot is not ready")
	}
	next := newRuntime()
	next.collection, next.loadedPartitions = collection, missing
	defer func() {
		// next borrows the pinned parent collection; only its untransferred segments
		// belong to it. Closing a failed stage cannot close the serving collection.
		next.collection = nil
		next.Close()
	}()
	if err := next.prepareSegments(ctx, view); err != nil {
		return err
	}
	// The serving dispatcher may still contain pre-snapshot messages. Catch it
	// up before merging so those inserts cannot be applied twice. Only events
	// after this boundary enter the stage queue, so backpressure cannot block it.
	// Growing advances for every WAL message. Reaching T therefore also proves
	// all transforms through T were applied, even when T itself is an Insert
	// and has no transform entry. Do not wait for a nonexistent transform at T.
	if err := r.WaitMVCCVisible(ctx, view.BaseGrowingTimeTick, 0); err != nil {
		return err
	}
	for {
		r.eventMu.Lock()
		if ctx.Err() != nil {
			r.eventMu.Unlock()
			return ctx.Err()
		}
		batch := stage.pending
		stage.pending = nil
		r.eventCond.Broadcast()
		if len(batch) != 0 {
			r.eventMu.Unlock()
			for _, event := range batch {
				next.applyEvent(ctx, event)
			}
			continue
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			r.eventMu.Unlock()
			return context.Canceled
		}
		for id, segment := range next.segments {
			r.segments[id] = segment
			r.segmentIDs = append(r.segmentIDs, id)
		}
		for id := range missing {
			r.loadedPartitions[id] = struct{}{}
		}
		next.segments = nil
		next.segmentIDs = nil
		r.refresh = nil
		r.mu.Unlock()
		r.eventMu.Unlock()
		return nil
	}
}
