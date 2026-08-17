package service

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NewReleaseManualFlushPreparer creates a process-local release manual flush preparer.
func NewReleaseManualFlushPreparer(walManager walmanager.Manager, writeBufferManager writebuffer.BufferManager) *releaseManualFlushPreparer {
	return &releaseManualFlushPreparer{
		walManager:         walManager,
		writeBufferManager: writeBufferManager,
	}
}

type releaseManualFlushPreparer struct {
	walManager         walmanager.Manager
	writeBufferManager writebuffer.BufferManager
	// nudgeLimiter bounds how often PrepareReleaseSegments appends its
	// collection-scoped ManualFlush. It never bounds how often the debt is
	// REPORTED — a suppressed nudge still reports pending.
	nudgeLimiter nudgeLimiter
}

// PrepareReleaseManualFlush appends a normal ManualFlush and prepares a
// channel-level release handoff.
func (p *releaseManualFlushPreparer) PrepareReleaseManualFlush(ctx context.Context, pchannel types.PChannelInfo, collectionID int64, vchannel string, releaseSegmentIDs []int64) error {
	if p.writeBufferManager == nil {
		return status.NewInner("write buffer manager is not initialized")
	}
	if vchannel == "" {
		return status.NewInvalidArgument("vchannel is empty")
	}
	if collectionID == 0 {
		return status.NewInvalidArgument("collection id is empty")
	}
	// Only "present AND feature disabled" is a safe skip. found=false is NOT:
	// DropChannel/RemoveChannel detach the buffer from the manager's map before
	// the long final Close, so a detached channel may still owe a growing-source
	// flush. Fall through instead — GetGrowingFlushProgress answers
	// merr.WrapErrChannelNotFound for a detached channel, which the release
	// guards classify as a transient refusal (see
	// growingflush.IsPrepareStructurallyUnavailable), so the caller retries
	// rather than dropping the only copy of the rows inside the detach window.
	enabled, found := p.writeBufferManager.AllowGrowingSourceFlush(vchannel)
	if found && !enabled {
		mlog.Info(ctx, "skip release manual flush prepare because channel does not use growing-source flush",
			mlog.String("vchannel", vchannel),
			mlog.Int64("collectionID", collectionID),
			mlog.Int64s("releaseSegmentIDs", releaseSegmentIDs))
		return nil
	}
	if !found {
		mlog.Warn(ctx, "write buffer not found for channel on release manual flush prepare, possibly detached mid-drop; proceeding so the progress check refuses transiently",
			mlog.String("vchannel", vchannel),
			mlog.Int64("collectionID", collectionID),
			mlog.Int64s("releaseSegmentIDs", releaseSegmentIDs))
	}
	// Fence growing-source admission BEFORE appending, never after. Everything
	// admitted up to here was created by an insert already in the WAL, so the
	// ManualFlush appended next — whose timestamp is above all of them — seals
	// every one of them; everything admitted after it buffers its rows in the
	// write buffer and needs no delegator. Fencing after the append instead
	// leaves a window for a segment that is growing-source AND unsealed, which
	// the drain below would then wait on until this call's deadline.
	p.writeBufferManager.FenceGrowingSourceAdmission(vchannel)

	// The ManualFlush is appended unconditionally. It is cheap to skip when
	// nothing needs sealing, but a release path with two shapes is not: the
	// old skipping variant appended no fence message at all, which was a second
	// set of orderings to reason about on the one path where getting the order
	// wrong strands data. One path, always fenced.
	flushTs, flushMsgResponse, err := p.appendManualFlush(ctx, pchannel, collectionID, vchannel)
	if err != nil {
		return err
	}

	handoffSegmentIDs := unionSegmentIDs(releaseSegmentIDs, flushMsgResponse.GetSegmentIds())
	progress, err := p.snapshotAndDrain(ctx, vchannel, handoffSegmentIDs)
	if err != nil {
		return err
	}
	mlog.Info(ctx, "prepared release manual flush",
		mlog.String("vchannel", vchannel),
		mlog.Int64("collectionID", collectionID),
		mlog.Uint64("flushTs", flushTs),
		mlog.Int64s("releaseSegmentIDs", releaseSegmentIDs),
		mlog.Int64s("affectedSegmentIDs", flushMsgResponse.GetSegmentIds()),
		mlog.Int64s("handoffSegmentIDs", handoffSegmentIDs),
		mlog.Any("progress", progress))
	return nil
}

// PrepareReleaseSegments reports whether the LOCAL write buffer still owes a
// growing-source flush for segmentIDs, and drives those flushes forward when it
// does. It never waits for the drain.
//
// This is the partial-release counterpart of PrepareReleaseManualFlush. A
// ReleaseSegments(DataScope_Streaming) drops SOME growing segments of a channel
// that stays subscribed, so it must NOT use the channel-wide pieces of the
// channel-release path:
//
//   - No FenceGrowingSourceAdmission. The fence is per write buffer (per
//     channel) and only reopens when a NEWER growing-source provider
//     registration appears, i.e. on re-subscription. Closing it for a partial
//     release would degrade the surviving partitions of a live channel to
//     write-buffer mode until the channel is watched again.
//   - No WaitGrowingFlushDrained. Blocking a release RPC on a flush is what the
//     channel-release path can afford (the channel is going away anyway); here
//     the caller retries instead.
//
// The nudge is a plain ManualFlush. It is collection-scoped, not segment-scoped:
// a caller cannot scope a ManualFlush to segment IDs, because
// ManualFlushMessageHeader.SegmentIds is written by the shard interceptor
// (handleManualFlushMessage) as the OUTPUT of FlushAndFenceSegmentAllocUntil,
// and the only segment-scoped seal message (FlushMessageV2) is rejected by the
// interceptor unless the shard manager already marked the segment flushed. So
// the debt CHECK is segment-scoped and the seal that resolves it is
// collection-scoped — the extra segments it seals are simply flushed early.
func (p *releaseManualFlushPreparer) PrepareReleaseSegments(ctx context.Context, pchannel types.PChannelInfo, collectionID int64, vchannel string, segmentIDs []int64) (bool, error) {
	if p.writeBufferManager == nil {
		return false, status.NewInner("write buffer manager is not initialized")
	}
	if vchannel == "" {
		return false, status.NewInvalidArgument("vchannel is empty")
	}
	if collectionID == 0 {
		return false, status.NewInvalidArgument("collection id is empty")
	}
	if len(segmentIDs) == 0 {
		return false, nil
	}
	// Same distinction as in PrepareReleaseManualFlush: only "present AND
	// feature disabled" may skip. A channel absent from the manager (found=false)
	// may be mid-DropChannel with the buffer still alive and owed — fall through
	// so GetGrowingFlushProgress surfaces merr.WrapErrChannelNotFound as a
	// transient refusal instead of reporting "no debt".
	enabled, found := p.writeBufferManager.AllowGrowingSourceFlush(vchannel)
	if found && !enabled {
		return false, nil
	}
	if !found {
		mlog.Warn(ctx, "write buffer not found for channel on release segments prepare, possibly detached mid-drop; proceeding so the progress check refuses transiently",
			mlog.String("vchannel", vchannel),
			mlog.Int64("collectionID", collectionID),
			mlog.Int64s("releaseSegmentIDs", segmentIDs))
	}

	// GetGrowingFlushProgress unions the requested ids with every tracked
	// growing-source segment of the channel, so the result must be narrowed back
	// to the segments this release actually drops. A sibling segment that owes a
	// flush is not this release's problem — it is not being dropped.
	requested := typeutil.NewSet(segmentIDs...)
	progress, err := p.writeBufferManager.GetGrowingFlushProgress(ctx, vchannel, segmentIDs)
	if err != nil {
		return false, err
	}
	pending := make([]int64, 0, len(segmentIDs))
	for _, segmentProgress := range progress {
		if segmentProgress.NeedReleaseHandoff && requested.Contain(segmentProgress.SegmentID) {
			pending = append(pending, segmentProgress.SegmentID)
		}
	}
	if len(pending) == 0 {
		return false, nil
	}

	// The debt is real, so the release is refused either way. Whether a fresh
	// ManualFlush is appended is rate-limited: the caller retries this check on
	// the coordinator's cadence (queryCoord.checkSegmentInterval, 3s), and on a
	// channel whose flush is stuck that would otherwise mean one collection-scoped
	// seal + segment-alloc fence per retry for as long as it stays stuck.
	key := nudgeKey{collectionID: collectionID, vchannel: vchannel}
	if !p.nudgeLimiter.allow(key, time.Now()) {
		mlog.Info(ctx, "growing-source flush still owed for released segments, nudge suppressed by rate limit",
			mlog.String("vchannel", vchannel),
			mlog.Int64("collectionID", collectionID),
			mlog.Int64s("releaseSegmentIDs", segmentIDs),
			mlog.Int64s("pendingSegmentIDs", pending))
		return true, nil
	}

	flushTs, flushMsgResponse, err := p.appendManualFlush(ctx, pchannel, collectionID, vchannel)
	if err != nil {
		// Best effort: the limiter slot is already consumed, so this failure
		// wastes one interval's nudge. Harmless — the debt keeps being reported
		// on every check, the release stays refused, and the next interval
		// re-nudges.
		return false, err
	}
	mlog.Info(ctx, "nudged growing-source flush for released segments, release must be retried",
		mlog.String("vchannel", vchannel),
		mlog.Int64("collectionID", collectionID),
		mlog.Uint64("flushTs", flushTs),
		mlog.Int64s("releaseSegmentIDs", segmentIDs),
		mlog.Int64s("pendingSegmentIDs", pending),
		mlog.Int64s("affectedSegmentIDs", flushMsgResponse.GetSegmentIds()))
	return true, nil
}

// appendManualFlush appends a ManualFlush message for the collection on the
// vchannel and returns its time tick together with the segments it sealed.
func (p *releaseManualFlushPreparer) appendManualFlush(ctx context.Context, pchannel types.PChannelInfo, collectionID int64, vchannel string) (uint64, *message.ManualFlushExtraResponse, error) {
	wal, err := p.walManager.GetAvailableWAL(pchannel)
	if err != nil {
		return 0, nil, err
	}
	flushMsg, err := message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		BuildMutable()
	if err != nil {
		return 0, nil, err
	}
	appendResult, err := wal.Append(ctx, flushMsg)
	if err != nil {
		return 0, nil, err
	}
	flushMsgResponse := &message.ManualFlushExtraResponse{}
	if err := appendResult.GetExtra(flushMsgResponse); err != nil {
		return 0, nil, err
	}
	return appendResult.TimeTick, flushMsgResponse, nil
}

// snapshotAndDrain reads growing-source progress at the fence, then blocks until
// no segment on the channel still owes a growing-source flush.
//
// It converges because admission was already fenced before the ManualFlush was
// appended: no NEW segment can start owing a growing-source flush, so the wait
// only has to drain what the fence caught, and the ManualFlush sealed all of
// that.
//
// The caller unsubscribes the channel right after this returns, dropping the
// growing segments. In growing-source mode those segments hold the only copy of
// the unflushed rows, so a flush left in flight can never complete afterwards —
// it loses its source permanently and keeps the channel checkpoint pinned. This
// wait is what makes the release safe; an error here must fail the release.
func (p *releaseManualFlushPreparer) snapshotAndDrain(ctx context.Context, vchannel string, segmentIDs []int64) ([]writebuffer.GrowingFlushSegmentProgress, error) {
	progress, err := p.writeBufferManager.GetGrowingFlushProgress(ctx, vchannel, segmentIDs)
	if err != nil {
		return nil, err
	}

	// The wait itself re-scans the write buffer's full progress map; this list
	// only seeds it with the segments the snapshot saw, for logging.
	pending := make([]int64, 0, len(progress))
	for _, segmentProgress := range progress {
		if segmentProgress.NeedReleaseHandoff {
			pending = append(pending, segmentProgress.SegmentID)
		}
	}
	if err := p.writeBufferManager.WaitGrowingFlushDrained(ctx, vchannel, pending); err != nil {
		return nil, err
	}
	return progress, nil
}

func unionSegmentIDs(first []int64, second []int64) []int64 {
	seen := make(map[int64]struct{}, len(first)+len(second))
	result := make([]int64, 0, len(first)+len(second))
	for _, segmentID := range first {
		if _, ok := seen[segmentID]; ok {
			continue
		}
		seen[segmentID] = struct{}{}
		result = append(result, segmentID)
	}
	for _, segmentID := range second {
		if _, ok := seen[segmentID]; ok {
			continue
		}
		seen[segmentID] = struct{}{}
		result = append(result, segmentID)
	}
	return result
}
