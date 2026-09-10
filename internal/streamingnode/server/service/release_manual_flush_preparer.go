package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
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
}

// PrepareReleaseManualFlush appends a normal ManualFlush and prepares a
// channel-level release handoff.
func (p *releaseManualFlushPreparer) PrepareReleaseManualFlush(ctx context.Context, pchannel types.PChannelInfo, collectionID int64, vchannel string, releaseSegmentIDs []int64) (bool, error) {
	if p.writeBufferManager == nil {
		return false, status.NewInner("write buffer manager is not initialized")
	}
	if vchannel == "" {
		return false, status.NewInvalidArgument("vchannel is empty")
	}
	if collectionID == 0 {
		return false, status.NewInvalidArgument("collection id is empty")
	}
	if !p.writeBufferManager.AllowGrowingSourceFlush(vchannel) {
		mlog.Info(ctx, "skip release manual flush prepare because channel does not use growing-source flush",
			mlog.String("vchannel", vchannel),
			mlog.Int64("collectionID", collectionID),
			mlog.Int64s("releaseSegmentIDs", releaseSegmentIDs))
		return false, nil
	}
	rollbackHandoff, err := syncmgr.DefaultGrowingSourceRegistry().BeginGrowingSourceReleaseHandoff(vchannel, releaseSegmentIDs)
	if err != nil {
		return false, err
	}
	handoffCommitted := false
	defer func() {
		if !handoffCommitted && rollbackHandoff != nil {
			rollbackHandoff()
		}
	}()

	if checker, ok := p.writeBufferManager.(writebuffer.ReleaseManualFlushNeedChecker); ok {
		needManualFlush, err := checker.CheckReleaseManualFlushNeed(ctx, vchannel, releaseSegmentIDs)
		if err != nil {
			return false, err
		}
		if !needManualFlush {
			progress, err := p.writeBufferManager.GetGrowingFlushProgress(ctx, vchannel, releaseSegmentIDs, 0)
			if err != nil {
				return false, err
			}
			handoffCommitted = true
			prepared := hasReleaseHandoffProgress(progress)
			mlog.Info(ctx, "prepared release handoff without manual flush",
				mlog.String("vchannel", vchannel),
				mlog.Int64("collectionID", collectionID),
				mlog.Int64s("releaseSegmentIDs", releaseSegmentIDs),
				mlog.Bool("retained", prepared),
				mlog.Any("progress", progress))
			return prepared, nil
		}
	}

	wal, err := p.walManager.GetAvailableWAL(pchannel)
	if err != nil {
		return false, err
	}
	flushMsg, err := message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		BuildMutable()
	if err != nil {
		return false, err
	}
	appendResult, err := wal.Append(ctx, flushMsg)
	if err != nil {
		return false, err
	}
	var flushMsgResponse message.ManualFlushExtraResponse
	if err := appendResult.GetExtra(&flushMsgResponse); err != nil {
		return false, err
	}

	handoffSegmentIDs := unionSegmentIDs(releaseSegmentIDs, flushMsgResponse.GetSegmentIds())
	progress, err := p.writeBufferManager.GetGrowingFlushProgress(ctx, vchannel, handoffSegmentIDs, appendResult.TimeTick)
	if err != nil {
		return false, err
	}
	handoffCommitted = true
	prepared := hasReleaseHandoffProgress(progress)
	mlog.Info(ctx, "prepared release manual flush",
		mlog.String("vchannel", vchannel),
		mlog.Int64("collectionID", collectionID),
		mlog.Uint64("flushTs", appendResult.TimeTick),
		mlog.Int64s("releaseSegmentIDs", releaseSegmentIDs),
		mlog.Int64s("affectedSegmentIDs", flushMsgResponse.GetSegmentIds()),
		mlog.Int64s("handoffSegmentIDs", handoffSegmentIDs),
		mlog.Bool("retained", prepared),
		mlog.Any("progress", progress))
	return prepared, nil
}

func hasReleaseHandoffProgress(progress []writebuffer.GrowingFlushSegmentProgress) bool {
	for _, segmentProgress := range progress {
		if segmentProgress.NeedReleaseHandoff {
			return true
		}
	}
	return false
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
