package service

import (
	"context"

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

// PrepareReleaseManualFlush appends a normal ManualFlush and retains the requested local growing segments.
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
	if len(releaseSegmentIDs) == 0 {
		return false, nil
	}
	if !p.writeBufferManager.UseGrowingSourceFlush(vchannel) {
		mlog.Info(ctx, "skip release manual flush prepare because channel does not use growing-source flush",
			mlog.String("vchannel", vchannel),
			mlog.Int64("collectionID", collectionID),
			mlog.Int64s("releaseSegmentIDs", releaseSegmentIDs))
		return false, nil
	}
	if checker, ok := p.writeBufferManager.(writebuffer.ReleaseManualFlushNeedChecker); ok {
		needManualFlush, err := checker.CheckReleaseManualFlushNeed(ctx, vchannel, releaseSegmentIDs)
		if err != nil {
			return false, err
		}
		if !needManualFlush {
			mlog.Info(ctx, "skip release manual flush prepare because target segments do not need release handoff",
				mlog.String("vchannel", vchannel),
				mlog.Int64("collectionID", collectionID),
				mlog.Int64s("releaseSegmentIDs", releaseSegmentIDs))
			return false, nil
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

	progress, err := p.writeBufferManager.GetGrowingFlushProgress(ctx, vchannel, releaseSegmentIDs, appendResult.TimeTick)
	if err != nil {
		return false, err
	}
	prepared := false
	for _, segmentProgress := range progress {
		if segmentProgress.NeedReleaseHandoff {
			prepared = true
			break
		}
	}
	mlog.Info(ctx, "prepared release manual flush",
		mlog.String("vchannel", vchannel),
		mlog.Int64("collectionID", collectionID),
		mlog.Uint64("flushTs", appendResult.TimeTick),
		mlog.Int64s("releaseSegmentIDs", releaseSegmentIDs),
		mlog.Int64s("affectedSegmentIDs", flushMsgResponse.GetSegmentIds()),
		mlog.Bool("retained", prepared),
		mlog.Any("progress", progress))
	return prepared, nil
}
