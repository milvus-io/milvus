package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type releaseManualFlushHandoffProvider struct {
	beginSegmentIDs []int64
	rolledBack      bool
}

type releaseManualFlushCheckBufferManager struct {
	*writebuffer.MockBufferManager
	needManualFlush bool
	err             error
}

func (m *releaseManualFlushCheckBufferManager) CheckReleaseManualFlushNeed(ctx context.Context, channel string, segmentIDs []int64) (bool, error) {
	return m.needManualFlush, m.err
}

func (p *releaseManualFlushHandoffProvider) GetGrowingFlushSource(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	return nil, syncmgr.GrowingSourceUnavailable
}

func (p *releaseManualFlushHandoffProvider) BeginGrowingSourceReleaseHandoff(segmentIDs []int64) func() {
	p.beginSegmentIDs = append([]int64(nil), segmentIDs...)
	return func() {
		p.rolledBack = true
	}
}

func (p *releaseManualFlushHandoffProvider) PrepareGrowingSourceReleaseHandoff(ctx context.Context, fenceTs uint64, segments []syncmgr.GrowingSourceReleaseHandoffSegment) error {
	return nil
}

func (p *releaseManualFlushHandoffProvider) IsReleaseAllowed(segmentID int64, checkpointTs uint64) bool {
	return false
}

func (p *releaseManualFlushHandoffProvider) IsReleasePrepared(segmentID int64, checkpointTs uint64) bool {
	return false
}

func (p *releaseManualFlushHandoffProvider) MarkReleaseDetached(segmentID int64) {
}

func (p *releaseManualFlushHandoffProvider) ClearReleasePrepared(segmentID int64) {
}

func (p *releaseManualFlushHandoffProvider) ReleasePreparedSegments() []int64 {
	return nil
}

func registerReleaseManualFlushHandoffProvider(t *testing.T, channel string) *releaseManualFlushHandoffProvider {
	provider := &releaseManualFlushHandoffProvider{}
	registration := syncmgr.DefaultGrowingSourceRegistry().Register(channel, provider)
	t.Cleanup(func() {
		syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)
	})
	return provider
}

func TestReleaseManualFlushPreparer(t *testing.T) {
	ctx := context.Background()
	releaseSegmentIDs := []int64{1001}
	affectedSegmentIDs := []int64{1002}
	handoffProvider := registerReleaseManualFlushHandoffProvider(t, "vchannel")

	extra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: affectedSegmentIDs})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		flushMsg, err := message.AsMutableManualFlushMessageV2(msg)
		if err != nil {
			return false
		}
		return flushMsg.VChannel() == "vchannel" &&
			flushMsg.Header().GetCollectionId() == 10
	})).Return(&types.AppendResult{
		TimeTick: 200,
		Extra:    extra,
	}, nil)

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().UseGrowingSourceFlush("vchannel").Return(true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001, 1002}, uint64(200)).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{
				SegmentID:          1001,
				TargetOffset:       10,
				NeedReleaseHandoff: true,
				SourceMode:         metacache.FlushSourceGrowing,
			},
		}, nil)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	prepared, err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", releaseSegmentIDs)

	assert.NoError(t, err)
	assert.True(t, prepared)
	assert.Equal(t, releaseSegmentIDs, handoffProvider.beginSegmentIDs)
	assert.False(t, handoffProvider.rolledBack)
}

func TestReleaseManualFlushPreparerNoGrowingProgress(t *testing.T) {
	ctx := context.Background()
	releaseSegmentIDs := []int64{1001}
	handoffProvider := registerReleaseManualFlushHandoffProvider(t, "vchannel")

	extra, err := anypb.New(&message.ManualFlushExtraResponse{})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		TimeTick: 200,
		Extra:    extra,
	}, nil)

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().UseGrowingSourceFlush("vchannel").Return(true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", releaseSegmentIDs, uint64(200)).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{
				SegmentID:          1001,
				NeedReleaseHandoff: false,
				SourceMode:         metacache.FlushSourceUnknown,
			},
		}, nil)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	prepared, err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", releaseSegmentIDs)

	assert.NoError(t, err)
	assert.False(t, prepared)
	assert.Equal(t, releaseSegmentIDs, handoffProvider.beginSegmentIDs)
	assert.False(t, handoffProvider.rolledBack)
}

func TestReleaseManualFlushPreparerSkipsManualFlushWhenCurrentSegmentsDoNotNeedHandoff(t *testing.T) {
	ctx := context.Background()
	releaseSegmentIDs := []int64{1001}
	handoffProvider := registerReleaseManualFlushHandoffProvider(t, "vchannel")

	wbManager := &releaseManualFlushCheckBufferManager{
		MockBufferManager: writebuffer.NewMockBufferManager(t),
		needManualFlush:   false,
	}
	wbManager.EXPECT().UseGrowingSourceFlush("vchannel").Return(true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", releaseSegmentIDs, uint64(0)).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{
				SegmentID:          1001,
				NeedReleaseHandoff: false,
				SourceMode:         metacache.FlushSourceWriteBuffer,
			},
		}, nil)

	preparer := NewReleaseManualFlushPreparer(mock_walmanager.NewMockManager(t), wbManager)
	prepared, err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", releaseSegmentIDs)

	assert.NoError(t, err)
	assert.False(t, prepared)
	assert.Equal(t, releaseSegmentIDs, handoffProvider.beginSegmentIDs)
	assert.False(t, handoffProvider.rolledBack)
}

func TestReleaseManualFlushPreparerPreparesExistingProgressWithoutManualFlush(t *testing.T) {
	ctx := context.Background()
	releaseSegmentIDs := []int64{1001}
	handoffProvider := registerReleaseManualFlushHandoffProvider(t, "vchannel")

	wbManager := &releaseManualFlushCheckBufferManager{
		MockBufferManager: writebuffer.NewMockBufferManager(t),
		needManualFlush:   false,
	}
	wbManager.EXPECT().UseGrowingSourceFlush("vchannel").Return(true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", releaseSegmentIDs, uint64(0)).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{
				SegmentID:          1001,
				TargetOffset:       10,
				NeedReleaseHandoff: true,
				SourceMode:         metacache.FlushSourceGrowing,
			},
		}, nil)

	preparer := NewReleaseManualFlushPreparer(mock_walmanager.NewMockManager(t), wbManager)
	prepared, err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", releaseSegmentIDs)

	assert.NoError(t, err)
	assert.True(t, prepared)
	assert.Equal(t, releaseSegmentIDs, handoffProvider.beginSegmentIDs)
	assert.False(t, handoffProvider.rolledBack)
}

func TestReleaseManualFlushPreparerSkipsManualFlushForEmptyInitialSegments(t *testing.T) {
	ctx := context.Background()
	handoffProvider := registerReleaseManualFlushHandoffProvider(t, "vchannel")

	wbManager := &releaseManualFlushCheckBufferManager{
		MockBufferManager: writebuffer.NewMockBufferManager(t),
		needManualFlush:   false,
	}
	wbManager.EXPECT().UseGrowingSourceFlush("vchannel").Return(true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64(nil), uint64(0)).
		Return(nil, nil)

	preparer := NewReleaseManualFlushPreparer(mock_walmanager.NewMockManager(t), wbManager)
	prepared, err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", nil)

	assert.NoError(t, err)
	assert.False(t, prepared)
	assert.Empty(t, handoffProvider.beginSegmentIDs)
	assert.False(t, handoffProvider.rolledBack)
}

func TestReleaseManualFlushPreparerFencesEmptyInitialSegments(t *testing.T) {
	ctx := context.Background()
	affectedSegmentIDs := []int64{1002}
	handoffProvider := registerReleaseManualFlushHandoffProvider(t, "vchannel")

	extra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: affectedSegmentIDs})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		TimeTick: 200,
		Extra:    extra,
	}, nil)

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().UseGrowingSourceFlush("vchannel").Return(true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", affectedSegmentIDs, uint64(200)).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{
				SegmentID:          1002,
				TargetOffset:       10,
				NeedReleaseHandoff: true,
				SourceMode:         metacache.FlushSourceGrowing,
			},
		}, nil)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	prepared, err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", nil)

	assert.NoError(t, err)
	assert.True(t, prepared)
	assert.Empty(t, handoffProvider.beginSegmentIDs)
	assert.False(t, handoffProvider.rolledBack)
}

func TestReleaseManualFlushPreparerSkipNonGrowingSource(t *testing.T) {
	ctx := context.Background()
	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().UseGrowingSourceFlush("vchannel").Return(false)

	preparer := NewReleaseManualFlushPreparer(mock_walmanager.NewMockManager(t), wbManager)
	prepared, err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})

	assert.NoError(t, err)
	assert.False(t, prepared)
}
