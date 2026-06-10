package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type releaseManualFlushCheckBufferManager struct {
	*writebuffer.MockBufferManager
	needManualFlush bool
	err             error
}

func (m *releaseManualFlushCheckBufferManager) CheckReleaseManualFlushNeed(ctx context.Context, channel string, segmentIDs []int64) (bool, error) {
	return m.needManualFlush, m.err
}

func TestReleaseManualFlushPreparer(t *testing.T) {
	ctx := context.Background()
	releaseSegmentIDs := []int64{1001}
	affectedSegmentIDs := []int64{1002}

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
		GetGrowingFlushProgress(mock.Anything, "vchannel", releaseSegmentIDs, uint64(200)).
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
}

func TestReleaseManualFlushPreparerNoGrowingProgress(t *testing.T) {
	ctx := context.Background()
	releaseSegmentIDs := []int64{1001}

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
}

func TestReleaseManualFlushPreparerSkipNotNeeded(t *testing.T) {
	ctx := context.Background()
	wbManager := &releaseManualFlushCheckBufferManager{
		MockBufferManager: writebuffer.NewMockBufferManager(t),
		needManualFlush:   false,
	}
	wbManager.EXPECT().UseGrowingSourceFlush("vchannel").Return(true)

	preparer := NewReleaseManualFlushPreparer(mock_walmanager.NewMockManager(t), wbManager)
	prepared, err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})

	assert.NoError(t, err)
	assert.False(t, prepared)
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
