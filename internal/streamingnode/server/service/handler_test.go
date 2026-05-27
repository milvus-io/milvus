package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestHandlerServiceGetGrowingFlushProgress(t *testing.T) {
	ctx := context.Background()
	segmentIDs := []int64{1001, 1002}

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(mock_wal.NewMockWAL(t), nil)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", segmentIDs, uint64(200)).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{
				SegmentID:          1001,
				TargetOffset:       10,
				HasGrowingProgress: true,
				SourceMode:         metacache.FlushSourceGrowing,
			},
			{
				SegmentID:    1002,
				SourceMode:   metacache.FlushSourceWriteBuffer,
				TargetOffset: 0,
			},
		}, nil)

	service := NewHandlerService(manager, wbManager)
	resp, err := service.GetGrowingFlushProgress(ctx, &streamingpb.GetGrowingFlushProgressRequest{
		Pchannel: &streamingpb.PChannelInfo{
			Name: "pchannel",
			Term: 1,
		},
		Vchannel:   "vchannel",
		SegmentIds: segmentIDs,
		FenceTs:    200,
	})

	assert.NoError(t, err)
	assert.Len(t, resp.GetProgress(), 2)
	assert.Equal(t, int64(1001), resp.GetProgress()[0].GetSegmentId())
	assert.Equal(t, int64(10), resp.GetProgress()[0].GetTargetOffset())
	assert.True(t, resp.GetProgress()[0].GetHasGrowingProgress())
	assert.Equal(t, streamingpb.GrowingFlushSourceMode_GROWING_FLUSH_SOURCE_GROWING, resp.GetProgress()[0].GetSourceMode())
	assert.Equal(t, streamingpb.GrowingFlushSourceMode_GROWING_FLUSH_SOURCE_WRITE_BUFFER, resp.GetProgress()[1].GetSourceMode())
}
