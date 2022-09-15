package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func TestServerBroker_ReleaseCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withUnhealthyQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseCollection(ctx, 1)
		assert.Error(t, err)
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseCollection(ctx, 1)
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseCollection(ctx, 1)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseCollection(ctx, 1)
		assert.NoError(t, err)
	})
}

func TestServerBroker_GetSegmentInfo(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		_, err := b.GetQuerySegmentInfo(ctx, 1, []int64{1, 2})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		resp, err := b.GetQuerySegmentInfo(ctx, 1, []int64{1, 2})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		resp, err := b.GetQuerySegmentInfo(ctx, 1, []int64{1, 2})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestServerBroker_WatchChannels(t *testing.T) {
	t.Run("unhealthy", func(t *testing.T) {
		c := newTestCore(withUnhealthyDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.WatchChannels(ctx, &watchInfo{})
		assert.Error(t, err)
	})

	t.Run("failed to execute", func(t *testing.T) {
		defer cleanTestEnv()

		c := newTestCore(withInvalidDataCoord(), withRocksMqTtSynchronizer())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.WatchChannels(ctx, &watchInfo{})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		defer cleanTestEnv()

		c := newTestCore(withFailedDataCoord(), withRocksMqTtSynchronizer())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.WatchChannels(ctx, &watchInfo{})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		defer cleanTestEnv()

		c := newTestCore(withValidDataCoord(), withRocksMqTtSynchronizer())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.WatchChannels(ctx, &watchInfo{})
		assert.NoError(t, err)
	})
}

func TestServerBroker_UnwatchChannels(t *testing.T) {
	// TODO: implement
	b := newServerBroker(newTestCore())
	ctx := context.Background()
	b.UnwatchChannels(ctx, &watchInfo{})
}

func TestServerBroker_AddSegRefLock(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.AddSegRefLock(ctx, 1, []int64{1, 2})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.AddSegRefLock(ctx, 1, []int64{1, 2})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.AddSegRefLock(ctx, 1, []int64{1, 2})
		assert.NoError(t, err)
	})
}

func TestServerBroker_ReleaseSegRefLock(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseSegRefLock(ctx, 1, []int64{1, 2})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseSegRefLock(ctx, 1, []int64{1, 2})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseSegRefLock(ctx, 1, []int64{1, 2})
		assert.NoError(t, err)
	})
}

func TestServerBroker_Flush(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.Flush(ctx, 1, []int64{1, 2})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.Flush(ctx, 1, []int64{1, 2})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.Flush(ctx, 1, []int64{1, 2})
		assert.NoError(t, err)
	})
}

func TestServerBroker_Import(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		resp, err := b.Import(ctx, &datapb.ImportTaskRequest{})
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		resp, err := b.Import(ctx, &datapb.ImportTaskRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		resp, err := b.Import(ctx, &datapb.ImportTaskRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestServerBroker_DropCollectionIndex(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withUnhealthyIndexCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.DropCollectionIndex(ctx, 1)
		assert.Error(t, err)
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidIndexCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.DropCollectionIndex(ctx, 1)
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedIndexCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.DropCollectionIndex(ctx, 1)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidIndexCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.DropCollectionIndex(ctx, 1)
		assert.NoError(t, err)
	})
}

func TestServerBroker_GetSegmentIndexState(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidIndexCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		_, err := b.GetSegmentIndexState(ctx, 1, "index_name", []UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedIndexCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		_, err := b.GetSegmentIndexState(ctx, 1, "index_name", []UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidIndexCoord())
		c.indexCoord.(*mockIndexCoord).GetSegmentIndexStateFunc = func(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
			return &indexpb.GetSegmentIndexStateResponse{
				Status: succStatus(),
				States: []*indexpb.SegmentIndexState{
					{
						SegmentID:  1,
						State:      commonpb.IndexState_Finished,
						FailReason: "",
					},
				},
			}, nil
		}
		b := newServerBroker(c)
		ctx := context.Background()
		states, err := b.GetSegmentIndexState(ctx, 1, "index_name", []UniqueID{1})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(states))
		assert.Equal(t, commonpb.IndexState_Finished, states[0].GetState())
	})
}
