package idalloc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

func TestTimestampAllocator(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	client := NewMockRootCoordClient(t)
	f := syncutil.NewFuture[types.RootCoordClient]()
	f.Set(client)

	allocator := NewTSOAllocator(f)

	for i := 0; i < 5000; i++ {
		ts, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.NotZero(t, ts)
	}

	for i := 0; i < 100; i++ {
		ts, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.NotZero(t, ts)
		time.Sleep(time.Millisecond * 1)
		allocator.Sync()
	}

	// error test
	client.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Unset()
	client.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, atr *rootcoordpb.AllocTimestampRequest, co ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
			return &rootcoordpb.AllocTimestampResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_ForceDeny,
				},
			}, nil
		},
	)
	f = syncutil.NewFuture[types.RootCoordClient]()
	f.Set(client)

	allocator = NewTSOAllocator(f)
	_, err := allocator.Allocate(context.Background())
	assert.Error(t, err)
}

func TestIDAllocator(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	client := NewMockRootCoordClient(t)
	f := syncutil.NewFuture[types.RootCoordClient]()
	f.Set(client)

	allocator := NewIDAllocator(f)

	// Make local dirty
	allocator.Allocate(context.Background())
	// Test barrier fast path.
	resp, err := client.AllocID(context.Background(), &rootcoordpb.AllocIDRequest{
		Count: 100,
	})
	assert.NoError(t, err)
	err = allocator.BarrierUntil(context.Background(), uint64(resp.ID))
	assert.NoError(t, err)
	newBarrierTimeTick, err := allocator.Allocate(context.Background())
	assert.NoError(t, err)
	assert.Greater(t, newBarrierTimeTick, uint64(resp.ID))

	// Test slow path.
	ch := make(chan struct{})
	go func() {
		barrier := newBarrierTimeTick + 1*batchAllocateSize
		err := allocator.BarrierUntil(context.Background(), barrier)
		assert.NoError(t, err)
		newBarrierTimeTick, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.Greater(t, newBarrierTimeTick, barrier)
		close(ch)
	}()
	select {
	case <-ch:
		assert.Fail(t, "should not finish")
	case <-time.After(time.Millisecond * 20):
	}
	allocator.Sync()
	_, err = allocator.Allocate(context.Background())
	assert.NoError(t, err)
	<-ch

	allocator.SyncIfExpired(time.Millisecond * 50)
	time.Sleep(time.Millisecond * 10)
	allocator.SyncIfExpired(time.Millisecond * 10)
}
