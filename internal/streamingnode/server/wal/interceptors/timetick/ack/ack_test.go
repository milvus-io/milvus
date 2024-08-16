package ack

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestAck(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	ctx := context.Background()

	counter := atomic.NewUint64(1)
	rc := mocks.NewMockRootCoordClient(t)
	rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, atr *rootcoordpb.AllocTimestampRequest, co ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
			if atr.Count > 1000 {
				panic(fmt.Sprintf("count %d is too large", atr.Count))
			}
			c := counter.Add(uint64(atr.Count))
			return &rootcoordpb.AllocTimestampResponse{
				Status:    merr.Success(),
				Timestamp: c - uint64(atr.Count),
				Count:     atr.Count,
			}, nil
		},
	)
	resource.InitForTest(t, resource.OptRootCoordClient(rc))

	ackManager := NewAckManager(0, nil)

	ackers := map[uint64]*Acker{}
	for i := 0; i < 10; i++ {
		acker, err := ackManager.Allocate(ctx)
		assert.NoError(t, err)
		ackers[acker.Timestamp()] = acker
	}

	// notAck: [1, 2, 3, ..., 10]
	// ack: []
	details, err := ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [1, 3, ..., 10]
	// ack: [2]
	ackers[2].Ack(OptSync())
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [1, 3, 5, ..., 10]
	// ack: [2, 4]
	ackers[4].Ack(OptSync())
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [3, 5, ..., 10]
	// ack: [1, 2, 4]
	ackers[1].Ack(OptSync())
	// notAck: [3, 5, ..., 10]
	// ack: [4]
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(details))
	assert.Equal(t, uint64(1), details[0].BeginTimestamp)
	assert.Equal(t, uint64(2), details[1].BeginTimestamp)

	// notAck: [3, 5, ..., 10]
	// ack: [4]
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [3]
	// ack: [4, ..., 10]
	for i := 5; i <= 10; i++ {
		ackers[uint64(i)].Ack(OptSync())
	}
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [3, ...,x, y]
	// ack: [4, ..., 10]
	tsX, err := ackManager.Allocate(ctx)
	assert.NoError(t, err)
	tsY, err := ackManager.Allocate(ctx)
	assert.NoError(t, err)
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [...,x, y]
	// ack: [3, ..., 10]
	ackers[3].Ack(OptSync())

	// notAck: [...,x, y]
	// ack: []
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Greater(t, len(details), 8) // with some sync operation.

	// notAck: []
	// ack: [11, 12]
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	tsX.Ack(OptSync())
	tsY.Ack(OptSync())

	// notAck: []
	// ack: []
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Greater(t, len(details), 2) // with some sync operation.

	// no more timestamp to ack.
	assert.Zero(t, ackManager.notAckHeap.Len())
}
