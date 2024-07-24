package ack

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource/idalloc"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestAck(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	ctx := context.Background()

	rc := idalloc.NewMockRootCoordClient(t)
	resource.InitForTest(t, resource.OptRootCoordClient(rc))

	ackManager := NewAckManager()
	msgID := mock_message.NewMockMessageID(t)
	msgID.EXPECT().EQ(msgID).Return(true)
	ackManager.AdvanceLastConfirmedMessageID(msgID)

	ackers := map[uint64]*Acker{}
	for i := 0; i < 10; i++ {
		acker, err := ackManager.Allocate(ctx)
		assert.NoError(t, err)
		assert.True(t, acker.LastConfirmedMessageID().EQ(msgID))
		ackers[acker.Timestamp()] = acker
	}

	// notAck: [1, 2, 3, ..., 10]
	// ack: []
	details, err := ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [1, 3, ..., 10]
	// ack: [2]
	ackers[2].Ack()
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [1, 3, 5, ..., 10]
	// ack: [2, 4]
	ackers[4].Ack()
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [3, 5, ..., 10]
	// ack: [1, 2, 4]
	ackers[1].Ack()
	// notAck: [3, 5, ..., 10]
	// ack: [4]
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(details))
	assert.Equal(t, uint64(1), details[0].Timestamp)
	assert.Equal(t, uint64(2), details[1].Timestamp)

	// notAck: [3, 5, ..., 10]
	// ack: [4]
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [3]
	// ack: [4, ..., 10]
	for i := 5; i <= 10; i++ {
		ackers[uint64(i)].Ack()
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
	ackers[3].Ack()

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

	tsX.Ack()
	tsY.Ack()

	// notAck: []
	// ack: []
	details, err = ackManager.SyncAndGetAcknowledged(ctx)
	assert.NoError(t, err)
	assert.Greater(t, len(details), 2) // with some sync operation.

	// no more timestamp to ack.
	assert.Zero(t, ackManager.notAckHeap.Len())
}
