package timestamp

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestTimestampAck(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	ctx := context.Background()

	client := newMockRootCoordClient(t)
	allocator := NewAllocator(client)
	ackManager := NewTimestampAckManager(allocator)

	timestamps := map[uint64]*Timestamp{}
	for i := 0; i < 10; i++ {
		ts, err := ackManager.Allocate(ctx)
		assert.NoError(t, err)
		timestamps[ts.detail.Timestamp] = ts
	}

	// notAck: [1, 2, 3, ..., 10]
	// ack: []
	details, err := ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [1, 3, ..., 10]
	// ack: [2]
	timestamps[2].Ack()
	details, err = ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [1, 3, 5, ..., 10]
	// ack: [2, 4]
	timestamps[4].Ack()
	details, err = ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [3, 5, ..., 10]
	// ack: [1, 2, 4]
	timestamps[1].Ack()
	// notAck: [3, 5, ..., 10]
	// ack: [4]
	details, err = ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(details))
	assert.Equal(t, uint64(1), details[0].Timestamp)
	assert.Equal(t, uint64(2), details[1].Timestamp)

	// notAck: [3, 5, ..., 10]
	// ack: [4]
	details, err = ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [3]
	// ack: [4, ..., 10]
	for i := 5; i <= 10; i++ {
		timestamps[uint64(i)].Ack()
	}
	details, err = ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [3, ...,x, y]
	// ack: [4, ..., 10]
	tsX, err := ackManager.Allocate(ctx)
	tsY, err := ackManager.Allocate(ctx)
	details, err = ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	// notAck: [...,x, y]
	// ack: [3, ..., 10]
	timestamps[3].Ack()

	// notAck: [...,x, y]
	// ack: []
	details, err = ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Greater(t, len(details), 8) // with some sync operation.

	// notAck: []
	// ack: [11, 12]
	details, err = ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Empty(t, details)

	tsX.Ack()
	tsY.Ack()

	// notAck: []
	// ack: []
	details, err = ackManager.Sync(ctx)
	assert.NoError(t, err)
	assert.Greater(t, len(details), 2) // with some sync operation.

	// no more timestamp to ack.
	assert.Zero(t, ackManager.notAckHeap.Len())
}
