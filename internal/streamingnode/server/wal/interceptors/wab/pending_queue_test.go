package wab

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestPendingQueue(t *testing.T) {
	pq := newPendingQueue(100, 5*time.Second, newImmutableTimeTickMessage(t, 99))
	snapshot, err := pq.CreateSnapshotFromOffset(0)
	assert.NoError(t, err)
	assert.Len(t, snapshot, 1)

	snapshot, err = pq.CreateSnapshotFromExclusiveTimeTick(100)
	assert.ErrorIs(t, err, io.EOF)
	assert.Nil(t, snapshot)

	pq.Push([]message.ImmutableMessage{
		newImmutableMessage(t, 100, 10),
		newImmutableMessage(t, 101, 20),
		newImmutableMessage(t, 103, 30),
		newImmutableMessage(t, 104, 40),
	})
	assert.Equal(t, pq.CurrentOffset(), 4)
	assert.Len(t, pq.buf, 5)

	snapshot, err = pq.CreateSnapshotFromExclusiveTimeTick(100)
	assert.NoError(t, err)
	assert.Len(t, snapshot, 3)
	assert.Equal(t, snapshot[0].Message.TimeTick(), uint64(101))
	assert.Equal(t, snapshot[2].Message.TimeTick(), uint64(104))

	snapshot, err = pq.CreateSnapshotFromExclusiveTimeTick(98)
	assert.ErrorIs(t, err, ErrEvicted)
	assert.Nil(t, snapshot)

	snapshot, err = pq.CreateSnapshotFromExclusiveTimeTick(102)
	assert.NoError(t, err)
	assert.Len(t, snapshot, 2)
	assert.Equal(t, snapshot[0].Message.TimeTick(), uint64(103))
	assert.Equal(t, snapshot[1].Message.TimeTick(), uint64(104))

	snapshot, err = pq.CreateSnapshotFromExclusiveTimeTick(104)
	assert.ErrorIs(t, err, io.EOF)
	assert.Nil(t, snapshot)

	snapshot, err = pq.CreateSnapshotFromExclusiveTimeTick(105)
	assert.ErrorIs(t, err, io.EOF)
	assert.Nil(t, snapshot)

	snapshot, err = pq.CreateSnapshotFromOffset(1)
	assert.NoError(t, err)
	assert.Len(t, snapshot, 4)
	assert.Equal(t, snapshot[0].Message.TimeTick(), uint64(100))
	assert.Equal(t, snapshot[3].Message.TimeTick(), uint64(104))

	snapshot, err = pq.CreateSnapshotFromOffset(3)
	assert.NoError(t, err)
	assert.Len(t, snapshot, 2)
	assert.Equal(t, snapshot[0].Message.TimeTick(), uint64(103))
	assert.Equal(t, snapshot[1].Message.TimeTick(), uint64(104))

	snapshot, err = pq.CreateSnapshotFromOffset(4)
	assert.NoError(t, err)
	assert.Len(t, snapshot, 1)
	assert.Equal(t, snapshot[0].Message.TimeTick(), uint64(104))

	snapshot, err = pq.CreateSnapshotFromOffset(5)
	assert.ErrorIs(t, err, io.EOF)
	assert.Nil(t, snapshot)

	// push a new item will trigger eviction
	snapshot, err = pq.CreateSnapshotFromOffset(1)
	assert.NoError(t, err, io.EOF)
	assert.Len(t, snapshot, 4)
	pq.Push([]message.ImmutableMessage{
		newImmutableMessage(t, 105, 60),
	})
	assert.Equal(t, pq.CurrentOffset(), 5)
	assert.Len(t, pq.buf, 2)

	// Previous snapshot should be available.
	assert.Equal(t, snapshot[0].Message.TimeTick(), uint64(100))
	assert.Equal(t, snapshot[1].Message.TimeTick(), uint64(101))
	assert.Equal(t, snapshot[2].Message.TimeTick(), uint64(103))
	assert.Equal(t, snapshot[3].Message.TimeTick(), uint64(104))

	// offset 2 should be evcited
	snapshot, err = pq.CreateSnapshotFromOffset(3)
	assert.ErrorIs(t, err, ErrEvicted)
	assert.Nil(t, snapshot)

	// offset 3 should be ok.
	snapshot, err = pq.CreateSnapshotFromOffset(4)
	assert.NoError(t, err)
	assert.Len(t, snapshot, 2)
	assert.Equal(t, snapshot[0].Message.TimeTick(), uint64(104))
	assert.Equal(t, snapshot[1].Message.TimeTick(), uint64(105))

	// Test time based eviction
	pq = newPendingQueue(100, 10*time.Millisecond, newImmutableTimeTickMessage(t, 99))
	pq.Push([]message.ImmutableMessage{
		newImmutableMessage(t, 100, 10),
	})
	assert.Equal(t, pq.CurrentOffset(), 1)
	assert.Len(t, pq.buf, 2)
	time.Sleep(20 * time.Millisecond)
	pq.Evict()
	assert.Len(t, pq.buf, 0)

	assert.Panics(t, func() {
		pq.Push([]message.ImmutableMessage{newImmutableMessage(t, 99, 10)})
	})
}

func newImmutableMessage(t *testing.T, timetick uint64, estimateSize int) message.ImmutableMessage {
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(timetick).Maybe()
	msg.EXPECT().EstimateSize().Return(estimateSize).Maybe()
	msg.EXPECT().Version().Return(message.VersionV1).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeInsert).Maybe()
	return msg
}

func newImmutableTimeTickMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(timetick).Maybe()
	msg.EXPECT().EstimateSize().Return(0).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeTimeTick).Maybe()
	msg.EXPECT().Version().Return(message.VersionV1).Maybe()
	return msg
}
