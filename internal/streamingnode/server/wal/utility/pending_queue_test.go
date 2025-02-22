package utility

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestPendingQueue(t *testing.T) {
	pq := NewPendingQueue()

	// Test initial state
	assert.Equal(t, 0, pq.Bytes())

	// Test AddOne
	msg1 := mock_message.NewMockImmutableMessage(t)
	msg1.EXPECT().EstimateSize().Return(1)
	pq.AddOne(msg1)
	assert.Equal(t, msg1.EstimateSize(), pq.Bytes())

	// Test Add
	msg2 := mock_message.NewMockImmutableMessage(t)
	msg2.EXPECT().EstimateSize().Return(2)
	msg3 := mock_message.NewMockImmutableMessage(t)
	msg3.EXPECT().EstimateSize().Return(3)
	pq.Add([]message.ImmutableMessage{msg2, msg3})
	expectedBytes := msg1.EstimateSize() + msg2.EstimateSize() + msg3.EstimateSize()
	assert.Equal(t, expectedBytes, pq.Bytes())

	// Test UnsafeAdvance
	pq.UnsafeAdvance()
	expectedBytes -= msg1.EstimateSize()
	assert.Equal(t, expectedBytes, pq.Bytes())
}
