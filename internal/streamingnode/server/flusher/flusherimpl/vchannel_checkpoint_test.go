package flusherimpl

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/rmq"
)

func TestVChannelCheckpointManager(t *testing.T) {
	exists, vchannel, minimumX := generateRandomExistsMessageID()
	m := newVChannelCheckpointManager(exists)
	assert.True(t, m.MinimumCheckpoint().EQ(minimumX))

	err := m.Add("vchannel-999", rmq.NewRmqID(1000000))
	assert.Error(t, err)
	assert.True(t, m.MinimumCheckpoint().EQ(minimumX))

	err = m.Drop("vchannel-1000")
	assert.Error(t, err)
	assert.True(t, m.MinimumCheckpoint().EQ(minimumX))

	err = m.Update("vchannel-1000", rmq.NewRmqID(1000001))
	assert.Error(t, err)
	assert.True(t, m.MinimumCheckpoint().EQ(minimumX))

	err = m.Add("vchannel-1000", rmq.NewRmqID(1000001))
	assert.NoError(t, err)
	assert.True(t, m.MinimumCheckpoint().EQ(minimumX))

	err = m.Update(vchannel, rmq.NewRmqID(1000001))
	assert.NoError(t, err)
	assert.False(t, m.MinimumCheckpoint().EQ(minimumX))

	err = m.Update(vchannel, minimumX)
	assert.Error(t, err)

	err = m.Drop("vchannel-501")
	assert.NoError(t, err)
	for i := 0; i < 1001; i++ {
		m.Drop(fmt.Sprintf("vchannel-%d", i))
	}
	assert.Len(t, m.index, 0)
	assert.Len(t, m.checkpointHeap, 0)
	assert.Equal(t, m.Len(), 0)
	assert.Nil(t, m.MinimumCheckpoint())
}

func generateRandomExistsMessageID() (map[string]message.MessageID, string, message.MessageID) {
	minimumX := int64(10000000)
	var vchannel string
	exists := make(map[string]message.MessageID)
	for i := 0; i < 1000; i++ {
		x := rand.Int63n(999999) + 2
		exists[fmt.Sprintf("vchannel-%d", i)] = rmq.NewRmqID(x)
		if x < minimumX {
			minimumX = x
			vchannel = fmt.Sprintf("vchannel-%d", i)
		}
	}
	return exists, vchannel, rmq.NewRmqID(minimumX)
}
