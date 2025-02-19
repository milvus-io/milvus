package flusherimpl

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
)

func TestVChannelCheckpointManager(t *testing.T) {
	exists, vchannels, minimumX := generateRandomExistsMessageID()
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

	for _, vchannel := range vchannels {
		err = m.Update(vchannel, rmq.NewRmqID(1000001))
		assert.NoError(t, err)
	}
	assert.False(t, m.MinimumCheckpoint().EQ(minimumX))

	err = m.Update(vchannels[0], minimumX)
	assert.Error(t, err)

	err = m.Drop("vchannel-501")
	assert.NoError(t, err)
	lastMinimum := m.MinimumCheckpoint()
	for i := 0; i < 1001; i++ {
		m.Update(fmt.Sprintf("vchannel-%d", i), rmq.NewRmqID(rand.Int63n(9999999)+2))
		newMinimum := m.MinimumCheckpoint()
		assert.True(t, lastMinimum.LTE(newMinimum))
		lastMinimum = newMinimum
	}
	for i := 0; i < 1001; i++ {
		m.Drop(fmt.Sprintf("vchannel-%d", i))
		newMinimum := m.MinimumCheckpoint()
		if newMinimum != nil {
			assert.True(t, lastMinimum.LTE(newMinimum))
			lastMinimum = newMinimum
		}
	}
	assert.Len(t, m.index, 0)
	assert.Len(t, m.checkpointHeap, 0)
	assert.Equal(t, m.Len(), 0)
	assert.Nil(t, m.MinimumCheckpoint())
}

func generateRandomExistsMessageID() (map[string]message.MessageID, []string, message.MessageID) {
	minimumX := int64(10000000)
	var vchannel []string
	exists := make(map[string]message.MessageID)
	for i := 0; i < 1000; i++ {
		x := rand.Int63n(999999) + 2
		exists[fmt.Sprintf("vchannel-%d", i)] = rmq.NewRmqID(x)
		if x < minimumX {
			minimumX = x
			vchannel = []string{fmt.Sprintf("vchannel-%d", i)}
		} else if x == minimumX {
			vchannel = append(vchannel, fmt.Sprintf("vchannel-%d", i))
		}
	}
	vchannel = append(vchannel, "vchannel-1")
	exists["vchannel-1"] = rmq.NewRmqID(minimumX)
	return exists, vchannel, rmq.NewRmqID(minimumX)
}
