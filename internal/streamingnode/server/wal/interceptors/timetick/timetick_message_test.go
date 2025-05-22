package timetick

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestNewTimeTickMsg(t *testing.T) {
	ts := uint64(123456789)
	lastConfirmedMessageID := walimplstest.NewTestMessageID(1)
	messageID := walimplstest.NewTestMessageID(2)
	sourceID := int64(42)
	persist := true

	// Test with persist = true and lastConfirmedMessageID provided
	msg := NewTimeTickMsg(ts, lastConfirmedMessageID, sourceID, persist)
	immutableMsg := msg.IntoImmutableMessage(messageID)
	assert.NotNil(t, msg)
	assert.Equal(t, ts, msg.TimeTick())
	assert.True(t, immutableMsg.IsPersisted())
	assert.True(t, immutableMsg.LastConfirmedMessageID().EQ(lastConfirmedMessageID))

	// Test with persist = false and lastConfirmedMessageID provided
	persist = false
	msg = NewTimeTickMsg(ts, lastConfirmedMessageID, sourceID, persist)
	immutableMsg = msg.IntoImmutableMessage(messageID)
	assert.NotNil(t, msg)
	assert.Equal(t, ts, msg.TimeTick())
	assert.True(t, immutableMsg.LastConfirmedMessageID().EQ(lastConfirmedMessageID))
	assert.False(t, msg.IsPersisted())

	// Test with persist = false and lastConfirmedMessageID nil
	msg = NewTimeTickMsg(ts, nil, sourceID, persist)
	immutableMsg = msg.IntoImmutableMessage(messageID)
	assert.NotNil(t, msg)
	assert.Equal(t, ts, msg.TimeTick())
	assert.True(t, immutableMsg.LastConfirmedMessageID().EQ(messageID))
	assert.False(t, msg.IsPersisted())
}
