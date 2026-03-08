package ack

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestAckDetails(t *testing.T) {
	details := NewAckDetails()
	assert.True(t, details.Empty())
	assert.Equal(t, 0, details.Len())
	details.AddDetails(sortedDetails{
		&AckDetail{BeginTimestamp: 1, IsSync: true},
	})
	assert.True(t, details.IsNoPersistedMessage())
	assert.Equal(t, uint64(1), details.LastAllAcknowledgedTimestamp())
	details.AddDetails(sortedDetails{
		&AckDetail{BeginTimestamp: 2, LastConfirmedMessageID: walimplstest.NewTestMessageID(2)},
		&AckDetail{BeginTimestamp: 3, LastConfirmedMessageID: walimplstest.NewTestMessageID(1)},
	})
	assert.False(t, details.IsNoPersistedMessage())
	assert.Equal(t, uint64(3), details.LastAllAcknowledgedTimestamp())
	assert.True(t, details.EarliestLastConfirmedMessageID().EQ(walimplstest.NewTestMessageID(1)))

	assert.Panics(t, func() {
		details.AddDetails(sortedDetails{
			&AckDetail{BeginTimestamp: 1, IsSync: true},
		})
	})

	details.Clear()
	assert.True(t, details.Empty())
	assert.Equal(t, 0, details.Len())
}
