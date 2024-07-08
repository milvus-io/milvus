package options

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
)

func TestDeliver(t *testing.T) {
	policy := DeliverPolicyAll()
	assert.Equal(t, DeliverPolicyTypeAll, policy.Policy())
	assert.Panics(t, func() {
		policy.MessageID()
	})

	policy = DeliverPolicyLatest()
	assert.Equal(t, DeliverPolicyTypeLatest, policy.Policy())
	assert.Panics(t, func() {
		policy.MessageID()
	})

	messageID := mock_message.NewMockMessageID(t)
	policy = DeliverPolicyStartFrom(messageID)
	assert.Equal(t, DeliverPolicyTypeStartFrom, policy.Policy())
	assert.Equal(t, messageID, policy.MessageID())

	policy = DeliverPolicyStartAfter(messageID)
	assert.Equal(t, DeliverPolicyTypeStartAfter, policy.Policy())
	assert.Equal(t, messageID, policy.MessageID())
}
