package options

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
)

func TestDeliverPolicy(t *testing.T) {
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

func TestDeliverFilter(t *testing.T) {
	filter := DeliverFilterTimeTickGT(1)
	assert.Equal(t, uint64(1), filter.(interface{ TimeTick() uint64 }).TimeTick())
	assert.Equal(t, DeliverFilterTypeTimeTickGT, filter.Type())
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(uint64(1))
	assert.False(t, filter.Filter(msg))
	msg.EXPECT().TimeTick().Unset()
	msg.EXPECT().TimeTick().Return(uint64(2))
	assert.True(t, filter.Filter(msg))

	filter = DeliverFilterTimeTickGTE(2)
	assert.Equal(t, uint64(2), filter.(interface{ TimeTick() uint64 }).TimeTick())
	assert.Equal(t, DeliverFilterTypeTimeTickGTE, filter.Type())
	msg.EXPECT().TimeTick().Unset()
	msg.EXPECT().TimeTick().Return(uint64(1))
	assert.False(t, filter.Filter(msg))
	msg.EXPECT().TimeTick().Unset()
	msg.EXPECT().TimeTick().Return(uint64(2))
	assert.True(t, filter.Filter(msg))

	filter = DeliverFilterVChannel("vchannel")
	assert.Equal(t, "vchannel", filter.(interface{ VChannel() string }).VChannel())
	assert.Equal(t, DeliverFilterTypeVChannel, filter.Type())
	msg.EXPECT().VChannel().Unset()
	msg.EXPECT().VChannel().Return("vchannel2")
	assert.False(t, filter.Filter(msg))
	msg.EXPECT().VChannel().Unset()
	msg.EXPECT().VChannel().Return("vchannel")
	assert.True(t, filter.Filter(msg))
}
