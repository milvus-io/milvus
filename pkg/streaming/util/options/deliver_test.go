package options

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func TestDeliverPolicy(t *testing.T) {
	policy := DeliverPolicyAll()
	_ = policy.GetPolicy().(*streamingpb.DeliverPolicy_All)

	policy = DeliverPolicyLatest()
	_ = policy.GetPolicy().(*streamingpb.DeliverPolicy_Latest)

	messageID := mock_message.NewMockMessageID(t)
	messageID.EXPECT().Marshal().Return("messageID")
	policy = DeliverPolicyStartFrom(messageID)
	_ = policy.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)

	policy = DeliverPolicyStartAfter(messageID)
	_ = policy.GetPolicy().(*streamingpb.DeliverPolicy_StartAfter)
}

func TestDeliverFilter(t *testing.T) {
	filter := DeliverFilterTimeTickGT(1)
	_ = filter.GetFilter().(*streamingpb.DeliverFilter_TimeTickGt)

	filter = DeliverFilterTimeTickGTE(2)
	_ = filter.GetFilter().(*streamingpb.DeliverFilter_TimeTickGte)

	filter = DeliverFilterMessageType(message.MessageTypeDelete)
	_ = filter.GetFilter().(*streamingpb.DeliverFilter_MessageType)
}

func TestNewMessageFilter(t *testing.T) {
	filters := []DeliverFilter{
		DeliverFilterTimeTickGT(1),
	}
	filterFunc := GetFilterFunc(filters)

	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(nil).Maybe()
	assert.False(t, filterFunc(msg))

	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(nil).Maybe()
	assert.False(t, filterFunc(msg))

	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(2).Maybe()
	msg.EXPECT().TxnContext().Return(nil).Maybe()
	assert.True(t, filterFunc(msg))

	// if message is a txn message, it should be only filterred by time tick when the message type is commit.
	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(&message.TxnContext{}).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeCommitTxn).Maybe()
	assert.False(t, filterFunc(msg))

	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(&message.TxnContext{}).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeInsert).Maybe()
	assert.True(t, filterFunc(msg))

	// if message is a txn message, it should be only filterred by time tick when the message type is commit.
	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(&message.TxnContext{}).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeCommitTxn).Maybe()
	assert.False(t, filterFunc(msg))

	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(&message.TxnContext{}).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeInsert).Maybe()
	assert.True(t, filterFunc(msg))

	filters = []*streamingpb.DeliverFilter{
		DeliverFilterTimeTickGTE(1),
	}
	filterFunc = GetFilterFunc(filters)

	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(nil).Maybe()
	assert.True(t, filterFunc(msg))

	// if message is a txn message, it should be only filterred by time tick when the message type is commit.
	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(&message.TxnContext{}).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeCommitTxn).Maybe()
	assert.True(t, filterFunc(msg))

	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().TimeTick().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(&message.TxnContext{}).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeInsert).Maybe()
	assert.True(t, filterFunc(msg))

	filters = []*streamingpb.DeliverFilter{
		DeliverFilterMessageType(message.MessageTypeInsert, message.MessageTypeDelete),
	}
	filterFunc = GetFilterFunc(filters)

	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageType().Return(message.MessageTypeInsert).Maybe()
	assert.True(t, filterFunc(msg))

	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageType().Return(message.MessageTypeDelete).Maybe()
	assert.True(t, filterFunc(msg))

	msg = mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageType().Return(message.MessageTypeFlush).Maybe()
	assert.False(t, filterFunc(msg))
}
