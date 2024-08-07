package options

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
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

	filter = DeliverFilterVChannel("vchannel")
	_ = filter.GetFilter().(*streamingpb.DeliverFilter_Vchannel)
}
