package typeconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
)

func TestDeliverFilter(t *testing.T) {
	filters := []options.DeliverFilter{
		options.DeliverFilterTimeTickGT(1),
		options.DeliverFilterTimeTickGTE(2),
		options.DeliverFilterVChannel("vchannel"),
	}
	pbFilters, err := NewProtosFromDeliverFilters(filters)
	assert.NoError(t, err)
	assert.Equal(t, len(filters), len(pbFilters))
	filters2, err := NewDeliverFiltersFromProtos(pbFilters)
	assert.NoError(t, err)
	assert.Equal(t, len(filters), len(filters2))
	for idx, filter := range filters {
		filter2 := filters2[idx]
		assert.Equal(t, filter.Type(), filter2.Type())
		switch filter.Type() {
		case options.DeliverFilterTypeTimeTickGT:
			assert.Equal(t, filter.(interface{ TimeTick() uint64 }).TimeTick(), filter2.(interface{ TimeTick() uint64 }).TimeTick())
		case options.DeliverFilterTypeTimeTickGTE:
			assert.Equal(t, filter.(interface{ TimeTick() uint64 }).TimeTick(), filter2.(interface{ TimeTick() uint64 }).TimeTick())
		case options.DeliverFilterTypeVChannel:
			assert.Equal(t, filter.(interface{ VChannel() string }).VChannel(), filter2.(interface{ VChannel() string }).VChannel())
		}
	}
}

func TestDeliverPolicy(t *testing.T) {
	policy := options.DeliverPolicyAll()
	pbPolicy, err := NewProtoFromDeliverPolicy(policy)
	assert.NoError(t, err)
	policy2, err := NewDeliverPolicyFromProto("mock", pbPolicy)
	assert.NoError(t, err)
	assert.Equal(t, policy.Policy(), policy2.Policy())

	policy = options.DeliverPolicyLatest()
	pbPolicy, err = NewProtoFromDeliverPolicy(policy)
	assert.NoError(t, err)
	policy2, err = NewDeliverPolicyFromProto("mock", pbPolicy)
	assert.NoError(t, err)
	assert.Equal(t, policy.Policy(), policy2.Policy())

	msgID := mock_message.NewMockMessageID(t)
	msgID.EXPECT().Marshal().Return([]byte("mock"))
	message.RegisterMessageIDUnmsarshaler("mock", func(b []byte) (message.MessageID, error) {
		return msgID, nil
	})

	policy = options.DeliverPolicyStartFrom(msgID)
	pbPolicy, err = NewProtoFromDeliverPolicy(policy)
	assert.NoError(t, err)
	policy2, err = NewDeliverPolicyFromProto("mock", pbPolicy)
	assert.NoError(t, err)
	assert.Equal(t, policy.Policy(), policy2.Policy())

	policy = options.DeliverPolicyStartAfter(msgID)
	pbPolicy, err = NewProtoFromDeliverPolicy(policy)
	assert.NoError(t, err)
	policy2, err = NewDeliverPolicyFromProto("mock", pbPolicy)
	assert.NoError(t, err)
	assert.Equal(t, policy.Policy(), policy2.Policy())
}
