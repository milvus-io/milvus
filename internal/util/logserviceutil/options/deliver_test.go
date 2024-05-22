package options

import (
	"testing"

	mock_message "github.com/milvus-io/milvus/internal/mocks/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/stretchr/testify/assert"
)

func TestDeliver(t *testing.T) {
	policy := DeliverAll()
	_ = policy.Policy.(*logpb.DeliverPolicy_All)

	policy = DeliverLatest()
	_ = policy.Policy.(*logpb.DeliverPolicy_Latest)

	// TODO: No assert should be in NewPBMessageIDFromMessageID.
	// Remove the panic testing.
	assert.Panics(t, func() {
		messageID := mock_message.NewMockMessageID(t)
		policy = DeliverStartFrom(messageID)
		from := policy.Policy.(*logpb.DeliverPolicy_StartFrom)
		assert.Equal(t, messageID, from.StartFrom)
	})

	assert.Panics(t, func() {
		messageID := mock_message.NewMockMessageID(t)
		policy = DeliverStartAfter(messageID)
		after := policy.Policy.(*logpb.DeliverPolicy_StartAfter)
		assert.Equal(t, messageID, after.StartAfter)
	})
}
