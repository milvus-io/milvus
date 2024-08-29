package adaptor

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	msgpulsar "github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/rmq"
)

func TestIDConvension(t *testing.T) {
	id := MustGetMessageIDFromMQWrapperID(MustGetMQWrapperIDFromMessage(rmq.NewRmqID(1)))
	assert.True(t, id.EQ(rmq.NewRmqID(1)))

	msgID := pulsar.EarliestMessageID()
	id = MustGetMessageIDFromMQWrapperID(MustGetMQWrapperIDFromMessage(msgpulsar.NewPulsarID(msgID)))
	assert.True(t, id.EQ(msgpulsar.NewPulsarID(msgID)))
}
