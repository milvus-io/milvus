package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClearReplicateHeader(t *testing.T) {
	t.Run("nil_message", func(t *testing.T) {
		result := ClearReplicateHeader(nil)
		assert.Nil(t, result)
	})

	t.Run("message_without_replicate_header", func(t *testing.T) {
		msg := NewMutableMessageBeforeAppend([]byte("payload"), map[string]string{"key": "val"})
		assert.Nil(t, msg.ReplicateHeader())

		result := ClearReplicateHeader(msg)
		assert.NotNil(t, result)
		assert.Nil(t, result.ReplicateHeader())
	})

	t.Run("message_with_replicate_header_property", func(t *testing.T) {
		// Directly set the replicate header property to simulate a replicated message.
		props := map[string]string{
			messageReplicateMesssageHeader: "some-header-data",
		}
		msg := NewMutableMessageBeforeAppend([]byte("payload"), props)

		// Property exists before clear.
		_, exists := msg.Properties().Get(messageReplicateMesssageHeader)
		assert.True(t, exists)

		result := ClearReplicateHeader(msg)
		assert.NotNil(t, result)

		// Property removed after clear.
		_, exists = result.Properties().Get(messageReplicateMesssageHeader)
		assert.False(t, exists)
	})
}
