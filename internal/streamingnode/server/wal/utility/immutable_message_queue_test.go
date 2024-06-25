package utility

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func TestImmutableMessageQueue(t *testing.T) {
	q := NewImmutableMessageQueue()
	for i := 0; i < 100; i++ {
		q.Add([]message.ImmutableMessage{
			mock_message.NewMockImmutableMessage(t),
		})
		assert.Equal(t, i+1, q.Len())
	}
	for i := 100; i > 0; i-- {
		assert.NotNil(t, q.Next())
		q.UnsafeAdvance()
		assert.Equal(t, i-1, q.Len())
	}
}
