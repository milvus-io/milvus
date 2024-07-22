package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/rmq"
)

func TestMsgPackAdaptorHandler(t *testing.T) {
	messageID := rmq.NewRmqID(1)
	tt := uint64(100)
	msg := message.CreateTestInsertMessage(
		t,
		1,
		1000,
		tt,
		messageID,
	)
	immutableMsg := msg.IntoImmutableMessage(messageID)

	upstream := make(chan message.ImmutableMessage, 1)

	ctx := context.Background()
	h := NewMsgPackAdaptorHandler()
	done := make(chan struct{})
	go func() {
		for range h.Chan() {
		}
		close(done)
	}()
	upstream <- immutableMsg
	newMsg, ok, err := h.Handle(ctx, upstream, nil)
	assert.Equal(t, newMsg, immutableMsg)
	assert.False(t, ok)
	assert.NoError(t, err)

	newMsg, ok, err = h.Handle(ctx, upstream, newMsg)
	assert.NoError(t, err)
	assert.Nil(t, newMsg)
	assert.True(t, ok)
	h.Close()

	<-done
}

func TestDefaultHandler(t *testing.T) {
	h := make(defaultMessageHandler, 1)
	done := make(chan struct{})
	go func() {
		for range h {
		}
		close(done)
	}()

	upstream := make(chan message.ImmutableMessage, 1)
	msg := mock_message.NewMockImmutableMessage(t)
	upstream <- msg
	newMsg, ok, err := h.Handle(context.Background(), upstream, nil)
	assert.NotNil(t, newMsg)
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, newMsg, msg)

	newMsg, ok, err = h.Handle(context.Background(), upstream, newMsg)
	assert.NoError(t, err)
	assert.Nil(t, newMsg)
	assert.True(t, ok)

	h.Close()
	<-done
}
