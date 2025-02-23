package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
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
	resp := h.Handle(message.HandleParam{
		Ctx:      ctx,
		Upstream: upstream,
		Message:  nil,
	})
	assert.Equal(t, resp.Incoming, immutableMsg)
	assert.False(t, resp.MessageHandled)
	assert.NoError(t, resp.Error)

	resp = h.Handle(message.HandleParam{
		Ctx:      ctx,
		Upstream: upstream,
		Message:  resp.Incoming,
	})
	assert.NoError(t, resp.Error)
	assert.Nil(t, resp.Incoming)
	assert.True(t, resp.MessageHandled)
	h.Close()

	<-done
}

func TestDefaultHandler(t *testing.T) {
	h := make(ChanMessageHandler, 1)
	done := make(chan struct{})
	go func() {
		for range h {
		}
		close(done)
	}()

	upstream := make(chan message.ImmutableMessage, 1)
	msg := mock_message.NewMockImmutableMessage(t)
	upstream <- msg
	resp := h.Handle(message.HandleParam{
		Ctx:      context.Background(),
		Upstream: upstream,
		Message:  nil,
	})
	assert.NotNil(t, resp.Incoming)
	assert.NoError(t, resp.Error)
	assert.False(t, resp.MessageHandled)
	assert.Equal(t, resp.Incoming, msg)

	resp = h.Handle(message.HandleParam{
		Ctx:      context.Background(),
		Upstream: upstream,
		Message:  resp.Incoming,
	})
	assert.NoError(t, resp.Error)
	assert.Nil(t, resp.Incoming)
	assert.True(t, resp.MessageHandled)

	h.Close()
	<-done
}
