package adaptor

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
)

var (
	_ wal.MessageHandler = defaultMessageHandler(nil)
	_ wal.MessageHandler = (*MsgPackAdaptorHandler)(nil)
)

type defaultMessageHandler chan message.ImmutableMessage

func (h defaultMessageHandler) Handle(param wal.HandleParam) wal.HandleResult {
	var sendingCh chan message.ImmutableMessage
	if param.Message != nil {
		sendingCh = h
	}
	select {
	case <-param.Ctx.Done():
		return wal.HandleResult{Error: param.Ctx.Err()}
	case msg, ok := <-param.Upstream:
		if !ok {
			return wal.HandleResult{Error: wal.ErrUpstreamClosed}
		}
		return wal.HandleResult{Incoming: msg}
	case sendingCh <- param.Message:
		return wal.HandleResult{MessageHandled: true}
	case <-param.TimeTickChan:
		return wal.HandleResult{TimeTickUpdated: true}
	}
}

func (d defaultMessageHandler) Close() {
	close(d)
}

// NewMsgPackAdaptorHandler create a new message pack adaptor handler.
func NewMsgPackAdaptorHandler() *MsgPackAdaptorHandler {
	return &MsgPackAdaptorHandler{
		base: adaptor.NewBaseMsgPackAdaptorHandler(),
	}
}

type MsgPackAdaptorHandler struct {
	base *adaptor.BaseMsgPackAdaptorHandler
}

// Chan is the channel for message.
func (m *MsgPackAdaptorHandler) Chan() <-chan *msgstream.MsgPack {
	return m.base.Channel
}

// Handle is the callback for handling message.
func (m *MsgPackAdaptorHandler) Handle(param wal.HandleParam) wal.HandleResult {
	messageHandled := false
	// not handle new message if there are pending msgPack.
	if param.Message != nil && m.base.PendingMsgPack.Len() == 0 {
		m.base.GenerateMsgPack(param.Message)
		messageHandled = true
	}

	for {
		var sendCh chan<- *msgstream.MsgPack
		if m.base.PendingMsgPack.Len() != 0 {
			sendCh = m.base.Channel
		}

		select {
		case <-param.Ctx.Done():
			return wal.HandleResult{
				MessageHandled: messageHandled,
				Error:          param.Ctx.Err(),
			}
		case msg, notClose := <-param.Upstream:
			if !notClose {
				return wal.HandleResult{
					MessageHandled: messageHandled,
					Error:          wal.ErrUpstreamClosed,
				}
			}
			return wal.HandleResult{
				Incoming:       msg,
				MessageHandled: messageHandled,
			}
		case sendCh <- m.base.PendingMsgPack.Next():
			m.base.PendingMsgPack.UnsafeAdvance()
			if m.base.PendingMsgPack.Len() > 0 {
				continue
			}
			return wal.HandleResult{MessageHandled: messageHandled}
		case <-param.TimeTickChan:
			return wal.HandleResult{
				MessageHandled:  messageHandled,
				TimeTickUpdated: true,
			}
		}
	}
}

// Close closes the handler.
func (m *MsgPackAdaptorHandler) Close() {
	close(m.base.Channel)
}
