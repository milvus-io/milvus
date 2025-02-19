package adaptor

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ChanMessageHandler chan message.ImmutableMessage

func (h ChanMessageHandler) Handle(param message.HandleParam) message.HandleResult {
	var sendingCh chan message.ImmutableMessage
	if param.Message != nil {
		sendingCh = h
	}
	select {
	case <-param.Ctx.Done():
		return message.HandleResult{Error: param.Ctx.Err()}
	case msg, ok := <-param.Upstream:
		if !ok {
			panic("unreachable code: upstream should never closed")
		}
		return message.HandleResult{Incoming: msg}
	case sendingCh <- param.Message:
		return message.HandleResult{MessageHandled: true}
	}
}

func (d ChanMessageHandler) Close() {
	close(d)
}

// NewMsgPackAdaptorHandler create a new message pack adaptor handler.
func NewMsgPackAdaptorHandler() *MsgPackAdaptorHandler {
	return &MsgPackAdaptorHandler{
		channel: make(chan *msgstream.MsgPack),
		base:    NewBaseMsgPackAdaptorHandler(),
	}
}

type MsgPackAdaptorHandler struct {
	channel chan *msgstream.MsgPack
	base    *BaseMsgPackAdaptorHandler
}

// Chan is the channel for message.
func (m *MsgPackAdaptorHandler) Chan() <-chan *msgstream.MsgPack {
	return m.channel
}

// Handle is the callback for handling message.
func (m *MsgPackAdaptorHandler) Handle(param message.HandleParam) message.HandleResult {
	messageHandled := false
	// not handle new message if there are pending msgPack.
	if param.Message != nil && m.base.PendingMsgPack.Len() == 0 {
		m.base.GenerateMsgPack(param.Message)
		messageHandled = true
	}

	for {
		var sendCh chan<- *msgstream.MsgPack
		if m.base.PendingMsgPack.Len() != 0 {
			sendCh = m.channel
		}

		select {
		case <-param.Ctx.Done():
			return message.HandleResult{
				MessageHandled: messageHandled,
				Error:          param.Ctx.Err(),
			}
		case msg, ok := <-param.Upstream:
			if !ok {
				panic("unreachable code: upstream should never closed")
			}
			return message.HandleResult{
				Incoming:       msg,
				MessageHandled: messageHandled,
			}
		case sendCh <- m.base.PendingMsgPack.Next():
			m.base.PendingMsgPack.UnsafeAdvance()
			if m.base.PendingMsgPack.Len() > 0 {
				continue
			}
			return message.HandleResult{MessageHandled: messageHandled}
		}
	}
}

// Close closes the handler.
func (m *MsgPackAdaptorHandler) Close() {
	close(m.channel)
}

// NewBaseMsgPackAdaptorHandler create a new base message pack adaptor handler.
func NewBaseMsgPackAdaptorHandler() *BaseMsgPackAdaptorHandler {
	return &BaseMsgPackAdaptorHandler{
		Logger:         log.With(),
		Pendings:       make([]message.ImmutableMessage, 0),
		PendingMsgPack: typeutil.NewMultipartQueue[*msgstream.MsgPack](),
	}
}

// BaseMsgPackAdaptorHandler is the handler for message pack.
type BaseMsgPackAdaptorHandler struct {
	Logger         *log.MLogger
	Pendings       []message.ImmutableMessage                   // pendings hold the vOld message which has same time tick.
	PendingMsgPack *typeutil.MultipartQueue[*msgstream.MsgPack] // pendingMsgPack hold unsent msgPack.
}

// GenerateMsgPack generate msgPack from message.
func (m *BaseMsgPackAdaptorHandler) GenerateMsgPack(msg message.ImmutableMessage) {
	switch msg.Version() {
	case message.VersionOld:
		if len(m.Pendings) != 0 {
			// multiple message from old version may share the same time tick.
			// should be packed into one msgPack.
			if msg.TimeTick() > m.Pendings[0].TimeTick() {
				m.addMsgPackIntoPending(m.Pendings...)
				m.Pendings = nil
			}
		}
		m.Pendings = append(m.Pendings, msg)
	case message.VersionV1, message.VersionV2:
		if len(m.Pendings) != 0 { // all previous message should be vOld.
			m.addMsgPackIntoPending(m.Pendings...)
			m.Pendings = nil
		}
		m.addMsgPackIntoPending(msg)
	default:
		panic("unsupported message version")
	}
}

// addMsgPackIntoPending add message into pending msgPack.
func (m *BaseMsgPackAdaptorHandler) addMsgPackIntoPending(msgs ...message.ImmutableMessage) {
	newPack, err := NewMsgPackFromMessage(msgs...)
	if err != nil {
		m.Logger.Warn("failed to convert message to msgpack", zap.Error(err))
	}
	if newPack != nil {
		m.PendingMsgPack.AddOne(newPack)
	}
}
