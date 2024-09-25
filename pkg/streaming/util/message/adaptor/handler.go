package adaptor

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// NewMsgPackAdaptorHandler create a new message pack adaptor handler.
func NewMsgPackAdaptorHandler() *MsgPackAdaptorHandler {
	return &MsgPackAdaptorHandler{
		base: NewBaseMsgPackAdaptorHandler(),
	}
}

// MsgPackAdaptorHandler is the handler for message pack.
type MsgPackAdaptorHandler struct {
	base *BaseMsgPackAdaptorHandler
}

// Chan is the channel for message.
func (m *MsgPackAdaptorHandler) Chan() <-chan *msgstream.MsgPack {
	return m.base.Channel
}

// Handle is the callback for handling message.
func (m *MsgPackAdaptorHandler) Handle(msg message.ImmutableMessage) {
	m.base.GenerateMsgPack(msg)
	for m.base.PendingMsgPack.Len() > 0 {
		m.base.Channel <- m.base.PendingMsgPack.Next()
		m.base.PendingMsgPack.UnsafeAdvance()
	}
}

// Close is the callback for closing message.
func (m *MsgPackAdaptorHandler) Close() {
	close(m.base.Channel)
}

// NewBaseMsgPackAdaptorHandler create a new base message pack adaptor handler.
func NewBaseMsgPackAdaptorHandler() *BaseMsgPackAdaptorHandler {
	return &BaseMsgPackAdaptorHandler{
		Logger:         log.With(),
		Channel:        make(chan *msgstream.MsgPack),
		Pendings:       make([]message.ImmutableMessage, 0),
		PendingMsgPack: typeutil.NewMultipartQueue[*msgstream.MsgPack](),
	}
}

// BaseMsgPackAdaptorHandler is the handler for message pack.
type BaseMsgPackAdaptorHandler struct {
	Logger         *log.MLogger
	Channel        chan *msgstream.MsgPack
	Pendings       []message.ImmutableMessage                   // pendings hold the vOld message which has same time tick.
	PendingMsgPack *typeutil.MultipartQueue[*msgstream.MsgPack] // pendingMsgPack hold unsent msgPack.
}

// GenerateMsgPack generate msgPack from message.
func (m *BaseMsgPackAdaptorHandler) GenerateMsgPack(msg message.ImmutableMessage) {
	switch msg.Version() {
	case message.VersionOld:
		if len(m.Pendings) != 0 {
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
