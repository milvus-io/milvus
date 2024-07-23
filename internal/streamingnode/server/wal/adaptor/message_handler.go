package adaptor

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type defaultMessageHandler chan message.ImmutableMessage

func (d defaultMessageHandler) Handle(ctx context.Context, upstream <-chan message.ImmutableMessage, msg message.ImmutableMessage) (incoming message.ImmutableMessage, ok bool, err error) {
	var sendingCh chan message.ImmutableMessage
	if msg != nil {
		sendingCh = d
	}
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case msg, ok := <-upstream:
		if !ok {
			return nil, false, wal.ErrUpstreamClosed
		}
		return msg, false, nil
	case sendingCh <- msg:
		return nil, true, nil
	}
}

func (d defaultMessageHandler) Close() {
	close(d)
}

// NewMsgPackAdaptorHandler create a new message pack adaptor handler.
func NewMsgPackAdaptorHandler() *MsgPackAdaptorHandler {
	return &MsgPackAdaptorHandler{
		logger:         log.With(),
		channel:        make(chan *msgstream.MsgPack),
		pendings:       make([]message.ImmutableMessage, 0),
		pendingMsgPack: typeutil.NewMultipartQueue[*msgstream.MsgPack](),
	}
}

// MsgPackAdaptorHandler is the handler for message pack.
type MsgPackAdaptorHandler struct {
	logger         *log.MLogger
	closeOnce      sync.Once
	channel        chan *msgstream.MsgPack
	pendings       []message.ImmutableMessage                   // pendings hold the vOld message which has same time tick.
	pendingMsgPack *typeutil.MultipartQueue[*msgstream.MsgPack] // pendingMsgPack hold unsent msgPack.
}

// Chan is the channel for message.
func (m *MsgPackAdaptorHandler) Chan() <-chan *msgstream.MsgPack {
	return m.channel
}

// Handle is the callback for handling message.
func (m *MsgPackAdaptorHandler) Handle(ctx context.Context, upstream <-chan message.ImmutableMessage, msg message.ImmutableMessage) (incoming message.ImmutableMessage, ok bool, err error) {
	// not handle new message if there are pending msgPack.
	if msg != nil && m.pendingMsgPack.Len() == 0 {
		m.generateMsgPack(msg)
		ok = true
	}

	for {
		var sendCh chan<- *msgstream.MsgPack
		if m.pendingMsgPack.Len() != 0 {
			sendCh = m.channel
		}

		select {
		case <-ctx.Done():
			return nil, ok, ctx.Err()
		case msg, notClose := <-upstream:
			if !notClose {
				return nil, ok, wal.ErrUpstreamClosed
			}
			return msg, ok, nil
		case sendCh <- m.pendingMsgPack.Next():
			m.pendingMsgPack.UnsafeAdvance()
			if m.pendingMsgPack.Len() > 0 {
				continue
			}
			return nil, ok, nil
		}
	}
}

// generateMsgPack generate msgPack from message.
func (m *MsgPackAdaptorHandler) generateMsgPack(msg message.ImmutableMessage) {
	switch msg.Version() {
	case message.VersionOld:
		if len(m.pendings) != 0 {
			if msg.TimeTick() > m.pendings[0].TimeTick() {
				m.addMsgPackIntoPending(m.pendings...)
				m.pendings = nil
			}
		}
		m.pendings = append(m.pendings, msg)
	case message.VersionV1:
		if len(m.pendings) != 0 { // all previous message should be vOld.
			m.addMsgPackIntoPending(m.pendings...)
			m.pendings = nil
		}
		m.addMsgPackIntoPending(msg)
	default:
		panic("unsupported message version")
	}
}

// addMsgPackIntoPending add message into pending msgPack.
func (m *MsgPackAdaptorHandler) addMsgPackIntoPending(msgs ...message.ImmutableMessage) {
	newPack, err := adaptor.NewMsgPackFromMessage(msgs...)
	if err != nil {
		m.logger.Warn("failed to convert message to msgpack", zap.Error(err))
	}
	if newPack != nil {
		m.pendingMsgPack.AddOne(newPack)
	}
}

// Close closes the handler.
func (m *MsgPackAdaptorHandler) Close() {
	m.closeOnce.Do(func() {
		close(m.channel)
	})
}
