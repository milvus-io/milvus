package adaptor

import (
	"context"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// newConsumeMsgPackFromMessage leaves DML payloads encrypted until the consumer
// selects their vchannel. A pchannel replay can contain messages whose database
// and encryption zone have already been dropped.
func newConsumeMsgPackFromMessage(msgs ...message.ImmutableMessage) (*msgstream.ConsumeMsgPack, error) {
	pack := &msgstream.ConsumeMsgPack{}
	var finalErr error
	for _, msg := range msgs {
		next, err := newConsumeMsgPackFromSingleMessage(msg)
		if err != nil {
			finalErr = errors.CombineErrors(finalErr, err)
			continue
		}
		if next == nil {
			continue
		}
		if pack.StartPositions == nil {
			pack.BeginTs = next.BeginTs
			pack.StartPositions = next.StartPositions
		}
		pack.EndTs = next.EndTs
		pack.EndPositions = next.EndPositions
		pack.Msgs = append(pack.Msgs, next.Msgs...)
	}
	if pack.StartPositions == nil {
		return nil, finalErr
	}
	return pack, finalErr
}

func newConsumeMsgPackFromSingleMessage(msg message.ImmutableMessage) (*msgstream.ConsumeMsgPack, error) {
	if msg.MessageType() == message.MessageTypeTxn {
		txn := message.AsImmutableTxnMessage(msg)
		messages := make([]message.ImmutableMessage, 0, txn.Size())
		if err := txn.RangeOver(func(inner message.ImmutableMessage) error {
			messages = append(messages, inner)
			return nil
		}); err != nil {
			return nil, err
		}
		pack, err := newConsumeMsgPackFromMessage(messages...)
		if err != nil || pack == nil {
			return nil, err
		}
		traceCtx := message.ExtractTraceContext(context.Background(), msg)
		for _, inner := range pack.Msgs {
			inner.SetTraceCtx(traceCtx)
		}
		return pack, nil
	}
	if msg.Version() == message.VersionV1 &&
		(msg.MessageType() == message.MessageTypeInsert || msg.MessageType() == message.MessageTypeDelete) {
		var collectionID int64
		if msg.MessageType() == message.MessageTypeInsert {
			typed, err := message.AsImmutableInsertMessageV1(msg)
			if err != nil {
				return nil, err
			}
			collectionID = typed.Header().GetCollectionId()
		} else {
			typed, err := message.AsImmutableDeleteMessageV1(msg)
			if err != nil {
				return nil, err
			}
			collectionID = typed.Header().GetCollectionId()
		}
		pos := &msgpb.MsgPosition{
			ChannelName: msg.VChannel(),
			MsgID:       MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize(),
			Timestamp:   msg.TimeTick(),
			WALName:     commonpb.WALName(msg.WALName()),
		}
		return &msgstream.ConsumeMsgPack{
			BeginTs: msg.TimeTick(), EndTs: msg.TimeTick(),
			StartPositions: []*msgstream.MsgPosition{pos},
			EndPositions:   []*msgstream.MsgPosition{pos},
			Msgs: []msgstream.ConsumeMsg{&lazyDMLMessage{
				msg: msg, position: pos, collectionID: collectionID,
				traceCtx: message.ExtractTraceContext(context.Background(), msg),
			}},
		}, nil
	}

	// Legacy messages and non-DML messages keep their existing conversion path.
	pack, err := NewMsgPackFromMessage(msg)
	if err != nil || pack == nil {
		return nil, err
	}
	return msgstream.BuildConsumeMsgPack(pack), nil
}

var _ msgstream.ConsumeMsg = (*lazyDMLMessage)(nil)

type lazyDMLMessage struct {
	msg          message.ImmutableMessage
	position     *msgpb.MsgPosition
	collectionID int64
	traceCtx     context.Context
	mu           sync.Mutex
	decoded      msgstream.TsMsg
}

func (m *lazyDMLMessage) GetPosition() *msgpb.MsgPosition    { return m.position }
func (m *lazyDMLMessage) SetPosition(pos *msgpb.MsgPosition) { m.position = pos }
func (m *lazyDMLMessage) GetSize() int                       { return m.msg.EstimateSize() }
func (m *lazyDMLMessage) GetTimestamp() uint64               { return m.msg.TimeTick() }
func (m *lazyDMLMessage) GetVChannel() string                { return m.msg.VChannel() }
func (m *lazyDMLMessage) GetPChannel() string                { return m.position.GetChannelName() }
func (m *lazyDMLMessage) GetMessageID() []byte               { return m.position.GetMsgID() }
func (m *lazyDMLMessage) GetCollectionID() string            { return strconv.FormatInt(m.collectionID, 10) }
func (m *lazyDMLMessage) GetType() commonpb.MsgType {
	return MustGetCommonpbMsgTypeFromMessageType(m.msg.MessageType())
}
func (m *lazyDMLMessage) SetTraceCtx(ctx context.Context) { m.traceCtx = ctx }

// The application message ID and replication metadata are stored in the body.
// Dispatcher routing only uses the header-backed getters above.
func (m *lazyDMLMessage) GetID() int64 { return m.mustUnmarshal().ID() }

func (m *lazyDMLMessage) GetReplicateID() string {
	return m.mustUnmarshal().(interface{ GetBase() *commonpb.MsgBase }).GetBase().GetReplicateInfo().GetReplicateID()
}

func (m *lazyDMLMessage) mustUnmarshal() msgstream.TsMsg {
	msg, err := m.Unmarshal(UnmashalerDispatcher)
	if err != nil {
		panic(err)
	}
	return msg
}

func (m *lazyDMLMessage) Unmarshal(_ msgstream.UnmarshalDispatcher) (msgstream.TsMsg, error) {
	return m.TryUnmarshal()
}

// TryUnmarshal decrypts once and caches only a successfully decoded message.
func (m *lazyDMLMessage) TryUnmarshal() (msgstream.TsMsg, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.decoded == nil {
		payload, err := message.TryDecryptPayload(m.msg)
		if err != nil {
			return nil, err
		}
		decoded, err := fromMessagePayloadToTsMsgV1(m.msg, payload)
		if err != nil {
			return nil, err
		}
		m.decoded = decoded
	}
	m.decoded.SetPosition(m.position)
	m.decoded.SetTraceCtx(m.traceCtx)
	return m.decoded, nil
}
