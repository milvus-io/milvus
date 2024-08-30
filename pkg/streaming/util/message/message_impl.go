package message

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
)

type messageImpl struct {
	payload    []byte
	properties propertiesImpl
}

// MessageType returns the type of message.
func (m *messageImpl) MessageType() MessageType {
	val, ok := m.properties.Get(messageTypeKey)
	if !ok {
		return MessageTypeUnknown
	}
	return unmarshalMessageType(val)
}

// Version returns the message format version.
func (m *messageImpl) Version() Version {
	value, ok := m.properties.Get(messageVersion)
	if !ok {
		return VersionOld
	}
	return newMessageVersionFromString(value)
}

// Payload returns payload of current message.
func (m *messageImpl) Payload() []byte {
	return m.payload
}

// Properties returns the message properties.
func (m *messageImpl) Properties() RProperties {
	return m.properties
}

// EstimateSize returns the estimated size of current message.
func (m *messageImpl) EstimateSize() int {
	// TODO: more accurate size estimation.
	return len(m.payload) + m.properties.EstimateSize()
}

// WithBarrierTimeTick sets the barrier time tick of current message.
func (m *messageImpl) WithBarrierTimeTick(tt uint64) MutableMessage {
	if m.properties.Exist(messageBarrierTimeTick) {
		panic("barrier time tick already set in properties of message")
	}
	m.properties.Set(messageBarrierTimeTick, EncodeUint64(tt))
	return m
}

// WithWALTerm sets the wal term of current message.
func (m *messageImpl) WithWALTerm(term int64) MutableMessage {
	if m.properties.Exist(messageWALTerm) {
		panic("wal term already set in properties of message")
	}
	m.properties.Set(messageWALTerm, EncodeInt64(term))
	return m
}

// WithTimeTick sets the time tick of current message.
func (m *messageImpl) WithTimeTick(tt uint64) MutableMessage {
	if m.properties.Exist(messageTimeTick) {
		panic("time tick already set in properties of message")
	}
	m.properties.Set(messageTimeTick, EncodeUint64(tt))
	return m
}

// WithLastConfirmed sets the last confirmed message id of current message.
func (m *messageImpl) WithLastConfirmed(id MessageID) MutableMessage {
	if m.properties.Exist(messageLastConfirmedIDSameWithMessageID) {
		panic("last confirmed message already set in properties of message")
	}
	if m.properties.Exist(messageLastConfirmed) {
		panic("last confirmed message already set in properties of message")
	}
	m.properties.Set(messageLastConfirmed, id.Marshal())
	return m
}

// WithLastConfirmedUseMessageID sets the last confirmed message id of current message to be the same as message id.
func (m *messageImpl) WithLastConfirmedUseMessageID() MutableMessage {
	if m.properties.Exist(messageLastConfirmedIDSameWithMessageID) {
		panic("last confirmed message already set in properties of message")
	}
	if m.properties.Exist(messageLastConfirmed) {
		panic("last confirmed message already set in properties of message")
	}
	m.properties.Set(messageLastConfirmedIDSameWithMessageID, "")
	return m
}

// WithTxnContext sets the transaction context of current message.
func (m *messageImpl) WithTxnContext(txnCtx TxnContext) MutableMessage {
	pb, err := EncodeProto(txnCtx.IntoProto())
	if err != nil {
		panic("should not happen on txn proto")
	}
	m.properties.Set(messageTxnContext, pb)
	return m
}

// IntoImmutableMessage converts current message to immutable message.
func (m *messageImpl) IntoImmutableMessage(id MessageID) ImmutableMessage {
	return &immutableMessageImpl{
		messageImpl: *m,
		id:          id,
	}
}

// TxnContext returns the transaction context of current message.
func (m *messageImpl) TxnContext() *TxnContext {
	value, ok := m.properties.Get(messageTxnContext)
	if !ok {
		return nil
	}
	txnCtx := &messagespb.TxnContext{}
	if err := DecodeProto(value, txnCtx); err != nil {
		panic(fmt.Sprintf("there's a bug in the message codes, dirty txn context %s in properties of message", value))
	}
	return NewTxnContextFromProto(txnCtx)
}

// TimeTick returns the time tick of current message.
func (m *messageImpl) TimeTick() uint64 {
	value, ok := m.properties.Get(messageTimeTick)
	if !ok {
		panic("there's a bug in the message codes, timetick lost in properties of message")
	}
	tt, err := DecodeUint64(value)
	if err != nil {
		panic(fmt.Sprintf("there's a bug in the message codes, dirty timetick %s in properties of message", value))
	}
	return tt
}

// BarrierTimeTick returns the barrier time tick of current message.
func (m *messageImpl) BarrierTimeTick() uint64 {
	value, ok := m.properties.Get(messageBarrierTimeTick)
	if !ok {
		return 0
	}
	tt, err := DecodeUint64(value)
	if err != nil {
		panic(fmt.Sprintf("there's a bug in the message codes, dirty barrier timetick %s in properties of message", value))
	}
	return tt
}

// VChannel returns the vchannel of current message.
// If the message is broadcasted, the vchannel will be empty.
func (m *messageImpl) VChannel() string {
	value, ok := m.properties.Get(messageVChannel)
	if !ok {
		return ""
	}
	return value
}

type immutableMessageImpl struct {
	messageImpl
	id MessageID
}

// WALName returns the name of message related wal.
func (m *immutableMessageImpl) WALName() string {
	return m.id.WALName()
}

// MessageID returns the message id.
func (m *immutableMessageImpl) MessageID() MessageID {
	return m.id
}

func (m *immutableMessageImpl) LastConfirmedMessageID() MessageID {
	// same with message id
	if _, ok := m.properties.Get(messageLastConfirmedIDSameWithMessageID); ok {
		return m.MessageID()
	}
	value, ok := m.properties.Get(messageLastConfirmed)
	if !ok {
		panic(fmt.Sprintf("there's a bug in the message codes, last confirmed message lost in properties of message, id: %+v", m.id))
	}
	id, err := UnmarshalMessageID(m.id.WALName(), value)
	if err != nil {
		panic(fmt.Sprintf("there's a bug in the message codes, dirty last confirmed message in properties of message, id: %+v", m.id))
	}
	return id
}

// overwriteTimeTick overwrites the time tick of current message.
func (m *immutableMessageImpl) overwriteTimeTick(timetick uint64) {
	m.properties.Delete(messageTimeTick)
	m.WithTimeTick(timetick)
}

// overwriteLastConfirmedMessageID overwrites the last confirmed message id of current message.
func (m *immutableMessageImpl) overwriteLastConfirmedMessageID(id MessageID) {
	m.properties.Delete(messageLastConfirmed)
	m.properties.Delete(messageLastConfirmedIDSameWithMessageID)
	m.WithLastConfirmed(id)
}

// EvictPayload evicts the payload of current message.
func (m *immutableMessageImpl) EvictPayload() ImmutableMessage {
	return &immutableMessageImpl{
		messageImpl: messageImpl{
			properties: m.properties,
		},
		id: m.id,
	}
}

// immutableTxnMessageImpl is a immutable transaction message.
type immutableTxnMessageImpl struct {
	immutableMessageImpl
	begin    ImmutableMessage
	messages []ImmutableMessage // the messages that wrapped by the transaction message.
	commit   ImmutableMessage
}

// Begin returns the begin message of the transaction message.
func (m *immutableTxnMessageImpl) Begin() ImmutableMessage {
	return m.begin
}

// RangeOver iterates over the underlying messages in the transaction message.
func (m *immutableTxnMessageImpl) RangeOver(fn func(ImmutableMessage) error) error {
	for _, msg := range m.messages {
		if err := fn(msg); err != nil {
			return err
		}
	}
	return nil
}

// Commit returns the commit message of the transaction message.
func (m *immutableTxnMessageImpl) Commit() ImmutableMessage {
	return m.commit
}

// Size returns the number of messages in the transaction message.
func (m *immutableTxnMessageImpl) Size() int {
	return len(m.messages)
}
