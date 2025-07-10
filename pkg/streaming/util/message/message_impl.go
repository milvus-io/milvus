package message

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
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
	if ch := m.cipherHeader(); ch != nil {
		cipher := mustGetCipher()
		decryptor, err := cipher.GetDecryptor(ch.EzId, ch.CollectionId, ch.SafeKey)
		if err != nil {
			panic(fmt.Sprintf("can not get decryptor for message: %s", err))
		}
		payload, err := decryptor.Decrypt(m.payload)
		if err != nil {
			panic(fmt.Sprintf("can not decrypt message: %s", err))
		}
		return payload
	}
	return m.payload
}

// Properties returns the message properties.
func (m *messageImpl) Properties() RProperties {
	return m.properties
}

// IsPersisted returns true if the message is persisted.
func (m *messageImpl) IsPersisted() bool {
	return !m.properties.Exist(messageNotPersisteted)
}

// EstimateSize returns the estimated size of current message.
func (m *messageImpl) EstimateSize() int {
	if ch := m.cipherHeader(); ch != nil {
		// if it's a cipher message, we need to estimate the size of payload before encryption.
		return int(ch.PayloadBytes) + m.properties.EstimateSize()
	}
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
	m.properties.Set(messageTimeTick, EncodeUint64(tt))
	return m
}

// WithLastConfirmed sets the last confirmed message id of current message.
func (m *messageImpl) WithLastConfirmed(id MessageID) MutableMessage {
	m.properties.Delete(messageLastConfirmedIDSameWithMessageID)
	m.properties.Set(messageLastConfirmed, id.Marshal())
	return m
}

// WithOldVersion sets the version of current message to be old version.
func (m *messageImpl) WithOldVersion() MutableMessage {
	m.properties.Set(messageVersion, VersionOld.String())
	return m
}

// WithLastConfirmedUseMessageID sets the last confirmed message id of current message to be the same as message id.
func (m *messageImpl) WithLastConfirmedUseMessageID() MutableMessage {
	m.properties.Delete(messageLastConfirmed)
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

// WithBroadcastID sets the broadcast id of current message.
func (m *messageImpl) WithBroadcastID(id uint64) BroadcastMutableMessage {
	bh := m.broadcastHeader()
	if bh == nil {
		panic("there's a bug in the message codes, broadcast header lost in properties of broadcast message")
	}
	if bh.BroadcastId != 0 {
		panic("broadcast id already set in properties of broadcast message")
	}
	bh.BroadcastId = id
	bhVal, err := EncodeProto(bh)
	if err != nil {
		panic("should not happen on broadcast header proto")
	}
	m.properties.Set(messageBroadcastHeader, bhVal)
	return m
}

// IntoImmutableMessage converts current message to immutable message.
func (m *messageImpl) IntoImmutableMessage(id MessageID) ImmutableMessage {
	// payload and id is always immutable, so we only clone the prop here is ok.
	prop := m.properties.Clone()
	return &immutableMessageImpl{
		id: id,
		messageImpl: messageImpl{
			payload:    m.payload,
			properties: prop,
		},
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
// If the message is a all channel message, it will return "".
// If the message is a broadcast message, it will panic.
func (m *messageImpl) VChannel() string {
	if m.properties.Exist(messageBroadcastHeader) && !m.properties.Exist(messageVChannel) {
		// If a message is a broadcast message, it must have a vchannel properties in it after split.
		panic("there's a bug in the message codes, vchannel lost in properties of broadcast message")
	}
	value, ok := m.properties.Get(messageVChannel)
	if !ok {
		return ""
	}
	return value
}

// BroadcastHeader returns the broadcast header of current message.
func (m *messageImpl) BroadcastHeader() *BroadcastHeader {
	header := m.broadcastHeader()
	if header == nil {
		return nil
	}
	return newBroadcastHeaderFromProto(header)
}

// broadcastHeader returns the broadcast header of current message.
func (m *messageImpl) broadcastHeader() *messagespb.BroadcastHeader {
	value, ok := m.properties.Get(messageBroadcastHeader)
	if !ok {
		return nil
	}
	header := &messagespb.BroadcastHeader{}
	if err := DecodeProto(value, header); err != nil {
		panic("can not decode broadcast header")
	}
	return header
}

// cipherHeader returns the cipher header of current message.
func (m *messageImpl) cipherHeader() *messagespb.CipherHeader {
	value, ok := m.properties.Get(messageCipherHeader)
	if !ok {
		return nil
	}
	header := &messagespb.CipherHeader{}
	if err := DecodeProto(value, header); err != nil {
		panic("can not decode cipher header")
	}
	return header
}

// SplitIntoMutableMessage splits the current broadcast message into multiple messages.
func (m *messageImpl) SplitIntoMutableMessage() []MutableMessage {
	bh := m.broadcastHeader()
	if bh == nil {
		panic("there's a bug in the message codes, broadcast header lost in properties of broadcast message")
	}
	if len(bh.Vchannels) == 0 {
		panic("there's a bug in the message codes, no vchannel in broadcast message")
	}
	if bh.BroadcastId == 0 {
		panic("there's a bug in the message codes, no broadcast id in broadcast message")
	}
	vchannels := bh.Vchannels

	vchannelExist := make(map[string]struct{}, len(vchannels))
	msgs := make([]MutableMessage, 0, len(vchannels))
	for _, vchannel := range vchannels {
		newPayload := make([]byte, len(m.payload))
		copy(newPayload, m.payload)

		newProperties := make(propertiesImpl, len(m.properties))
		for key, val := range m.properties {
			newProperties.Set(key, val)
		}
		newProperties.Set(messageVChannel, vchannel)
		if _, ok := vchannelExist[vchannel]; ok {
			panic("there's a bug in the message codes, duplicate vchannel in broadcast message")
		}
		msgs = append(msgs, &messageImpl{
			payload:    newPayload,
			properties: newProperties,
		})
		vchannelExist[vchannel] = struct{}{}
	}
	return msgs
}

// CloneMutableMessage clones the current mutable message.
func CloneMutableMessage(msg MutableMessage) MutableMessage {
	if msg == nil {
		return nil
	}
	inner := msg.(*messageImpl)
	return &messageImpl{
		payload:    inner.payload,
		properties: inner.properties.Clone(),
	}
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

// cloneForTxnBody clone the message and update timetick and last confirmed message id.
func (m *immutableMessageImpl) cloneForTxnBody(timetick uint64, LastConfirmedMessageID MessageID) *immutableMessageImpl {
	newMsg := m.clone()
	newMsg.overwriteTimeTick(timetick)
	newMsg.overwriteLastConfirmedMessageID(LastConfirmedMessageID)
	return newMsg
}

// clone clones the current message.
func (m *immutableMessageImpl) clone() *immutableMessageImpl {
	// payload and message id is always immutable, so we only clone the prop here is ok.
	return &immutableMessageImpl{
		id: m.id,
		messageImpl: messageImpl{
			payload:    m.payload,
			properties: m.properties.Clone(),
		},
	}
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

// EstimateSize returns the estimated size of current message.
func (m *immutableTxnMessageImpl) EstimateSize() int {
	size := 0
	for _, msg := range m.messages {
		size += msg.EstimateSize()
	}
	return size
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
