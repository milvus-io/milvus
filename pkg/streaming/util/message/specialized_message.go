package message

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// A system preserved message, should not allowed to provide outside of the streaming system.
var systemMessageType = map[MessageType]struct{}{
	MessageTypeTimeTick:    {},
	MessageTypeBeginTxn:    {},
	MessageTypeCommitTxn:   {},
	MessageTypeRollbackTxn: {},
	MessageTypeTxn:         {},
}

var selfControlledMessageType = map[MessageType]struct{}{
	MessageTypeTimeTick:      {},
	MessageTypeCreateSegment: {},
	MessageTypeFlush:         {},
}

var cipherMessageType = map[MessageType]struct{}{
	MessageTypeInsert: {},
	MessageTypeDelete: {},
}

var exclusiveRequiredMessageType = map[MessageType]struct{}{
	MessageTypeCreateCollection:     {},
	MessageTypeDropCollection:       {},
	MessageTypeCreatePartition:      {},
	MessageTypeDropPartition:        {},
	MessageTypeManualFlush:          {},
	MessageTypeSchemaChange:         {},
	MessageTypeAlterReplicateConfig: {},
	MessageTypeAlterCollection:      {},
}

// mustAsSpecializedMutableMessage converts a MutableMessage to a specialized MutableMessage.
// It will panic if the message is not the target specialized message or failed to decode the specialized header.
func mustAsSpecializedMutableMessage[H proto.Message, B proto.Message](msg BasicMessage) specializedMutableMessage[H, B] {
	smsg, err := asSpecializedMutableMessage[H, B](msg)
	if err != nil {
		panic(
			fmt.Sprintf("failed to parse mutable message: %s @ %s, %d, %d",
				err.Error(),
				msg.MessageType(),
				msg.TimeTick(),
				msg.Version(),
			))
	}
	return smsg
}

// asSpecializedMutableMessage converts a MutableMessage to a specialized MutableMessage.
// Return nil, error if the message is the target specialized message but failed to decode the specialized header.
// Return specializedMutableMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedMutableMessage[H proto.Message, B proto.Message](msg BasicMessage) (specializedMutableMessage[H, B], error) {
	if already, ok := msg.(specializedMutableMessage[H, B]); ok {
		return already, nil
	}
	underlying := msg.(*messageImpl)

	var header H
	msgType := MustGetMessageTypeWithVersion[H, B]()
	if underlying.MessageType() != msgType.MessageType {
		// The message type do not match the specialized header.
		return nil, errors.New("message type do not match specialized header")
	}

	// Get the specialized header from the message.
	val, ok := underlying.properties.Get(messageHeader)
	if !ok {
		return nil, errors.Errorf("lost specialized header, %s", msgType.String())
	}

	// Decode the specialized header.
	// Must be pointer type.
	t := reflect.TypeOf(header)
	t.Elem()
	header = reflect.New(t.Elem()).Interface().(H)

	// must be a pointer to a proto message
	if err := DecodeProto(val, header); err != nil {
		return nil, errors.Wrap(err, "failed to decode specialized header")
	}
	return &specializedMutableMessageImpl[H, B]{
		header:      header,
		messageImpl: underlying,
	}, nil
}

// MustAsSpecializedImmutableMessage converts a ImmutableMutableMessage to a specialized ImmutableMutableMessage.
// It will panic if the message is not the target specialized message or failed to decode the specialized header.
func MustAsSpecializedImmutableMessage[H proto.Message, B proto.Message](msg ImmutableMessage) SpecializedImmutableMessage[H, B] {
	smsg, err := asSpecializedImmutableMessage[H, B](msg)
	if err != nil {
		panic(
			fmt.Sprintf("failed to parse immutable message: %s @ %s, %s, %s, %d, %d",
				err.Error(),
				msg.MessageID(),
				msg.MessageType(),
				msg.LastConfirmedMessageID(),
				msg.TimeTick(),
				msg.Version(),
			))
	}
	return smsg
}

// asSpecializedImmutableMessage converts a ImmutableMessage to a specialized ImmutableMessage.
// Return nil, error if the message is the target specialized message but failed to decode the specialized header.
// Return asSpecializedImmutableMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedImmutableMessage[H proto.Message, B proto.Message](msg ImmutableMessage) (SpecializedImmutableMessage[H, B], error) {
	if already, ok := msg.(SpecializedImmutableMessage[H, B]); ok {
		return already, nil
	}
	underlying, ok := msg.(*immutableMessageImpl)
	if !ok {
		// maybe a txn message.
		return nil, errors.New("not a specialized immutable message, txn message maybe")
	}

	var header H
	msgType := MustGetMessageTypeWithVersion[H, B]()
	if underlying.MessageType() != msgType.MessageType {
		// The message type do not match the specialized header.
		return nil, errors.New("message type do not match specialized header")
	}

	// Get the specialized header from the message.
	val, ok := underlying.properties.Get(messageHeader)
	if !ok {
		return nil, errors.Errorf("lost specialized header, %s", msgType.String())
	}

	// Decode the specialized header.
	// Must be pointer type.
	t := reflect.TypeOf(header)
	header = reflect.New(t.Elem()).Interface().(H)

	// must be a pointer to a proto message
	if err := DecodeProto(val, header); err != nil {
		return nil, errors.Wrap(err, "failed to decode specialized header")
	}
	return &specializedImmutableMessageImpl[H, B]{
		header:               header,
		immutableMessageImpl: underlying,
	}, nil
}

// asSpecializedBroadcastMessage converts a BasicMessage to a specialized BroadcastMessage.
// Return nil, error if the message is not the target specialized message or failed to decode the specialized header.
// Return specializedBroadcastMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedBroadcastMessage[H proto.Message, B proto.Message](msg BasicMessage) (SpecializedBroadcastMessage[H, B], error) {
	if already, ok := msg.(SpecializedBroadcastMessage[H, B]); ok {
		return already, nil
	}
	sm, err := asSpecializedMutableMessage[H, B](msg)
	if err != nil {
		return nil, err
	}
	return sm.(*specializedMutableMessageImpl[H, B]), nil
}

// MustAsSpecializedBroadcastMessage converts a BasicMessage to a specialized BroadcastMessage.
// It will panic if the message is not the target specialized message or failed to decode the specialized header.
func MustAsSpecializedBroadcastMessage[H proto.Message, B proto.Message](msg BasicMessage) SpecializedBroadcastMessage[H, B] {
	smsg, err := asSpecializedBroadcastMessage[H, B](msg)
	if err != nil {
		panic(err)
	}
	return smsg
}

// specializedMutableMessageImpl is the specialized mutable message implementation.
type specializedMutableMessageImpl[H proto.Message, B proto.Message] struct {
	header H
	*messageImpl
}

// MessageHeader returns the message header.
func (m *specializedMutableMessageImpl[H, B]) Header() H {
	return m.header
}

// Body returns the message body.
func (m *specializedMutableMessageImpl[H, B]) Body() (B, error) {
	return unmarshalProtoB[B](m.Payload())
}

// MustBody returns the message body.
func (m *specializedMutableMessageImpl[H, B]) MustBody() B {
	b, err := m.Body()
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal specialized body,%s", err.Error()))
	}
	return b
}

// OverwriteMessageHeader overwrites the message header.
func (m *specializedMutableMessageImpl[H, B]) OverwriteHeader(header H) {
	m.header = header
	newHeader, err := EncodeProto(m.header)
	if err != nil {
		panic(fmt.Sprintf("failed to encode insert header, there's a bug, %+v, %s", m.header, err.Error()))
	}
	m.messageImpl.properties.Set(messageHeader, newHeader)
}

// OverwriteBody overwrites the message body.
func (m *specializedMutableMessageImpl[H, B]) OverwriteBody(body B) {
	payload, err := proto.Marshal(body)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal specialized body, %s", err.Error()))
	}
	m.messageImpl.payload = payload
}

// BroadcastMessage returns the broadcast message.
func (m *specializedMutableMessageImpl[H, B]) BroadcastMessage() BroadcastMutableMessage {
	return m.messageImpl
}

// specializedImmutableMessageImpl is the specialized immmutable message implementation.
type specializedImmutableMessageImpl[H proto.Message, B proto.Message] struct {
	header H
	*immutableMessageImpl
}

// Header returns the message header.
func (m *specializedImmutableMessageImpl[H, B]) Header() H {
	return m.header
}

// Body returns the message body.
func (m *specializedImmutableMessageImpl[H, B]) Body() (B, error) {
	return unmarshalProtoB[B](m.Payload())
}

// Must Body returns the message body.
func (m *specializedImmutableMessageImpl[H, B]) MustBody() B {
	b, err := m.Body()
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal specialized body, %s, %s", m.MessageID().String(), err.Error()))
	}
	return b
}

func unmarshalProtoB[B proto.Message](data []byte) (B, error) {
	var nilBody B
	// Decode the specialized header.
	// Must be pointer type.
	t := reflect.TypeOf(nilBody)
	t.Elem()
	body := reflect.New(t.Elem()).Interface().(B)

	err := proto.Unmarshal(data, body)
	if err != nil {
		return nilBody, err
	}
	return body, nil
}
