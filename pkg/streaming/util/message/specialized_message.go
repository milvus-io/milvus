package message

import (
	"fmt"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/milvus-io/milvus/pkg/streaming/util/message/messagepb"
)

type (
	SegmentAssignment             = messagepb.SegmentAssignment
	PartitionSegmentAssignment    = messagepb.PartitionSegmentAssignment
	TimeTickMessageHeader         = messagepb.TimeTickMessageHeader
	InsertMessageHeader           = messagepb.InsertMessageHeader
	DeleteMessageHeader           = messagepb.DeleteMessageHeader
	CreateCollectionMessageHeader = messagepb.CreateCollectionMessageHeader
	DropCollectionMessageHeader   = messagepb.DropCollectionMessageHeader
	CreatePartitionMessageHeader  = messagepb.CreatePartitionMessageHeader
	DropPartitionMessageHeader    = messagepb.DropPartitionMessageHeader
)

// messageTypeMap maps the proto message type to the message type.
var messageTypeMap = map[reflect.Type]MessageType{
	reflect.TypeOf(&TimeTickMessageHeader{}):         MessageTypeTimeTick,
	reflect.TypeOf(&InsertMessageHeader{}):           MessageTypeInsert,
	reflect.TypeOf(&DeleteMessageHeader{}):           MessageTypeDelete,
	reflect.TypeOf(&CreateCollectionMessageHeader{}): MessageTypeCreateCollection,
	reflect.TypeOf(&DropCollectionMessageHeader{}):   MessageTypeDropCollection,
	reflect.TypeOf(&CreatePartitionMessageHeader{}):  MessageTypeCreatePartition,
	reflect.TypeOf(&DropPartitionMessageHeader{}):    MessageTypeDropPartition,
}

// List all specialized message types.
type (
	MutableTimeTickMessage  = specializedMutableMessage[*TimeTickMessageHeader]
	MutableInsertMessage    = specializedMutableMessage[*InsertMessageHeader]
	MutableDeleteMessage    = specializedMutableMessage[*DeleteMessageHeader]
	MutableCreateCollection = specializedMutableMessage[*CreateCollectionMessageHeader]
	MutableDropCollection   = specializedMutableMessage[*DropCollectionMessageHeader]
	MutableCreatePartition  = specializedMutableMessage[*CreatePartitionMessageHeader]
	MutableDropPartition    = specializedMutableMessage[*DropPartitionMessageHeader]

	ImmutableTimeTickMessage  = specializedImmutableMessage[*TimeTickMessageHeader]
	ImmutableInsertMessage    = specializedImmutableMessage[*InsertMessageHeader]
	ImmutableDeleteMessage    = specializedImmutableMessage[*DeleteMessageHeader]
	ImmutableCreateCollection = specializedImmutableMessage[*CreateCollectionMessageHeader]
	ImmutableDropCollection   = specializedImmutableMessage[*DropCollectionMessageHeader]
	ImmutableCreatePartition  = specializedImmutableMessage[*CreatePartitionMessageHeader]
	ImmutableDropPartition    = specializedImmutableMessage[*DropPartitionMessageHeader]
)

// List all as functions for specialized messages.
var (
	AsMutableTimeTickMessage  = asSpecializedMutableMessage[*TimeTickMessageHeader]
	AsMutableInsertMessage    = asSpecializedMutableMessage[*InsertMessageHeader]
	AsMutableDeleteMessage    = asSpecializedMutableMessage[*DeleteMessageHeader]
	AsMutableCreateCollection = asSpecializedMutableMessage[*CreateCollectionMessageHeader]
	AsMutableDropCollection   = asSpecializedMutableMessage[*DropCollectionMessageHeader]
	AsMutableCreatePartition  = asSpecializedMutableMessage[*CreatePartitionMessageHeader]
	AsMutableDropPartition    = asSpecializedMutableMessage[*DropPartitionMessageHeader]

	AsImmutableTimeTickMessage  = asSpecializedImmutableMessage[*TimeTickMessageHeader]
	AsImmutableInsertMessage    = asSpecializedImmutableMessage[*InsertMessageHeader]
	AsImmutableDeleteMessage    = asSpecializedImmutableMessage[*DeleteMessageHeader]
	AsImmutableCreateCollection = asSpecializedImmutableMessage[*CreateCollectionMessageHeader]
	AsImmutableDropCollection   = asSpecializedImmutableMessage[*DropCollectionMessageHeader]
	AsImmutableCreatePartition  = asSpecializedImmutableMessage[*CreatePartitionMessageHeader]
	AsImmutableDropPartition    = asSpecializedImmutableMessage[*DropPartitionMessageHeader]
)

// asSpecializedMutableMessage converts a MutableMessage to a specialized MutableMessage.
// Return nil, nil if the message is not the target specialized message.
// Return nil, error if the message is the target specialized message but failed to decode the specialized header.
// Return specializedMutableMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedMutableMessage[H proto.Message](msg MutableMessage) (specializedMutableMessage[H], error) {
	underlying := msg.(*messageImpl)

	var header H
	msgType := mustGetMessageTypeFromMessageHeader(header)
	if underlying.MessageType() != msgType {
		// The message type do not match the specialized header.
		return nil, nil
	}

	// Get the specialized header from the message.
	val, ok := underlying.properties.Get(messageSpecialiedHeader)
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
	return &specializedMutableMessageImpl[H]{
		header:      header,
		messageImpl: underlying,
	}, nil
}

// asSpecializedImmutableMessage converts a ImmutableMessage to a specialized ImmutableMessage.
// Return nil, nil if the message is not the target specialized message.
// Return nil, error if the message is the target specialized message but failed to decode the specialized header.
// Return asSpecializedImmutableMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedImmutableMessage[H proto.Message](msg ImmutableMessage) (specializedImmutableMessage[H], error) {
	underlying := msg.(*immutableMessageImpl)

	var header H
	msgType := mustGetMessageTypeFromMessageHeader(header)
	if underlying.MessageType() != msgType {
		// The message type do not match the specialized header.
		return nil, nil
	}

	// Get the specialized header from the message.
	val, ok := underlying.properties.Get(messageSpecialiedHeader)
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
	return &specializedImmutableMessageImpl[H]{
		header:               header,
		immutableMessageImpl: underlying,
	}, nil
}

// mustGetMessageTypeFromMessageHeader returns the message type of the given message header.
func mustGetMessageTypeFromMessageHeader(msg proto.Message) MessageType {
	t := reflect.TypeOf(msg)
	mt, ok := messageTypeMap[t]
	if !ok {
		panic(fmt.Sprintf("unsupported message type of proto header: %s", t.Name()))
	}
	return mt
}

// specializedMutableMessageImpl is the specialized mutable message implementation.
type specializedMutableMessageImpl[H proto.Message] struct {
	header H
	*messageImpl
}

// MessageHeader returns the message header.
func (m *specializedMutableMessageImpl[H]) MessageHeader() H {
	return m.header
}

// OverwriteMessageHeader overwrites the message header.
func (m *specializedMutableMessageImpl[H]) OverwriteMessageHeader(header H) {
	m.header = header
	newHeader, err := EncodeProto(m.header)
	if err != nil {
		panic(fmt.Sprintf("failed to encode insert header, there's a bug, %+v, %s", m.header, err.Error()))
	}
	m.messageImpl.properties.Set(messageSpecialiedHeader, newHeader)
}

// specializedImmutableMessageImpl is the specialized immmutable message implementation.
type specializedImmutableMessageImpl[H proto.Message] struct {
	header H
	*immutableMessageImpl
}

// MessageHeader returns the message header.
func (m *specializedImmutableMessageImpl[H]) MessageHeader() H {
	return m.header
}
