package message

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
)

type (
	SegmentAssignment             = messagespb.SegmentAssignment
	PartitionSegmentAssignment    = messagespb.PartitionSegmentAssignment
	TimeTickMessageHeader         = messagespb.TimeTickMessageHeader
	InsertMessageHeader           = messagespb.InsertMessageHeader
	DeleteMessageHeader           = messagespb.DeleteMessageHeader
	CreateCollectionMessageHeader = messagespb.CreateCollectionMessageHeader
	DropCollectionMessageHeader   = messagespb.DropCollectionMessageHeader
	CreatePartitionMessageHeader  = messagespb.CreatePartitionMessageHeader
	DropPartitionMessageHeader    = messagespb.DropPartitionMessageHeader
	FlushMessageHeader            = messagespb.FlushMessageHeader
	ManualFlushMessageHeader      = messagespb.ManualFlushMessageHeader
	BeginTxnMessageHeader         = messagespb.BeginTxnMessageHeader
	CommitTxnMessageHeader        = messagespb.CommitTxnMessageHeader
	RollbackTxnMessageHeader      = messagespb.RollbackTxnMessageHeader
	TxnMessageHeader              = messagespb.TxnMessageHeader
)

type (
	FlushMessageBody       = messagespb.FlushMessageBody
	ManualFlushMessageBody = messagespb.ManualFlushMessageBody
	BeginTxnMessageBody    = messagespb.BeginTxnMessageBody
	CommitTxnMessageBody   = messagespb.CommitTxnMessageBody
	RollbackTxnMessageBody = messagespb.RollbackTxnMessageBody
	TxnMessageBody         = messagespb.TxnMessageBody
)

type (
	ManualFlushExtraResponse = messagespb.ManualFlushExtraResponse
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
	reflect.TypeOf(&FlushMessageHeader{}):            MessageTypeFlush,
	reflect.TypeOf(&ManualFlushMessageHeader{}):      MessageTypeManualFlush,
	reflect.TypeOf(&BeginTxnMessageHeader{}):         MessageTypeBeginTxn,
	reflect.TypeOf(&CommitTxnMessageHeader{}):        MessageTypeCommitTxn,
	reflect.TypeOf(&RollbackTxnMessageHeader{}):      MessageTypeRollbackTxn,
	reflect.TypeOf(&TxnMessageHeader{}):              MessageTypeTxn,
}

// A system preserved message, should not allowed to provide outside of the streaming system.
var systemMessageType = map[MessageType]struct{}{
	MessageTypeTimeTick:    {},
	MessageTypeBeginTxn:    {},
	MessageTypeCommitTxn:   {},
	MessageTypeRollbackTxn: {},
	MessageTypeTxn:         {},
}

// List all specialized message types.
type (
	MutableTimeTickMessageV1         = specializedMutableMessage[*TimeTickMessageHeader, *msgpb.TimeTickMsg]
	MutableInsertMessageV1           = specializedMutableMessage[*InsertMessageHeader, *msgpb.InsertRequest]
	MutableDeleteMessageV1           = specializedMutableMessage[*DeleteMessageHeader, *msgpb.DeleteRequest]
	MutableCreateCollectionMessageV1 = specializedMutableMessage[*CreateCollectionMessageHeader, *msgpb.CreateCollectionRequest]
	MutableDropCollectionMessageV1   = specializedMutableMessage[*DropCollectionMessageHeader, *msgpb.DropCollectionRequest]
	MutableCreatePartitionMessageV1  = specializedMutableMessage[*CreatePartitionMessageHeader, *msgpb.CreatePartitionRequest]
	MutableDropPartitionMessageV1    = specializedMutableMessage[*DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	MutableFlushMessageV2            = specializedMutableMessage[*FlushMessageHeader, *FlushMessageBody]
	MutableBeginTxnMessageV2         = specializedMutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	MutableCommitTxnMessageV2        = specializedMutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	MutableRollbackTxnMessageV2      = specializedMutableMessage[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]

	ImmutableTimeTickMessageV1         = specializedImmutableMessage[*TimeTickMessageHeader, *msgpb.TimeTickMsg]
	ImmutableInsertMessageV1           = specializedImmutableMessage[*InsertMessageHeader, *msgpb.InsertRequest]
	ImmutableDeleteMessageV1           = specializedImmutableMessage[*DeleteMessageHeader, *msgpb.DeleteRequest]
	ImmutableCreateCollectionMessageV1 = specializedImmutableMessage[*CreateCollectionMessageHeader, *msgpb.CreateCollectionRequest]
	ImmutableDropCollectionMessageV1   = specializedImmutableMessage[*DropCollectionMessageHeader, *msgpb.DropCollectionRequest]
	ImmutableCreatePartitionMessageV1  = specializedImmutableMessage[*CreatePartitionMessageHeader, *msgpb.CreatePartitionRequest]
	ImmutableDropPartitionMessageV1    = specializedImmutableMessage[*DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	ImmutableFlushMessageV2            = specializedImmutableMessage[*FlushMessageHeader, *FlushMessageBody]
	ImmutableManualFlushMessageV2      = specializedImmutableMessage[*ManualFlushMessageHeader, *ManualFlushMessageBody]
	ImmutableBeginTxnMessageV2         = specializedImmutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	ImmutableCommitTxnMessageV2        = specializedImmutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	ImmutableRollbackTxnMessageV2      = specializedImmutableMessage[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]
)

// List all as functions for specialized messages.
var (
	AsMutableTimeTickMessageV1         = asSpecializedMutableMessage[*TimeTickMessageHeader, *msgpb.TimeTickMsg]
	AsMutableInsertMessageV1           = asSpecializedMutableMessage[*InsertMessageHeader, *msgpb.InsertRequest]
	AsMutableDeleteMessageV1           = asSpecializedMutableMessage[*DeleteMessageHeader, *msgpb.DeleteRequest]
	AsMutableCreateCollectionMessageV1 = asSpecializedMutableMessage[*CreateCollectionMessageHeader, *msgpb.CreateCollectionRequest]
	AsMutableDropCollectionMessageV1   = asSpecializedMutableMessage[*DropCollectionMessageHeader, *msgpb.DropCollectionRequest]
	AsMutableCreatePartitionMessageV1  = asSpecializedMutableMessage[*CreatePartitionMessageHeader, *msgpb.CreatePartitionRequest]
	AsMutableDropPartitionMessageV1    = asSpecializedMutableMessage[*DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	AsMutableFlushMessageV2            = asSpecializedMutableMessage[*FlushMessageHeader, *FlushMessageBody]
	AsMutableManualFlushMessageV2      = asSpecializedMutableMessage[*ManualFlushMessageHeader, *ManualFlushMessageBody]
	AsMutableBeginTxnMessageV2         = asSpecializedMutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	AsMutableCommitTxnMessageV2        = asSpecializedMutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	AsMutableRollbackTxnMessageV2      = asSpecializedMutableMessage[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]

	AsImmutableTimeTickMessageV1         = asSpecializedImmutableMessage[*TimeTickMessageHeader, *msgpb.TimeTickMsg]
	AsImmutableInsertMessageV1           = asSpecializedImmutableMessage[*InsertMessageHeader, *msgpb.InsertRequest]
	AsImmutableDeleteMessageV1           = asSpecializedImmutableMessage[*DeleteMessageHeader, *msgpb.DeleteRequest]
	AsImmutableCreateCollectionMessageV1 = asSpecializedImmutableMessage[*CreateCollectionMessageHeader, *msgpb.CreateCollectionRequest]
	AsImmutableDropCollectionMessageV1   = asSpecializedImmutableMessage[*DropCollectionMessageHeader, *msgpb.DropCollectionRequest]
	AsImmutableCreatePartitionMessageV1  = asSpecializedImmutableMessage[*CreatePartitionMessageHeader, *msgpb.CreatePartitionRequest]
	AsImmutableDropPartitionMessageV1    = asSpecializedImmutableMessage[*DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	AsImmutableFlushMessageV2            = asSpecializedImmutableMessage[*FlushMessageHeader, *FlushMessageBody]
	AsImmutableManualFlushMessageV2      = asSpecializedImmutableMessage[*ManualFlushMessageHeader, *ManualFlushMessageBody]
	AsImmutableBeginTxnMessageV2         = asSpecializedImmutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	AsImmutableCommitTxnMessageV2        = asSpecializedImmutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	AsImmutableRollbackTxnMessageV2      = asSpecializedImmutableMessage[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]
	AsImmutableTxnMessage                = func(msg ImmutableMessage) ImmutableTxnMessage {
		underlying, ok := msg.(*immutableTxnMessageImpl)
		if !ok {
			return nil
		}
		return underlying
	}
)

// asSpecializedMutableMessage converts a MutableMessage to a specialized MutableMessage.
// Return nil, nil if the message is not the target specialized message.
// Return nil, error if the message is the target specialized message but failed to decode the specialized header.
// Return specializedMutableMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedMutableMessage[H proto.Message, B proto.Message](msg MutableMessage) (specializedMutableMessage[H, B], error) {
	underlying := msg.(*messageImpl)

	var header H
	msgType := mustGetMessageTypeFromHeader(header)
	if underlying.MessageType() != msgType {
		// The message type do not match the specialized header.
		return nil, nil
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

// asSpecializedImmutableMessage converts a ImmutableMessage to a specialized ImmutableMessage.
// Return nil, nil if the message is not the target specialized message.
// Return nil, error if the message is the target specialized message but failed to decode the specialized header.
// Return asSpecializedImmutableMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedImmutableMessage[H proto.Message, B proto.Message](msg ImmutableMessage) (specializedImmutableMessage[H, B], error) {
	underlying, ok := msg.(*immutableMessageImpl)
	if !ok {
		// maybe a txn message.
		return nil, nil
	}

	var header H
	msgType := mustGetMessageTypeFromHeader(header)
	if underlying.MessageType() != msgType {
		// The message type do not match the specialized header.
		return nil, nil
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
	return &specializedImmutableMessageImpl[H, B]{
		header:               header,
		immutableMessageImpl: underlying,
	}, nil
}

// mustGetMessageTypeFromMessageHeader returns the message type of the given message header.
func mustGetMessageTypeFromHeader(msg proto.Message) MessageType {
	t := reflect.TypeOf(msg)
	mt, ok := messageTypeMap[t]
	if !ok {
		panic(fmt.Sprintf("unsupported message type of proto header: %s", t.Name()))
	}
	return mt
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
	return unmarshalProtoB[B](m.payload)
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
	return unmarshalProtoB[B](m.payload)
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
