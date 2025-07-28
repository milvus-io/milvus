package message

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
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
	CreateSegmentMessageHeader    = messagespb.CreateSegmentMessageHeader
	ManualFlushMessageHeader      = messagespb.ManualFlushMessageHeader
	BeginTxnMessageHeader         = messagespb.BeginTxnMessageHeader
	CommitTxnMessageHeader        = messagespb.CommitTxnMessageHeader
	RollbackTxnMessageHeader      = messagespb.RollbackTxnMessageHeader
	TxnMessageHeader              = messagespb.TxnMessageHeader
	ImportMessageHeader           = messagespb.ImportMessageHeader
	SchemaChangeMessageHeader     = messagespb.SchemaChangeMessageHeader
)

type (
	FlushMessageBody         = messagespb.FlushMessageBody
	CreateSegmentMessageBody = messagespb.CreateSegmentMessageBody
	ManualFlushMessageBody   = messagespb.ManualFlushMessageBody
	BeginTxnMessageBody      = messagespb.BeginTxnMessageBody
	CommitTxnMessageBody     = messagespb.CommitTxnMessageBody
	RollbackTxnMessageBody   = messagespb.RollbackTxnMessageBody
	TxnMessageBody           = messagespb.TxnMessageBody
	SchemaChangeMessageBody  = messagespb.SchemaChangeMessageBody
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
	reflect.TypeOf(&CreateSegmentMessageHeader{}):    MessageTypeCreateSegment,
	reflect.TypeOf(&FlushMessageHeader{}):            MessageTypeFlush,
	reflect.TypeOf(&ManualFlushMessageHeader{}):      MessageTypeManualFlush,
	reflect.TypeOf(&BeginTxnMessageHeader{}):         MessageTypeBeginTxn,
	reflect.TypeOf(&CommitTxnMessageHeader{}):        MessageTypeCommitTxn,
	reflect.TypeOf(&RollbackTxnMessageHeader{}):      MessageTypeRollbackTxn,
	reflect.TypeOf(&TxnMessageHeader{}):              MessageTypeTxn,
	reflect.TypeOf(&ImportMessageHeader{}):           MessageTypeImport,
	reflect.TypeOf(&SchemaChangeMessageHeader{}):     MessageTypeSchemaChange,
}

// messageTypeToCustomHeaderMap maps the message type to the proto message type.
var messageTypeToCustomHeaderMap = map[MessageType]reflect.Type{
	MessageTypeTimeTick:         reflect.TypeOf(&TimeTickMessageHeader{}),
	MessageTypeInsert:           reflect.TypeOf(&InsertMessageHeader{}),
	MessageTypeDelete:           reflect.TypeOf(&DeleteMessageHeader{}),
	MessageTypeCreateCollection: reflect.TypeOf(&CreateCollectionMessageHeader{}),
	MessageTypeDropCollection:   reflect.TypeOf(&DropCollectionMessageHeader{}),
	MessageTypeCreatePartition:  reflect.TypeOf(&CreatePartitionMessageHeader{}),
	MessageTypeDropPartition:    reflect.TypeOf(&DropPartitionMessageHeader{}),
	MessageTypeCreateSegment:    reflect.TypeOf(&CreateSegmentMessageHeader{}),
	MessageTypeFlush:            reflect.TypeOf(&FlushMessageHeader{}),
	MessageTypeManualFlush:      reflect.TypeOf(&ManualFlushMessageHeader{}),
	MessageTypeBeginTxn:         reflect.TypeOf(&BeginTxnMessageHeader{}),
	MessageTypeCommitTxn:        reflect.TypeOf(&CommitTxnMessageHeader{}),
	MessageTypeRollbackTxn:      reflect.TypeOf(&RollbackTxnMessageHeader{}),
	MessageTypeTxn:              reflect.TypeOf(&TxnMessageHeader{}),
	MessageTypeImport:           reflect.TypeOf(&ImportMessageHeader{}),
	MessageTypeSchemaChange:     reflect.TypeOf(&SchemaChangeMessageHeader{}),
}

// A system preserved message, should not allowed to provide outside of the streaming system.
var systemMessageType = map[MessageType]struct{}{
	MessageTypeTimeTick:    {},
	MessageTypeBeginTxn:    {},
	MessageTypeCommitTxn:   {},
	MessageTypeRollbackTxn: {},
	MessageTypeTxn:         {},
}

var cipherMessageType = map[MessageType]struct{}{
	MessageTypeInsert: {},
	MessageTypeDelete: {},
}

var exclusiveRequiredMessageType = map[MessageType]struct{}{
	MessageTypeCreateCollection: {},
	MessageTypeDropCollection:   {},
	MessageTypeCreatePartition:  {},
	MessageTypeDropPartition:    {},
	MessageTypeManualFlush:      {},
	MessageTypeSchemaChange:     {},
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
	MutableImportMessageV1           = specializedMutableMessage[*ImportMessageHeader, *msgpb.ImportMsg]
	MutableCreateSegmentMessageV2    = specializedMutableMessage[*CreateSegmentMessageHeader, *CreateSegmentMessageBody]
	MutableFlushMessageV2            = specializedMutableMessage[*FlushMessageHeader, *FlushMessageBody]
	MutableBeginTxnMessageV2         = specializedMutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	MutableCommitTxnMessageV2        = specializedMutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	MutableRollbackTxnMessageV2      = specializedMutableMessage[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]
	MutableSchemaChangeMessageV2     = specializedMutableMessage[*SchemaChangeMessageHeader, *SchemaChangeMessageBody]

	ImmutableTimeTickMessageV1         = specializedImmutableMessage[*TimeTickMessageHeader, *msgpb.TimeTickMsg]
	ImmutableInsertMessageV1           = specializedImmutableMessage[*InsertMessageHeader, *msgpb.InsertRequest]
	ImmutableDeleteMessageV1           = specializedImmutableMessage[*DeleteMessageHeader, *msgpb.DeleteRequest]
	ImmutableCreateCollectionMessageV1 = specializedImmutableMessage[*CreateCollectionMessageHeader, *msgpb.CreateCollectionRequest]
	ImmutableDropCollectionMessageV1   = specializedImmutableMessage[*DropCollectionMessageHeader, *msgpb.DropCollectionRequest]
	ImmutableCreatePartitionMessageV1  = specializedImmutableMessage[*CreatePartitionMessageHeader, *msgpb.CreatePartitionRequest]
	ImmutableDropPartitionMessageV1    = specializedImmutableMessage[*DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	ImmutableImportMessageV1           = specializedImmutableMessage[*ImportMessageHeader, *msgpb.ImportMsg]
	ImmutableCreateSegmentMessageV2    = specializedImmutableMessage[*CreateSegmentMessageHeader, *CreateSegmentMessageBody]
	ImmutableFlushMessageV2            = specializedImmutableMessage[*FlushMessageHeader, *FlushMessageBody]
	ImmutableManualFlushMessageV2      = specializedImmutableMessage[*ManualFlushMessageHeader, *ManualFlushMessageBody]
	ImmutableBeginTxnMessageV2         = specializedImmutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	ImmutableCommitTxnMessageV2        = specializedImmutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	ImmutableRollbackTxnMessageV2      = specializedImmutableMessage[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]
	ImmutableSchemaChangeMessageV2     = specializedImmutableMessage[*SchemaChangeMessageHeader, *SchemaChangeMessageBody]
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
	AsMutableImportMessageV1           = asSpecializedMutableMessage[*ImportMessageHeader, *msgpb.ImportMsg]
	AsMutableCreateSegmentMessageV2    = asSpecializedMutableMessage[*CreateSegmentMessageHeader, *CreateSegmentMessageBody]
	AsMutableFlushMessageV2            = asSpecializedMutableMessage[*FlushMessageHeader, *FlushMessageBody]
	AsMutableManualFlushMessageV2      = asSpecializedMutableMessage[*ManualFlushMessageHeader, *ManualFlushMessageBody]
	AsMutableBeginTxnMessageV2         = asSpecializedMutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	AsMutableCommitTxnMessageV2        = asSpecializedMutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	AsMutableRollbackTxnMessageV2      = asSpecializedMutableMessage[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]

	MustAsMutableTimeTickMessageV1         = mustAsSpecializedMutableMessage[*TimeTickMessageHeader, *msgpb.TimeTickMsg]
	MustAsMutableInsertMessageV1           = mustAsSpecializedMutableMessage[*InsertMessageHeader, *msgpb.InsertRequest]
	MustAsMutableDeleteMessageV1           = mustAsSpecializedMutableMessage[*DeleteMessageHeader, *msgpb.DeleteRequest]
	MustAsMutableCreateCollectionMessageV1 = mustAsSpecializedMutableMessage[*CreateCollectionMessageHeader, *msgpb.CreateCollectionRequest]
	MustAsMutableDropCollectionMessageV1   = mustAsSpecializedMutableMessage[*DropCollectionMessageHeader, *msgpb.DropCollectionRequest]
	MustAsMutableCreatePartitionMessageV1  = mustAsSpecializedMutableMessage[*CreatePartitionMessageHeader, *msgpb.CreatePartitionRequest]
	MustAsMutableDropPartitionMessageV1    = mustAsSpecializedMutableMessage[*DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	MustAsMutableImportMessageV1           = mustAsSpecializedMutableMessage[*ImportMessageHeader, *msgpb.ImportMsg]
	MustAsMutableCreateSegmentMessageV2    = mustAsSpecializedMutableMessage[*CreateSegmentMessageHeader, *CreateSegmentMessageBody]
	MustAsMutableFlushMessageV2            = mustAsSpecializedMutableMessage[*FlushMessageHeader, *FlushMessageBody]
	MustAsMutableManualFlushMessageV2      = mustAsSpecializedMutableMessage[*ManualFlushMessageHeader, *ManualFlushMessageBody]
	MustAsMutableBeginTxnMessageV2         = mustAsSpecializedMutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	MustAsMutableCommitTxnMessageV2        = mustAsSpecializedMutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	MustAsMutableRollbackTxnMessageV2      = mustAsSpecializedMutableMessage[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]
	MustAsMutableCollectionSchemaChangeV2  = mustAsSpecializedMutableMessage[*SchemaChangeMessageHeader, *SchemaChangeMessageBody]

	AsImmutableTimeTickMessageV1         = asSpecializedImmutableMessage[*TimeTickMessageHeader, *msgpb.TimeTickMsg]
	AsImmutableInsertMessageV1           = asSpecializedImmutableMessage[*InsertMessageHeader, *msgpb.InsertRequest]
	AsImmutableDeleteMessageV1           = asSpecializedImmutableMessage[*DeleteMessageHeader, *msgpb.DeleteRequest]
	AsImmutableCreateCollectionMessageV1 = asSpecializedImmutableMessage[*CreateCollectionMessageHeader, *msgpb.CreateCollectionRequest]
	AsImmutableDropCollectionMessageV1   = asSpecializedImmutableMessage[*DropCollectionMessageHeader, *msgpb.DropCollectionRequest]
	AsImmutableCreatePartitionMessageV1  = asSpecializedImmutableMessage[*CreatePartitionMessageHeader, *msgpb.CreatePartitionRequest]
	AsImmutableDropPartitionMessageV1    = asSpecializedImmutableMessage[*DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	AsImmutableImportMessageV1           = asSpecializedImmutableMessage[*ImportMessageHeader, *msgpb.ImportMsg]
	AsImmutableCreateSegmentMessageV2    = asSpecializedImmutableMessage[*CreateSegmentMessageHeader, *CreateSegmentMessageBody]
	AsImmutableFlushMessageV2            = asSpecializedImmutableMessage[*FlushMessageHeader, *FlushMessageBody]
	AsImmutableManualFlushMessageV2      = asSpecializedImmutableMessage[*ManualFlushMessageHeader, *ManualFlushMessageBody]
	AsImmutableBeginTxnMessageV2         = asSpecializedImmutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	AsImmutableCommitTxnMessageV2        = asSpecializedImmutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	AsImmutableRollbackTxnMessageV2      = asSpecializedImmutableMessage[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]
	AsImmutableCollectionSchemaChangeV2  = asSpecializedImmutableMessage[*SchemaChangeMessageHeader, *SchemaChangeMessageBody]

	MustAsImmutableTimeTickMessageV1         = mustAsSpecializedImmutableMessage[*TimeTickMessageHeader, *msgpb.TimeTickMsg]
	MustAsImmutableInsertMessageV1           = mustAsSpecializedImmutableMessage[*InsertMessageHeader, *msgpb.InsertRequest]
	MustAsImmutableDeleteMessageV1           = mustAsSpecializedImmutableMessage[*DeleteMessageHeader, *msgpb.DeleteRequest]
	MustAsImmutableCreateCollectionMessageV1 = mustAsSpecializedImmutableMessage[*CreateCollectionMessageHeader, *msgpb.CreateCollectionRequest]
	MustAsImmutableDropCollectionMessageV1   = mustAsSpecializedImmutableMessage[*DropCollectionMessageHeader, *msgpb.DropCollectionRequest]
	MustAsImmutableCreatePartitionMessageV1  = mustAsSpecializedImmutableMessage[*CreatePartitionMessageHeader, *msgpb.CreatePartitionRequest]
	MustAsImmutableDropPartitionMessageV1    = mustAsSpecializedImmutableMessage[*DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	MustAsImmutableImportMessageV1           = mustAsSpecializedImmutableMessage[*ImportMessageHeader, *msgpb.ImportMsg]
	MustAsImmutableCreateSegmentMessageV2    = mustAsSpecializedImmutableMessage[*CreateSegmentMessageHeader, *CreateSegmentMessageBody]
	MustAsImmutableFlushMessageV2            = mustAsSpecializedImmutableMessage[*FlushMessageHeader, *FlushMessageBody]
	MustAsImmutableManualFlushMessageV2      = mustAsSpecializedImmutableMessage[*ManualFlushMessageHeader, *ManualFlushMessageBody]
	MustAsImmutableBeginTxnMessageV2         = mustAsSpecializedImmutableMessage[*BeginTxnMessageHeader, *BeginTxnMessageBody]
	MustAsImmutableCommitTxnMessageV2        = mustAsSpecializedImmutableMessage[*CommitTxnMessageHeader, *CommitTxnMessageBody]
	MustAsImmutableCollectionSchemaChangeV2  = mustAsSpecializedImmutableMessage[*SchemaChangeMessageHeader, *SchemaChangeMessageBody]
	AsImmutableTxnMessage                    = func(msg ImmutableMessage) ImmutableTxnMessage {
		underlying, ok := msg.(*immutableTxnMessageImpl)
		if !ok {
			return nil
		}
		return underlying
	}
)

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
	msgType := mustGetMessageTypeFromHeader(header)
	if underlying.MessageType() != msgType {
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

// mustAsSpecializedMutableMessage converts a ImmutableMutableMessage to a specialized ImmutableMutableMessage.
// It will panic if the message is not the target specialized message or failed to decode the specialized header.
func mustAsSpecializedImmutableMessage[H proto.Message, B proto.Message](msg ImmutableMessage) specializedImmutableMessage[H, B] {
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
func asSpecializedImmutableMessage[H proto.Message, B proto.Message](msg ImmutableMessage) (specializedImmutableMessage[H, B], error) {
	if already, ok := msg.(specializedImmutableMessage[H, B]); ok {
		return already, nil
	}
	underlying, ok := msg.(*immutableMessageImpl)
	if !ok {
		// maybe a txn message.
		return nil, errors.New("not a specialized immutable message, txn message maybe")
	}

	var header H
	msgType := mustGetMessageTypeFromHeader(header)
	if underlying.MessageType() != msgType {
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
