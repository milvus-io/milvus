package message

import (
	"reflect"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

// NewMutableMessage creates a new mutable message.
// !!! Only used at server side for streamingnode internal service, don't use it at client side.
func NewMutableMessage(payload []byte, properties map[string]string) MutableMessage {
	return &messageImpl{
		payload:    payload,
		properties: properties,
	}
}

// NewImmutableMessage creates a new immutable message.
// !!! Only used at server side for streaming internal service, don't use it at client side.
func NewImmutableMesasge(
	id MessageID,
	payload []byte,
	properties map[string]string,
) ImmutableMessage {
	return &immutableMessageImpl{
		id: id,
		messageImpl: messageImpl{
			payload:    payload,
			properties: properties,
		},
	}
}

// List all type-safe mutable message builders here.
var (
	NewTimeTickMessageBuilderV1         = createNewMessageBuilderV1[*TimeTickMessageHeader, *msgpb.TimeTickMsg]()
	NewInsertMessageBuilderV1           = createNewMessageBuilderV1[*InsertMessageHeader, *msgpb.InsertRequest]()
	NewDeleteMessageBuilderV1           = createNewMessageBuilderV1[*DeleteMessageHeader, *msgpb.DeleteRequest]()
	NewCreateCollectionMessageBuilderV1 = createNewMessageBuilderV1[*CreateCollectionMessageHeader, *msgpb.CreateCollectionRequest]()
	NewDropCollectionMessageBuilderV1   = createNewMessageBuilderV1[*DropCollectionMessageHeader, *msgpb.DropCollectionRequest]()
	NewCreatePartitionMessageBuilderV1  = createNewMessageBuilderV1[*CreatePartitionMessageHeader, *msgpb.CreatePartitionRequest]()
	NewDropPartitionMessageBuilderV1    = createNewMessageBuilderV1[*DropPartitionMessageHeader, *msgpb.DropPartitionRequest]()
	NewFlushMessageBuilderV2            = createNewMessageBuilderV2[*FlushMessageHeader, *FlushMessageBody]()
	NewManualFlushMessageBuilderV2      = createNewMessageBuilderV2[*ManualFlushMessageHeader, *ManualFlushMessageBody]()
	NewBeginTxnMessageBuilderV2         = createNewMessageBuilderV2[*BeginTxnMessageHeader, *BeginTxnMessageBody]()
	NewCommitTxnMessageBuilderV2        = createNewMessageBuilderV2[*CommitTxnMessageHeader, *CommitTxnMessageBody]()
	NewRollbackTxnMessageBuilderV2      = createNewMessageBuilderV2[*RollbackTxnMessageHeader, *RollbackTxnMessageBody]()
	newTxnMessageBuilderV2              = createNewMessageBuilderV2[*TxnMessageHeader, *TxnMessageBody]()
)

// createNewMessageBuilderV1 creates a new message builder with v1 marker.
func createNewMessageBuilderV1[H proto.Message, B proto.Message]() func() *mutableMesasgeBuilder[H, B] {
	return func() *mutableMesasgeBuilder[H, B] {
		return newMutableMessageBuilder[H, B](VersionV1)
	}
}

// List all type-safe mutable message builders here.
func createNewMessageBuilderV2[H proto.Message, B proto.Message]() func() *mutableMesasgeBuilder[H, B] {
	return func() *mutableMesasgeBuilder[H, B] {
		return newMutableMessageBuilder[H, B](VersionV2)
	}
}

// newMutableMessageBuilder creates a new builder.
// Should only used at client side.
func newMutableMessageBuilder[H proto.Message, B proto.Message](v Version) *mutableMesasgeBuilder[H, B] {
	var h H
	messageType := mustGetMessageTypeFromHeader(h)
	properties := make(propertiesImpl)
	properties.Set(messageTypeKey, messageType.marshal())
	properties.Set(messageVersion, v.String())
	return &mutableMesasgeBuilder[H, B]{
		properties: properties,
	}
}

// mutableMesasgeBuilder is the builder for message.
type mutableMesasgeBuilder[H proto.Message, B proto.Message] struct {
	header     H
	body       B
	properties propertiesImpl
	broadcast  bool
}

// WithMessageHeader creates a new builder with determined message type.
func (b *mutableMesasgeBuilder[H, B]) WithHeader(h H) *mutableMesasgeBuilder[H, B] {
	b.header = h
	return b
}

// WithBody creates a new builder with message body.
func (b *mutableMesasgeBuilder[H, B]) WithBody(body B) *mutableMesasgeBuilder[H, B] {
	b.body = body
	return b
}

// WithVChannel creates a new builder with virtual channel.
func (b *mutableMesasgeBuilder[H, B]) WithVChannel(vchannel string) *mutableMesasgeBuilder[H, B] {
	if b.broadcast {
		panic("a broadcast message cannot hold vchannel")
	}
	b.WithProperty(messageVChannel, vchannel)
	return b
}

// WithBroadcast creates a new builder with broadcast property.
func (b *mutableMesasgeBuilder[H, B]) WithBroadcast() *mutableMesasgeBuilder[H, B] {
	b.broadcast = true
	return b
}

// WithProperty creates a new builder with message property.
// A key started with '_' is reserved for streaming system, should never used at user of client.
func (b *mutableMesasgeBuilder[H, B]) WithProperty(key string, val string) *mutableMesasgeBuilder[H, B] {
	b.properties.Set(key, val)
	return b
}

// WithProperties creates a new builder with message properties.
// A key started with '_' is reserved for streaming system, should never used at user of client.
func (b *mutableMesasgeBuilder[H, B]) WithProperties(kvs map[string]string) *mutableMesasgeBuilder[H, B] {
	for key, val := range kvs {
		b.properties.Set(key, val)
	}
	return b
}

// BuildMutable builds a mutable message.
// Panic if not set payload and message type.
// should only used at client side.
func (b *mutableMesasgeBuilder[H, B]) BuildMutable() (MutableMessage, error) {
	// payload and header must be a pointer
	if reflect.ValueOf(b.header).IsNil() {
		panic("message builder not ready for header field")
	}
	if reflect.ValueOf(b.body).IsNil() {
		panic("message builder not ready for body field")
	}
	if !b.broadcast && !b.properties.Exist(messageVChannel) {
		panic("a non broadcast message builder not ready for vchannel field")
	}

	// setup header.
	sp, err := EncodeProto(b.header)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode header")
	}
	b.properties.Set(messageHeader, sp)

	payload, err := proto.Marshal(b.body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal body")
	}
	return &messageImpl{
		payload:    payload,
		properties: b.properties,
	}, nil
}

// NewImmutableTxnMessageBuilder creates a new txn builder.
func NewImmutableTxnMessageBuilder(begin ImmutableBeginTxnMessageV2) *ImmutableTxnMessageBuilder {
	return &ImmutableTxnMessageBuilder{
		txnCtx:   *begin.TxnContext(),
		begin:    begin,
		messages: make([]ImmutableMessage, 0),
	}
}

// ImmutableTxnMessageBuilder is a builder for txn message.
type ImmutableTxnMessageBuilder struct {
	txnCtx   TxnContext
	begin    ImmutableBeginTxnMessageV2
	messages []ImmutableMessage
}

// ExpiredTimeTick returns the expired time tick of the txn.
func (b *ImmutableTxnMessageBuilder) ExpiredTimeTick() uint64 {
	if len(b.messages) > 0 {
		return tsoutil.AddPhysicalDurationOnTs(b.messages[len(b.messages)-1].TimeTick(), b.txnCtx.Keepalive)
	}
	return tsoutil.AddPhysicalDurationOnTs(b.begin.TimeTick(), b.txnCtx.Keepalive)
}

// Push pushes a message into the txn builder.
func (b *ImmutableTxnMessageBuilder) Add(msg ImmutableMessage) *ImmutableTxnMessageBuilder {
	b.messages = append(b.messages, msg)
	return b
}

// Build builds a txn message.
func (b *ImmutableTxnMessageBuilder) Build(commit ImmutableCommitTxnMessageV2) (ImmutableTxnMessage, error) {
	msg, err := newImmutableTxnMesasgeFromWAL(b.begin, b.messages, commit)
	b.begin = nil
	b.messages = nil
	return msg, err
}

// newImmutableTxnMesasgeFromWAL creates a new immutable transaction message.
func newImmutableTxnMesasgeFromWAL(
	begin ImmutableBeginTxnMessageV2,
	body []ImmutableMessage,
	commit ImmutableCommitTxnMessageV2,
) (ImmutableTxnMessage, error) {
	// combine begin and commit messages into one.
	msg, err := newTxnMessageBuilderV2().
		WithHeader(&TxnMessageHeader{}).
		WithBody(&TxnMessageBody{}).
		WithVChannel(begin.VChannel()).
		BuildMutable()
	if err != nil {
		return nil, err
	}
	// we don't need to modify the begin message's timetick, but set all the timetick of body messages.
	for _, m := range body {
		m.(*immutableMessageImpl).overwriteTimeTick(commit.TimeTick())
		m.(*immutableMessageImpl).overwriteLastConfirmedMessageID(commit.LastConfirmedMessageID())
	}
	immutableMsg := msg.WithTimeTick(commit.TimeTick()).
		WithLastConfirmed(commit.LastConfirmedMessageID()).
		WithTxnContext(*commit.TxnContext()).
		IntoImmutableMessage(commit.MessageID())
	return &immutableTxnMessageImpl{
		immutableMessageImpl: *immutableMsg.(*immutableMessageImpl),
		begin:                begin,
		messages:             body,
		commit:               commit,
	}, nil
}
