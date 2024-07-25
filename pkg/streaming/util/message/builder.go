package message

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
)

// NewMutableMessage creates a new mutable message.
// Only used at server side for streamingnode internal service, don't use it at client side.
func NewMutableMessage(payload []byte, properties map[string]string) MutableMessage {
	return &messageImpl{
		payload:    payload,
		properties: properties,
	}
}

// NewImmutableMessage creates a new immutable message.
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
)

// createNewMessageBuilderV1 creates a new message builder with v1 marker.
func createNewMessageBuilderV1[H proto.Message, B proto.Message]() func() *mutableMesasgeBuilder[H, B] {
	return func() *mutableMesasgeBuilder[H, B] {
		return newMutableMessageBuilder[H, B](VersionV1)
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

// WithProperty creates a new builder with message property.
// A key started with '_' is reserved for streaming system, should never used at user of client.
func (b *mutableMesasgeBuilder[H, B]) WithProperty(key string, val string) *mutableMesasgeBuilder[H, B] {
	if b.properties.Exist(key) {
		panic(fmt.Sprintf("message builder already set property field, key = %s", key))
	}
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

	// setup header.
	sp, err := EncodeProto(b.header)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode header")
	}
	b.properties.Set(messageSpecialiedHeader, sp)

	payload, err := proto.Marshal(b.body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal body")
	}
	return &messageImpl{
		payload:    payload,
		properties: b.properties,
	}, nil
}
