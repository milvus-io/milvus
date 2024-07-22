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
func createNewMessageBuilderV1[H proto.Message, P proto.Message]() func() *mutableMesasgeBuilder[H, P] {
	return func() *mutableMesasgeBuilder[H, P] {
		return newMutableMessageBuilder[H, P](VersionV1)
	}
}

// newMutableMessageBuilder creates a new builder.
// Should only used at client side.
func newMutableMessageBuilder[H proto.Message, P proto.Message](v Version) *mutableMesasgeBuilder[H, P] {
	var h H
	messageType := mustGetMessageTypeFromMessageHeader(h)
	properties := make(propertiesImpl)
	properties.Set(messageTypeKey, messageType.marshal())
	properties.Set(messageVersion, v.String())
	return &mutableMesasgeBuilder[H, P]{
		properties: properties,
	}
}

// mutableMesasgeBuilder is the builder for message.
type mutableMesasgeBuilder[H proto.Message, P proto.Message] struct {
	header     H
	payload    P
	properties propertiesImpl
}

// WithMessageHeader creates a new builder with determined message type.
func (b *mutableMesasgeBuilder[H, P]) WithMessageHeader(h H) *mutableMesasgeBuilder[H, P] {
	b.header = h
	return b
}

// WithPayload creates a new builder with message payload.
func (b *mutableMesasgeBuilder[H, P]) WithPayload(p P) *mutableMesasgeBuilder[H, P] {
	b.payload = p
	return b
}

// WithProperty creates a new builder with message property.
// A key started with '_' is reserved for streaming system, should never used at user of client.
func (b *mutableMesasgeBuilder[H, P]) WithProperty(key string, val string) *mutableMesasgeBuilder[H, P] {
	if b.properties.Exist(key) {
		panic(fmt.Sprintf("message builder already set property field, key = %s", key))
	}
	b.properties.Set(key, val)
	return b
}

// WithProperties creates a new builder with message properties.
// A key started with '_' is reserved for streaming system, should never used at user of client.
func (b *mutableMesasgeBuilder[H, P]) WithProperties(kvs map[string]string) *mutableMesasgeBuilder[H, P] {
	for key, val := range kvs {
		b.properties.Set(key, val)
	}
	return b
}

// BuildMutable builds a mutable message.
// Panic if not set payload and message type.
// should only used at client side.
func (b *mutableMesasgeBuilder[H, P]) BuildMutable() (MutableMessage, error) {
	// payload and header must be a pointer
	if reflect.ValueOf(b.header).IsNil() {
		panic("message builder not ready for header field")
	}
	if reflect.ValueOf(b.payload).IsNil() {
		panic("message builder not ready for payload field")
	}

	// setup header.
	sp, err := EncodeProto(b.header)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode header")
	}
	b.properties.Set(messageSpecialiedHeader, sp)

	payload, err := proto.Marshal(b.payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal payload")
	}
	return &messageImpl{
		payload:    payload,
		properties: b.properties,
	}, nil
}
