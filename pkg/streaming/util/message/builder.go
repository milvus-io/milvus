package message

// NewBuilder creates a new builder.
func NewBuilder() *Builder {
	return &Builder{
		id:         nil,
		payload:    nil,
		properties: make(propertiesImpl),
	}
}

// Builder is the builder for message.
type Builder struct {
	id         MessageID
	payload    []byte
	properties propertiesImpl
}

// WithMessageID creates a new builder with message id.
func (b *Builder) WithMessageID(id MessageID) *Builder {
	b.id = id
	return b
}

// WithMessageType creates a new builder with message type.
func (b *Builder) WithMessageType(t MessageType) *Builder {
	b.properties.Set(messageTypeKey, t.marshal())
	return b
}

// WithProperty creates a new builder with message property.
// A key started with '_' is reserved for log system, should never used at user of client.
func (b *Builder) WithProperty(key string, val string) *Builder {
	b.properties.Set(key, val)
	return b
}

// WithProperties creates a new builder with message properties.
// A key started with '_' is reserved for log system, should never used at user of client.
func (b *Builder) WithProperties(kvs map[string]string) *Builder {
	for key, val := range kvs {
		b.properties.Set(key, val)
	}
	return b
}

// WithPayload creates a new builder with message payload.
func (b *Builder) WithPayload(payload []byte) *Builder {
	b.payload = payload
	return b
}

// BuildMutable builds a mutable message.
// Panic if set the message id.
func (b *Builder) BuildMutable() MutableMessage {
	if b.id != nil {
		panic("build a mutable message, message id should be nil")
	}
	// Set message version.
	b.properties.Set(messageVersion, VersionV1.String())
	return &messageImpl{
		payload:    b.payload,
		properties: b.properties,
	}
}

// BuildImmutable builds a immutable message.
// Panic if not set the message id.
func (b *Builder) BuildImmutable() ImmutableMessage {
	if b.id == nil {
		panic("build a immutable message, message id should not be nil")
	}
	return &immutableMessageImpl{
		id: b.id,
		messageImpl: messageImpl{
			payload:    b.payload,
			properties: b.properties,
		},
	}
}
