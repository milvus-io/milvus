package message

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

type messageImpl struct {
	payload    []byte
	properties propertiesImpl
}

// ToPBMessage returns the pb message.
func (m *messageImpl) ToPBMessage() *logpb.Message {
	return &logpb.Message{
		Payload:    m.payload,
		Properties: map[string]string(m.properties),
	}
}

// MessageType returns the type of message.
func (m *messageImpl) MessageType() MessageType {
	val, ok := m.properties.Get(messageTypeKey)
	if !ok {
		return MessageTypeUnknown
	}
	return unmarshalMessageType(val)
}

// Payload returns payload of current message.
func (m *messageImpl) Payload() []byte {
	return m.payload
}

// Properties returns the message properties.
func (m *messageImpl) Properties() Properties {
	return m.properties
}

// EstimateSize returns the estimated size of current message.
func (m *messageImpl) EstimateSize() int {
	// TODO: more accurate size estimation.
	return len(m.payload) + m.properties.EstimateSize()
}

// WithTimeTick sets the time tick of current message.
func (m *messageImpl) WithTimeTick(tt uint64) MutableMessage {
	m.properties.Set(messageTimeTick, strconv.FormatUint(tt, 10))
	return m
}

// ToMQProducerMessage returns the producer message.
func (m *messageImpl) ToMQProducerMessage() *mqwrapper.ProducerMessage {
	return &mqwrapper.ProducerMessage{
		Payload:    m.payload,
		Properties: map[string]string(m.properties),
	}
}

type immutableMessageImpl struct {
	messageImpl
	id MessageID
}

// TimeTick returns the time tick of current message.
func (m *immutableMessageImpl) TimeTick() uint64 {
	value, ok := m.properties.Get(messageTimeTick)
	if !ok {
		panic(fmt.Sprintf("there's a bug in the message codes, timetick lost in properties of message, id: %+v", m.id))
	}
	tt, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("there's a bug in the message codes, dirty timetick in properties of message, id: %+v", m.id))
	}
	return tt
}

// MessageID returns the message id.
func (m *immutableMessageImpl) MessageID() MessageID {
	return m.id
}

// Properties returns the message read only properties.
func (m *immutableMessageImpl) Properties() RProperties {
	return m.properties
}

// Version returns the message format version.
func (m *immutableMessageImpl) Version() Version {
	value, ok := m.properties.Get(messageVersion)
	if !ok {
		return VersionOld
	}
	return newMessageVersionFromString(value)
}
