package message

import (
	"fmt"

	"github.com/golang/protobuf/proto"
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
	t := proto.EncodeVarint(tt)
	m.properties.Set(messageTimeTick, string(t))
	return m
}

// WithLastConfirmed sets the last confirmed message id of current message.
func (m *messageImpl) WithLastConfirmed(id MessageID) MutableMessage {
	m.properties.Set(messageLastConfirmed, string(id.Marshal()))
	return m
}

type immutableMessageImpl struct {
	messageImpl
	id MessageID
}

// WALName returns the name of message related wal.
func (m *immutableMessageImpl) WALName() string {
	return m.id.WALName()
}

// TimeTick returns the time tick of current message.
func (m *immutableMessageImpl) TimeTick() uint64 {
	value, ok := m.properties.Get(messageTimeTick)
	if !ok {
		panic(fmt.Sprintf("there's a bug in the message codes, timetick lost in properties of message, id: %+v", m.id))
	}
	v := []byte(value)
	tt, n := proto.DecodeVarint(v)
	if n != len(v) {
		panic(fmt.Sprintf("there's a bug in the message codes, dirty timetick in properties of message, id: %+v", m.id))
	}
	return tt
}

func (m *immutableMessageImpl) LastConfirmedMessageID() MessageID {
	value, ok := m.properties.Get(messageLastConfirmed)
	if !ok {
		panic(fmt.Sprintf("there's a bug in the message codes, last confirmed message lost in properties of message, id: %+v", m.id))
	}
	id, err := UnmarshalMessageID(m.id.WALName(), []byte(value))
	if err != nil {
		panic(fmt.Sprintf("there's a bug in the message codes, dirty last confirmed message in properties of message, id: %+v", m.id))
	}
	return id
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
