package message

import (
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
)

// newPayloadControlMessage creates a new payload control message.
func newPayloadControlMessage[B proto.Message](msg *immutableMessageImpl, bodyCalledHint int8) *payloadControlMessageImpl[B] {
	return &payloadControlMessageImpl[B]{
		mu:                   sync.Mutex{},
		bodyCalledHint:       bodyCalledHint,
		bodyCalledCounter:    0,
		immutableMessageImpl: msg,
	}
}

// payloadControlMessageImpl is the implementation of the message with payload control layer.
// payload of a message may cost the most memory, a payload control layer to avoid redundant unmarshal operation.
// Meanwhile, the payload can be evicted from memory, and be persistented into local disk by cache of wal.
type payloadControlMessageImpl[B proto.Message] struct {
	mu             sync.Mutex
	bodyCalledHint int8 // The hint that how many times the Body() will be called.
	// For write ahead cache, the variable is 2 (one for flusher, one for growing), otherwise it is 0.
	bodyCalledCounter int8 // The counter that how many times the Body() has been called when hit the Body cache,
	// When the bodyCalledCounter is equal or greater than bodyCalledHint, the body cache will be evicted.
	body B
	*immutableMessageImpl
}

// Body returns the body.
func (c *payloadControlMessageImpl[B]) Body() (B, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isBodyExists() {
		body, err := unmarshalProtoB[B](c.getPayload())
		if err != nil {
			return body, err
		}
		c.body = body
		c.bodyCalledCounter = 0
		return c.body, nil
	}

	body := c.body
	c.bodyCalledCounter++
	if c.bodyCalledCounter >= c.bodyCalledHint {
		c.resetBody()
	}
	return body, nil
}

// getPayload returns the payload.
func (c *payloadControlMessageImpl[B]) getPayload() []byte {
	return c.payload
}

// isBodyExists returns true if the body is exists.
func (c *payloadControlMessageImpl[B]) isBodyExists() bool {
	return !reflect.ValueOf(c.body).IsNil()
}

// resetBody resets the body.
func (c *payloadControlMessageImpl[B]) resetBody() {
	var nilBody B
	c.body = nilBody
}

// unmarshalProtoB unmarshals the proto message from the given data into given B.
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
