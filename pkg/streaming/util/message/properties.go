package message

const (
	// preserved properties
	messageVersion                          = "_v"   // message version for compatibility, see `Version` for more information.
	messageWALTerm                          = "_wt"  // wal term of a message, always increase by MessageID order, should never rollback.
	messageTypeKey                          = "_t"   // message type key.
	messageTimeTick                         = "_tt"  // message time tick.
	messageBarrierTimeTick                  = "_btt" // message barrier time tick.
	messageLastConfirmed                    = "_lc"  // message last confirmed message id.
	messageLastConfirmedIDSameWithMessageID = "_lcs" // message last confirmed message id is the same with message id.
	messageVChannel                         = "_vc"  // message virtual channel.
	messageBroadcastHeader                  = "_bh"  // message broadcast header.
	messageHeader                           = "_h"   // specialized message header.
	messageTxnContext                       = "_tx"  // transaction context.
	messageCipherHeader                     = "_ch"  // message cipher header.
	messageNotPersisteted                   = "_np"  // check if the message is unpersisted.
)

var (
	_ RProperties = propertiesImpl{}
	_ Properties  = propertiesImpl{}
)

// RProperties is the read-only properties for message.
type RProperties interface {
	// Get find a value by key.
	Get(key string) (value string, ok bool)

	// Exist check if a key exists.
	Exist(key string) bool

	// ToRawMap returns the raw map of properties.
	ToRawMap() map[string]string
}

// Properties is the write and readable properties for message.
type Properties interface {
	RProperties

	// Set a key-value pair in Properties.
	Set(key, value string)
}

// propertiesImpl is the implementation of Properties.
type propertiesImpl map[string]string

func (prop propertiesImpl) Get(key string) (value string, ok bool) {
	value, ok = prop[key]
	return
}

func (prop propertiesImpl) Exist(key string) bool {
	_, ok := prop[key]
	return ok
}

func (prop propertiesImpl) Set(key, value string) {
	prop[key] = value
}

func (prop propertiesImpl) Delete(key string) {
	delete(prop, key)
}

func (prop propertiesImpl) ToRawMap() map[string]string {
	return map[string]string(prop)
}

func (prop propertiesImpl) Clone() propertiesImpl {
	cloned := make(map[string]string, len(prop))
	for k, v := range prop {
		cloned[k] = v
	}
	return cloned
}

// EstimateSize returns the estimated size of properties.
func (prop propertiesImpl) EstimateSize() int {
	size := 0
	for k, v := range prop {
		size += len(k) + len(v)
	}
	return size
}

// CheckIfMessageFromStreaming checks if the message is from streaming.
func CheckIfMessageFromStreaming(props map[string]string) bool {
	if props == nil {
		return false
	}
	if props[messageVersion] != "" {
		return true
	}
	return false
}
