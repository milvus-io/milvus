package message

const (
	// preserved properties
	messageVersion       = "_v"  // message version for compatibility.
	messageTypeKey       = "_t"  // message type key.
	messageTimeTick      = "_tt" // message time tick.
	messageLastConfirmed = "_lc" // message last confirmed message id.
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

func (prop propertiesImpl) ToRawMap() map[string]string {
	return map[string]string(prop)
}

// EstimateSize returns the estimated size of properties.
func (prop propertiesImpl) EstimateSize() int {
	size := 0
	for k, v := range prop {
		size += len(k) + len(v)
	}
	return size
}
