package mqclient

type ConsumerMessage interface {
	// Topic get the topic from which this message originated from
	Topic() string

	// Properties are application defined key/value pairs that will be attached to the message.
	// Return the properties attached to the message.
	Properties() map[string]string

	// Payload get the payload of the message
	Payload() []byte

	// ID get the unique message ID associated with this message.
	// The message id can be used to univocally refer to a message without having the keep the entire payload in memory.
	ID() MessageID
}
