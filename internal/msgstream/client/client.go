package client

type Client interface {
	// Create a producer instance
	CreateProducer(options ProducerOptions) (Producer, error)

	// Create a consumer instance and subscribe a topic
	Subscribe(options ConsumerOptions) (Consumer, error)

	// Get the earliest MessageID
	EarliestMessageID() MessageID

	// String to msg ID
	StringToMsgID(string) (MessageID, error)

	// Close the client and free associated resources
	Close()
}
