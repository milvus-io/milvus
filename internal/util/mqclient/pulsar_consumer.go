package mqclient

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

type pulsarConsumer struct {
	c          pulsar.Consumer
	msgChannel chan ConsumerMessage
}

func (pc *pulsarConsumer) Subscription() string {
	return pc.c.Subscription()
}

func (pc *pulsarConsumer) Chan() <-chan ConsumerMessage {
	return pc.msgChannel
}

func (pc *pulsarConsumer) Seek(id MessageID) error {
	messageID := id.(*pulsarID).messageID
	return pc.c.Seek(messageID)
}

func (pc *pulsarConsumer) Ack(message ConsumerMessage) {
	pm := message.(*pulsarMessage)
	pc.c.Ack(pm.msg)
}

func (pc *pulsarConsumer) Close() {
	pc.c.Close()
}
