package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/client"
)

type pulsarConsumer struct {
	c          pulsar.Consumer
	msgChannel chan client.ConsumerMessage
}

func (pc *pulsarConsumer) Subscription() string {
	return pc.c.Subscription()
}

func (pc *pulsarConsumer) Chan() <-chan client.ConsumerMessage {
	return pc.msgChannel
}

func (pc *pulsarConsumer) Seek(id client.MessageID) error {
	messageID := id.(*pulsarID).messageID
	return pc.c.Seek(messageID)
}

func (pc *pulsarConsumer) Ack(message client.ConsumerMessage) {
	pm := message.(*pulsarMessage)
	pc.c.Ack(pm.msg)
}

func (pc *pulsarConsumer) Close() {
	pc.c.Close()
}
