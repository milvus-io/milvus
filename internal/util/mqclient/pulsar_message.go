package mqclient

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

type pulsarMessage struct {
	msg pulsar.ConsumerMessage
}

func (pm *pulsarMessage) Topic() string {
	return pm.msg.Topic()
}

func (pm *pulsarMessage) Properties() map[string]string {
	return pm.msg.Properties()
}

func (pm *pulsarMessage) Payload() []byte {
	return pm.msg.Payload()
}

func (pm *pulsarMessage) ID() MessageID {
	id := pm.msg.ID()
	pid := &pulsarID{messageID: id}
	return pid
}
