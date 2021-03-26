package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

type pulsarID struct {
	messageID pulsar.MessageID
}

func (pid *pulsarID) Serialize() []byte {
	return pid.messageID.Serialize()
}
