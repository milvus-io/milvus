package mqclient

import (
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
)

type pulsarID struct {
	messageID pulsar.MessageID
}

func (pid *pulsarID) Serialize() []byte {
	return pid.messageID.Serialize()
}

func SerializePulsarMsgID(messageID pulsar.MessageID) []byte {
	return messageID.Serialize()
}

func DeserializePulsarMsgID(messageID []byte) (pulsar.MessageID, error) {
	return pulsar.DeserializeMessageID(messageID)
}

func PulsarMsgIDToString(messageID pulsar.MessageID) string {
	return strings.ToValidUTF8(string(messageID.Serialize()), "")
}

func StringToPulsarMsgID(msgString string) (pulsar.MessageID, error) {
	return pulsar.DeserializeMessageID([]byte(msgString))
}
