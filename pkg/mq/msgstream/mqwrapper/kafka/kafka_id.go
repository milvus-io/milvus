package kafka

import (
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

type kafkaID struct {
	messageID int64
}

var _ mqwrapper.MessageID = &kafkaID{}

func NewKafkaID(messageID int64) mqwrapper.MessageID {
	return &kafkaID{
		messageID: messageID,
	}
}

// TODO: remove in future, remove mqwrapper layer.
func (kid *kafkaID) KafkaID() int64 {
	return kid.messageID
}

func (kid *kafkaID) Serialize() []byte {
	return SerializeKafkaID(kid.messageID)
}

func (kid *kafkaID) AtEarliestPosition() bool {
	return kid.messageID <= 0
}

func (kid *kafkaID) Equal(msgID []byte) (bool, error) {
	return kid.messageID == DeserializeKafkaID(msgID), nil
}

func (kid *kafkaID) LessOrEqualThan(msgID []byte) (bool, error) {
	return kid.messageID <= DeserializeKafkaID(msgID), nil
}

// LT less than
func (kid *kafkaID) LT(id2 mqwrapper.MessageID) bool {
	return kid.messageID < id2.(*kafkaID).messageID
}

// LTE less than or equal to
func (kid *kafkaID) LTE(id2 mqwrapper.MessageID) bool {
	return kid.messageID <= id2.(*kafkaID).messageID
}

// EQ Equal to.
func (kid *kafkaID) EQ(id2 mqwrapper.MessageID) bool {
	return kid.messageID == id2.(*kafkaID).messageID
}

func SerializeKafkaID(messageID int64) []byte {
	b := make([]byte, 8)
	common.Endian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) int64 {
	return int64(common.Endian.Uint64(messageID))
}
