package kafka

import (
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
)

type kafkaID struct {
	messageID int64
}

var _ mqwrapper.MessageID = &kafkaID{}

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

func SerializeKafkaID(messageID int64) []byte {
	b := make([]byte, 8)
	common.Endian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) int64 {
	return int64(common.Endian.Uint64(messageID))
}
