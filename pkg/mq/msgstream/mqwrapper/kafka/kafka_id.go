package kafka

import (
	"github.com/milvus-io/milvus/pkg/v2/common"
	mqcommon "github.com/milvus-io/milvus/pkg/v2/mq/common"
)

type kafkaID struct {
	messageID int64
}

var _ mqcommon.MessageID = &kafkaID{}

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
