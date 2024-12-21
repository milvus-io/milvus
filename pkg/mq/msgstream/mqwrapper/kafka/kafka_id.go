package kafka

import (
	"github.com/milvus-io/milvus/pkg/common"
	mqcommon "github.com/milvus-io/milvus/pkg/mq/common"
)

func NewKafkaID(messageID int64) mqcommon.MessageID {
	return &KafkaID{
		MessageID: messageID,
	}
}

type KafkaID struct {
	MessageID int64
}

var _ mqcommon.MessageID = &KafkaID{}

func (kid *KafkaID) Serialize() []byte {
	return SerializeKafkaID(kid.MessageID)
}

func (kid *KafkaID) AtEarliestPosition() bool {
	return kid.MessageID <= 0
}

func (kid *KafkaID) Equal(msgID []byte) (bool, error) {
	return kid.MessageID == DeserializeKafkaID(msgID), nil
}

func (kid *KafkaID) LessOrEqualThan(msgID []byte) (bool, error) {
	return kid.MessageID <= DeserializeKafkaID(msgID), nil
}

func SerializeKafkaID(messageID int64) []byte {
	b := make([]byte, 8)
	common.Endian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) int64 {
	return int64(common.Endian.Uint64(messageID))
}
