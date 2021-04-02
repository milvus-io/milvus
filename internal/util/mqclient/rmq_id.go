package mqclient

import (
	"encoding/binary"

	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

type rmqID struct {
	messageID rocksmq.UniqueID
}

func (rid *rmqID) Serialize() []byte {
	return SerializeRmqID(rid.messageID)
}

func SerializeRmqID(messageID int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeRmqID(messageID []byte) (int64, error) {
	return int64(binary.LittleEndian.Uint64(messageID)), nil
}
