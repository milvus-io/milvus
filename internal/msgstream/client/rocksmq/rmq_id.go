package rocksmq

import (
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

type rmqID struct {
	messageID rocksmq.UniqueID
}

func (rid *rmqID) Serialize() []byte {
	return []byte(strconv.Itoa((int(rid.messageID))))
}
