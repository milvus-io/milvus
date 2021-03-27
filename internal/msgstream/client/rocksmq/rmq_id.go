package rocksmq

import (
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type rmqID struct {
	messageID rocksmq.UniqueID
}

func (rid *rmqID) Serialize() []byte {
	return typeutil.SerializeRmqID(rid.messageID)
}
