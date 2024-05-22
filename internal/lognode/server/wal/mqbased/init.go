package mqbased

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
)

// init registers the mq based wal builder.
func init() {
	wal.RegisterBuilder(&builder{
		util.MQTypeRocksmq,
	})
	wal.RegisterBuilder(&builder{
		util.MQTypeNatsmq,
	})
	wal.RegisterBuilder(&builder{
		util.MQTypeKafka,
	})
	wal.RegisterBuilder(&builder{
		util.MQTypePulsar,
	})
}
