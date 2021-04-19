package timesync

import (
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type (
	UniqueID  = typeutil.UniqueID
	Timestamp = typeutil.Timestamp
)

type MsgProducer interface {
	SetProxyTtBarrier(proxyTtBarrier TimeTickBarrier)
	SetWriteNodeTtBarrier(writeNodeTtBarrier TimeTickBarrier)
	SetDMSyncStream(dmSync ms.MsgStream)
	SetK2sSyncStream(k2sSync ms.MsgStream)
	Start() error
	Close()
}

type TimeTickBarrier interface {
	GetTimeTick() (Timestamp, error)
	Start() error
	Close()
}
