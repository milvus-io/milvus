package timesync

import "github.com/zilliztech/milvus-distributed/internal/util/typeutil"

type (
	UniqueID  = typeutil.UniqueID
	Timestamp = typeutil.Timestamp
)

type TimeTickBarrier interface {
	GetTimeTick() (Timestamp, error)
	Start() error
	Close()
}
