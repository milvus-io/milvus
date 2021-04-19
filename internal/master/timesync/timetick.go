package timesync

import "github.com/zilliztech/milvus-distributed/internal/util/typeutil"

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type TimeTickBarrier interface {
	GetTimeTick() (Timestamp,error)
	Start() error
}
