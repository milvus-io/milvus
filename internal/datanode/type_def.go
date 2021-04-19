package datanode

import "github.com/zilliztech/milvus-distributed/internal/util/typeutil"

type (
	UniqueID      = typeutil.UniqueID
	Timestamp     = typeutil.Timestamp
	IntPrimaryKey = typeutil.IntPrimaryKey
	DSL           = string
)

type TimeRange struct {
	timestampMin Timestamp
	timestampMax Timestamp
}
