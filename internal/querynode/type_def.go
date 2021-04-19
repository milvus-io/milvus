package querynode

import (
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const rowIDFieldID = 0
const timestampFieldID = 1

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp
type IntPrimaryKey = typeutil.IntPrimaryKey
type DSL = string

type TimeRange struct {
	timestampMin Timestamp
	timestampMax Timestamp
}
