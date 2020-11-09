package flowgraph

import "github.com/zilliztech/milvus-distributed/internal/util/typeutil"

type Timestamp = typeutil.Timestamp
type NodeName = string

const MaxQueueLength = 1024
const MaxParallelism = 1024
