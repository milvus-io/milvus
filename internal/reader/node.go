package reader

import "github.com/zilliztech/milvus-distributed/internal/util/flowgraph"

const maxQueueLength = 1024
const maxParallelism = 1024

type BaseNode = flowgraph.BaseNode
type Node = flowgraph.Node
