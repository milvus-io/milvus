package reader

import "github.com/zilliztech/milvus-distributed/internal/util/flowgraph"

const maxQueueLength = flowgraph.MaxQueueLength
const maxParallelism = flowgraph.MaxQueueLength

type BaseNode = flowgraph.BaseNode
type Node = flowgraph.Node
type InputNode = flowgraph.InputNode
