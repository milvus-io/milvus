package qviews

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

// WorkNode is the enum type for query node and streaming node.
type WorkNode interface {
	fmt.Stringer

	isWorkNode()
}

// NewQueryNode creates a new query node.
func NewQueryNode(id int64) QueryNode {
	return QueryNode{ID: id}
}

type QueryNode struct {
	ID int64 // ID is the node id of streaming or querynode, if the id is -1, it means the node is a streaming node.
}

func (QueryNode) isWorkNode() {}

func (q QueryNode) String() string {
	return fmt.Sprintf("qn@%d", q.ID)
}

// NewStreamingNodeFromVChannel creates a new streaming node by vchannel.
func NewStreamingNodeFromVChannel(vchannel string) StreamingNode {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	return StreamingNode{PChannel: pchannel}
}

type StreamingNode struct {
	PChannel string
}

func (StreamingNode) isWorkNode() {}

func (s StreamingNode) String() string {
	return fmt.Sprintf("sn@%s", s.PChannel)
}

type BalanceAttrAtWorkNode interface {
	WorkNode() WorkNode
}
