package qviews

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var (
	_ BalanceAttrAtWorkNode = (*BalanceAttrAtStreamingNode)(nil)
	_ BalanceAttrAtWorkNode = (*BalanceAttrAtQueryNode)(nil)
)

// NewBalanceAttrAtWorkNodeFromProto creates a new balance attribute at work node from proto.
func NewBalanceAttrAtWorkNodeFromProto(n WorkNode, pb *viewpb.SyncQueryViewsResponse) BalanceAttrAtWorkNode {
	switch b := pb.BalanceAttributes.(type) {
	case *viewpb.SyncQueryViewsResponse_QueryNode:
		_ = n.(QueryNode) // assertion
		return &BalanceAttrAtQueryNode{
			workNode: n,
			inner:    b.QueryNode,
		}
	case *viewpb.SyncQueryViewsResponse_StreamingNode:
		_ = n.(StreamingNode) // assertion
		return &BalanceAttrAtStreamingNode{
			workNode: n,
			inner:    b.StreamingNode,
		}
	default:
		panic("unknown balance attribute type")
	}
}

// BalanceAttrAtQueryNode is the balance attributes reported by a query node.
type BalanceAttrAtQueryNode struct {
	workNode WorkNode
	inner    *viewpb.QueryNodeBalanceAttributes
}

func (qv *BalanceAttrAtQueryNode) WorkNode() WorkNode {
	return qv.workNode
}

func (qv *BalanceAttrAtQueryNode) BalanceAttrOfQueryNode() *viewpb.QueryNodeBalanceAttributes {
	return qv.inner
}

// BalanceAttrAtStreamingNode is the balance attributes reported by a streaming node.
type BalanceAttrAtStreamingNode struct {
	workNode WorkNode
	inner    *viewpb.StreamingNodeBalanceAttributes
}

func (qv *BalanceAttrAtStreamingNode) WorkNode() WorkNode {
	return qv.workNode
}

func (qv *BalanceAttrAtStreamingNode) BalanceAttrOfStreamingNode() *viewpb.StreamingNodeBalanceAttributes {
	return qv.inner
}
