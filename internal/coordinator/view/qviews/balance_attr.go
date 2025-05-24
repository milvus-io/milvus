package qviews

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

var (
	_ BalanceAttrAtWorkNode = (*BalanceAttrAtStreamingNode)(nil)
	_ BalanceAttrAtWorkNode = (*BalanceAttributesAtQueryNode)(nil)
)

func NewBalanceAttrAtWorkNodeFromProto(n WorkNode, proto *viewpb.SyncQueryViewsResponse) BalanceAttrAtWorkNode {
	switch b := proto.BalanceAttributes.(type) {
	case *viewpb.SyncQueryViewsResponse_QueryNode:
		_ = n.(QueryNode) // assertion
		return &BalanceAttributesAtQueryNode{
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

type BalanceAttributesAtQueryNode struct {
	workNode WorkNode
	inner    *viewpb.QueryNodeBalanceAttributes
}

func (qv *BalanceAttributesAtQueryNode) WorkNode() WorkNode {
	return qv.workNode
}

func (qv *BalanceAttributesAtQueryNode) BalanceAttrOfQueryNode() *viewpb.QueryNodeBalanceAttributes {
	return qv.inner
}

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
