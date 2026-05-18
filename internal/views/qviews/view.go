package qviews

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var (
	_ QueryViewAtWorkNode = (*QueryViewAtQueryNode)(nil)
	_ QueryViewAtWorkNode = (*QueryViewAtStreamingNode)(nil)
)

// QueryViewAtWorkNode represents the query view of a shard at a work node.
type QueryViewAtWorkNode interface {
	IntoProto() *viewpb.QueryViewOfShard

	ShardID() ShardID

	WorkNode() WorkNode

	State() QueryViewState

	Version() QueryViewVersion

	QueryViewKey() QueryViewKey
}

type queryViewAtWorkNodeBase struct {
	inner *viewpb.QueryViewOfShard
}

func (qv *queryViewAtWorkNodeBase) ShardID() ShardID {
	return NewShardIDFromQVMeta(qv.inner.Meta)
}

func (qv *queryViewAtWorkNodeBase) State() QueryViewState {
	return QueryViewState(qv.inner.Meta.State)
}

func (qv *queryViewAtWorkNodeBase) Version() QueryViewVersion {
	return FromProtoQueryViewVersion(qv.inner.Meta.Version)
}

func (qv *queryViewAtWorkNodeBase) QueryViewKey() QueryViewKey {
	return QueryViewKey{
		ShardID:          qv.ShardID(),
		QueryViewVersion: qv.Version(),
	}
}

func (qv *queryViewAtWorkNodeBase) IntoProto() *viewpb.QueryViewOfShard {
	return proto.Clone(qv.inner).(*viewpb.QueryViewOfShard)
}

// NewQueryViewAtWorkNodeFromProto creates a new query view at work node from proto.
func NewQueryViewAtWorkNodeFromProto(pb *viewpb.QueryViewOfShard) QueryViewAtWorkNode {
	if pb.StreamingNode != nil && pb.QueryNode != nil {
		panic("invalid node view proto, should be streaming or query")
	}
	if pb.StreamingNode != nil {
		return &QueryViewAtStreamingNode{
			queryViewAtWorkNodeBase: queryViewAtWorkNodeBase{inner: pb},
		}
	} else if len(pb.QueryNode) == 1 {
		return &QueryViewAtQueryNode{
			queryViewAtWorkNodeBase: queryViewAtWorkNodeBase{inner: pb},
		}
	}
	panic("invalid node view proto")
}

// NewQueryViewAtStreamingNode creates a new query view at streaming node.
func NewQueryViewAtStreamingNode(meta *viewpb.QueryViewMeta, view *viewpb.QueryViewOfStreamingNode) QueryViewAtWorkNode {
	return &QueryViewAtStreamingNode{
		queryViewAtWorkNodeBase: queryViewAtWorkNodeBase{
			inner: &viewpb.QueryViewOfShard{
				Meta:          proto.Clone(meta).(*viewpb.QueryViewMeta),
				StreamingNode: proto.Clone(view).(*viewpb.QueryViewOfStreamingNode),
			},
		},
	}
}

// QueryViewAtStreamingNode represents the query view of a shard at a streaming node.
type QueryViewAtStreamingNode struct {
	queryViewAtWorkNodeBase
}

func (qv *QueryViewAtStreamingNode) WorkNode() WorkNode {
	return NewStreamingNodeFromVChannel(qv.inner.Meta.Vchannel)
}

func (qv *QueryViewAtStreamingNode) ViewOfStreamingNode() *viewpb.QueryViewOfStreamingNode {
	return qv.inner.StreamingNode
}

// NewQueryViewAtQueryNode creates a new query view at query node.
func NewQueryViewAtQueryNode(meta *viewpb.QueryViewMeta, view *viewpb.QueryViewOfQueryNode) QueryViewAtWorkNode {
	return &QueryViewAtQueryNode{
		queryViewAtWorkNodeBase: queryViewAtWorkNodeBase{
			inner: &viewpb.QueryViewOfShard{
				Meta: proto.Clone(meta).(*viewpb.QueryViewMeta),
				QueryNode: []*viewpb.QueryViewOfQueryNode{
					proto.Clone(view).(*viewpb.QueryViewOfQueryNode),
				},
			},
		},
	}
}

// QueryViewAtQueryNode represents the query view of a shard at a query node.
type QueryViewAtQueryNode struct {
	queryViewAtWorkNodeBase
}

func (qv *QueryViewAtQueryNode) WorkNode() WorkNode {
	return NewQueryNode(qv.NodeID())
}

func (qv *QueryViewAtQueryNode) NodeID() int64 {
	return qv.ViewOfQueryNode().NodeId
}

func (qv *QueryViewAtQueryNode) ViewOfQueryNode() *viewpb.QueryViewOfQueryNode {
	if len(qv.inner.QueryNode) != 1 {
		panic("query view at query node should have only one query node")
	}
	return qv.inner.QueryNode[0]
}

// SegmentIDs returns all segment IDs assigned to this query node across all partitions.
func (qv *QueryViewAtQueryNode) SegmentIDs() []int64 {
	var ids []int64
	for _, p := range qv.ViewOfQueryNode().Partitions {
		ids = append(ids, p.SegmentIds...)
	}
	return ids
}
