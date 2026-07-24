package qviews

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestNewQueryViewAtWorkNodeFromProto(t *testing.T) {
	pb := &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1,
			ReplicaId:    1,
			Vchannel:     "v1",
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
				QueryVersion: 1,
			},
			State:           viewpb.QueryViewState_QueryViewStatePreparing,
			LoadInfoVersion: 1,
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
	qv := NewQueryViewAtWorkNodeFromProto(pb)
	qv.(*QueryViewAtStreamingNode).ViewOfStreamingNode()
	assert.Equal(t, NewStreamingNodeFromVChannel("v1"), qv.WorkNode())
	assert.Equal(t, "sn@v1", qv.WorkNode().String())
	assert.Equal(t, ShardID{ReplicaID: 1, VChannel: "v1"}, qv.ShardID())
	assert.Equal(t, QueryViewStatePreparing, qv.State())
	assert.Equal(t, QueryViewVersion{
		DataVersion:  DataVersion{StreamingVersion: 1, CompactVersion: 0},
		QueryVersion: 1,
	}, qv.Version())
	assert.True(t, proto.Equal(qv.IntoProto(), pb))

	pb.StreamingNode = nil
	pb.QueryNode = []*viewpb.QueryViewOfQueryNode{{NodeId: 1}}
	qv = NewQueryViewAtWorkNodeFromProto(pb)
	qv.(*QueryViewAtQueryNode).ViewOfQueryNode()
	assert.Equal(t, NewQueryNode(1), qv.WorkNode())
	assert.Equal(t, "qn@1", qv.WorkNode().String())
	assert.Equal(t, ShardID{ReplicaID: 1, VChannel: "v1"}, qv.ShardID())
	assert.Equal(t, QueryViewStatePreparing, qv.State())
	assert.True(t, proto.Equal(qv.IntoProto(), pb))

	qv = NewQueryViewAtStreamingNode(pb.Meta, &viewpb.QueryViewOfStreamingNode{})
	qv.(*QueryViewAtStreamingNode).ViewOfStreamingNode()
	assert.Equal(t, NewStreamingNodeFromVChannel("v1"), qv.WorkNode())
	assert.Equal(t, ShardID{ReplicaID: 1, VChannel: "v1"}, qv.ShardID())
	assert.Equal(t, QueryViewStatePreparing, qv.State())

	qv = NewFullQueryViewAtStreamingNode(
		pb.Meta,
		&viewpb.QueryViewOfStreamingNode{},
		[]*viewpb.QueryViewOfQueryNode{{NodeId: 1}},
	)
	assert.Len(t, qv.IntoProto().GetQueryNode(), 1)

	qv = NewQueryViewAtQueryNode(pb.Meta, &viewpb.QueryViewOfQueryNode{
		NodeId: 1,
		Partitions: []*viewpb.QueryViewOfPartition{
			{PartitionId: 10, SegmentIds: []int64{100, 101}},
		},
	})
	qv.(*QueryViewAtQueryNode).ViewOfQueryNode()
	assert.Equal(t, NewQueryNode(1), qv.WorkNode())
	assert.Equal(t, ShardID{ReplicaID: 1, VChannel: "v1"}, qv.ShardID())
	assert.Equal(t, QueryViewStatePreparing, qv.State())
	assert.Equal(t, QueryViewKey{
		ShardID: ShardID{ReplicaID: 1, VChannel: "v1"},
		QueryViewVersion: QueryViewVersion{
			DataVersion:  DataVersion{StreamingVersion: 1, CompactVersion: 0},
			QueryVersion: 1,
		},
	}, qv.QueryViewKey())
	assert.ElementsMatch(t, []int64{100, 101}, qv.(*QueryViewAtQueryNode).SegmentIDs())
}

func TestNewQueryViewAtWorkNodeFromProto_InvalidInputPanics(t *testing.T) {
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		ReplicaId:    1,
		Vchannel:     "v1",
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			QueryVersion: 1,
		},
		State: viewpb.QueryViewState_QueryViewStatePreparing,
	}

	qv := NewQueryViewAtWorkNodeFromProto(&viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
		QueryNode:     []*viewpb.QueryViewOfQueryNode{{NodeId: 1}},
	})
	assert.Equal(t, NewStreamingNodeFromVChannel("v1"), qv.WorkNode())
	assert.Len(t, qv.IntoProto().GetQueryNode(), 1)

	require.Panics(t, func() {
		NewQueryViewAtWorkNodeFromProto(&viewpb.QueryViewOfShard{Meta: meta})
	})

	qnView := &QueryViewAtQueryNode{
		queryViewAtWorkNodeBase: queryViewAtWorkNodeBase{
			inner: &viewpb.QueryViewOfShard{
				Meta: meta,
				QueryNode: []*viewpb.QueryViewOfQueryNode{
					{NodeId: 1},
					{NodeId: 2},
				},
			},
		},
	}
	require.Panics(t, func() {
		qnView.ViewOfQueryNode()
	})
}
