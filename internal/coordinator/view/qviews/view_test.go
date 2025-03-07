package qviews

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

func TestNewQueryViewAtWorkNodeFromProto(t *testing.T) {
	pb := &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1,
			ReplicaId:    1,
			Vchannel:     "v1",
			Version: &viewpb.QueryViewVersion{
				DataVersion:  1,
				QueryVersion: 1,
			},
			State:    viewpb.QueryViewState_QueryViewStatePreparing,
			Settings: &viewpb.QueryViewSettings{},
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
	qv := NewQueryViewAtWorkNodeFromProto(pb)
	qv.(*QueryViewAtStreamingNode).ViewOfStreamingNode()
	assert.Equal(t, qv.WorkNode(), NewStreamingNodeFromVChannel("v1"))
	assert.Equal(t, qv.WorkNode().String(), "sn@v1")
	assert.Equal(t, qv.ShardID(), ShardID{ReplicaID: 1, VChannel: "v1"})
	assert.Equal(t, qv.State(), QueryViewStatePreparing)
	assert.Equal(t, qv.Version(), QueryViewVersion{QueryVersion: 1, DataVersion: 1})
	assert.True(t, proto.Equal(qv.IntoProto(), pb))

	pb.StreamingNode = nil
	pb.QueryNode = []*viewpb.QueryViewOfQueryNode{{NodeId: 1}}
	qv = NewQueryViewAtWorkNodeFromProto(pb)
	qv.(*QueryViewAtQueryNode).ViewOfQueryNode()
	assert.Equal(t, qv.WorkNode(), NewQueryNode(1))
	assert.Equal(t, qv.WorkNode().String(), "qn@1")
	assert.Equal(t, qv.ShardID(), ShardID{ReplicaID: 1, VChannel: "v1"})
	assert.Equal(t, qv.State(), QueryViewStatePreparing)
	assert.Equal(t, qv.Version(), QueryViewVersion{QueryVersion: 1, DataVersion: 1})
	assert.True(t, proto.Equal(qv.IntoProto(), pb))

	qv = NewQueryViewAtStreamingNode(pb.Meta, &viewpb.QueryViewOfStreamingNode{})
	qv.(*QueryViewAtStreamingNode).ViewOfStreamingNode()
	assert.Equal(t, qv.WorkNode(), NewStreamingNodeFromVChannel("v1"))
	assert.Equal(t, qv.ShardID(), ShardID{ReplicaID: 1, VChannel: "v1"})
	assert.Equal(t, qv.State(), QueryViewStatePreparing)
	assert.Equal(t, qv.Version(), QueryViewVersion{QueryVersion: 1, DataVersion: 1})

	qv = NewQueryViewAtQueryNode(pb.Meta, &viewpb.QueryViewOfQueryNode{NodeId: 1})
	qv.(*QueryViewAtQueryNode).ViewOfQueryNode()
	assert.Equal(t, qv.WorkNode(), NewQueryNode(1))
	assert.Equal(t, qv.ShardID(), ShardID{ReplicaID: 1, VChannel: "v1"})
	assert.Equal(t, qv.State(), QueryViewStatePreparing)
	assert.Equal(t, qv.Version(), QueryViewVersion{QueryVersion: 1, DataVersion: 1})
}
