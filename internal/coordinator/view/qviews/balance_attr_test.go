package qviews

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

func TestNewBalanceAttrAtWorkNodeFromProto_QueryNode(t *testing.T) {
	proto := &viewpb.SyncQueryViewsResponse{
		BalanceAttributes: &viewpb.SyncQueryViewsResponse_QueryNode{
			QueryNode: &viewpb.QueryNodeBalanceAttributes{},
		},
	}

	result := NewBalanceAttrAtWorkNodeFromProto(NewQueryNode(1), proto)
	result.(*BalanceAttributesAtQueryNode).BalanceAttrOfQueryNode()
	assert.IsType(t, &BalanceAttributesAtQueryNode{}, result)
	assert.Equal(t, NewQueryNode(1), result.WorkNode())

	proto = &viewpb.SyncQueryViewsResponse{
		BalanceAttributes: &viewpb.SyncQueryViewsResponse_StreamingNode{
			StreamingNode: &viewpb.StreamingNodeBalanceAttributes{},
		},
	}

	result = NewBalanceAttrAtWorkNodeFromProto(NewStreamingNodeFromVChannel("v1"), proto)
	result.(*BalanceAttrAtStreamingNode).BalanceAttrOfStreamingNode()
	assert.IsType(t, &BalanceAttrAtStreamingNode{}, result)
	assert.Equal(t, NewStreamingNodeFromVChannel("v1"), result.WorkNode())
}
