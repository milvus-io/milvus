//go:build test && dynamic

package qviews

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestNewBalanceAttrAtWorkNodeFromProto(t *testing.T) {
	resp := &viewpb.SyncQueryViewsResponse{
		BalanceAttributes: &viewpb.SyncQueryViewsResponse_QueryNode{
			QueryNode: &viewpb.QueryNodeBalanceAttributes{},
		},
	}

	result := NewBalanceAttrAtWorkNodeFromProto(NewQueryNode(1), resp)
	result.(*BalanceAttrAtQueryNode).BalanceAttrOfQueryNode()
	assert.IsType(t, &BalanceAttrAtQueryNode{}, result)
	assert.Equal(t, NewQueryNode(1), result.WorkNode())

	resp = &viewpb.SyncQueryViewsResponse{
		BalanceAttributes: &viewpb.SyncQueryViewsResponse_StreamingNode{
			StreamingNode: &viewpb.StreamingNodeBalanceAttributes{},
		},
	}

	result = NewBalanceAttrAtWorkNodeFromProto(NewStreamingNodeFromVChannel("v1"), resp)
	result.(*BalanceAttrAtStreamingNode).BalanceAttrOfStreamingNode()
	assert.IsType(t, &BalanceAttrAtStreamingNode{}, result)
	assert.Equal(t, NewStreamingNodeFromVChannel("v1"), result.WorkNode())
}

func TestNewBalanceAttrAtWorkNodeFromProto_InvalidInputPanics(t *testing.T) {
	queryNodeResp := &viewpb.SyncQueryViewsResponse{
		BalanceAttributes: &viewpb.SyncQueryViewsResponse_QueryNode{
			QueryNode: &viewpb.QueryNodeBalanceAttributes{},
		},
	}
	require.Panics(t, func() {
		NewBalanceAttrAtWorkNodeFromProto(NewStreamingNodeFromVChannel("v1"), queryNodeResp)
	})

	require.Panics(t, func() {
		NewBalanceAttrAtWorkNodeFromProto(NewQueryNode(1), &viewpb.SyncQueryViewsResponse{})
	})
}
