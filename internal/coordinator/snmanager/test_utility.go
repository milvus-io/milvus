//go:build test
// +build test

package snmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func ResetStreamingNodeManager() {
	StaticStreamingNodeManager = newStreamingNodeManager()
}

func ResetDoNothingStreamingNodeManager(t *testing.T) {
	ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb func(version typeutil.VersionInt64Pair, relations []types.PChannelInfoAssigned) error) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfo{}, nil).Maybe()
	StaticStreamingNodeManager.SetBalancerReady(b)
}
