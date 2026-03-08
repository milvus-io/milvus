//go:build test
// +build test

package snmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func ResetStreamingNodeManager() {
	StaticStreamingNodeManager.Close()
	balance.ResetBalancer()
	StaticStreamingNodeManager = newStreamingNodeManager()
}

func ResetDoNothingStreamingNodeManager(t *testing.T) {
	ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfo{}, nil).Maybe()
	balance.Register(b)
}
