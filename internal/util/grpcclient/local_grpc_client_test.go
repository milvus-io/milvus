package grpcclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
)

type mockRootCoordServer struct {
	t *testing.T
	*rootcoordpb.UnimplementedRootCoordServer
}

func (s *mockRootCoordServer) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	assert.NotNil(s.t, req)
	assert.Equal(s.t, uint32(100), req.Count)
	return &rootcoordpb.AllocIDResponse{
		ID:    1,
		Count: 2,
	}, nil
}

func TestLocalGRPCClient(t *testing.T) {
	localClient := NewLocalGRPCClient(
		&rootcoordpb.RootCoord_ServiceDesc,
		&mockRootCoordServer{
			t:                            t,
			UnimplementedRootCoordServer: &rootcoordpb.UnimplementedRootCoordServer{},
		},
		rootcoordpb.NewRootCoordClient,
	)
	result, err := localClient.AllocTimestamp(context.Background(), &rootcoordpb.AllocTimestampRequest{})
	assert.Error(t, err)
	assert.Nil(t, result)

	result2, err := localClient.AllocID(context.Background(), &rootcoordpb.AllocIDRequest{
		Count: 100,
	})
	assert.NoError(t, err)
	assert.NotNil(t, result2)
	assert.Equal(t, int64(1), result2.ID)
	assert.Equal(t, uint32(2), result2.Count)
}
