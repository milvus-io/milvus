package replicatestream

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// ReplicateStreamClientManager is the manager to create.
type ReplicateStreamClientManager interface {
	// CreateReplicateStreamClient creates a new ReplicateStreamClient.
	CreateReplicateStreamClient(ctx context.Context, targetCluster *milvuspb.MilvusCluster, targetChannel string) ReplicateStreamClient
}

type replicateStreamClientManager struct{}

func NewReplicateStreamClientManager() ReplicateStreamClientManager {
	return &replicateStreamClientManager{}
}

func (m *replicateStreamClientManager) CreateReplicateStreamClient(ctx context.Context, targetCluster *milvuspb.MilvusCluster, targetChannel string) ReplicateStreamClient {
	rsc := NewReplicateStreamClient(ctx, targetCluster, targetChannel)
	return rsc
}
