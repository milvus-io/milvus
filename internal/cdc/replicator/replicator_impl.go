package replicator

import "github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"

var _ Replicator = (*replicator)(nil)

// replicator is the implementation of Replicator.
type replicator struct {
	cluster *milvuspb.MilvusCluster
}

// NewReplicator creates a new Replicator.
func NewReplicator(cluster *milvuspb.MilvusCluster) Replicator {
	return &replicator{
		cluster: cluster,
	}
}

func (r *replicator) StartReplication() error {
	return nil
}

func (r *replicator) StopReplication() error {
	return nil
}
