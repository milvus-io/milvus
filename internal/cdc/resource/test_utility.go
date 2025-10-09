//go:build test
// +build test

package resource

import (
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/replication"
)

// InitForTest initializes the singleton of resources for test.
func InitForTest(t *testing.T, opts ...optResourceInit) {
	r = &resourceImpl{}
	for _, opt := range opts {
		opt(r)
	}
	if r.etcdClient == nil {
		r.etcdClient = &clientv3.Client{}
	}
	if r.clusterClient == nil {
		r.clusterClient = cluster.NewMockClusterClient(t)
	}
	if r.replicateManagerClient == nil {
		r.replicateManagerClient = replication.NewMockReplicateManagerClient(t)
	}
}
