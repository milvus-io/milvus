//go:build test
// +build test

package resource

import (
	"testing"

	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/controller"
	"github.com/milvus-io/milvus/internal/cdc/replication"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/pkg/v2/mocks/mock_kv"
)

// InitForTest initializes the singleton of resources for test.
func InitForTest(t *testing.T, opts ...optResourceInit) {
	r = &resourceImpl{}
	for _, opt := range opts {
		opt(r)
	}
	if r.metaKV == nil {
		r.metaKV = mock_kv.NewMockMetaKv(t)
	}
	if r.catalog == nil {
		r.catalog = mock_metastore.NewMockReplicationCatalog(t)
	}
	if r.clusterClient == nil {
		r.clusterClient = cluster.NewMockClusterClient(t)
	}
	if r.replicateManagerClient == nil {
		r.replicateManagerClient = replication.NewMockReplicateManagerClient(t)
	}
	if r.controller == nil {
		r.controller = controller.NewMockController(t)
	}
}
