package resource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestApply(t *testing.T) {
	paramtable.Init()

	Apply()
	Apply(OptETCD(&clientv3.Client{}))
	Apply(OptRootCoordClient(mocks.NewMockRootCoordClient(t)))

	assert.Panics(t, func() {
		Done()
	})

	Apply(
		OptETCD(&clientv3.Client{}),
		OptRootCoordClient(mocks.NewMockRootCoordClient(t)),
		OptDataCoordClient(mocks.NewMockDataCoordClient(t)),
		OptStreamingNodeCatalog(mock_metastore.NewMockStreamingNodeCataLog(t)),
	)
	Done()

	assert.NotNil(t, Resource().TSOAllocator())
	assert.NotNil(t, Resource().ETCD())
	assert.NotNil(t, Resource().RootCoordClient())
}

func TestInitForTest(t *testing.T) {
	InitForTest(t)
}
