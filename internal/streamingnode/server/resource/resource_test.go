package resource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
)

func TestInit(t *testing.T) {
	assert.Panics(t, func() {
		Init()
	})
	assert.Panics(t, func() {
		Init(OptETCD(&clientv3.Client{}))
	})
	assert.Panics(t, func() {
		Init(OptRootCoordClient(mocks.NewMockRootCoordClient(t)))
	})
	Init(
		OptETCD(&clientv3.Client{}),
		OptRootCoordClient(mocks.NewMockRootCoordClient(t)),
		OptDataCoordClient(mocks.NewMockDataCoordClient(t)),
		OptStreamingNodeCatalog(mock_metastore.NewMockStreamingNodeCataLog(t)),
	)

	assert.NotNil(t, Resource().TSOAllocator())
	assert.NotNil(t, Resource().ETCD())
	assert.NotNil(t, Resource().RootCoordClient())
}

func TestInitForTest(t *testing.T) {
	InitForTest(t)
}
