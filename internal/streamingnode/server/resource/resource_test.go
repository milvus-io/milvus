package resource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

func TestApply(t *testing.T) {
	paramtable.Init()

	Apply()
	Apply(OptETCD(&clientv3.Client{}))
	Apply(OptRootCoordClient(syncutil.NewFuture[types.RootCoordClient]()))

	assert.Panics(t, func() {
		Done()
	})

	Apply(
		OptETCD(&clientv3.Client{}),
		OptRootCoordClient(syncutil.NewFuture[types.RootCoordClient]()),
		OptDataCoordClient(syncutil.NewFuture[types.DataCoordClient]()),
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
