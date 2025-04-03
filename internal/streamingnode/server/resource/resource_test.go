package resource

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	os.Exit(m.Run())
}

func TestApply(t *testing.T) {
	Apply()
	Apply(OptETCD(&clientv3.Client{}))
	Apply(OptMixCoordClient(syncutil.NewFuture[types.MixCoordClient]()))

	assert.Panics(t, func() {
		Done()
	})

	Apply(
		OptChunkManager(mock_storage.NewMockChunkManager(t)),
		OptETCD(&clientv3.Client{}),
		OptMixCoordClient(syncutil.NewFuture[types.MixCoordClient]()),
		OptStreamingNodeCatalog(mock_metastore.NewMockStreamingNodeCataLog(t)),
	)
	Done()

	assert.NotNil(t, Resource().TSOAllocator())
	assert.NotNil(t, Resource().ETCD())
	assert.NotNil(t, Resource().MixCoordClient())
	Release()
}

func TestInitForTest(t *testing.T) {
	InitForTest(t)
}
