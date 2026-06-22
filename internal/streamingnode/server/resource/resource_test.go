package resource

import (
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	os.Exit(m.Run())
}

func TestInit(t *testing.T) {
	assert.Panics(t, func() {
		Init(OptETCD(&clientv3.Client{}),
			OptMixCoordClient(syncutil.NewFuture[types.MixCoordClient]()))
	})

	Init(
		OptChunkManager(mock_storage.NewMockChunkManager(t)),
		OptETCD(&clientv3.Client{}),
		OptMixCoordClient(syncutil.NewFuture[types.MixCoordClient]()),
		OptStreamingNodeCatalog(mock_metastore.NewMockStreamingNodeCataLog(t)),
	)
	assert.NotNil(t, Resource().TSOAllocator())
	assert.NotNil(t, Resource().ETCD())
	assert.NotNil(t, Resource().MixCoordClient())
	assert.NotNil(t, Resource().QueryViewRouter())
	Release()
}

func TestReleaseClosesTimeTickInspector(t *testing.T) {
	backgrounds := countTimeTickInspectorBackgrounds()
	Init(
		OptChunkManager(mock_storage.NewMockChunkManager(t)),
		OptETCD(&clientv3.Client{}),
		OptMixCoordClient(syncutil.NewFuture[types.MixCoordClient]()),
		OptStreamingNodeCatalog(mock_metastore.NewMockStreamingNodeCataLog(t)),
	)
	inspector := Resource().TimeTickInspector()
	t.Cleanup(inspector.Close)

	assert.Eventually(t, func() bool {
		return countTimeTickInspectorBackgrounds() == backgrounds+1
	}, time.Second, 10*time.Millisecond)

	Release()
	assert.Eventually(t, func() bool {
		return countTimeTickInspectorBackgrounds() == backgrounds
	}, time.Second, 10*time.Millisecond)
}

func TestInitForTest(t *testing.T) {
	InitForTest(t)
}

func countTimeTickInspectorBackgrounds() int {
	buffer := make([]byte, 64*1024)
	for {
		n := runtime.Stack(buffer, true)
		if n < len(buffer) {
			return strings.Count(string(buffer[:n]), "timeTickSyncInspectorImpl).background")
		}
		buffer = make([]byte, len(buffer)*2)
	}
}
