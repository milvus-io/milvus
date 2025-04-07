//go:build test
// +build test

package resource

import (
	"testing"

	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	tinspector "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// InitForTest initializes the singleton of resources for test.
func InitForTest(t *testing.T, opts ...optResourceInit) {
	r = &resourceImpl{
		logger: log.With(),
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.chunkManager != nil {
		r.syncMgr = syncmgr.NewSyncManager(r.chunkManager)
		r.wbMgr = writebuffer.NewManager(r.syncMgr)
	}
	if r.rootCoordClient != nil {
		r.timestampAllocator = idalloc.NewTSOAllocator(r.rootCoordClient)
		r.idAllocator = idalloc.NewIDAllocator(r.rootCoordClient)
	} else {
		f := syncutil.NewFuture[types.RootCoordClient]()
		f.Set(idalloc.NewMockRootCoordClient(t))
		r.rootCoordClient = f
		r.timestampAllocator = idalloc.NewTSOAllocator(r.rootCoordClient)
		r.idAllocator = idalloc.NewIDAllocator(r.rootCoordClient)
	}
	r.segmentAssignStatsManager = stats.NewStatsManager()
	r.timeTickInspector = tinspector.NewTimeTickSyncInspector()
}
