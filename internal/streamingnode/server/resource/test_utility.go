//go:build test
// +build test

package resource

import (
	"testing"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource/idalloc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	tinspector "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
)

// InitForTest initializes the singleton of resources for test.
func InitForTest(t *testing.T, opts ...optResourceInit) {
	r = &resourceImpl{}
	for _, opt := range opts {
		opt(r)
	}
	if r.rootCoordClient != nil {
		r.timestampAllocator = idalloc.NewTSOAllocator(r.rootCoordClient)
		r.idAllocator = idalloc.NewIDAllocator(r.rootCoordClient)
	} else {
		r.rootCoordClient = idalloc.NewMockRootCoordClient(t)
		r.timestampAllocator = idalloc.NewTSOAllocator(r.rootCoordClient)
		r.idAllocator = idalloc.NewIDAllocator(r.rootCoordClient)
	}
	r.segmentAssignStatsManager = stats.NewStatsManager()
	r.timeTickInspector = tinspector.NewTimeTickSyncInspector()
}
