package coordview

import (
	"testing"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

var benchmarkFailureSnapshot map[int64]map[ResourceKey]qviews.QueryViewVersion

func BenchmarkEmptyFailureSnapshot(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkFailureSnapshot = cloneFailedResources(nil)
	}
}

func BenchmarkHealthyShardStats(b *testing.B) {
	sm := NewCoordQueryViewStateMachineWithRef(buildTestView(1), nil)
	mgr := &ShardViewManager{views: map[qviews.QueryViewVersion]*CoordQueryViewStateMachine{sm.Version(): sm}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(mgr.Stats().FailedResources) != 0 {
			b.Fatal("healthy shard acquired failure facts")
		}
	}
}
