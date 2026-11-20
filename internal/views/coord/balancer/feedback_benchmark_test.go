package balancer

import (
	"fmt"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/viewfeedback"
)

type benchmarkFeedbackReader struct {
	calls    int
	snapshot *viewfeedback.PlanningSnapshot
}

func (r *benchmarkFeedbackReader) GetPlanningSnapshot() *viewfeedback.PlanningSnapshot {
	r.calls++
	return r.snapshot
}

// One round contains 100 operations that each create a view. Unrelated loaded
// collections must not make each operation rebuild all feedback aggregates.
func BenchmarkPreparationFeedback(b *testing.B) {
	for _, count := range []int{100, 10000} {
		b.Run(fmt.Sprintf("collections%d", count), func(b *testing.B) {
			c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
			for i := 2; i <= count; i++ {
				c.PublishLoadConfig(int64(i), cfgFor(int64(i), int64(i*10), nil, nil), 1)
			}
			configs := make([]*loadmgr.LoadConfig, 0, count)
			samples := make(map[int64]*viewfeedback.Snapshot, count)
			for i := 1; i <= count; i++ {
				configs = append(configs, cfgFor(int64(i), int64(i*10), nil, nil))
				samples[int64(i*10)] = &viewfeedback.Snapshot{LastAccessUnixNanos: time.Now().UnixNano(), RequestRates: viewfeedback.RequestRates{Last6Windows: 1}}
			}
			reader := &benchmarkFeedbackReader{snapshot: priorityFeedbackSnapshot(b, configs, samples)}
			id := cacheShard(1, 10)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < 100; j++ {
					p := newPlanningContext(c)
					p.feedback = reader
					_ = p.preparationPriority(id)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(reader.calls)/float64(b.N), "feedback_reads/op")
		})
	}
}
