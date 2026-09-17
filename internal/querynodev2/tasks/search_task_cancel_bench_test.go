package tasks

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func benchTask(ctx context.Context, nq int64) *SearchTask {
	return &SearchTask{
		ctx:       ctx,
		nq:        nq,
		topk:      10,
		groupSize: 1,
		req: &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				DbID:               1,
				CollectionID:       1000,
				MvccTimestamp:      100,
				PartitionIDs:       []int64{1},
				SerializedExprPlan: []byte("plan"),
			},
			DmlChannels: []string{"channel1"},
			SegmentIDs:  []int64{1, 2},
		},
		originTopks: []int64{10},
		originNqs:   []int64{nq},
		notifier:    make(chan error, 8),
	}
}

// Every member carries a cancellable context, as a real request does. A
// background context has no Done channel at all, so Err takes no lock and
// AfterFunc registers nothing: benchmarking against one measures almost
// nothing of what these functions do in service.
func benchGroup(b *testing.B, size int) *SearchTask {
	newCtx := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		b.Cleanup(cancel)
		return ctx
	}
	owner := benchTask(newCtx(), 10)
	for i := 1; i < size; i++ {
		owner.Merge(benchTask(newCtx(), int64(10+i)))
	}
	return owner
}

// PruneCancelled runs twice for every search the node executes, at dequeue and
// again before execution, and almost always finds nothing to prune. These
// measure that path, which is the one that has to stay cheap.
func BenchmarkPruneCancelledStandalone(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t := benchTask(ctx, 10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = t.PruneCancelled()
	}
}

func BenchmarkPruneCancelledGroupOf8(b *testing.B) {
	t := benchGroup(b, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = t.PruneCancelled()
	}
}

func BenchmarkPruneCancelledGroupOf3(b *testing.B) {
	t := benchGroup(b, 3)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = t.PruneCancelled()
	}
}

// useGroupContext runs once per group execution and registers a listener on
// every member's context.
func BenchmarkUseGroupContextOf8(b *testing.B) {
	t := benchGroup(b, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		restore := t.useGroupContext()
		restore()
	}
}
