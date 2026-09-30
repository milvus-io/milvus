package tasks

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Hold the executor before Submit, leaving exactly one prefetched guard and
// the real SearchTasks in the policy queue; no segcore/collection is needed.
type mergeDiagnosticBlocker struct {
	scheduler.Task
	started chan struct{}
	release chan struct{}
}

func (t *mergeDiagnosticBlocker) PreExecute() error {
	if t.started != nil {
		close(t.started)
		<-t.release
	}
	return nil
}
func (t *mergeDiagnosticBlocker) Execute() error   { return nil }
func (t *mergeDiagnosticBlocker) IsGpuIndex() bool { return false }

func TestSearchMergeDiagnosticReasons(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().QueryNodeCfg
	require.NoError(t, paramtable.Get().Save(cfg.SchedulerDiagnosticsEnabled.Key, "true"))
	t.Cleanup(func() { paramtable.Get().Reset(cfg.SchedulerDiagnosticsEnabled.Key) })
	base := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{Nq: 1, Topk: 100, DbID: 1, CollectionID: 2, MvccTimestamp: 3,
			PartitionIDs: []int64{4}, SerializedExprPlan: []byte("plan")},
		DmlChannels: []string{"channel"}, SegmentIDs: []int64{5},
	}
	for _, tc := range []struct {
		reason string
		change func(*querypb.SearchRequest)
	}{
		{"filter_only", func(r *querypb.SearchRequest) { r.FilterOnly = true; r.Req.DbID++ }},
		{"expr_cache", func(r *querypb.SearchRequest) { r.EnableExprCache = true }},
		{"database", func(r *querypb.SearchRequest) { r.Req.DbID++ }},
		{"collection", func(r *querypb.SearchRequest) { r.Req.CollectionID++ }},
		{"mvcc", func(r *querypb.SearchRequest) { r.Req.MvccTimestamp++ }},
		{"dsl", func(r *querypb.SearchRequest) { r.Req.DslType = commonpb.DslType(10) }},
		{"channel", func(r *querypb.SearchRequest) { r.DmlChannels[0] = "other" }},
		{"topk", func(r *querypb.SearchRequest) { r.Req.Topk = 10000 }},
		{"partitions", func(r *querypb.SearchRequest) { r.Req.PartitionIDs = []int64{6} }},
		{"segments", func(r *querypb.SearchRequest) { r.SegmentIDs = []int64{7} }},
		{"plan", func(r *querypb.SearchRequest) { r.Req.SerializedExprPlan = []byte("other") }},
		{"compatible", func(*querypb.SearchRequest) {}},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			makeTask := func() *SearchTask {
				return NewSearchTask(context.Background(), nil, nil, proto.Clone(base).(*querypb.SearchRequest), paramtable.GetNodeID())
			}
			// Independent behavior oracle also covers diagnostics-off nil pointers.
			left, right := makeTask(), makeTask()
			tc.change(right.req)
			right.topk = right.req.Req.Topk
			require.Equal(t, tc.reason == "compatible", left.Merge(right))
			counter := metrics.QueryNodeSchedulerDiagnosticMerge.WithLabelValues(paramtable.GetStringNodeID(), "fifo", tc.reason)
			before := testutil.ToFloat64(counter)
			business := metrics.QueryNodeSchedulerDiagnosticMerge.WithLabelValues(paramtable.GetStringNodeID(), "fifo", "candidate_business")
			beforeBusiness := testutil.ToFloat64(business)
			child := metrics.QueryNodeSchedulerDiagnosticChildren.WithLabelValues(paramtable.GetStringNodeID(), "fifo", "live", "true")
			beforeChild := testutil.ToFloat64(child)
			s := scheduler.NewScheduler("fifo")
			blocker := &mergeDiagnosticBlocker{Task: makeTask(), started: make(chan struct{}), release: make(chan struct{})}
			s.Start()
			require.NoError(t, s.Add(blocker))
			<-blocker.started
			guard := &mergeDiagnosticBlocker{Task: makeTask()}
			require.NoError(t, s.Add(guard))
			pending := metrics.QueryNodeSchedulerDiagnosticQueue.WithLabelValues(paramtable.GetStringNodeID(), "fifo", "other", "selected_not_started")
			require.Eventually(t, func() bool { return testutil.ToFloat64(pending) == 2 }, time.Second, time.Millisecond)
			parent, input := makeTask(), makeTask()
			tc.change(input.req)
			// topk is cached by NewSearchTask; mirror a request constructed with that value.
			input.topk = input.req.Req.Topk
			require.NoError(t, s.Add(parent))
			require.NoError(t, s.Add(input))
			_, err := s.ClearQueued(context.Background(), nil, "test only")
			require.NoError(t, err)
			close(blocker.release)
			require.NoError(t, blocker.Wait())
			s.Stop() // Includes final scheduler-counter flush, without waiting for another request.
			if tc.reason == "compatible" {
				require.True(t, input.merged)
				require.EqualValues(t, beforeBusiness, testutil.ToFloat64(business))
				require.EqualValues(t, beforeChild+1, testutil.ToFloat64(child))
			} else {
				require.False(t, input.merged)
				require.EqualValues(t, before+1, testutil.ToFloat64(counter))
				require.EqualValues(t, beforeBusiness+1, testutil.ToFloat64(business))
			}
		})
	}
}
