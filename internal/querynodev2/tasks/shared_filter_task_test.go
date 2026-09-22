// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func groupedRequest(extras ...*internalpb.SubSearchRequest) *querypb.SearchRequest {
	return &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			Base:                    &commonpb.MsgBase{MsgID: 7},
			CollectionID:            42,
			OutputFieldsId:          []int64{100},
			MvccTimestamp:           11,
			GuaranteeTimestamp:      12,
			TimeoutTimestamp:        13,
			Username:                "u",
			GroupByFieldIds:         []int64{5},
			IsIterator:              true,
			CollectionTtlTimestamps: 14,
			EntityTtlPhysicalTime:   15,
			// branch 0's own payload
			SerializedExprPlan: []byte("plan-0"),
			PlaceholderGroup:   []byte("ph-0"),
			Nq:                 3,
			Topk:               10,
			FieldId:            1,
			MetricType:         "L2",
		},
		DmlChannels:            []string{"ch-0"},
		SegmentIDs:             []int64{1, 2},
		Scope:                  querypb.DataScope_Historical,
		TotalChannelNum:        1,
		ExtraFilterSharingReqs: extras,
	}
}

func extraBranch(fieldID int64, nq, topk int64) *internalpb.SubSearchRequest {
	return &internalpb.SubSearchRequest{
		SerializedExprPlan: []byte("plan-x"),
		PlaceholderGroup:   []byte("ph-x"),
		Nq:                 nq,
		Topk:               topk,
		FieldId:            fieldID,
		MetricType:         "BM25",
		AnalyzerName:       "standard",
	}
}

func TestBuildSharedFilterBranches(t *testing.T) {
	req := groupedRequest(extraBranch(2, 5, 20))
	branches := buildSharedFilterBranches(req)
	require.Len(t, branches, 2)

	t.Run("branch 0 is the request itself", func(t *testing.T) {
		assert.Same(t, req, branches[0])
	})

	t.Run("an extra branch carries its own payload", func(t *testing.T) {
		got := branches[1].GetReq()
		assert.Equal(t, []byte("plan-x"), got.GetSerializedExprPlan())
		assert.Equal(t, []byte("ph-x"), got.GetPlaceholderGroup())
		assert.EqualValues(t, 5, got.GetNq())
		assert.EqualValues(t, 20, got.GetTopk())
		assert.EqualValues(t, 2, got.GetFieldId())
		assert.Equal(t, "BM25", got.GetMetricType())
		assert.Equal(t, "standard", got.GetAnalyzerName())
		assert.False(t, got.GetIsAdvanced())
		assert.EqualValues(t, common.PkFilterNoPkFilter, got.GetPkFilter())
	})

	t.Run("envelope fields are copied from the request", func(t *testing.T) {
		got := branches[1].GetReq()
		base := req.GetReq()
		assert.Equal(t, base.GetBase(), got.GetBase())
		assert.Equal(t, base.GetCollectionID(), got.GetCollectionID())
		assert.Equal(t, base.GetOutputFieldsId(), got.GetOutputFieldsId())
		assert.Equal(t, base.GetMvccTimestamp(), got.GetMvccTimestamp())
		assert.Equal(t, base.GetGuaranteeTimestamp(), got.GetGuaranteeTimestamp())
		assert.Equal(t, base.GetTimeoutTimestamp(), got.GetTimeoutTimestamp())
		assert.Equal(t, base.GetUsername(), got.GetUsername())
		assert.Equal(t, base.GetGroupByFieldIds(), got.GetGroupByFieldIds())
		assert.Equal(t, base.GetIsIterator(), got.GetIsIterator())
		assert.Equal(t, base.GetCollectionTtlTimestamps(), got.GetCollectionTtlTimestamps())
		assert.Equal(t, base.GetEntityTtlPhysicalTime(), got.GetEntityTtlPhysicalTime())
	})

	// The delegator's flattening does not carry Offset, ConsistencyLevel or
	// IsRecallEvaluation into a hybrid sub-request. Branch 0 therefore never
	// has them, and an extra branch must not either -- otherwise two branches
	// of the same group would be built differently from the same source.
	t.Run("field set matches branch 0 exactly", func(t *testing.T) {
		got := branches[1].GetReq()
		assert.Zero(t, got.GetOffset())
		assert.Zero(t, got.GetConsistencyLevel())
		assert.False(t, got.GetIsRecallEvaluation())
	})

	t.Run("routing fields follow the group", func(t *testing.T) {
		assert.Equal(t, req.GetDmlChannels(), branches[1].GetDmlChannels())
		assert.Equal(t, req.GetSegmentIDs(), branches[1].GetSegmentIDs())
		assert.Equal(t, req.GetScope(), branches[1].GetScope())
	})

	t.Run("an ungrouped request yields exactly one branch", func(t *testing.T) {
		assert.Len(t, buildSharedFilterBranches(groupedRequest()), 1)
	})
}

func TestSharedFilterTaskAccounting(t *testing.T) {
	newTask := func(req *querypb.SearchRequest) *SearchTask {
		return &SearchTask{
			req:         req,
			nq:          req.GetReq().GetNq(),
			topk:        req.GetReq().GetTopk(),
			originNqs:   []int64{req.GetReq().GetNq()},
			originTopks: []int64{req.GetReq().GetTopk()},
		}
	}

	t.Run("NQ sums every branch", func(t *testing.T) {
		// The scheduler counter feeds the proxy's load estimate, so a group
		// must report what it actually processes, not just branch 0.
		task := newTask(groupedRequest(extraBranch(2, 5, 20), extraBranch(3, 4, 20)))
		assert.EqualValues(t, 3+5+4, task.NQ())
	})

	t.Run("MinNQ spans every branch", func(t *testing.T) {
		task := newTask(groupedRequest(extraBranch(2, 1, 20)))
		assert.EqualValues(t, 1, task.MinNQ())
	})

	t.Run("ungrouped accounting is unchanged", func(t *testing.T) {
		task := newTask(groupedRequest())
		assert.EqualValues(t, 3, task.NQ())
		assert.EqualValues(t, 3, task.MinNQ())
	})

	// The NQ-axis merge (same plan, concatenated placeholder groups) and a
	// shared-filter group (same rows, different vector fields) are different
	// axes and must not compose.
	t.Run("a grouped task never merges", func(t *testing.T) {
		grouped := newTask(groupedRequest(extraBranch(2, 5, 20)))
		plain := newTask(groupedRequest())
		assert.False(t, grouped.Merge(plain))
		assert.False(t, plain.Merge(grouped))
	})
}

// The envelope is where every per-branch figure the caller still needs gets
// attributed, because SubSearchResults has no cost or storage fields. Each
// rule below is relied on downstream: scanned bytes feed the proxy's
// storage-cost metrics, ServiceTime feeds the balancer, TotalRelatedDataSize
// feeds metering.
func TestAssembleSharedFilterEnvelope(t *testing.T) {
	branch := func(topk, remote, total, service, related int64) *internalpb.SearchResults {
		return &internalpb.SearchResults{
			MetricType:         "L2",
			NumQueries:         1,
			TopK:               topk,
			ScannedRemoteBytes: remote,
			ScannedTotalBytes:  total,
			ChannelsMvcc:       map[string]uint64{"ch": uint64(topk)},
			CostAggregation: &internalpb.CostAggregation{
				ServiceTime:          service,
				TotalRelatedDataSize: related,
			},
		}
	}

	t.Run("scanned bytes are summed across branches", func(t *testing.T) {
		env := assembleSharedFilterEnvelope(7, []*internalpb.SearchResults{
			branch(10, 100, 1000, 3, 5000),
			branch(20, 250, 2500, 4, 5000),
			branch(30, 50, 500, 5, 5000),
		}, 0)
		assert.EqualValues(t, 400, env.GetScannedRemoteBytes())
		assert.EqualValues(t, 4000, env.GetScannedTotalBytes())
	})

	t.Run("service time is the task's, related data size is counted once", func(t *testing.T) {
		// The branches reduce concurrently on their own recorders, so their
		// readings (3, 7) are neither additive nor the task's duration; the
		// caller measures the task once and hands it in.
		env := assembleSharedFilterEnvelope(7, []*internalpb.SearchResults{
			branch(10, 0, 0, 3, 5000),
			branch(20, 0, 0, 7, 5000),
		}, 9)
		assert.EqualValues(t, 9, env.GetCostAggregation().GetServiceTime(),
			"summing the branch readings would report 10, taking one of them 3 or 7")
		assert.EqualValues(t, 5000, env.GetCostAggregation().GetTotalRelatedDataSize(),
			"every branch touched the same segments; N copies would be double counting")
	})

	t.Run("sub-results keep branch order via req_index", func(t *testing.T) {
		env := assembleSharedFilterEnvelope(7, []*internalpb.SearchResults{
			branch(10, 0, 0, 0, 0), branch(20, 0, 0, 0, 0), branch(30, 0, 0, 0, 0),
		}, 0)
		require.Len(t, env.GetSubResults(), 3)
		for i, sub := range env.GetSubResults() {
			assert.EqualValues(t, i, sub.GetReqIndex())
			assert.EqualValues(t, 10*(i+1), sub.GetTopK())
		}
		assert.True(t, env.GetIsAdvanced())
		assert.EqualValues(t, 7, env.GetBase().GetSourceID())
	})

	t.Run("cost aggregation is never nil, even when no branch set one", func(t *testing.T) {
		env := assembleSharedFilterEnvelope(7, []*internalpb.SearchResults{
			{TopK: 10}, {TopK: 20},
		}, 0)
		require.NotNil(t, env.GetCostAggregation(),
			"services.go assigns through GetCostAggregation() unconditionally")
	})

	t.Run("channel mvcc is merged across branches", func(t *testing.T) {
		env := assembleSharedFilterEnvelope(7, []*internalpb.SearchResults{
			{ChannelsMvcc: map[string]uint64{"a": 1}},
			{ChannelsMvcc: map[string]uint64{"b": 2}},
		}, 0)
		assert.Equal(t, map[string]uint64{"a": 1, "b": 2}, env.GetChannelsMvcc())
	})
}

// groupMetricSum reads back what Done observed. Each case uses a node ID of
// its own so the histogram it reads holds only its own samples.
func groupMetricSum(t *testing.T, vec *prometheus.HistogramVec, nodeID int64) float64 {
	t.Helper()
	observer, err := vec.GetMetricWithLabelValues(fmt.Sprint(nodeID))
	require.NoError(t, err)
	metric := &dto.Metric{}
	require.NoError(t, observer.(prometheus.Metric).Write(metric))
	return metric.GetHistogram().GetSampleSum()
}

// The scheduler admits a shared-filter group for every branch it carries, so
// the operational metrics have to describe the same workload -- otherwise an
// overloaded node looks idle in exactly the case that overloaded it.
func TestSharedFilterTaskMetrics(t *testing.T) {
	done := func(nodeID int64, req *querypb.SearchRequest) {
		task := &SearchTask{
			req:       req,
			serverID:  nodeID,
			groupSize: 1,
			nq:        req.GetReq().GetNq(),
			topk:      req.GetReq().GetTopk(),
			notifier:  make(chan error, 1),
		}
		task.Done(nil)
	}

	t.Run("a group reports its branches", func(t *testing.T) {
		const nodeID = 90001
		done(nodeID, groupedRequest(extraBranch(2, 5, 20), extraBranch(3, 4, 15)))
		assert.EqualValues(t, 3+5+4, groupMetricSum(t, metrics.QueryNodeSearchGroupNQ, nodeID),
			"reporting branch 0's nq hides the queries the group actually ran")
		assert.EqualValues(t, 20, groupMetricSum(t, metrics.QueryNodeSearchGroupTopK, nodeID),
			"reporting branch 0's topk hides the widest result the group built")
	})

	t.Run("an ungrouped task reports what it always did", func(t *testing.T) {
		const nodeID = 90002
		done(nodeID, groupedRequest())
		assert.EqualValues(t, 3, groupMetricSum(t, metrics.QueryNodeSearchGroupNQ, nodeID))
		assert.EqualValues(t, 10, groupMetricSum(t, metrics.QueryNodeSearchGroupTopK, nodeID))
	})
}

func TestSharedFilterMaxTopK(t *testing.T) {
	newTask := func(req *querypb.SearchRequest) *SearchTask {
		return &SearchTask{req: req, topk: req.GetReq().GetTopk()}
	}

	t.Run("spans every branch", func(t *testing.T) {
		assert.EqualValues(t, 20, newTask(groupedRequest(extraBranch(2, 5, 20), extraBranch(3, 4, 15))).maxTopK())
	})

	t.Run("branch 0 can be the widest", func(t *testing.T) {
		assert.EqualValues(t, 10, newTask(groupedRequest(extraBranch(2, 5, 4))).maxTopK())
	})

	t.Run("ungrouped is the task's own topk", func(t *testing.T) {
		assert.EqualValues(t, 10, newTask(groupedRequest()).maxTopK())
	})
}

// sharedFilterFanOut drives executeSharedFilter with the two heavy things
// around it replaced: the segment search that feeds it, and the per-branch
// reduce it fans out to. What is left running is the part the fan-out itself
// owns -- how often the searched segments are walked for their size, which
// context a branch runs on, and which branches get to run at all.
type sharedFilterFanOut struct {
	task *SearchTask
	// reqs[i] is the search request branch i is handed, which is also how a
	// reduce call identifies the branch it belongs to.
	reqs []*segcore.SearchRequest
	// sizeCalls counts how often a searched segment was asked for its size.
	sizeCalls   atomic.Int64
	searchCalls atomic.Int64

	mu    sync.Mutex
	calls []fanOutCall
}

type fanOutCall struct {
	branch          int
	relatedDataSize int64
	ctxErr          error
}

// reduce stands in for a branch reduce: it records what the branch was handed
// and then does what the case asks of it.
func (f *sharedFilterFanOut) reduce(branchTask *SearchTask, searchReq *segcore.SearchRequest, relatedDataSize int64, body func(ctx context.Context, branch int) error) error {
	branch := lo.IndexOf(f.reqs, searchReq)
	err := body(branchTask.ctx, branch)
	f.mu.Lock()
	f.calls = append(f.calls, fanOutCall{branch: branch, relatedDataSize: relatedDataSize, ctxErr: branchTask.ctx.Err()})
	f.mu.Unlock()
	return err
}

func (f *sharedFilterFanOut) branches() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return lo.Map(f.calls, func(c fanOutCall, _ int) int { return c.branch })
}

func newSharedFilterFanOut(t *testing.T, branches, segCount int, body func(ctx context.Context, branch int) error) *sharedFilterFanOut {
	t.Helper()
	paramtable.Init()

	const collectionID = 100
	schema := mock_segcore.GenTestCollectionSchema("shared-filter-fan-out", schemapb.DataType_Int64, false)
	manager := segments.NewManager()
	manager.Collection.PutOrRef(collectionID, schema,
		mock_segcore.GenTestIndexMeta(collectionID, schema),
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: collectionID,
			PartitionIDs: []int64{10},
		})
	collection := manager.Collection.Get(collectionID)
	require.NotNil(t, collection)
	t.Cleanup(func() { manager.Collection.Unref(collectionID, 1) })

	fanOut := &sharedFilterFanOut{}
	// Built before segcore.NewSearchRequest is patched, since that is what
	// builds them. executeSharedFilter deletes each one exactly once.
	for i := 0; i < branches; i++ {
		req, err := mock_segcore.GenSearchPlanAndRequests(
			collection.GetCCollection(), []int64{1}, mock_segcore.IndexFaissIDMap, 1)
		require.NoError(t, err)
		fanOut.reqs = append(fanOut.reqs, req)
	}

	searched := make([]segments.Segment, 0, segCount)
	for i := 0; i < segCount; i++ {
		seg := segments.NewMockSegment(t)
		seg.EXPECT().Type().Return(segments.SegmentTypeGrowing).Maybe()
		seg.EXPECT().MemSize().RunAndReturn(func() int64 {
			fanOut.sizeCalls.Add(1)
			return 1000
		}).Maybe()
		seg.EXPECT().Unpin().Maybe()
		searched = append(searched, seg)
	}

	var handedOut atomic.Int64
	patches := []*mockey.Mocker{
		mockey.Mock(segcore.NewSearchRequest).To(
			func(*segcore.CCollection, *querypb.SearchRequest, []byte) (*segcore.SearchRequest, error) {
				return fanOut.reqs[handedOut.Add(1)-1], nil
			}).Build(),
		mockey.Mock(segments.SearchHistoricalGrouped).To(
			func(context.Context, *segments.Manager, []*segcore.SearchRequest, int64, []int64, []int64) ([][]*segcore.SearchResult, []segments.Segment, error) {
				fanOut.searchCalls.Add(1)
				return make([][]*segcore.SearchResult, branches), searched, nil
			}).Build(),
		mockey.Mock((*SearchTask).reduceSegmentResults).To(
			func(branchTask *SearchTask, searchReq *segcore.SearchRequest, _ []*segcore.SearchResult, _ []segments.Segment, relatedDataSize int64, _ *timerecord.TimeRecorder) error {
				return fanOut.reduce(branchTask, searchReq, relatedDataSize, body)
			}).Build(),
	}
	t.Cleanup(func() {
		for _, patch := range patches {
			patch.UnPatch()
		}
	})

	extras := make([]*internalpb.SubSearchRequest, 0, branches-1)
	for i := 1; i < branches; i++ {
		extras = append(extras, extraBranch(int64(i+1), 1, 10))
	}
	request := groupedRequest(extras...)
	request.Req.Nq = 1
	fanOut.task = &SearchTask{
		ctx:            context.Background(),
		collection:     collection,
		segmentManager: manager,
		req:            request,
		notifier:       make(chan error, 1),
	}
	return fanOut
}

// A sealed segment walks every binlog, statslog and deltalog entry to report
// its size, so a group of a thousand branches reducing over a thousand
// segments must not repeat that walk per branch.
func TestExecuteSharedFilterSizesSegmentsOnce(t *testing.T) {
	const branches, segCount = 3, 2

	fanOut := newSharedFilterFanOut(t, branches, segCount, func(context.Context, int) error { return nil })
	require.NoError(t, fanOut.task.executeSharedFilter(timerecord.NewTimeRecorder("test")))

	assert.EqualValues(t, segCount, fanOut.sizeCalls.Load(),
		"the searched segments are walked once for the group, not once per branch")
	assert.ElementsMatch(t, []int{0, 1, 2}, fanOut.branches())
	for _, call := range fanOut.calls {
		assert.EqualValues(t, segCount*1000, call.relatedDataSize, "every branch reduces with the group's figure")
	}
}

func TestExecuteSharedFilterRejectsParsedNQMismatchBeforeSearch(t *testing.T) {
	fanOut := newSharedFilterFanOut(t, 2, 1, func(context.Context, int) error { return nil })
	fanOut.task.req.ExtraFilterSharingReqs[0].Nq = 2

	err := fanOut.task.executeSharedFilter(timerecord.NewTimeRecorder("test"))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorContains(t, err, "parsed NQ 1 does not match declared NQ 2")
	assert.Zero(t, fanOut.searchCalls.Load(), "no segment search starts after the internal NQ contract fails")
}

// A failing branch has to reach its siblings: a group may carry a thousand of
// them, and Wait would otherwise sit through every remaining reduce before
// returning the error it already has.
func TestExecuteSharedFilterFailureReachesSiblings(t *testing.T) {
	const branches, segCount, inFlight = 6, 1, 2
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(inFlight))
	require.Equal(t, inFlight, hardware.GetCPUNum())

	boom := errors.New("branch reduce blew up")
	// stop is the way out for a sibling if the cancellation never arrives, so
	// a regression fails this test instead of hanging it with patched code
	// still running under it.
	stop := make(chan struct{})
	fanOut := newSharedFilterFanOut(t, branches, segCount, func(ctx context.Context, branch int) error {
		if branch == 0 {
			return boom
		}
		// A sibling that is already running only stops if the cancellation
		// reaches it, and the branch's own context is the only channel it has.
		select {
		case <-ctx.Done():
		case <-stop:
		}
		return nil
	})

	var running sync.WaitGroup
	running.Add(1)
	done := make(chan error, 1)
	go func() {
		defer running.Done()
		done <- fanOut.task.executeSharedFilter(timerecord.NewTimeRecorder("test"))
	}()
	t.Cleanup(func() {
		close(stop)
		running.Wait()
	})

	select {
	case err := <-done:
		assert.ErrorIs(t, err, boom)
	case <-time.After(30 * time.Second):
		t.Fatal("a failed branch did not reach its siblings; the fan-out never returned")
	}

	ran := fanOut.branches()
	assert.LessOrEqual(t, len(ran), inFlight,
		"branches queued behind the failure must not reduce at all, they were %v", ran)
	for _, call := range fanOut.calls {
		if call.branch != 0 {
			assert.Error(t, call.ctxErr, "a sibling must see the group's cancellation on its own context")
		}
	}
}
