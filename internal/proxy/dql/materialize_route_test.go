package dql

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newMaterializeTestTask(t *testing.T) *QueryTask {
	mgr := shardclient.NewMockShardClientManager(t)
	mgr.EXPECT().InvalidateShardLeaderCache(mock.Anything).Maybe()
	lb := shardclient.NewMockLBPolicy(t)
	lb.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything).Maybe()
	return &QueryTask{
		RetrieveRequest: &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()},
		queryParams:     &queryParams{limit: typeutil.Unlimited},
		schema:          mustNewSchemaInfo(mzSchema()),
		reQuery:         true,
		resultBuf:       typeutil.NewConcurrentSet[*internalpb.RetrieveResults](),
		shardclientMgr:  mgr,
		lb:              lb,
	}
}

// materializeAndFinish runs one shard's Materialize and then finishes the
// fill, as Execute does.
func materializeAndFinish(task *QueryTask, qn *mocks.MockQueryNodeClient) error {
	ctx := context.Background()
	fill := task.newMaterializeFill()
	if err := task.materializeShard(ctx, 1, qn, "ch", fill); err != nil {
		return err
	}
	return task.finishMaterialize(ctx, fill)
}

func TestMaterializeInScope(t *testing.T) {
	paramtable.Init()
	base := func() *QueryTask {
		return &QueryTask{
			RetrieveRequest: &internalpb.RetrieveRequest{OutputFieldsId: []int64{100, 101}},
			queryParams:     &queryParams{limit: typeutil.Unlimited},
			schema:          mustNewSchemaInfo(mzSchema()),
			reQuery:         true,
		}
	}
	cases := []struct {
		name   string
		mutate func(*QueryTask)
		want   bool
	}{
		{"requery", func(*QueryTask) {}, true},
		{"filter query", func(q *QueryTask) { q.reQuery = false }, false},
		{"external collection", func(q *QueryTask) {
			s := mzSchema()
			s.ExternalSource = "s3://bucket/path"
			q.schema = mustNewSchemaInfo(s)
		}, true},
		{"limited", func(q *QueryTask) { q.queryParams.limit = 10 }, false},
		{"offset", func(q *QueryTask) { q.queryParams.offset = 3 }, false},
		{"iterator", func(q *QueryTask) { q.queryParams.isIterator = true }, false},
		{"order by", func(q *QueryTask) { q.queryParams.orderByFields = []string{"text:desc"} }, false},
		{"aggregate", func(q *QueryTask) { q.Aggregates = []*planpb.Aggregate{{}} }, false},
		{"group by", func(q *QueryTask) { q.GroupByFieldIds = []int64{101} }, false},
		{"nil params", func(q *QueryTask) { q.queryParams = nil }, false},
		{"array output", func(q *QueryTask) {
			q.schema = mustNewSchemaInfo(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "a", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
			}})
		}, false},
		{"unknown field", func(q *QueryTask) { q.OutputFieldsId = []int64{999} }, false},
		{"element level", func(q *QueryTask) {
			q.plan = &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
				Predicates: &planpb.Expr{Expr: &planpb.Expr_ElementFilterExpr{ElementFilterExpr: &planpb.ElementFilterExpr{StructName: "s"}}},
			}}}
		}, false},
		{"row level filter", func(q *QueryTask) {
			q.plan = &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
				Predicates: &planpb.Expr{Expr: &planpb.Expr_AlwaysTrueExpr{AlwaysTrueExpr: &planpb.AlwaysTrueExpr{}}},
			}}}
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := base()
			c.mutate(q)
			require.Equal(t, c.want, q.materializeInScope())
		})
	}
}

func TestQueryTask_MaterializeShard_Success(t *testing.T) {
	paramtable.Init()
	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Materialize(mock.Anything, mock.MatchedBy(func(req *querypb.QueryRequest) bool {
		return req.GetMaterializeFormatVersion() == materialize.FormatVersion
	})).Return(relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"})), nil)
	task := newMaterializeTestTask(t)
	fill := newTestFill(task.schema.CollectionSchema, 0, nil)
	counter := metrics.ProxyMaterializeShardTotal.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10))
	before := testutil.ToFloat64(counter)
	require.NoError(t, task.materializeShard(context.Background(), 1, qn, "ch", fill))
	// The reply is decoded on arrival, but nothing is visible until the
	// fan-out ends.
	require.Equal(t, before, testutil.ToFloat64(counter))
	require.Empty(t, task.resultBuf.Collect())
	require.Len(t, fill.batches, 1)
	require.NoError(t, task.finishMaterialize(context.Background(), fill))
	require.Equal(t, before+1, testutil.ToFloat64(counter))
	var got []*internalpb.RetrieveResults
	task.resultBuf.Range(func(r *internalpb.RetrieveResults) bool { got = append(got, r); return true })
	require.Len(t, got, 1)
	require.Equal(t, []int64{1}, got[0].GetIds().GetIntId().GetData())
}

func TestQueryTask_MaterializeFallback(t *testing.T) {
	paramtable.Init()
	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceUnimplemented(errors.New("old node")))
	task := newMaterializeTestTask(t)
	fill := newTestFill(task.schema.CollectionSchema, 0, nil)
	counter := metrics.ProxyMaterializeShardTotal.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10))
	before := testutil.ToFloat64(counter)
	// The load balancer must not see the error, or it blacklists the node.
	require.NoError(t, task.materializeShard(context.Background(), 1, qn, "ch", fill))
	require.Equal(t, before, testutil.ToFloat64(counter))
	reason, err := fill.failure()
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.Equal(t, fallbackUnimplemented, reason)

	qn2 := mocks.NewMockQueryNodeClient(t)
	qn2.EXPECT().Materialize(mock.Anything, mock.Anything).Return(
		&internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Status(merr.WrapErrServiceUnimplemented(errors.New("worker")))}}, nil)
	task2 := newMaterializeTestTask(t)
	fill2 := newTestFill(task2.schema.CollectionSchema, 0, nil)
	require.NoError(t, task2.materializeShard(context.Background(), 1, qn2, "ch", fill2))
	reason, err = fill2.failure()
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.Equal(t, fallbackUnsupported, reason)
}

// Shards that reply with different schemas (version skew) fall back with
// their own reason.
func TestQueryTask_MaterializeShard_SchemaSkewFallsBack(t *testing.T) {
	paramtable.Init()
	withMaterializeSchemas(t, materializeSchemaCacheSize)
	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"})), nil).Once()
	qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(relay(mzWorkerReplyWithPK(t, []int64{2}, []int64{1}, []string{"y"})), nil).Once()
	task := newMaterializeTestTask(t)
	fill := task.newMaterializeFill()
	defer fill.release()
	require.NoError(t, task.materializeShard(context.Background(), 1, qn, "ch1", fill))
	require.NoError(t, task.materializeShard(context.Background(), 2, qn, "ch2", fill))
	reason, err := fill.failure()
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.Equal(t, fallbackSchemaSkew, reason)
}

// A proxy-side failure while reading a reply is kept on the fill and Exec
// returns nil, so the load balancer sees a healthy delegator; the request
// fails when the fill finishes.
func TestQueryTask_MaterializeShard_ProxySideErrorIsKept(t *testing.T) {
	paramtable.Init()
	cases := []struct {
		name    string
		replies func() []*internalpb.MaterializeResults
		wantErr error
	}{
		{"corrupt body", func() []*internalpb.MaterializeResults {
			r := relay(mzWorkerReply(t, []int64{1, 2}, []int64{1, 1}, []string{"x", "y"}))
			r.Body[0] = r.Body[0][:len(r.Body[0])/2]
			return []*internalpb.MaterializeResults{r}
		}, merr.ErrServiceInternal},
		{"pk on two shards", func() []*internalpb.MaterializeResults {
			return []*internalpb.MaterializeResults{
				relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"})),
				relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"y"})),
			}
		}, merr.ErrDataIntegrity},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			replies := c.replies()
			qn := mocks.NewMockQueryNodeClient(t)
			for _, r := range replies {
				qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(r, nil).Once()
			}
			task := newMaterializeTestTask(t)
			fill := newTestFill(task.schema.CollectionSchema, 0, nil)
			for i := range replies {
				require.NoError(t, task.materializeShard(context.Background(), int64(i), qn, strconv.Itoa(i), fill))
			}
			reason, err := fill.failure()
			require.ErrorIs(t, err, c.wantErr)
			require.Empty(t, reason)
			require.ErrorIs(t, task.finishMaterialize(context.Background(), fill), c.wantErr)
			require.Empty(t, task.resultBuf.Collect())
			require.True(t, fill.closed)
		})
	}
}

func TestQueryTask_MaterializeShard_OtherError(t *testing.T) {
	paramtable.Init()
	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(nil, errors.New("boom"))
	task := newMaterializeTestTask(t)
	fill := newTestFill(task.schema.CollectionSchema, 0, nil)
	require.Error(t, task.materializeShard(context.Background(), 1, qn, "ch", fill))
	reason, err := fill.failure()
	require.NoError(t, err)
	require.Empty(t, reason)
}

// Execute must rerun every shard with Query, on a clean result buffer, when
// any shard answered Unimplemented.
func TestQueryTask_ExecuteMaterializeFallback(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.MaterializeEnabled.Key, "true")
	defer pt.Reset(pt.ProxyCfg.MaterializeEnabled.Key)

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceUnimplemented(errors.New("old")))
	qn.EXPECT().Query(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{Status: merr.Success()}, nil)

	task := newMaterializeTestTask(t)
	task.OutputFieldsId = []int64{100, 101}
	task.request = &milvuspb.QueryRequest{}
	// A stale entry must not survive the fallback.
	lb := shardclient.NewMockLBPolicy(t)
	lb.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything).Maybe()
	lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, w shardclient.CollectionWorkLoad) error {
			return w.Exec(ctx, 1, qn, "ch")
		}).Times(2)
	task.lb = lb

	fallback := metrics.ProxyMaterializeFallbackTotal.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), fallbackUnimplemented)
	before := testutil.ToFloat64(fallback)
	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, before+1, testutil.ToFloat64(fallback))
	n := 0
	task.resultBuf.Range(func(*internalpb.RetrieveResults) bool { n++; return true })
	require.Equal(t, 1, n)
}

// One shard serves Materialize and the other answers Unimplemented: the
// request reruns with Query on every shard, and only the Query results stay.
func TestQueryTask_ExecuteMaterializeFallback_TwoShards(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.MaterializeEnabled.Key, "true")
	defer pt.Reset(pt.ProxyCfg.MaterializeEnabled.Key)

	ids := func(pk int64) *schemapb.IDs {
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}}}
	}
	queryOK := &internalpb.RetrieveResults{Status: merr.Success(), Ids: ids(1)}
	queryOld := &internalpb.RetrieveResults{Status: merr.Success(), Ids: ids(2)}

	qnOK := mocks.NewMockQueryNodeClient(t)
	qnOK.EXPECT().Materialize(mock.Anything, mock.Anything).Return(relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"})), nil).Once()
	qnOK.EXPECT().Query(mock.Anything, mock.Anything).Return(queryOK, nil).Once()
	qnOld := mocks.NewMockQueryNodeClient(t)
	qnOld.EXPECT().Materialize(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceUnimplemented(errors.New("old"))).Once()
	qnOld.EXPECT().Query(mock.Anything, mock.Anything).Return(queryOld, nil).Once()

	task := newMaterializeTestTask(t)
	task.OutputFieldsId = []int64{100, 101}
	task.request = &milvuspb.QueryRequest{}
	lb := shardclient.NewMockLBPolicy(t)
	lb.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything).Maybe()
	lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, w shardclient.CollectionWorkLoad) error {
			if err := w.Exec(ctx, 1, qnOK, "ch1"); err != nil {
				return err
			}
			return w.Exec(ctx, 2, qnOld, "ch2")
		}).Times(2)
	task.lb = lb

	node := strconv.FormatInt(paramtable.GetNodeID(), 10)
	fallback := metrics.ProxyMaterializeFallbackTotal.WithLabelValues(node, fallbackUnimplemented)
	before := testutil.ToFloat64(fallback)

	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, before+1, testutil.ToFloat64(fallback))
	// What the Materialize shard added was discarded with the fill.
	var got []*internalpb.RetrieveResults
	task.resultBuf.Range(func(r *internalpb.RetrieveResults) bool { got = append(got, r); return true })
	require.ElementsMatch(t, []*internalpb.RetrieveResults{queryOK, queryOld}, got)
}

// firstErr is the load balancer's outcome of a fan-out: the first error.
func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// A fallback wins over another shard's error: one shard answers
// Unimplemented, another fails, and the request reruns with Query instead of
// failing. Without a fallback the error still fails the request.
func TestQueryTask_ExecuteMaterialize_FallbackOverShardError(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.MaterializeEnabled.Key, "true")
	defer pt.Reset(pt.ProxyCfg.MaterializeEnabled.Key)

	for _, withFallback := range []bool{true, false} {
		t.Run(fmt.Sprintf("fallback=%v", withFallback), func(t *testing.T) {
			boom := merr.WrapErrServiceInternalMsg("boom")
			qnOld := mocks.NewMockQueryNodeClient(t)
			if withFallback {
				qnOld.EXPECT().Materialize(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceUnimplemented(errors.New("old"))).Once()
			} else {
				qnOld.EXPECT().Materialize(mock.Anything, mock.Anything).Return(relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"})), nil).Once()
			}
			qnBad := mocks.NewMockQueryNodeClient(t)
			qnBad.EXPECT().Materialize(mock.Anything, mock.Anything).Return(nil, boom).Once()
			queryOK := &internalpb.RetrieveResults{Status: merr.Success()}
			if withFallback {
				qnOld.EXPECT().Query(mock.Anything, mock.Anything).Return(queryOK, nil).Once()
				qnBad.EXPECT().Query(mock.Anything, mock.Anything).Return(queryOK, nil).Once()
			}

			task := newMaterializeTestTask(t)
			task.OutputFieldsId = []int64{100, 101}
			task.request = &milvuspb.QueryRequest{}
			lb := shardclient.NewMockLBPolicy(t)
			lb.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything).Maybe()
			calls := lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, w shardclient.CollectionWorkLoad) error {
					return firstErr(w.Exec(ctx, 1, qnOld, "ch1"), w.Exec(ctx, 2, qnBad, "ch2"))
				})
			if withFallback {
				calls.Times(2)
			} else {
				calls.Once()
			}
			task.lb = lb

			err := task.Execute(context.Background())
			if !withFallback {
				require.ErrorIs(t, err, boom)
				return
			}
			require.NoError(t, err)
			require.Len(t, task.resultBuf.Collect(), 1)
		})
	}
}

// A shard's reply side effects (the node's cost, the channel's MVCC
// timestamp) apply only when the request ends on Materialize: not while it
// runs, and not when it falls back to Query, whose replies apply their own.
func TestQueryTask_ExecuteMaterialize_ShardSideEffects(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.MaterializeEnabled.Key, "true")
	defer pt.Reset(pt.ProxyCfg.MaterializeEnabled.Key)

	mzCost := &internalpb.CostAggregation{ServiceTime: 111}
	reply := func() *internalpb.MaterializeResults {
		r := relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"}))
		r.Header.CostAggregation = mzCost
		r.Header.MvccTimestamp = 77
		return r
	}
	for _, withFallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("fallback=%v", withFallback), func(t *testing.T) {
			qnOK := mocks.NewMockQueryNodeClient(t)
			qnOK.EXPECT().Materialize(mock.Anything, mock.Anything).Return(reply(), nil).Once()
			qn2 := mocks.NewMockQueryNodeClient(t)
			qCost := &internalpb.CostAggregation{ServiceTime: 222}
			if withFallback {
				qn2.EXPECT().Materialize(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceUnimplemented(errors.New("old"))).Once()
				queryRes := &internalpb.RetrieveResults{Status: merr.Success(), CostAggregation: qCost, MvccTimestamp: 88}
				qnOK.EXPECT().Query(mock.Anything, mock.Anything).Return(queryRes, nil).Once()
				qn2.EXPECT().Query(mock.Anything, mock.Anything).Return(queryRes, nil).Once()
			} else {
				qn2.EXPECT().Materialize(mock.Anything, mock.Anything).Return(&internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{
					Status: merr.Success(), CostAggregation: mzCost, MvccTimestamp: 77,
				}}, nil).Once()
			}

			task := newMaterializeTestTask(t)
			task.OutputFieldsId = []int64{100, 101}
			task.request = &milvuspb.QueryRequest{}
			task.actualChannelsMvcc = typeutil.NewConcurrentMap[string, uint64]()
			var costs []*internalpb.CostAggregation
			lb := shardclient.NewMockLBPolicy(t)
			lb.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything).Run(func(_ int64, c *internalpb.CostAggregation) {
				costs = append(costs, c)
			}).Maybe()
			runs := 0
			lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, w shardclient.CollectionWorkLoad) error {
					runs++
					err := firstErr(w.Exec(ctx, 1, qnOK, "ch1"), w.Exec(ctx, 2, qn2, "ch2"))
					if runs == 1 {
						require.Empty(t, costs, "no Materialize side effect while the request runs")
					}
					return err
				})
			task.lb = lb

			require.NoError(t, task.Execute(context.Background()))
			want, wantMvcc := mzCost, uint64(77)
			if withFallback {
				want, wantMvcc = qCost, 88
			}
			require.Equal(t, []*internalpb.CostAggregation{want, want}, costs)
			for _, ch := range []string{"ch1", "ch2"} {
				got, ok := task.actualChannelsMvcc.Get(ch)
				require.True(t, ok)
				require.Equal(t, wantMvcc, got)
			}
		})
	}
}

// A reply the proxy cannot decode is a proxy-side failure: Exec still returns
// nil to the load balancer (no blacklist, no retry on another replica), and
// the request fails with ServiceInternal after the fan-out.
func TestQueryTask_ExecuteMaterialize_CorruptBodyFailsWithoutBlacklist(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.MaterializeEnabled.Key, "true")
	defer pt.Reset(pt.ProxyCfg.MaterializeEnabled.Key)

	reply := relay(mzWorkerReply(t, []int64{1, 2}, []int64{1, 1}, []string{"x", "y"}))
	require.NotEmpty(t, reply.GetBody())
	reply.Body[0] = reply.Body[0][:len(reply.Body[0])/2]

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(reply, nil).Once()

	task := newMaterializeTestTask(t)
	task.OutputFieldsId = []int64{100, 101}
	task.request = &milvuspb.QueryRequest{}
	lb := shardclient.NewMockLBPolicy(t)
	execCalls := 0
	lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, w shardclient.CollectionWorkLoad) error {
			execCalls++
			err := w.Exec(ctx, 1, qn, "ch")
			// The balancer sees success: nothing to blacklist or retry.
			require.NoError(t, err)
			return err
		}).Once()
	task.lb = lb

	node := strconv.FormatInt(paramtable.GetNodeID(), 10)
	counter := metrics.ProxyMaterializeShardTotal.WithLabelValues(node)
	before := testutil.ToFloat64(counter)

	err := task.Execute(context.Background())
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, 1, execCalls)
	require.Equal(t, before, testutil.ToFloat64(counter))
	require.Empty(t, task.resultBuf.Collect())
	// UpdateCostMetrics was never expected on this mock: a failed reduce
	// does not report the node's cost either.
}
