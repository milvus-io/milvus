// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

const queryExpansionTestFunction = "query_expansion_test"

var registerQueryExpansionTestOnce sync.Once

type queryExpansionTestExpr struct {
	counts []int64
}

func (*queryExpansionTestExpr) Name() string { return queryExpansionTestFunction }

func (*queryExpansionTestExpr) IsRunnable(stage string) bool { return stage == types.StagePreProcess }

func (*queryExpansionTestExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.ListOf(arrow.BinaryTypes.String)}
}

func (expr *queryExpansionTestExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	builder := array.NewListBuilder(ctx.Pool(), arrow.BinaryTypes.String)
	defer builder.Release()
	values := builder.ValueBuilder().(*array.StringBuilder)
	row := 0
	for _, chunk := range inputs[0].Chunks() {
		queries := chunk.(*array.String)
		for index := 0; index < queries.Len(); index++ {
			count := expr.counts[row%len(expr.counts)]
			builder.Append(true)
			for child := int64(0); child < count; child++ {
				values.Append(queries.Value(index) + ":" + strconv.FormatInt(child, 10))
			}
			row++
		}
	}
	result := builder.NewArray()
	defer result.Release()
	return []*arrow.Chunked{arrow.NewChunked(result.DataType(), []arrow.Array{result})}, nil
}

func queryExpansionTestPlan(t *testing.T, counts []int64, vector bool) *schemapb.FunctionChain {
	t.Helper()
	registerSearchTransformTest(t)
	registerQueryExpansionTestOnce.Do(func() {
		err := types.RegisterFunction(queryExpansionTestFunction, func(_ types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
			counts := make([]int64, len(cfg.Params["counts"].GetArrayValue().GetValues()))
			for index, count := range cfg.Params["counts"].GetArrayValue().GetValues() {
				counts[index] = count.GetInt64Value()
			}
			return &queryExpansionTestExpr{counts: counts}, nil
		})
		require.NoError(t, err)
	})
	parameters := make([]*schemapb.FunctionParamValue, len(counts))
	for index, count := range counts {
		parameters[index] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: count}}
	}
	expr := &schemapb.FunctionChainExpr{
		Name: queryExpansionTestFunction, Args: []*schemapb.FunctionChainExprArg{columnArg("query")},
		Params: map[string]*schemapb.FunctionParamValue{"counts": chainArrayParam(parameters...)},
	}
	op, err := transform.FlatMap("query", expr, transform.ExplodeOptions{})
	require.NoError(t, err)
	ops := []*schemapb.FunctionChainOp{op}
	if vector {
		embedding := transformTestMap("query", "query", "")
		embedding.Expr.Params["vector"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}}
		ops = append(ops, embedding)
	}
	plan, err := transform.PreProcess("expand", ops...)
	require.NoError(t, err)
	return plan
}

func queryExpansionSearchParams() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{Key: AnnsFieldKey, Value: "vec"},
		{Key: TopKKey, Value: "3"},
		{Key: common.MetricTypeKey, Value: metric.IP},
		{Key: ParamsKey, Value: `{}`},
	}
}

func TestSearchQueryExpansionBindsActualNQAndAggregatesOriginalQueries(t *testing.T) {
	pre := queryExpansionTestPlan(t, []int64{2, 0, 1}, true)
	post := postTableTestPlan(t, false, &schemapb.FunctionChainOp{Op: "union_all"}, postTableCountOp(postQueryIndexColumn))
	request := &milvuspb.SearchRequest{
		Nq: 3, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "first", "dropped", "last")},
		SearchParams: queryExpansionSearchParams(), FunctionChains: []*schemapb.FunctionChain{pre, post},
	}
	original := proto.Clone(request)
	task := transformTestTask(request)
	task.ctx, task.tr = context.Background(), timerecord.NewTimeRecorder("query expansion")
	task.queryInfos = []*planpb.QueryInfo{{}}
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.Equal(t, [][]int64{{0, 0, 2}}, task.preQueryOrigins, "unchanged NQ does not imply unchanged query identity")
	require.True(t, proto.Equal(original, request))
	require.NoError(t, task.initSearchRequest(context.Background()))
	phg := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(task.GetPlaceholderGroup(), phg))
	require.Equal(t, commonpb.PlaceholderType_FloatVector, phg.GetPlaceholders()[0].GetType())
	require.Len(t, phg.GetPlaceholders()[0].GetValues(), 3)
	require.Equal(t, int64(3), task.GetNq())
	result := transformTestResults([]int64{2, 1, 0})
	require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
	table := decodePostTableTest(t, result.GetPostProcessResult())
	require.Equal(t, []int64{0}, postTableIntColumn(t, table, postQueryIndexColumn))
	require.Equal(t, []int64{3}, postTableIntColumn(t, table, "count"))
}

func TestSearchQueryExpansionUpdatesNQAndPreservesSubqueryProvenance(t *testing.T) {
	pre := queryExpansionTestPlan(t, []int64{2, 1}, true)
	post := postTableTestPlan(t, false, transformTestSelect(t, types.IDFieldName, postQueryIndexColumn, postSubqueryIndexColumn))
	task := transformTestTask(&milvuspb.SearchRequest{
		Nq: 2, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one", "two")},
		SearchParams: queryExpansionSearchParams(), FunctionChains: []*schemapb.FunctionChain{pre, post},
	})
	task.ctx, task.tr = context.Background(), timerecord.NewTimeRecorder("query expansion")
	task.queryInfos = []*planpb.QueryInfo{{}}
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.Equal(t, int64(3), task.request.GetNq())
	require.Equal(t, int64(3), task.GetNq())
	require.Equal(t, int64(2), task.ResultQueryCount(), "TABLE metrics count original queries")
	require.NoError(t, task.initSearchRequest(context.Background()))
	result := transformTestResults([]int64{1, 0, 2})
	require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
	table := decodePostTableTest(t, result.GetPostProcessResult())
	require.Equal(t, []int64{0, 1, 1}, postTableIntColumn(t, table, postQueryIndexColumn))
	require.Equal(t, []int64{0, 2, 2}, postTableIntColumn(t, table, postSubqueryIndexColumn))
	prepared := proto.Clone(task.request)
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.True(t, proto.Equal(prepared, task.request))
	nq, err := task.checkNq(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(3), nq)
}

func TestSearchQueryExpansionOriginalNQIncludesParentsWithNoChildren(t *testing.T) {
	pre := queryExpansionTestPlan(t, []int64{2, 0, 0}, false)
	post := postTableTestPlan(t, false, transformTestSelect(t, types.IDFieldName))
	task := transformTestTask(&milvuspb.SearchRequest{
		Nq: 3, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "expanded", "dropped", "also dropped")},
		FunctionChains: []*schemapb.FunctionChain{pre, post},
	})
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.Nil(t, task.queryContext, "native-only plans also preserve original NQ")
	require.Equal(t, [][]int64{{0, 0}}, task.preQueryOrigins)
	require.Equal(t, int64(2), task.GetNq())
	require.Equal(t, int64(3), task.ResultQueryCount())
}

func TestSearchQueryExpansionRequiresExplicitTableBeforeSearch(t *testing.T) {
	for _, counts := range [][]int64{{2, 1}, {2, 0}} {
		pre := queryExpansionTestPlan(t, counts, false)
		task := transformTestTask(&milvuspb.SearchRequest{
			Nq: 2, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one", "two")},
			FunctionChains: []*schemapb.FunctionChain{pre},
		})
		require.NoError(t, task.prepareSearchTransforms())
		original := proto.Clone(task.request)
		err := task.executeSearchPreProcess(context.Background())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.Contains(t, err.Error(), "requires POST_PROCESS TABLE")
		require.True(t, proto.Equal(original, task.request))
		require.Equal(t, int64(2), task.GetNq())
		require.False(t, task.preProcessed)
	}
	pre := queryExpansionTestPlan(t, []int64{1}, false)
	task := transformTestTask(&milvuspb.SearchRequest{
		Nq: 2, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one", "two")},
		FunctionChains: []*schemapb.FunctionChain{pre},
	})
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()), "identity flat_map retains ordinary HITS compatibility")
}

func TestSearchQueryExpansionEmptyAndNQBudgetFailWithoutPublishing(t *testing.T) {
	params := paramtable.Get()
	previousLimit := params.QuotaConfig.NQLimit.GetValue()
	require.NoError(t, params.Save(params.QuotaConfig.NQLimit.Key, "3"))
	t.Cleanup(func() { require.NoError(t, params.Save(params.QuotaConfig.NQLimit.Key, previousLimit)) })
	for _, counts := range [][]int64{{0}, {4}} {
		pre := queryExpansionTestPlan(t, counts, false)
		post := postTableTestPlan(t, false, transformTestSelect(t, types.IDFieldName))
		task := transformTestTask(&milvuspb.SearchRequest{
			Nq: 1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one")},
			FunctionChains: []*schemapb.FunctionChain{pre, post},
		})
		require.NoError(t, task.prepareSearchTransforms())
		original := proto.Clone(task.request)
		err := task.executeSearchPreProcess(context.Background())
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		require.True(t, proto.Equal(original, task.request))
		require.False(t, task.preProcessed)
	}
}

func TestHybridQueryExpansionDifferentBranchNQUsesNativeReduceAndTable(t *testing.T) {
	post := postTableTestPlan(t, true, transformTestSelect(t, types.IDFieldName, postQueryIndexColumn, postSubqueryIndexColumn, postANNIndexColumn))
	request := &milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{post}}
	for _, counts := range [][]int64{{2, 0}, {1, 2}} {
		request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{
			Nq: 2, PlaceholderGroup: transformTestPlaceholder(t, "one", "two"), SearchParams: queryExpansionSearchParams(),
			FunctionChains: []*schemapb.FunctionChain{queryExpansionTestPlan(t, counts, true)},
		})
	}
	original := proto.Clone(request)
	task := transformTestTask(request)
	task.ctx, task.tr = context.Background(), timerecord.NewTimeRecorder("hybrid query expansion")
	task.Base = &commonpb.MsgBase{}
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.True(t, proto.Equal(original, request))
	require.Equal(t, int64(2), task.GetNq(), "hybrid parent NQ stays unchanged")
	require.Equal(t, int64(2), task.request.SubReqs[0].GetNq())
	require.Equal(t, int64(3), task.request.SubReqs[1].GetNq())
	require.NoError(t, task.initAdvancedSearchRequest(context.Background()))
	require.Equal(t, int64(2), task.GetSubReqs()[0].GetNq())
	require.Equal(t, int64(3), task.GetSubReqs()[1].GetNq())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	nq, err := task.checkNq(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), nq)
	first, second := transformTestResults([]int64{1, 0}).Results, transformTestResults([]int64{1, 1, 0}).Results
	first.TopK, second.TopK = 3, 3
	sources := []*internalpb.SearchResults{{IsAdvanced: true, SubResults: []*internalpb.SubSearchResults{
		{ReqIndex: 0, MetricType: "IP", NumQueries: 2, TopK: 3, ResultData: first},
		{ReqIndex: 1, MetricType: "IP", NumQueries: 3, TopK: 3, ResultData: second},
	}}}
	pipeline, err := newSearchPipeline(task)
	require.NoError(t, err)
	result, _, err := pipeline.Run(context.Background(), trace.SpanFromContext(context.Background()), sources, segcore.StorageCost{})
	require.NoError(t, err)
	table := decodePostTableTest(t, result.GetPostProcessResult())
	require.Equal(t, []int64{0, 0, 1}, postTableIntColumn(t, table, postQueryIndexColumn))
	require.Equal(t, []int64{0, 0, 1}, postTableIntColumn(t, table, postSubqueryIndexColumn))
	require.Equal(t, []int64{0, 1, 1}, postTableIntColumn(t, table, postANNIndexColumn))
}

func TestHybridQueryExpansionRejectsFusionAndDoesNotPublishPartialBranch(t *testing.T) {
	for _, failingBranch := range []bool{false, true} {
		post := postTableTestPlan(t, failingBranch, transformTestSelect(t, types.IDFieldName))
		request := &milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{post}}
		pre := queryExpansionTestPlan(t, []int64{2}, false)
		second := queryExpansionTestPlan(t, []int64{1}, false)
		if failingBranch {
			op := transformTestMap("query", "query", "")
			op.Expr.Params["fail"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}}
			var err error
			second, err = transform.PreProcess("failing branch", op)
			require.NoError(t, err)
		}
		for _, plan := range []*schemapb.FunctionChain{pre, second} {
			request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{
				Nq: 2, PlaceholderGroup: transformTestPlaceholder(t, "one", "two"), FunctionChains: []*schemapb.FunctionChain{plan},
			})
		}
		task := transformTestTask(request)
		require.NoError(t, task.prepareSearchTransforms())
		original := proto.Clone(task.request)
		limiter := &queryExpansionQuotaLimiter{}
		ctx := taskmodel.WithSearchRateLimit(context.Background(), limiter, 100, nil, 4)
		err := task.executeSearchPreProcess(ctx)
		if failingBranch {
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		} else {
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Contains(t, err.Error(), "ANN_INPUTS")
		}
		require.True(t, proto.Equal(original, task.request))
		require.Nil(t, task.preQueryOrigins)
		require.False(t, task.preProcessed)
		require.Empty(t, limiter.tokens, "a failed branch or invalid result mode never reserves extra tokens")
	}
}

func TestSearchExpandedQueryContextKeepsOriginalAndPreparedSnapshots(t *testing.T) {
	pre := queryExpansionTestPlan(t, []int64{2, 1}, false)
	post := postTableTestPlan(t, false, queryContextUDFOp("annotation", types.IDFieldName))
	defer mockey.Mock(pyudf.CheckEnabled).Return(nil).Build().UnPatch()
	defer mockey.Mock(pyudf.ResolveResourcePath).Return("/tmp/query-context.whl", nil).Build().UnPatch()
	defer mockey.Mock(pyudf.RuntimeConfig).Return(pyudf.Config{}, nil).Build().UnPatch()
	defer mockey.Mock(pyudf.NewClient).Return(&pyudf.Client{}, nil).Build().UnPatch()
	observed := false
	defer mockey.Mock((*pyudf.Client).ExecuteWithSchema).To(func(_ *pyudf.Client, _ context.Context, call pyudf.ExecuteRequest) (*arrow.Schema, []*arrow.Chunked, error) {
		observed = true
		require.Equal(t, [][]byte{[]byte("one"), []byte("two")}, call.QueryContext.OriginalQueries[0].GetValues())
		require.Equal(t, [][]byte{[]byte("one:0"), []byte("one:1"), []byte("two:0")}, call.QueryContext.Queries[0].GetValues())
		require.Equal(t, [][]int64{{0, 0, 1}}, call.QueryContext.QueryOrigins)
		call.Inputs[0].Retain()
		return call.InputSchema, []*arrow.Chunked{call.Inputs[0]}, nil
	}).Build().UnPatch()
	task := transformTestTask(&milvuspb.SearchRequest{
		Nq: 2, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one", "two")},
		FunctionChains: []*schemapb.FunctionChain{pre, post},
	})
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.NoError(t, task.executeSearchPostProcess(context.Background(), transformTestResults([]int64{1, 0, 1})))
	require.True(t, observed)
}

type queryExpansionQuotaLimiter struct {
	tokens []int
	err    error
	cancel context.CancelFunc
}

func (l *queryExpansionQuotaLimiter) Check(_ int64, _ map[int64][]int64, rateType internalpb.RateType, tokens int) error {
	if rateType != internalpb.RateType_DQLSearch {
		return merr.WrapErrServiceInternalMsg("test expected DQLSearch rate type")
	}
	l.tokens = append(l.tokens, tokens)
	if l.cancel != nil {
		l.cancel()
	}
	return l.err
}

func (l *queryExpansionQuotaLimiter) Alloc(_ context.Context, dbID int64, parts map[int64][]int64, rateType internalpb.RateType, tokens int) error {
	return l.Check(dbID, parts, rateType, tokens)
}

func TestSearchQueryExpansionChargesOnlyAdditionalPreparedVectors(t *testing.T) {
	for _, test := range []struct {
		name     string
		counts   [][]int64
		original int64
		want     []int
	}{
		{name: "ordinary expansion", counts: [][]int64{{2, 1}}, original: 2, want: []int{1}},
		{name: "ordinary shrink", counts: [][]int64{{2, 0, 0}}, original: 3},
		{name: "hybrid unequal expansion", counts: [][]int64{{2, 0}, {1, 2}}, original: 2, want: []int{1}},
		{name: "hybrid shrink", counts: [][]int64{{1, 0}, {0, 1}}, original: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			limiter := &queryExpansionQuotaLimiter{}
			post := postTableTestPlan(t, len(test.counts) > 1, transformTestSelect(t, types.IDFieldName))
			request := &milvuspb.SearchRequest{Nq: test.original, FunctionChains: []*schemapb.FunctionChain{post}}
			queries := make([]string, test.original)
			for index := range queries {
				queries[index] = strconv.Itoa(index)
			}
			if len(test.counts) == 1 {
				request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, queries...)}
				request.FunctionChains = append(request.FunctionChains, queryExpansionTestPlan(t, test.counts[0], false))
			} else {
				for _, counts := range test.counts {
					request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{
						Nq: test.original, PlaceholderGroup: transformTestPlaceholder(t, queries...),
						FunctionChains: []*schemapb.FunctionChain{queryExpansionTestPlan(t, counts, false)},
					})
				}
			}
			ctx := taskmodel.WithSearchRateLimit(context.Background(), limiter, 100, map[int64][]int64{101: {201}}, int(test.original)*len(test.counts))
			for attempt := 0; attempt < 2; attempt++ {
				task := transformTestTask(request)
				require.NoError(t, task.prepareSearchTransforms())
				require.NoError(t, task.executeSearchPreProcess(ctx))
				require.NoError(t, task.executeSearchPreProcess(ctx), "same-task reentry does not charge again")
			}
			require.Equal(t, test.want, limiter.tokens, "another task in this RPC reuses the token reservation")
		})
	}
}

func TestSearchQueryExpansionQuotaFailureKeepsCodeAndDoesNotPublish(t *testing.T) {
	for _, failure := range []error{merr.ErrServiceRateLimit, merr.ErrServiceQuotaExceeded} {
		limiter := &queryExpansionQuotaLimiter{err: failure}
		ctx := taskmodel.WithSearchRateLimit(context.Background(), limiter, 100, map[int64][]int64{101: {201}}, 1)
		pre := queryExpansionTestPlan(t, []int64{2}, false)
		post := postTableTestPlan(t, false, transformTestSelect(t, types.IDFieldName))
		task := transformTestTask(&milvuspb.SearchRequest{
			Nq: 1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one")},
			FunctionChains: []*schemapb.FunctionChain{pre, post},
		})
		require.NoError(t, task.prepareSearchTransforms())
		original := proto.Clone(task.request)
		err := task.executeSearchPreProcess(ctx)
		require.ErrorIs(t, err, failure)
		require.Equal(t, merr.Code(failure), merr.Status(err).GetCode())
		require.True(t, proto.Equal(original, task.request))
		require.Equal(t, int64(1), task.GetNq())
		require.Zero(t, task.preOriginalNq)
		require.False(t, task.preProcessed)
		limiter.err = nil
		require.NoError(t, task.executeSearchPreProcess(ctx))
		require.NoError(t, task.executeSearchPreProcess(ctx))
		require.Equal(t, []int{1, 1}, limiter.tokens, "failed supplemental checks do not advance the watermark")
	}
}

func TestSearchQueryExpansionImplicitNQChargesActualPreparedCount(t *testing.T) {
	for _, hybrid := range []bool{false, true} {
		t.Run(strconv.FormatBool(hybrid), func(t *testing.T) {
			limiter := &queryExpansionQuotaLimiter{}
			// The interceptor sees the legacy zero NQ headers and reserves zero
			// tokens. Proxy infers two original queries from each placeholder.
			ctx := taskmodel.WithSearchRateLimit(context.Background(), limiter, 100, nil, 0)
			post := postTableTestPlan(t, hybrid, transformTestSelect(t, types.IDFieldName))
			request := &milvuspb.SearchRequest{FunctionChains: []*schemapb.FunctionChain{post}}
			pre := queryExpansionTestPlan(t, []int64{2, 1}, false)
			if hybrid {
				for branch := 0; branch < 2; branch++ {
					request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{
						PlaceholderGroup: transformTestPlaceholder(t, "one", "two"), FunctionChains: []*schemapb.FunctionChain{pre},
					})
				}
			} else {
				request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one", "two")}
				request.FunctionChains = append(request.FunctionChains, pre)
			}
			task := transformTestTask(request)
			require.NoError(t, task.prepareSearchTransforms())
			nq, err := task.checkNq(ctx)
			require.NoError(t, err)
			task.Nq = nq
			require.Equal(t, int64(2), nq)
			require.NoError(t, task.executeSearchPreProcess(ctx))
			if hybrid {
				require.Equal(t, []int{6}, limiter.tokens)
			} else {
				require.Equal(t, []int{3}, limiter.tokens)
			}
			require.NoError(t, task.executeSearchPreProcess(ctx))
		})
	}
}

func TestSearchQueryExpansionCancellationAfterEncodingOrQuotaDoesNotPublish(t *testing.T) {
	for _, afterEncoding := range []bool{false, true} {
		t.Run(strconv.FormatBool(afterEncoding), func(t *testing.T) {
			limiter := &queryExpansionQuotaLimiter{}
			ctx, cancel := context.WithCancel(taskmodel.WithSearchRateLimit(context.Background(), limiter, 100, nil, 1))
			defer cancel()
			pre := queryExpansionTestPlan(t, []int64{2}, false)
			post := postTableTestPlan(t, false, transformTestSelect(t, types.IDFieldName))
			task := transformTestTask(&milvuspb.SearchRequest{
				Nq: 1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one")},
				FunctionChains: []*schemapb.FunctionChain{pre, post},
			})
			require.NoError(t, task.prepareSearchTransforms())
			original := proto.Clone(task.request)
			if afterEncoding {
				encoded := transformTestPlaceholder(t, "one:0", "one:1")
				defer mockey.Mock(funcutil.FieldDataToPlaceholderGroupBytesWithCount).To(func(*schemapb.FieldData) ([]byte, int, error) {
					cancel()
					return encoded, 2, nil
				}).Build().UnPatch()
			} else {
				limiter.cancel = cancel
			}
			err := task.executeSearchPreProcess(ctx)
			require.ErrorIs(t, err, context.Canceled)
			require.True(t, proto.Equal(original, task.request))
			require.False(t, task.preProcessed)
			require.Zero(t, task.preOriginalNq)
			if afterEncoding {
				require.Empty(t, limiter.tokens, "encoding cancellation prevents supplemental charging")
			} else {
				require.Equal(t, []int{1}, limiter.tokens)
			}
		})
	}
}
