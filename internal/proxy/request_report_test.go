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

package proxy

import (
	"context"
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type usageTestExtension struct {
	hookutil.DefaultExtension
	report func(map[string]any) int
}

func (e usageTestExtension) Report(info any) int { return e.report(info.(map[string]any)) }

func enableUsageReport(t *testing.T, report func(map[string]any) int) {
	t.Helper()
	paramtable.Init()
	previous := extension.RequestUsageReporter()
	extension.SetRequestUsageReporter(usageTestExtension{report: report})
	t.Cleanup(func() { extension.SetRequestUsageReporter(previous) })
}

func TestReportReadUsage(t *testing.T) {
	ctx, db := t.Context(), t.Name()
	size := int64(123)
	var calls int
	enableUsageReport(t, func(info map[string]any) int {
		calls++
		require.Equal(t, map[string]any{
			hookutil.OpTypeKey: hookutil.OpTypeSearch, hookutil.DatabaseKey: db, hookutil.UsernameKey: "",
			hookutil.CollectionStorageBytesKey: size, hookutil.ReadCountKey: int64(4),
		}, info)
		return int(size * 4)
	})
	reporter := extension.RequestUsageReporter()
	counter := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeSearch, db, "")
	before := testutil.ToFloat64(counter)
	status := merr.Success()
	require.Same(t, status, reportReadUsage(ctx, reporter, hookutil.OpTypeSearch, db, status, &size, 4))
	require.Empty(t, status.GetExtraInfo())

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, failure := range []struct {
		ctx    context.Context
		status *commonpb.Status
	}{{canceled, status}, {ctx, merr.Status(merr.ErrServiceRateLimit)}, {ctx, nil}} {
		require.Same(t, failure.status, reportReadUsage(failure.ctx, reporter, hookutil.OpTypeSearch, db, failure.status, nil, 4))
	}
	require.Same(t, status, reportReadUsage(ctx, nil, hookutil.OpTypeSearch, db, status, nil, 4))
	require.ErrorIs(t, merr.Error(reportReadUsage(ctx, reporter, hookutil.OpTypeSearch, db, status, nil, 4)), merr.ErrServiceInternal)
	require.Equal(t, 1, calls)
	require.Equal(t, before+float64(size*4), testutil.ToFloat64(counter))
}

func TestSearchReportsFinalAttemptAcrossReload(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	limitCheck := &Params.AutoIndexConfig.EnableResultLimitCheck
	previous := limitCheck.GetValue()
	require.NoError(t, Params.Save(limitCheck.Key, "true"))
	t.Cleanup(func() { require.NoError(t, Params.Save(limitCheck.Key, previous)) })
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()

	const db = "final_search_attempt"
	reports := 0
	enableUsageReport(t, func(info map[string]any) int {
		reports++
		require.EqualValues(t, 200, info[hookutil.CollectionStorageBytesKey])
		require.EqualValues(t, 2, info[hookutil.ReadCountKey])
		return 400
	})
	calls := 0
	search := mockey.Mock((*Proxy).search).To(func(_ *Proxy, _ context.Context, _ *milvuspb.SearchRequest, opts searchOptions) (*milvuspb.SearchResults, searchAttempt, error) {
		calls++
		require.False(t, opts.reportLegacy)
		size := int64(calls * 100)
		attempt := searchAttempt{collectionBytes: &size, readCount: 2}
		switch calls {
		case 1:
			require.True(t, opts.optimized)
			extension.SetRequestUsageReporter(nil)
			attempt.resultSizeInsufficient, attempt.topKReduced = true, true
		case 2:
			require.False(t, opts.optimized)
			attempt.evaluateRecall = true
		case 3:
			require.True(t, opts.recallEvaluation)
		}
		return &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{NumQueries: 2}}, attempt, nil
	}).Build()
	defer search.UnPatch()
	result, err := node.Search(t.Context(), &milvuspb.SearchRequest{DbName: db, CollectionName: "search"})
	require.NoError(t, merr.CheckRPCCall(result, err))
	require.Equal(t, 3, calls)
	require.Equal(t, 1, reports)
}

func TestReportReadUsageLargeProduct(t *testing.T) {
	db, ctx := t.Name(), t.Context()
	size := int64(1) << 62
	enableUsageReport(t, func(info map[string]any) int {
		require.EqualValues(t, 1, info[hookutil.ReadCountKey])
		require.Equal(t, size, info[hookutil.CollectionStorageBytesKey])
		return int(size)
	})
	counter := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeSearch, db, "")
	before := testutil.ToFloat64(counter)
	status := reportReadUsage(ctx, extension.RequestUsageReporter(), hookutil.OpTypeSearch, db, merr.Success(), &size, 4)
	require.True(t, merr.Ok(status))
	require.Equal(t, before+math.Exp2(64), testutil.ToFloat64(counter))
}

func TestSearchReportsRequestedIDsOnEmptyResult(t *testing.T) {
	metacache.MockWriteSchemaForTest(t)
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionInfo(mock.Anything, "default", "test_collection", int64(0)).
		Return(&collectionInfo{CollID: 42, Schema: mustNewSchemaInfo(searchByPKTestSchema(true))}, nil)
	node := &Proxy{metaCache: cache}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	size := int64(123)
	queries := 0
	query := mockey.Mock((*Proxy).query).To(func(_ *Proxy, _ context.Context, qt *queryTask, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
		qt.CollectionSizeBytes = &size
		queries++
		return searchByPKQueryResult([]int64{1, 2, 3}, []float32{}, []bool{false, false, false}), segcore.StorageCost{}, nil
	}).Build()
	defer query.UnPatch()
	calls := 0
	enableUsageReport(t, func(info map[string]any) int {
		calls++
		require.Equal(t, hookutil.OpTypeSearch, info[hookutil.OpTypeKey])
		require.EqualValues(t, 3, info[hookutil.ReadCountKey])
		require.EqualValues(t, size, info[hookutil.CollectionStorageBytesKey])
		return int(size * 3)
	})
	result, err := node.Search(context.Background(), searchByPKRequest([]int64{3, 1, 2}))
	require.NoError(t, merr.CheckRPCCall(result, err))
	require.EqualValues(t, 3, result.GetResults().GetNumQueries())
	require.Equal(t, 1, queries)
	require.Equal(t, 1, calls)
	require.Empty(t, result.GetStatus().GetExtraInfo())
}

func TestFilteredDeletePublishesSeparateReadAndWriteBytes(t *testing.T) {
	ctx, db := t.Context(), t.Name()
	const collection = "delete_bytes"
	const size = int64(300)
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "score", DataType: schemapb.DataType_Int64},
	}})
	cache := NewMockCache(t)
	cache.EXPECT().GetDatabaseInfo(mock.Anything, db).Return(&databaseInfo{DBID: 9}, nil)
	cache.EXPECT().GetCollectionSchema(mock.Anything, db, collection).Return(schema, nil)
	cache.EXPECT().GetCollectionID(mock.Anything, db, collection).Return(10, nil)
	cache.EXPECT().GetCollectionInfo(mock.Anything, db, collection, int64(10)).Return(&collectionInfo{CollID: 10, Schema: schema}, nil)
	node := &Proxy{metaCache: cache, sched: &scheduler.TaskScheduler{}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	previous := rateCol
	require.NoError(t, node.initRateCollector())
	t.Cleanup(func() { rateCol = previous })

	var matchedBytes int64
	run := mockey.Mock((*deleteRunner).Run).Return(nil).Build()
	defer run.UnPatch()
	pkBytes := mockey.Mock((*deleteRunner).DeletedPKBytes).To(func(*deleteRunner) int64 { return matchedBytes }).Build()
	defer pkBytes.UnPatch()
	tableBytes := mockey.Mock((*deleteRunner).CollectionSizeBytes).Return(size).Build()
	defer tableBytes.UnPatch()

	write := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeDelete, db, "")
	read := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeQuery, db, "")
	beforeWrite, beforeRead := testutil.ToFloat64(write), testutil.ToFloat64(read)
	var writes []int64
	reads := 0
	enableUsageReport(t, func(info map[string]any) int {
		switch info[hookutil.OpTypeKey] {
		case hookutil.OpTypeDelete:
			require.NotContains(t, info, hookutil.ReadCountKey)
			n := info[hookutil.WriteBytesKey].(int64)
			writes = append(writes, n)
			extension.SetRequestUsageReporter(nil) // Reload between the write and the read report.
			return int(n)
		case hookutil.OpTypeQuery:
			require.NotContains(t, info, hookutil.WriteBytesKey)
			require.Equal(t, size, info[hookutil.CollectionStorageBytesKey])
			require.EqualValues(t, 1, info[hookutil.ReadCountKey])
			reads++
			return int(size)
		default:
			t.Fatalf("unexpected report: %v", info)
			return 0
		}
	})
	reporter := extension.RequestUsageReporter()
	for _, tc := range []struct {
		expr       string
		writeBytes int64
	}{{"score > 10", 24}, {"score > 10", 0}, {"id in [42]", 8}} {
		extension.SetRequestUsageReporter(reporter)
		matchedBytes = tc.writeBytes
		result, err := node.Delete(ctx, &milvuspb.DeleteRequest{DbName: db, CollectionName: collection, Expr: tc.expr})
		require.NoError(t, merr.CheckRPCCall(result, err))
		require.Empty(t, result.GetStatus().GetExtraInfo())
	}
	require.Equal(t, []int64{24, 0, 8}, writes)
	require.Equal(t, 2, reads)
	require.Equal(t, beforeWrite+32, testutil.ToFloat64(write))
	require.Equal(t, beforeRead+float64(size*2), testutil.ToFloat64(read))
}
