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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/querymeter"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

type queryUsageReporter struct {
	hookutil.DefaultExtension
	reports []any
	value   int
}

func (r *queryUsageReporter) Report(report any) int {
	r.reports = append(r.reports, report)
	return r.value
}

func TestQueryReportsOneUsageValue(t *testing.T) {
	paramtable.Init()
	previousRateCollector := rateCol
	var err error
	rateCol, err = ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity, false)
	require.NoError(t, err)
	t.Cleanup(func() { rateCol = previousRateCollector })

	mockey.PatchConvey("only a successful Query emits usage", t, func() {
		node := &Proxy{viewQueryClient: struct{ queryclient.Client }{}}
		request := &milvuspb.QueryRequest{DbName: "db", CollectionName: "collection"}
		reporter := &queryUsageReporter{}
		oldReporter := extension.RequestUsageReporter()
		extension.SetRequestUsageReporter(reporter)
		t.Cleanup(func() { extension.SetRequestUsageReporter(oldReporter) })
		mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
		usageBytes := int64(73)
		var execErr error
		var cancel context.CancelFunc
		mockey.Mock((*Proxy).executeQuery).To(func(_ *Proxy, _ context.Context, _ *milvuspb.QueryRequest, metered bool) (*milvuspb.QueryResults, int64, error) {
			require.True(t, metered)
			if cancel != nil {
				cancel()
			}
			return &milvuspb.QueryResults{Status: merr.Status(execErr)}, usageBytes, execErr
		}).Build()

		response, err := node.Query(t.Context(), request)
		require.NoError(t, err)
		require.True(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)
		report := reporter.reports[0].(map[string]any)
		require.Len(t, report, 4)
		require.Equal(t, int64(73), report["bytes"])

		execErr = merr.WrapErrServiceInternalMsg("missing query read usage")
		response, err = node.Query(t.Context(), request)
		require.NoError(t, err)
		require.False(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)

		execErr = nil
		ctx, cancelQuery := context.WithCancel(t.Context())
		cancel = cancelQuery
		response, err = node.Query(ctx, request)
		require.NoError(t, err)
		require.False(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)

		cancel = nil
		request.QueryParams = []*commonpb.KeyValuePair{{Key: IteratorField, Value: "true"}}
		for i, pageBytes := range []int64{90, 20} {
			request.GuaranteeTimestamp = uint64(i) * 100
			usageBytes = pageBytes
			response, err = node.Query(t.Context(), request)
			require.NoError(t, err)
			require.True(t, merr.Ok(response.GetStatus()))
			require.Len(t, reporter.reports, i+2)
			page := reporter.reports[i+1].(map[string]any)
			require.Equal(t, pageBytes, page["bytes"])
		}
	})
}

func TestSearchReportsOneUsageValue(t *testing.T) {
	paramtable.Init()
	mockey.PatchConvey("only a successful Search emits its accumulated usage", t, func() {
		node := &Proxy{viewQueryClient: struct{ queryclient.Client }{}}
		request := &milvuspb.SearchRequest{DbName: "db", CollectionName: "collection"}
		reporter := &queryUsageReporter{}
		oldReporter := extension.RequestUsageReporter()
		extension.SetRequestUsageReporter(reporter)
		t.Cleanup(func() { extension.SetRequestUsageReporter(oldReporter) })
		mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()

		attempts := 0
		var cancel context.CancelFunc
		var fail error
		mockey.Mock((*Proxy).search).To(func(_ *Proxy, ctx context.Context, _ *milvuspb.SearchRequest, _, _, _ bool) (*milvuspb.SearchResults, bool, bool, bool, error) {
			meter := querymeter.SearchUsageFromContext(ctx)
			require.NotNil(t, meter)
			attempts++
			require.NoError(t, meter.AddRead(&internalpb.QueryReadUsage{DataBytes: 40}))
			if attempts == 1 {
				err := merr.WrapErrInconsistentRequery("retry after receiving the ANN reads")
				return &milvuspb.SearchResults{Status: merr.Status(err)}, false, false, false, err
			}
			require.NoError(t, meter.AddRead(&internalpb.QueryReadUsage{DataBytes: 10}))
			if cancel != nil {
				cancel()
			}
			return &milvuspb.SearchResults{
				Status:  merr.Status(fail),
				Results: &schemapb.SearchResultData{Scores: []float32{1, 2, 3}},
			}, false, false, false, fail
		}).Build()

		response, err := node.Search(t.Context(), request)
		require.NoError(t, err)
		require.True(t, merr.Ok(response.GetStatus()))
		require.Equal(t, 2, attempts)
		require.Len(t, reporter.reports, 1)
		report := reporter.reports[0].(map[string]any)
		require.Len(t, report, 4)
		require.Equal(t, hookutil.OpTypeSearch, report[hookutil.OpTypeKey])
		require.EqualValues(t, 102, report[hookutil.BytesKey])

		fail = merr.WrapErrServiceInternalMsg("missing search read usage")
		response, err = node.Search(t.Context(), request)
		require.NoError(t, err)
		require.False(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)

		fail = nil
		ctx, cancelSearch := context.WithCancel(t.Context())
		defer cancelSearch()
		cancel = cancelSearch
		response, err = node.Search(ctx, request)
		require.NoError(t, err)
		require.False(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)
	})
}

func TestHybridSearchReportsOneUsageValue(t *testing.T) {
	paramtable.Init()
	mockey.PatchConvey("HybridSearch bills the final response and shared query stages once", t, func() {
		node := &Proxy{viewQueryClient: struct{ queryclient.Client }{}}
		request := &milvuspb.HybridSearchRequest{DbName: "db", CollectionName: "collection"}
		reporter := &queryUsageReporter{}
		oldReporter := extension.RequestUsageReporter()
		extension.SetRequestUsageReporter(reporter)
		t.Cleanup(func() { extension.SetRequestUsageReporter(oldReporter) })
		mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
		var execErr error
		mockey.Mock((*Proxy).hybridSearch).To(func(_ *Proxy, ctx context.Context, _ *milvuspb.HybridSearchRequest, _, _ bool) (*milvuspb.SearchResults, bool, bool, error) {
			meter := querymeter.SearchUsageFromContext(ctx)
			require.NotNil(t, meter)
			require.NoError(t, meter.AddRead(&internalpb.QueryReadUsage{DataBytes: 110}))
			require.NoError(t, meter.AddRead(&internalpb.QueryReadUsage{DataBytes: 16}))
			return &milvuspb.SearchResults{
				Status:  merr.Status(execErr),
				Results: &schemapb.SearchResultData{Scores: []float32{1, 2}},
			}, false, false, execErr
		}).Build()

		response, err := node.HybridSearch(t.Context(), request)
		require.NoError(t, err)
		require.True(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)
		report := reporter.reports[0].(map[string]any)
		require.Len(t, report, 4)
		require.Equal(t, hookutil.OpTypeHybridSearch, report[hookutil.OpTypeKey])
		require.EqualValues(t, 134, report[hookutil.BytesKey])

		execErr = merr.WrapErrServiceInternalMsg("missing sub-search usage")
		response, err = node.HybridSearch(t.Context(), request)
		require.NoError(t, err)
		require.False(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)
	})
}

func TestMutationsReportReadUsageOnSharedMetric(t *testing.T) {
	for _, op := range []string{hookutil.OpTypeDelete, hookutil.OpTypeUpsert} {
		t.Run(op, func(t *testing.T) { testMutationReadUsageMetric(t, op) })
	}
}

func testMutationReadUsageMetric(t *testing.T, op string) {
	paramtable.Init()
	ctx := t.Context()
	db := t.Name()
	reporter := &queryUsageReporter{value: 7}
	counter := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), op, db, GetCurUserFromContextOrDefault(ctx))
	before := testutil.ToFloat64(counter)
	newReport := func() map[string]any {
		return map[string]any{hookutil.SuccessCntKey: int64(0), hookutil.BytesKey: int64(61)}
	}
	reportMutationUsage(ctx, merr.Success(), reporter, op, db, newReport())
	require.Len(t, reporter.reports, 1)
	report := reporter.reports[0].(map[string]any)
	require.Equal(t, op, report[hookutil.OpTypeKey])
	require.EqualValues(t, 61, report[hookutil.BytesKey])
	require.Equal(t, before+7, testutil.ToFloat64(counter))

	reportMutationUsage(ctx, merr.Status(merr.ErrServiceUnavailable), reporter, op, db, newReport())
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	reportMutationUsage(canceled, merr.Success(), reporter, op, db, newReport())
	require.Len(t, reporter.reports, 1)
	require.Equal(t, before+7, testutil.ToFloat64(counter))
}
