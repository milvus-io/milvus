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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type requestReportExtension struct {
	hookutil.DefaultExtension
	publish extension.RequestReport
}

func (e *requestReportExtension) NewRequestReport() func(map[string]any) (int, error) {
	return e.publish
}

func TestRequestReport(t *testing.T) {
	original := hookutil.GetExtension()
	t.Cleanup(func() { hookutil.SetTestExtension(original) })
	ext := &requestReportExtension{}
	hookutil.SetTestExtension(ext)
	require.Nil(t, newRequestReport())

	db := t.Name()
	counter := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeSearch, db, "u")
	initial := testutil.ToFloat64(counter)
	calls := 0
	ext.publish = func(info map[string]any) (int, error) {
		calls++
		return int(info[hookutil.NqKey].(int64)), nil
	}
	report := newRequestReport()
	require.NotNil(t, report)
	hookutil.SetTestExtension(hookutil.DefaultExtension{})
	require.Nil(t, newRequestReport())

	attempt := func(nq int64) {
		report.record(merr.Success(), map[string]any{
			hookutil.OpTypeKey: hookutil.OpTypeSearch, hookutil.DatabaseKey: db, hookutil.UsernameKey: "u", hookutil.NqKey: nq,
		})
	}
	attempt(3)
	attempt(4)
	require.Zero(t, calls)
	status := merr.Success()
	report.finish(context.Background(), status)
	report.finish(context.Background(), status)
	require.Equal(t, 1, calls)
	require.Empty(t, status.GetExtraInfo())
	require.Equal(t, initial+4, testutil.ToFloat64(counter))

	attempt(7)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report.finish(ctx, merr.Success())
	report.finish(context.Background(), merr.Success())
	attempt(7)
	report.finish(context.Background(), merr.Status(merr.ErrServiceRateLimit))
	report.finish(context.Background(), merr.Success())
	attempt(7)
	report.finish(context.Background(), nil)
	report.finish(context.Background(), merr.Success())
	require.Equal(t, 1, calls)
	require.Equal(t, initial+4, testutil.ToFloat64(counter))
}

func TestRequestReportRejectsNegativeIncrements(t *testing.T) {
	for _, tc := range []struct {
		op               string
		value, extraRead int
	}{
		{hookutil.OpTypeSearch, -1, 0},
		{hookutil.OpTypeDelete, 24, -1},
	} {
		t.Run(tc.op, func(t *testing.T) {
			db, nodeID := t.Name(), paramtable.GetStringNodeID()
			counter := metrics.ProxyReportValue.WithLabelValues(nodeID, tc.op, db, "u")
			query := metrics.ProxyReportValue.WithLabelValues(nodeID, hookutil.OpTypeQuery, db, "u")
			before, beforeQuery := testutil.ToFloat64(counter), testutil.ToFloat64(query)
			report := &requestReport{publish: func(info map[string]any) (int, error) {
				info[hookutil.ExtraReadReportValueKey] = tc.extraRead
				return tc.value, nil
			}}
			status := merr.Success()
			report.record(status, map[string]any{hookutil.OpTypeKey: tc.op, hookutil.DatabaseKey: db, hookutil.UsernameKey: "u"})
			got := report.finish(t.Context(), status)
			if tc.op == hookutil.OpTypeDelete {
				require.Same(t, status, got)
			} else {
				require.ErrorIs(t, merr.Error(got), merr.ErrServiceInternal)
			}
			require.Equal(t, before, testutil.ToFloat64(counter))
			require.Equal(t, beforeQuery, testutil.ToFloat64(query))
		})
	}
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
		require.True(t, qt.CaptureCollectionSize)
		qt.CollectionSizeBytes = &size
		queries++
		return searchByPKQueryResult([]int64{1, 2, 3}, []float32{}, []bool{false, false, false}), segcore.StorageCost{}, nil
	}).Build()
	defer query.UnPatch()
	calls := 0
	hook := mockey.Mock(newRequestReport).To(func() *requestReport {
		return &requestReport{publish: func(info map[string]any) (int, error) {
			calls++
			require.Equal(t, hookutil.OpTypeSearch, info[hookutil.OpTypeKey])
			require.EqualValues(t, 3, info[hookutil.NqKey])
			require.EqualValues(t, size, info[hookutil.CollectionStorageBytesKey])
			return 21, nil
		}}
	}).Build()
	defer hook.UnPatch()
	result, err := node.Search(context.Background(), searchByPKRequest([]int64{3, 1, 2}))
	require.NoError(t, merr.CheckRPCCall(result, err))
	require.EqualValues(t, 3, result.GetResults().GetNumQueries())
	require.Equal(t, 1, queries)
	require.Equal(t, 1, calls)
	require.Empty(t, result.GetStatus().GetExtraInfo())
}

func TestFilteredDeletePublishesSeparateReadAndWriteUnits(t *testing.T) {
	db, user := t.Name(), "u"
	write := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeDelete, db, user)
	read := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeQuery, db, user)
	beforeWrite, beforeRead := testutil.ToFloat64(write), testutil.ToFloat64(read)
	r := &requestReport{publish: func(info map[string]any) (int, error) {
		info[hookutil.ExtraReadReportValueKey] = 17
		return 24, nil
	}}
	status := merr.Success()
	r.record(status, map[string]any{hookutil.OpTypeKey: hookutil.OpTypeDelete, hookutil.DatabaseKey: db, hookutil.UsernameKey: user})
	r.finish(context.Background(), status)
	r.finish(context.Background(), status)
	require.Equal(t, beforeWrite+24, testutil.ToFloat64(write))
	require.Equal(t, beforeRead+17, testutil.ToFloat64(read))
	require.Empty(t, status.GetExtraInfo())
}
