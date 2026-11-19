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
	report func(map[string]any) int
}

func (e requestReportExtension) Report(info any) int { return e.report(info.(map[string]any)) }

func enableUsageReport(t *testing.T, report func(map[string]any) int) {
	t.Helper()
	paramtable.Init()
	previous := extension.RequestUsageReporter()
	extension.SetRequestUsageReporter(requestReportExtension{report: report})
	t.Cleanup(func() { extension.SetRequestUsageReporter(previous) })
}

func TestRequestReport(t *testing.T) {
	ctx := t.Context()
	db := t.Name()
	size := int64(123)
	reports := 0
	enableUsageReport(t, func(info map[string]any) int {
		reports++
		require.Equal(t, map[string]any{
			hookutil.OpTypeKey: hookutil.OpTypeSearch, hookutil.DatabaseKey: db, hookutil.UsernameKey: "",
			hookutil.CollectionStorageBytesKey: size, hookutil.ReadCountKey: int64(4),
		}, info)
		return int(size * 4)
	})
	counter := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeSearch, db, "")
	before := testutil.ToFloat64(counter)
	r := newRequestReport(ctx, hookutil.OpTypeSearch, db)
	require.NotNil(t, r)
	r.readCount, r.collectionSize = 3, &size
	r.readCount = 4 // The final successful attempt supplies the measurement.
	status := merr.Success()
	require.Same(t, status, r.finish(ctx, status))
	r.finish(ctx, status)
	require.Equal(t, before+float64(size*4), testutil.ToFloat64(counter))
	require.Empty(t, status.GetExtraInfo())
	require.Equal(t, 1, reports)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, failure := range []struct {
		ctx    context.Context
		status *commonpb.Status
	}{{canceled, merr.Success()}, {ctx, merr.Status(merr.ErrServiceRateLimit)}, {ctx, nil}} {
		r = newRequestReport(ctx, hookutil.OpTypeSearch, db)
		r.readCount, r.collectionSize = 4, &size
		require.Same(t, failure.status, r.finish(failure.ctx, failure.status))
		r.finish(ctx, merr.Success())
	}
	r = newRequestReport(ctx, hookutil.OpTypeSearch, db)
	r.readCount = 1
	require.ErrorIs(t, merr.Error(r.finish(ctx, status)), merr.ErrServiceInternal)
	require.Equal(t, 1, reports)
	require.Equal(t, before+float64(size*4), testutil.ToFloat64(counter))
}

func TestRequestReportKeepsReporterAcrossReload(t *testing.T) {
	ctx, db := t.Context(), t.Name()
	var ops []string
	enableUsageReport(t, func(info map[string]any) int {
		ops = append(ops, info[hookutil.OpTypeKey].(string))
		return 0
	})
	r := newRequestReport(ctx, hookutil.OpTypeDelete, db)
	size := int64(16)
	r.writeBytes, r.readCount, r.collectionSize = 8, 1, &size

	extension.SetRequestUsageReporter(nil)
	require.Nil(t, newRequestReport(ctx, hookutil.OpTypeQuery, db))
	require.True(t, merr.Ok(r.finish(ctx, merr.Success())))
	require.Equal(t, []string{hookutil.OpTypeDelete, hookutil.OpTypeQuery}, ops)
}

func TestRequestReportLargeRead(t *testing.T) {
	db, ctx := t.Name(), t.Context()
	size := int64(1) << 62
	enableUsageReport(t, func(info map[string]any) int {
		require.EqualValues(t, 1, info[hookutil.ReadCountKey])
		require.Equal(t, size, info[hookutil.CollectionStorageBytesKey])
		return int(size)
	})
	counter := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeSearch, db, "")
	before := testutil.ToFloat64(counter)
	r := newRequestReport(ctx, hookutil.OpTypeSearch, db)
	r.collectionSize, r.readCount = &size, 4
	require.True(t, merr.Ok(r.finish(ctx, merr.Success())))
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
	write := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeDelete, db, "")
	read := metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), hookutil.OpTypeQuery, db, "")
	beforeWrite, beforeRead := testutil.ToFloat64(write), testutil.ToFloat64(read)
	size := int64(300)
	var writes []int64
	reads := 0
	enableUsageReport(t, func(info map[string]any) int {
		switch info[hookutil.OpTypeKey] {
		case hookutil.OpTypeDelete:
			require.NotContains(t, info, hookutil.ReadCountKey)
			n := info[hookutil.WriteBytesKey].(int64)
			writes = append(writes, n)
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
	for _, written := range []int64{24, 0} {
		r := newRequestReport(ctx, hookutil.OpTypeDelete, db)
		r.writeBytes, r.readCount, r.collectionSize = written, 1, &size
		status := merr.Success()
		require.Same(t, status, r.finish(ctx, status))
		r.finish(ctx, status)
		require.Empty(t, status.GetExtraInfo())
	}
	require.Equal(t, []int64{24, 0}, writes)
	require.Equal(t, 2, reads)
	require.Equal(t, beforeWrite+24, testutil.ToFloat64(write))
	require.Equal(t, beforeRead+float64(size*2), testutil.ToFloat64(read))
}
