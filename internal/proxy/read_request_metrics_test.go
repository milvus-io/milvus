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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestProxySearchMetricsRecordedBeforeEarlyReturn(t *testing.T) {
	paramtable.Init()
	const (
		databaseName   = "search_metrics_early_return_db"
		collectionName = "search_metrics_early_return_collection"
		nq             = int64(3)
	)

	request := &milvuspb.SearchRequest{
		DbName:         databaseName,
		CollectionName: collectionName,
		Nq:             nq,
	}

	run := func(t *testing.T, node *Proxy) {
		ctx := metrics.WrapRestfulContext(context.Background(), 1)
		receivedBytes := metrics.ProxyReceiveBytes.WithLabelValues(
			paramtable.GetStringNodeID(), metrics.SearchLabel, databaseName, collectionName,
		)
		receivedNQ := metrics.ProxyReceivedNQ.WithLabelValues(
			paramtable.GetStringNodeID(), metrics.SearchLabel, databaseName, collectionName,
		)
		bytesBefore := testutil.ToFloat64(receivedBytes)
		nqBefore := testutil.ToFloat64(receivedNQ)

		response, err := node.Search(ctx, request)
		require.NoError(t, err)
		require.Error(t, merr.Error(response.GetStatus()))
		metrics.RecordRestfulMetrics(ctx, 0, false)

		assert.Equal(t, bytesBefore+1, testutil.ToFloat64(receivedBytes))
		assert.Equal(t, nqBefore+float64(nq), testutil.ToFloat64(receivedNQ))
	}

	t.Run("unhealthy", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		run(t, node)
	})

	t.Run("snapshot resolution failure", func(t *testing.T) {
		oldCache := globalMetaCache
		defer func() { globalMetaCache = oldCache }()

		cache := NewMockCache(t)
		cache.EXPECT().
			GetCollectionInfo(mock.Anything, databaseName, collectionName, int64(0)).
			Return(nil, merr.ErrCollectionNotFound)
		globalMetaCache = cache

		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		run(t, node)
	})
}

func TestProxyHybridSearchStatsRecordedBeforeEarlyReturn(t *testing.T) {
	paramtable.Init()
	const (
		databaseName   = "hybrid_metrics_early_return_db"
		collectionName = "hybrid_metrics_early_return_collection"
	)

	request := &milvuspb.HybridSearchRequest{
		DbName:         databaseName,
		CollectionName: collectionName,
	}

	run := func(t *testing.T, node *Proxy) {
		ctx := metrics.WrapRestfulContext(context.Background(), 1)
		receivedBytes := metrics.ProxyReceiveBytes.WithLabelValues(
			paramtable.GetStringNodeID(), metrics.HybridSearchLabel, databaseName, collectionName,
		)
		before := testutil.ToFloat64(receivedBytes)

		response, err := node.HybridSearch(ctx, request)
		require.NoError(t, err)
		require.Error(t, merr.Error(response.GetStatus()))
		metrics.RecordRestfulMetrics(ctx, 0, false)

		assert.Equal(t, before+1, testutil.ToFloat64(receivedBytes))
	}

	t.Run("unhealthy", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		run(t, node)
	})

	t.Run("snapshot resolution failure", func(t *testing.T) {
		oldCache := globalMetaCache
		defer func() { globalMetaCache = oldCache }()

		cache := NewMockCache(t)
		cache.EXPECT().
			GetCollectionInfo(mock.Anything, databaseName, collectionName, int64(0)).
			Return(nil, merr.ErrCollectionNotFound)
		globalMetaCache = cache

		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		run(t, node)
	})
}

func TestSearchTaskSparseNNZMetricKeepsRequestedCollectionName(t *testing.T) {
	paramtable.Init()

	histogramCount := func(t *testing.T, observer prometheus.Observer) uint64 {
		metric, ok := observer.(prometheus.Metric)
		require.True(t, ok)
		value := &dto.Metric{}
		require.NoError(t, metric.Write(value))
		return value.GetHistogram().GetSampleCount()
	}

	for _, queryType := range []string{metrics.SearchLabel, metrics.HybridSearchLabel} {
		t.Run(queryType, func(t *testing.T) {
			requestedName := "requested_alias_" + queryType
			canonicalName := "canonical_collection_" + queryType
			fieldID := int64(101)
			requestedMetric := metrics.ProxySearchSparseNumNonZeros.WithLabelValues(
				paramtable.GetStringNodeID(), requestedName, queryType, "101",
			)
			canonicalMetric := metrics.ProxySearchSparseNumNonZeros.WithLabelValues(
				paramtable.GetStringNodeID(), canonicalName, queryType, "101",
			)
			requestedBefore := histogramCount(t, requestedMetric)
			canonicalBefore := histogramCount(t, canonicalMetric)

			task := &searchTask{
				collectionName: canonicalName,
				request: &milvuspb.SearchRequest{
					CollectionName: requestedName,
				},
			}
			task.observeSparseVectorNNZ(queryType, fieldID, make([]byte, 18), 1)

			assert.Equal(t, requestedBefore+1, histogramCount(t, requestedMetric))
			assert.Equal(t, canonicalBefore, histogramCount(t, canonicalMetric))
		})
	}
}
