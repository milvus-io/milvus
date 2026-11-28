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

package rootcoord

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQuotaCollectionMetricRefresh(t *testing.T) {
	paramtable.Init()
	metrics.RootCoordNumEntities.Reset()
	metrics.RootCoordIndexedNumEntities.Reset()
	metrics.RootCoordRateLimitRatio.Reset()
	t.Cleanup(metrics.RootCoordNumEntities.Reset)
	t.Cleanup(metrics.RootCoordIndexedNumEntities.Reset)
	t.Cleanup(metrics.RootCoordRateLimitRatio.Reset)
	coll := &model.Collection{DBName: "db", Name: "old", Fields: []*model.Field{{FieldID: 2}}}
	node := metricsinfo.QueryNodeInfos{CollectionMetrics: &metricsinfo.QueryNodeCollectionMetrics{CollectionRows: map[int64]int64{1: 9}}}
	query := &metricsinfo.QueryCoordTopology{Cluster: metricsinfo.QueryClusterTopology{ConnectedNodes: []metricsinfo.QueryNodeInfos{node}}}
	info := &metricsinfo.DataCoordCollectionInfo{NumEntitiesTotal: 12, IndexInfo: []*metricsinfo.DataCoordIndexInfo{{IndexName: "idx", FieldID: 2, NumEntitiesIndexed: 10}}}
	cm := &metricsinfo.DataCoordCollectionMetrics{Collections: map[int64]*metricsinfo.DataCoordCollectionInfo{1: info}}
	data := &metricsinfo.DataCoordTopology{}
	data.Cluster.Self.QuotaMetrics = &metricsinfo.DataCoordQuotaMetrics{}
	data.Cluster.Self.CollectionMetrics = cm
	coord := mocks.NewMixCoord(t)
	coord.EXPECT().GetQueryCoordTopology(mock.Anything, mock.Anything).Return(query, nil)
	coord.EXPECT().GetDataCoordTopology(mock.Anything, mock.Anything).Return(data, nil)
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(nil, nil)
	meta.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, int64(1)).Return(coll, nil)
	proxies := proxyutil.NewMockProxyClientManager(t)

	q := NewQuotaCenter(proxies, coord, nil, meta)
	require.NoError(t, q.collectMetrics())
	require.Equal(t, 2, testutil.CollectAndCount(metrics.RootCoordNumEntities))
	require.Equal(t, 1, testutil.CollectAndCount(metrics.RootCoordIndexedNumEntities))
	coll.Name = "new"
	query.Cluster.ConnectedNodes = nil
	data.Cluster.Self.CollectionMetrics = nil
	require.NoError(t, q.collectMetrics())
	require.Equal(t, 2, testutil.CollectAndCount(metrics.RootCoordNumEntities))
	require.Equal(t, 9.0, testutil.ToFloat64(metrics.RootCoordNumEntities.WithLabelValues("db", "old", metrics.LoadedLabel)))
	data.Cluster.Self.CollectionMetrics = cm
	require.NoError(t, q.collectMetrics())
	require.Equal(t, 1, testutil.CollectAndCount(metrics.RootCoordNumEntities))
	require.Equal(t, 12.0, testutil.ToFloat64(metrics.RootCoordNumEntities.WithLabelValues("db", "new", metrics.TotalLabel)))
	require.Equal(t, 1, testutil.CollectAndCount(metrics.RootCoordIndexedNumEntities))
	info.IndexInfo = nil
	require.NoError(t, q.collectMetrics())
	require.Zero(t, testutil.CollectAndCount(metrics.RootCoordIndexedNumEntities))
	delete(cm.Collections, 1)
	require.NoError(t, q.collectMetrics())
	require.Zero(t, testutil.CollectAndCount(metrics.RootCoordNumEntities))
	q.publishRateLimitRatios(map[int64]float64{1: 0.5, 2: 0.25})
	q.incompleteQuotaMetrics = true
	q.publishRateLimitRatios(nil)
	require.Equal(t, 2, testutil.CollectAndCount(metrics.RootCoordRateLimitRatio))
	q.incompleteQuotaMetrics = false
	q.publishRateLimitRatios(map[int64]float64{2: 0.1})
	require.Equal(t, 1, testutil.CollectAndCount(metrics.RootCoordRateLimitRatio))
	require.InDelta(t, 0.9, testutil.ToFloat64(metrics.RootCoordRateLimitRatio.WithLabelValues("2")), 0.0001)
	q.publishRateLimitRatios(nil)
	require.Zero(t, testutil.CollectAndCount(metrics.RootCoordRateLimitRatio))
}
