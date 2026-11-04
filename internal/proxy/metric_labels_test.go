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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

// existenceCache is a cache holding the collection "coll" in "db", plus the
// default database.
func existenceCache(t *testing.T) *MockCache {
	cache := NewMockCache(t)
	cache.EXPECT().HasCachedCollection(mock.Anything, mock.Anything).RunAndReturn(func(db, collection string) bool {
		return db == "db" && collection == "coll"
	}).Maybe()
	cache.EXPECT().HasCachedDatabase(mock.Anything).RunAndReturn(func(db string) bool {
		return db == "db" || db == "" || db == "default"
	}).Maybe()
	return cache
}

func TestCollectionMetricLabels(t *testing.T) {
	success := &milvuspb.SearchResults{Status: merr.Success()}
	notFound := &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrCollectionNotFound("ghost"))}
	denied := merr.WrapErrPrivilegeNotPermitted("denied")

	cases := []struct {
		name           string
		method         string
		db, collection string
		resp           any
		err            error
		wantDB         string
		wantCollection string
	}{
		{"an existing collection", "Search", "db", "coll", success, nil, "db", "coll"},
		{"an existing collection, failed", "Search", "db", "coll", nil, denied, "db", "coll"},
		{"a collection that does not exist", "Search", "db", "ghost", notFound, nil, "db", ""},
		{"a collection that does not exist, refused before resolving it", "Search", "db", "ghost", nil, denied, "db", ""},
		{"a database that does not exist", "Search", "ghostdb", "ghost", notFound, nil, "", ""},
		{"a database-level request", "ListCollections", "db", "", success, nil, "db", ""},
		{"a database-level request on a database that does not exist", "ListCollections", "ghostdb", "", nil, denied, "", ""},
		{"the default database", "ListCollections", "", "", success, nil, "", ""},
		{"a created collection", "CreateCollection", "db", "new", success, nil, "db", "new"},
		{"a collection that failed to be created", "CreateCollection", "db", "new", nil, denied, "db", ""},
		{"a created database", "CreateDatabase", "newdb", "", success, nil, "newdb", ""},
		{"a database that failed to be created", "CreateDatabase", "newdb", "", nil, denied, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, collection := CollectionMetricLabels(existenceCache(t), c.method, c.db, c.collection, c.resp, c.err)
			assert.Equal(t, c.wantDB, db)
			assert.Equal(t, c.wantCollection, collection)
		})
	}

	t.Run("no cache yet", func(t *testing.T) {
		db, collection := CollectionMetricLabels(nil, "Search", "db", "coll", success, nil)
		assert.Equal(t, "", db)
		assert.Equal(t, "", collection)
	})
}

func TestObserveReceivedNQLabelsOnlyExistingCollections(t *testing.T) {
	paramtable.Init()
	defer metrics.ProxyReceivedNQ.Reset()
	node := &Proxy{metaCache: existenceCache(t)}

	node.observeReceivedNQ(metrics.SearchLabel, "db", "coll", 3)
	node.observeReceivedNQ(metrics.SearchLabel, "db", "ghost", 2)

	assert.Equal(t, 1, metrics.ProxyReceivedNQ.DeletePartialMatch(prometheus.Labels{"collection_name": "coll"}))
	assert.Equal(t, 0, metrics.ProxyReceivedNQ.DeletePartialMatch(prometheus.Labels{"collection_name": "ghost"}))
	assert.Equal(t, 1, metrics.ProxyReceivedNQ.DeletePartialMatch(prometheus.Labels{"db_name": "db", "collection_name": ""}))
}

func TestAddQueryRateAllocatesSubRatesOnlyForExistingCollections(t *testing.T) {
	paramtable.Init()
	node := &Proxy{metaCache: existenceCache(t)}
	require.NoError(t, node.initRateCollector())

	node.addQueryRate(&milvuspb.QueryRequest{DbName: "db", CollectionName: "coll"})
	node.addQueryRate(&milvuspb.QueryRequest{DbName: "db", CollectionName: "ghost"})

	label := internalpb.RateType_DQLQuery.String()
	subRates, err := rateCol.RateSubLabel(label, ratelimitutil.DefaultAvgDuration)
	require.NoError(t, err)
	assert.Contains(t, subRates, ratelimitutil.FormatSubLabel(label, ratelimitutil.GetCollectionSubLabel("db", "coll")))
	assert.Len(t, subRates, 1, "no sub-rate may be allocated for a name that is not a collection")
}

func TestInvalidateCollectionMetaCacheOnRenameDeletesOldNameSeries(t *testing.T) {
	paramtable.Init()
	nodeID := paramtable.GetStringNodeID()
	defer metrics.ProxySearchVectors.Reset()

	cache := NewMockCache(t)
	cache.EXPECT().InvalidateCollectionMeta(mock.Anything, "db", "old", int64(7), false).Return(nil)
	shard := shardclient.NewMockShardClientManager(t)
	shard.EXPECT().InvalidateShardLeaderCache(mock.Anything).Return().Maybe()
	node := &Proxy{metaCache: cache, shardMgr: shard}
	require.NoError(t, node.initRateCollector())
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	metrics.ProxySearchVectors.WithLabelValues(nodeID, "db", "old").Add(1)
	metrics.ProxySearchVectors.WithLabelValues(nodeID, "db", "new").Add(1)

	status, err := node.InvalidateCollectionMetaCache(context.Background(), &proxypb.InvalidateCollMetaCacheRequest{
		Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_RenameCollection},
		DbName:         "db",
		CollectionName: "old",
		CollectionID:   7,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	assert.Equal(t, 0, metrics.ProxySearchVectors.DeletePartialMatch(prometheus.Labels{"collection_name": "old"}))
	assert.Equal(t, 1, metrics.ProxySearchVectors.DeletePartialMatch(prometheus.Labels{"collection_name": "new"}))
}

func TestInvalidateCredentialCacheDeletesTheUsersSeries(t *testing.T) {
	paramtable.Init()
	nodeID := paramtable.GetStringNodeID()
	defer metrics.UserRPCCounter.Reset()
	defer metrics.ProxyReportValue.Reset()

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	metrics.UserRPCCounter.WithLabelValues("dropped").Inc()
	metrics.UserRPCCounter.WithLabelValues("kept").Inc()
	metrics.ProxyReportValue.WithLabelValues(nodeID, "search", "db", "dropped").Add(1)
	metrics.ProxyReportValue.WithLabelValues(nodeID, "search", "db", "kept").Add(1)

	status, err := node.InvalidateCredentialCache(context.Background(), &proxypb.InvalidateCredCacheRequest{Username: "dropped"})
	require.NoError(t, merr.CheckRPCCall(status, err))

	assert.Equal(t, 0, metrics.UserRPCCounter.DeletePartialMatch(prometheus.Labels{"username": "dropped"}))
	assert.Equal(t, 0, metrics.ProxyReportValue.DeletePartialMatch(prometheus.Labels{"username": "dropped"}))
	assert.Equal(t, 1, metrics.UserRPCCounter.DeletePartialMatch(prometheus.Labels{"username": "kept"}))
	assert.Equal(t, 1, metrics.ProxyReportValue.DeletePartialMatch(prometheus.Labels{"username": "kept"}))
}
