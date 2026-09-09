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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/quota"
	rlinternal "github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

func setQuotaTestParam(t testing.TB, param *paramtable.ParamItem, value string) {
	t.Helper()
	previous, err := paramtable.GetBaseTable().Load(param.Key)
	require.NoError(t, Params.Save(param.Key, value))
	t.Cleanup(func() {
		if err != nil {
			require.NoError(t, Params.Reset(param.Key))
		} else {
			require.NoError(t, Params.Save(param.Key, previous))
		}
	})
}

func quotaReuseMetaForTest(collections int) *MetaTable {
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"quota": {ID: 10, Name: "quota"},
			"empty": {ID: 20, Name: "empty"},
		},
		collID2Meta: make(map[int64]*model.Collection, collections),
	}
	for i := range collections {
		id := int64(i + 100)
		mt.collID2Meta[id] = &model.Collection{
			DBID: 10, CollectionID: id, State: etcdpb.CollectionState_CollectionCreated,
			Partitions: []*model.Partition{
				{PartitionID: id * 10, State: etcdpb.PartitionState_PartitionCreated},
				{PartitionID: id*10 + 1, State: etcdpb.PartitionState_PartitionDropping},
			},
		}
	}
	return mt
}

type quotaRateSnapshot struct {
	Limit   Limit
	Updated bool
}

type quotaNodeSnapshot struct {
	Level  internalpb.RateScope
	ID     int64
	Rates  map[internalpb.RateType]quotaRateSnapshot
	States map[milvuspb.QuotaState]rlinternal.QuotaStateInfo
}

func quotaTreeSnapshot(root *rlinternal.RateLimiterNode) map[string]quotaNodeSnapshot {
	result := make(map[string]quotaNodeSnapshot)
	var visit func(string, *rlinternal.RateLimiterNode)
	visit = func(path string, node *rlinternal.RateLimiterNode) {
		snapshot := quotaNodeSnapshot{
			Level: node.Level(), ID: node.GetID(),
			Rates:  make(map[internalpb.RateType]quotaRateSnapshot),
			States: make(map[milvuspb.QuotaState]rlinternal.QuotaStateInfo),
		}
		node.GetLimiters().Range(func(rt internalpb.RateType, limiter *ratelimitutil.Limiter) bool {
			snapshot.Rates[rt] = quotaRateSnapshot{limiter.Limit(), limiter.HasUpdated()}
			return true
		})
		node.GetQuotaStates().Range(func(state milvuspb.QuotaState, info *rlinternal.QuotaStateInfo) bool {
			snapshot.States[state] = *info
			return true
		})
		result[path] = snapshot
		node.GetChildren().Range(func(id int64, child *rlinternal.RateLimiterNode) bool {
			visit(path+"/"+strconv.FormatInt(id, 10), child)
			return true
		})
	}
	visit("", root)
	return result
}

func TestQuotaTreeReuseMatchesFreshCalculation(t *testing.T) {
	setQuotaTestParam(t, &Params.QuotaConfig.DQLLimitEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.DMLLimitEnabled, "false")
	setQuotaTestParam(t, &Params.QuotaConfig.DQLMaxSearchRate, "1000")
	setQuotaTestParam(t, &Params.QuotaConfig.DQLMaxSearchRatePerDB, "800")
	setQuotaTestParam(t, &Params.QuotaConfig.DQLMaxSearchRatePerCollection, "600")
	setQuotaTestParam(t, &Params.QuotaConfig.DQLMaxSearchRatePerPartition, "400")
	setQuotaTestParam(t, &Params.QuotaConfig.DQLMaxQueryRatePerPartition, strconv.FormatFloat(float64(Inf), 'g', -1, 64))

	mt := quotaReuseMetaForTest(2)
	reused := NewQuotaCenter(nil, nil, nil, mt)
	fresh := NewQuotaCenter(nil, nil, nil, mt)
	t.Cleanup(reused.cancel)
	t.Cleanup(fresh.cancel)
	compareRound := func() {
		t.Helper()
		reused.clearMetrics()
		fresh.clearMetrics()
		require.NoError(t, reused.resetAllCurrentRates())
		require.NoError(t, resetQuotaFreshForTest(fresh))
		require.Equal(t, quotaTreeSnapshot(fresh.rateLimiter.GetRootLimiters()),
			quotaTreeSnapshot(reused.rateLimiter.GetRootLimiters()))
	}
	compareRound()
	root := reused.rateLimiter.GetRootLimiters()
	collection := reused.rateLimiter.GetCollectionLimiters(10, 100)
	partition := reused.rateLimiter.GetPartitionLimiters(10, 100, 1000)
	search, ok := collection.GetLimiters().Get(internalpb.RateType_DQLSearch)
	require.True(t, ok)

	// Simulate the previous calculation reducing rates and denying requests.
	rlinternal.TraverseRateLimiterTree(root, func(_ internalpb.RateType, limiter *ratelimitutil.Limiter) bool {
		limiter.SetLimit(0)
		return true
	}, nil)
	for _, node := range []*rlinternal.RateLimiterNode{root, reused.rateLimiter.GetDatabaseLimiters(10), collection, partition} {
		for _, state := range []milvuspb.QuotaState{milvuspb.QuotaState_DenyToWrite, milvuspb.QuotaState_DenyToRead, milvuspb.QuotaState_DenyToDDL} {
			node.GetQuotaStates().Insert(state,
				&rlinternal.QuotaStateInfo{ErrorCode: commonpb.ErrorCode_ForceDeny, Reason: "previous round"})
		}
	}
	collection.GetLimiters().Insert(internalpb.RateType(999), ratelimitutil.NewLimiter(0, 0))
	compareRound()
	require.Same(t, root, reused.rateLimiter.GetRootLimiters())
	require.Same(t, collection, reused.rateLimiter.GetCollectionLimiters(10, 100))
	require.Same(t, partition, reused.rateLimiter.GetPartitionLimiters(10, 100, 1000))
	nextSearch, _ := collection.GetLimiters().Get(internalpb.RateType_DQLSearch)
	require.Same(t, search, nextSearch)

	// Refresh overrides, drop membership, add a database and retain empty DBs.
	mt.collID2Meta[100].Properties = []*commonpb.KeyValuePair{{Key: common.CollectionSearchRateMaxKey, Value: "123"}}
	mt.collID2Meta[100].Partitions = mt.collID2Meta[100].Partitions[:1]
	delete(mt.collID2Meta, 101)
	mt.collID2Meta[200] = &model.Collection{DBID: 30, CollectionID: 200, State: etcdpb.CollectionState_CollectionCreated}
	compareRound()
	require.Nil(t, reused.rateLimiter.GetCollectionLimiters(10, 101))
	require.Nil(t, reused.rateLimiter.GetPartitionLimiters(10, 100, 1001))
	require.Equal(t, Limit(123), search.Limit())

	// Turning partition limits off must remove old partition nodes; a finite
	// collection property reverting to infinity must not retain HasUpdated.
	mt.collID2Meta[100].Properties = nil
	setQuotaTestParam(t, &Params.QuotaConfig.DQLMaxSearchRatePerCollection, strconv.FormatFloat(float64(Inf), 'g', -1, 64))
	setQuotaTestParam(t, &Params.QuotaConfig.DQLMaxSearchRatePerPartition, strconv.FormatFloat(float64(Inf), 'g', -1, 64))
	compareRound()
	require.Zero(t, collection.GetChildren().Len())
	require.Equal(t, Inf, search.Limit())
	require.False(t, search.HasUpdated())

	delete(mt.collID2Meta, 100)
	delete(mt.dbName2Meta, "quota")
	compareRound()
	require.Nil(t, reused.rateLimiter.GetDatabaseLimiters(10))
	require.NotNil(t, reused.rateLimiter.GetDatabaseLimiters(20))
	// A later re-created collection and re-enabled partition limit are included.
	mt.collID2Meta[100] = quotaReuseMetaForTest(1).collID2Meta[100]
	setQuotaTestParam(t, &Params.QuotaConfig.DQLLimitEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.DQLMaxSearchRatePerPartition, "200")
	compareRound()
	require.NotNil(t, reused.rateLimiter.GetPartitionLimiters(10, 100, 1000))
}

func resetQuotaFreshForTest(q *QuotaCenter) error {
	clusterLimiter := newParamLimiterFunc(internalpb.RateScope_Cluster, allOps)()
	q.rateLimiter = rlinternal.NewRateLimiterTree(clusterLimiter)

	enablePartitionRateLimit := false
	for rt := range getRateTypes(internalpb.RateScope_Partition, allOps) {
		r := quota.GetQuotaValue(internalpb.RateScope_Partition, rt, Params)
		if Limit(r) != Inf {
			enablePartitionRateLimit = true
		}
	}

	// updateLimiterHasUpdated checks all limiters in a RateLimiterNode and sets hasUpdated to true
	// for those with non-Inf values
	updateLimiterHasUpdated := func(node *rlinternal.RateLimiterNode) {
		if node == nil {
			return
		}
		node.GetLimiters().Range(func(rateType internalpb.RateType, limiter *ratelimitutil.Limiter) bool {
			if limiter.Limit() != Inf {
				limiter.SetHasUpdated(true)
			}
			return true
		})
	}

	collectionRateTypes := getRateTypes(internalpb.RateScope_Collection, allOps)
	initLimiters := func(sourceCollections map[int64]map[int64][]int64) {
		for dbID, collections := range sourceCollections {
			for collectionID, partitionIDs := range collections {
				collectionLimitVals := make(map[internalpb.RateType]Limit, collectionRateTypes.Len())
				collectionRateTypes.Range(func(rt internalpb.RateType) bool {
					limitVal, err := q.getCollectionMaxLimit(rt, collectionID)
					if err != nil {
						limitVal = Limit(quota.GetQuotaValue(internalpb.RateScope_Collection, rt, Params))
					}
					collectionLimitVals[rt] = limitVal
					return true
				})

				getCollectionLimitVal := func(rateType internalpb.RateType) Limit {
					return collectionLimitVals[rateType]
				}

				collectionLimiter := q.rateLimiter.GetOrCreateCollectionLimiters(dbID, collectionID,
					newParamLimiterFunc(internalpb.RateScope_Database, allOps),
					newParamLimiterFuncWithLimitFunc(internalpb.RateScope_Collection, allOps, getCollectionLimitVal))
				updateLimiterHasUpdated(collectionLimiter)

				if !enablePartitionRateLimit {
					continue
				}
				for _, partitionID := range partitionIDs {
					partitionLimiter := q.rateLimiter.GetOrCreatePartitionLimiters(dbID, collectionID, partitionID,
						newParamLimiterFunc(internalpb.RateScope_Database, allOps),
						newParamLimiterFuncWithLimitFunc(internalpb.RateScope_Collection, allOps, getCollectionLimitVal),
						newParamLimiterFunc(internalpb.RateScope_Partition, allOps))
					updateLimiterHasUpdated(partitionLimiter)
				}
			}
			if len(collections) == 0 {
				dbLimiter := q.rateLimiter.GetOrCreateDatabaseLimiters(dbID, newParamLimiterFunc(internalpb.RateScope_Database, allOps))
				updateLimiterHasUpdated(dbLimiter)
			}
		}
	}
	partitions := q.meta.ListAllAvailPartitions(q.ctx)
	initLimiters(partitions)
	return nil
}
