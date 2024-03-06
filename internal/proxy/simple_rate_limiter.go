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
	"fmt"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	rlinternal "github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// SimpleLimiter is implemented based on Limiter interface
type SimpleLimiter struct {
	quotaStatesMu sync.RWMutex
	rateLimiter   *rlinternal.RateLimiterTree
}

// NewSimpleLimiter returns a new SimpleLimiter.
func NewSimpleLimiter() *SimpleLimiter {
	rootRateLimiter := newClusterLimiter()
	m := &SimpleLimiter{rateLimiter: rlinternal.NewRateLimiterTree(rootRateLimiter)}
	return m
}

// Check checks if request would be limited or denied.
func (m *SimpleLimiter) Check(dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error {
	if !Params.QuotaConfig.QuotaAndLimitsEnabled.GetAsBool() {
		return nil
	}

	m.quotaStatesMu.RLock()
	defer m.quotaStatesMu.RUnlock()

	// 1. check global(cluster) level rate limits
	clusterRateLimiters := m.rateLimiter.GetRootLimiters()
	ret := clusterRateLimiters.Check(rt, n)

	if ret != nil {
		clusterRateLimiters.Cancel(rt, n)
		return ret
	}

	// store done limiters to cancel them when error occurs.
	doneLimiters := make([]*rlinternal.RateLimiterNode, 0)
	doneLimiters = append(doneLimiters, clusterRateLimiters)

	cancelAllLimiters := func() {
		for _, limiter := range doneLimiters {
			limiter.Cancel(rt, n)
		}
	}

	// 2. check database level rate limits
	if ret == nil {
		dbRateLimiters := m.rateLimiter.GetOrCreateDatabaseLimiters(dbID, newDatabaseLimiter)
		ret = dbRateLimiters.Check(rt, n)
		if ret != nil {
			cancelAllLimiters()
			return ret
		}
		doneLimiters = append(doneLimiters, dbRateLimiters)
	}

	// 3. check collection level rate limits
	if ret == nil && len(collectionIDToPartIDs) > 0 && !isNotCollectionLevelLimitRequest(rt) {
		for collectionID := range collectionIDToPartIDs {
			// only dml and dql have collection level rate limits
			collectionRateLimiters := m.rateLimiter.GetOrCreateCollectionLimiters(dbID, collectionID,
				newDatabaseLimiter, newCollectionLimiters)
			ret = collectionRateLimiters.Check(rt, n)
			if ret != nil {
				cancelAllLimiters()
				return ret
			}
			doneLimiters = append(doneLimiters, collectionRateLimiters)
		}
	}

	// 4. check partition level rate limits
	if ret == nil && len(collectionIDToPartIDs) > 0 {
		for collectionID, partitionIDs := range collectionIDToPartIDs {
			for _, partID := range partitionIDs {
				partitionRateLimiters := m.rateLimiter.GetOrCreatePartitionLimiters(dbID, collectionID, partID,
					newDatabaseLimiter, newCollectionLimiters, newPartitionLimiters)
				ret = partitionRateLimiters.Check(rt, n)
				if ret != nil {
					cancelAllLimiters()
					return ret
				}
				doneLimiters = append(doneLimiters, partitionRateLimiters)
			}
		}
	}

	return ret
}

func isNotCollectionLevelLimitRequest(rt internalpb.RateType) bool {
	// Most ddl is global level, only DDLFlush will be applied at collection
	switch rt {
	case internalpb.RateType_DDLCollection, internalpb.RateType_DDLPartition, internalpb.RateType_DDLIndex,
		internalpb.RateType_DDLCompaction:
		return true
	default:
		return false
	}
}

// GetQuotaStates returns quota states.
func (m *SimpleLimiter) GetQuotaStates() ([]milvuspb.QuotaState, []string) {
	m.quotaStatesMu.RLock()
	defer m.quotaStatesMu.RUnlock()
	serviceStates := make(map[milvuspb.QuotaState]typeutil.Set[commonpb.ErrorCode])

	rlinternal.TraverseRateLimiterTree(m.rateLimiter.GetRootLimiters(), nil,
		func(state milvuspb.QuotaState, errCode commonpb.ErrorCode) bool {
			if serviceStates[state] == nil {
				serviceStates[state] = typeutil.NewSet[commonpb.ErrorCode]()
			}
			serviceStates[state].Insert(errCode)
			return true
		})

	states := make([]milvuspb.QuotaState, 0)
	reasons := make([]string, 0)
	for state, errCodes := range serviceStates {
		for errCode := range errCodes {
			states = append(states, state)
			reasons = append(reasons, ratelimitutil.GetQuotaErrorString(errCode))
		}
	}

	return states, reasons
}

// SetRates sets quota states for SimpleLimiter.
func (m *SimpleLimiter) SetRates(rootLimiter *proxypb.LimiterNode) error {
	m.quotaStatesMu.Lock()
	defer m.quotaStatesMu.Unlock()
	if err := m.updateRateLimiter(rootLimiter); err != nil {
		return err
	}

	// TODO fubang: remove dropped database/collection/partition rate limiters
	return nil
}

func initLimiter(rln *rlinternal.RateLimiterNode, rateLimiterConfigs map[internalpb.RateType]*paramtable.ParamItem) {
	log := log.Ctx(context.TODO()).WithRateGroup("proxy.rateLimiter", 1.0, 60.0)
	for rt, p := range rateLimiterConfigs {
		limit := ratelimitutil.Limit(p.GetAsFloat())
		burst := p.GetAsFloat() // use rate as burst, because SimpleLimiter is with punishment mechanism, burst is insignificant.
		rln.GetLimiters().GetOrInsert(rt, ratelimitutil.NewLimiter(limit, burst))
		onEvent := func(rateType internalpb.RateType, formatFunc func(originValue string) string) func(*config.Event) {
			return func(event *config.Event) {
				f, err := strconv.ParseFloat(formatFunc(event.Value), 64)
				if err != nil {
					log.Info("Error format for rateLimit",
						zap.String("rateType", rateType.String()),
						zap.String("key", event.Key),
						zap.String("value", event.Value),
						zap.Error(err))
					return
				}
				l, ok := rln.GetLimiters().Get(rateType)
				if !ok {
					log.Info("rateLimiter not found for rateType", zap.String("rateType", rateType.String()))
					return
				}
				l.SetLimit(ratelimitutil.Limit(f))
			}
		}(rt, p.Formatter)
		paramtable.Get().Watch(p.Key, config.NewHandler(fmt.Sprintf("rateLimiter-%d", rt), onEvent))
		log.RatedDebug(30, "RateLimiter register for rateType",
			zap.String("rateType", internalpb.RateType_name[(int32(rt))]),
			zap.String("rateLimit", ratelimitutil.Limit(p.GetAsFloat()).String()),
			zap.String("burst", fmt.Sprintf("%v", burst)))
	}
}

// newClusterLimiter init limiter of cluster level for all rate types and rate scopes.
// Cluster rate limiter doesn't support to accumulate metrics dynamically, it only uses
// configurations as limit values.
func newClusterLimiter() *rlinternal.RateLimiterNode {
	clusterRateLimiters := rlinternal.NewRateLimiterNode()
	clusterLimiterConfigs := getDefaultLimiterConfig(internalpb.RateScope_Cluster)
	initLimiter(clusterRateLimiters, clusterLimiterConfigs)
	return clusterRateLimiters
}

func newDatabaseLimiter() *rlinternal.RateLimiterNode {
	dbRateLimiters := rlinternal.NewRateLimiterNode()
	databaseLimiterConfigs := getDefaultLimiterConfig(internalpb.RateScope_Database)
	initLimiter(dbRateLimiters, databaseLimiterConfigs)
	return dbRateLimiters
}

func newCollectionLimiters() *rlinternal.RateLimiterNode {
	collectionRateLimiters := rlinternal.NewRateLimiterNode()
	collectionLimiterConfigs := getDefaultLimiterConfig(internalpb.RateScope_Collection)
	initLimiter(collectionRateLimiters, collectionLimiterConfigs)
	return collectionRateLimiters
}

func newPartitionLimiters() *rlinternal.RateLimiterNode {
	partRateLimiters := rlinternal.NewRateLimiterNode()
	collectionLimiterConfigs := getDefaultLimiterConfig(internalpb.RateScope_Partition)
	initLimiter(partRateLimiters, collectionLimiterConfigs)
	return partRateLimiters
}

func (m *SimpleLimiter) updateRateLimiter(reqRootLimiterNode *proxypb.LimiterNode) error {
	// skip to update cluster rate limiters

	for dbID, reqDBRateLimiters := range reqRootLimiterNode.GetChildren() {
		// update database rate limiters

		// update collection rate limiters
		for collectionID, reqCollectionRateLimiter := range reqDBRateLimiters.GetChildren() {
			collectionRateLimiter := m.rateLimiter.GetOrCreateCollectionLimiters(dbID, collectionID,
				newDatabaseLimiter, newCollectionLimiters)
			collectionQuotaStates := typeutil.NewConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode]()

			reqLimiter := reqCollectionRateLimiter.GetLimiter()
			states := reqLimiter.GetStates()
			codes := reqLimiter.GetCodes()

			for _, r := range reqLimiter.GetRates() {
				limit, ok := collectionRateLimiter.GetLimiters().Get(r.GetRt())
				if !ok {
					return fmt.Errorf("unregister rateLimiter for rateType %s", r.GetRt().String())
				}
				limit.SetLimit(ratelimitutil.Limit(r.GetR()))
				setRateGaugeByRateType(r.GetRt(), paramtable.GetNodeID(), collectionID, r.GetR())
			}

			// reset collection quota states
			for i, state := range states {
				collectionQuotaStates.Insert(state, codes[i])
			}
			collectionRateLimiter.SetQuotaStates(collectionQuotaStates)

			// update partition rate limiters
			for partitionID, reqPartitionRateLimiters := range reqCollectionRateLimiter.GetChildren() {
				partitionQuotaStates := typeutil.NewConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode]()
				partitionRateLimiter := m.rateLimiter.GetOrCreatePartitionLimiters(dbID, collectionID, partitionID,
					newDatabaseLimiter, newCollectionLimiters, newPartitionLimiters)

				reqLimiter := reqPartitionRateLimiters.GetLimiter()
				states := reqLimiter.GetStates()
				codes := reqLimiter.GetCodes()

				for _, r := range reqLimiter.GetRates() {
					limit, ok := partitionRateLimiter.GetLimiters().Get(r.GetRt())
					if !ok {
						return fmt.Errorf("rateType %s not found within partition:%d rate limiter",
							r.GetRt().String(), partitionRateLimiter.GetID())
					}

					limit.SetLimit(ratelimitutil.Limit(r.GetR()))
					setRateGaugeByRateType(r.GetRt(), paramtable.GetNodeID(), collectionID, r.GetR())
				}

				for i, state := range states {
					partitionQuotaStates.Insert(state, codes[i])
				}
				partitionRateLimiter.SetQuotaStates(partitionQuotaStates)
			}
		}
	}

	return nil
}

// setRateGaugeByRateType sets ProxyLimiterRate metrics.
func setRateGaugeByRateType(rateType internalpb.RateType, nodeID int64, collectionID int64, rate float64) {
	if ratelimitutil.Limit(rate) == ratelimitutil.Inf {
		return
	}
	nodeIDStr := strconv.FormatInt(nodeID, 10)
	collectionIDStr := strconv.FormatInt(collectionID, 10)
	switch rateType {
	case internalpb.RateType_DMLInsert:
		metrics.ProxyLimiterRate.WithLabelValues(nodeIDStr, collectionIDStr, metrics.InsertLabel).Set(rate)
	case internalpb.RateType_DMLUpsert:
		metrics.ProxyLimiterRate.WithLabelValues(nodeIDStr, collectionIDStr, metrics.UpsertLabel).Set(rate)
	case internalpb.RateType_DMLDelete:
		metrics.ProxyLimiterRate.WithLabelValues(nodeIDStr, collectionIDStr, metrics.DeleteLabel).Set(rate)
	case internalpb.RateType_DQLSearch:
		metrics.ProxyLimiterRate.WithLabelValues(nodeIDStr, collectionIDStr, metrics.SearchLabel).Set(rate)
	case internalpb.RateType_DQLQuery:
		metrics.ProxyLimiterRate.WithLabelValues(nodeIDStr, collectionIDStr, metrics.QueryLabel).Set(rate)
	}
}

func getDefaultLimiterConfig(scope internalpb.RateScope) map[internalpb.RateType]*paramtable.ParamItem {
	quotaConfig := &Params.QuotaConfig
	switch scope {
	case internalpb.RateScope_Cluster:
		return map[internalpb.RateType]*paramtable.ParamItem{
			internalpb.RateType_DDLCollection: &quotaConfig.DDLCollectionRate,
			internalpb.RateType_DDLPartition:  &quotaConfig.DDLPartitionRate,
			internalpb.RateType_DDLIndex:      &quotaConfig.MaxIndexRate,
			internalpb.RateType_DDLFlush:      &quotaConfig.MaxFlushRate,
			internalpb.RateType_DDLCompaction: &quotaConfig.MaxCompactionRate,
			internalpb.RateType_DMLInsert:     &quotaConfig.DMLMaxInsertRate,
			internalpb.RateType_DMLUpsert:     &quotaConfig.DMLMaxUpsertRate,
			internalpb.RateType_DMLDelete:     &quotaConfig.DMLMaxDeleteRate,
			internalpb.RateType_DMLBulkLoad:   &quotaConfig.DMLMaxBulkLoadRate,
			internalpb.RateType_DQLSearch:     &quotaConfig.DQLMaxSearchRate,
			internalpb.RateType_DQLQuery:      &quotaConfig.DQLMaxQueryRate,
		}
	case internalpb.RateScope_Database:
		return map[internalpb.RateType]*paramtable.ParamItem{
			internalpb.RateType_DDLCollection: &quotaConfig.DDLCollectionRatePerDB,
			internalpb.RateType_DDLPartition:  &quotaConfig.DDLPartitionRatePerDB,
			internalpb.RateType_DDLIndex:      &quotaConfig.MaxIndexRatePerDB,
			internalpb.RateType_DDLFlush:      &quotaConfig.MaxFlushRatePerDB,
			internalpb.RateType_DDLCompaction: &quotaConfig.MaxCompactionRatePerDB,
			internalpb.RateType_DMLInsert:     &quotaConfig.DMLMaxInsertRatePerDB,
			internalpb.RateType_DMLUpsert:     &quotaConfig.DMLMaxUpsertRatePerDB,
			internalpb.RateType_DMLDelete:     &quotaConfig.DMLMaxDeleteRatePerDB,
			internalpb.RateType_DMLBulkLoad:   &quotaConfig.DMLMaxBulkLoadRatePerDB,
			internalpb.RateType_DQLSearch:     &quotaConfig.DQLMaxSearchRatePerDB,
			internalpb.RateType_DQLQuery:      &quotaConfig.DQLMaxQueryRatePerDB,
		}
	case internalpb.RateScope_Collection:
		return map[internalpb.RateType]*paramtable.ParamItem{
			internalpb.RateType_DMLInsert:   &quotaConfig.DMLMaxInsertRatePerCollection,
			internalpb.RateType_DMLUpsert:   &quotaConfig.DMLMaxUpsertRatePerCollection,
			internalpb.RateType_DMLDelete:   &quotaConfig.DMLMaxDeleteRatePerCollection,
			internalpb.RateType_DMLBulkLoad: &quotaConfig.DMLMaxBulkLoadRatePerCollection,
			internalpb.RateType_DQLSearch:   &quotaConfig.DQLMaxSearchRatePerCollection,
			internalpb.RateType_DQLQuery:    &quotaConfig.DQLMaxQueryRatePerCollection,
			internalpb.RateType_DDLFlush:    &quotaConfig.MaxFlushRatePerCollection,
		}
	case internalpb.RateScope_Partition:
		return map[internalpb.RateType]*paramtable.ParamItem{
			internalpb.RateType_DMLInsert:   &quotaConfig.DMLMaxInsertRatePerPartition,
			internalpb.RateType_DMLUpsert:   &quotaConfig.DMLMaxUpsertRatePerPartition,
			internalpb.RateType_DMLDelete:   &quotaConfig.DMLMaxDeleteRatePerPartition,
			internalpb.RateType_DMLBulkLoad: &quotaConfig.DMLMaxBulkLoadRatePerPartition,
			internalpb.RateType_DQLSearch:   &quotaConfig.DQLMaxSearchRatePerPartition,
			internalpb.RateType_DQLQuery:    &quotaConfig.DQLMaxQueryRatePerPartition,
		}
	default:
		panic("Unknown rate scope:" + scope.String())
	}
}

func IsDDLRequest(rt internalpb.RateType) bool {
	switch rt {
	case internalpb.RateType_DDLCollection, internalpb.RateType_DDLPartition, internalpb.RateType_DDLIndex,
		internalpb.RateType_DDLFlush, internalpb.RateType_DDLCompaction:
		return true
	default:
		return false
	}
}
