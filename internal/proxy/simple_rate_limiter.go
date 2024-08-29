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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/util/quota"
	rlinternal "github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// SimpleLimiter is implemented based on Limiter interface
type SimpleLimiter struct {
	quotaStatesMu sync.RWMutex
	rateLimiter   *rlinternal.RateLimiterTree

	// for alloc
	allocWaitInterval time.Duration
	allocRetryTimes   uint
}

// NewSimpleLimiter returns a new SimpleLimiter.
func NewSimpleLimiter(allocWaitInterval time.Duration, allocRetryTimes uint) *SimpleLimiter {
	rootRateLimiter := newClusterLimiter()
	m := &SimpleLimiter{rateLimiter: rlinternal.NewRateLimiterTree(rootRateLimiter), allocWaitInterval: allocWaitInterval, allocRetryTimes: allocRetryTimes}
	return m
}

// Alloc will retry till check pass or out of times.
func (m *SimpleLimiter) Alloc(ctx context.Context, dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error {
	return retry.Do(ctx, func() error {
		return m.Check(dbID, collectionIDToPartIDs, rt, n)
	}, retry.Sleep(m.allocWaitInterval), retry.Attempts(m.allocRetryTimes))
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
	if dbID != util.InvalidDBID {
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
			if collectionID == 0 || dbID == util.InvalidDBID {
				continue
			}
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
				if collectionID == 0 || partID == 0 || dbID == util.InvalidDBID {
					continue
				}
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
	case internalpb.RateType_DDLCollection,
		internalpb.RateType_DDLPartition,
		internalpb.RateType_DDLIndex,
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
		func(node *rlinternal.RateLimiterNode, state milvuspb.QuotaState, errCode commonpb.ErrorCode) bool {
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

	// Reset the limiter rates due to potential changes in configurations.
	var (
		clusterConfigs    = getDefaultLimiterConfig(internalpb.RateScope_Cluster)
		databaseConfigs   = getDefaultLimiterConfig(internalpb.RateScope_Database)
		collectionConfigs = getDefaultLimiterConfig(internalpb.RateScope_Collection)
		partitionConfigs  = getDefaultLimiterConfig(internalpb.RateScope_Partition)
	)
	initLimiter(m.rateLimiter.GetRootLimiters(), clusterConfigs)
	m.rateLimiter.GetRootLimiters().GetChildren().Range(func(_ int64, dbLimiter *rlinternal.RateLimiterNode) bool {
		initLimiter(dbLimiter, databaseConfigs)
		dbLimiter.GetChildren().Range(func(_ int64, collLimiter *rlinternal.RateLimiterNode) bool {
			initLimiter(collLimiter, collectionConfigs)
			collLimiter.GetChildren().Range(func(_ int64, partitionLimiter *rlinternal.RateLimiterNode) bool {
				initLimiter(partitionLimiter, partitionConfigs)
				return true
			})
			return true
		})
		return true
	})

	if err := m.updateRateLimiter(rootLimiter); err != nil {
		return err
	}

	m.rateLimiter.ClearInvalidLimiterNode(rootLimiter)
	return nil
}

func initLimiter(rln *rlinternal.RateLimiterNode, rateLimiterConfigs map[internalpb.RateType]*paramtable.ParamItem) {
	for rt, p := range rateLimiterConfigs {
		newLimit := ratelimitutil.Limit(p.GetAsFloat())
		burst := p.GetAsFloat() // use rate as burst, because SimpleLimiter is with punishment mechanism, burst is insignificant.
		old, ok := rln.GetLimiters().Get(rt)
		updated := false
		if ok {
			if old.Limit() != newLimit {
				old.SetLimit(newLimit)
				updated = true
			}
		} else {
			rln.GetLimiters().Insert(rt, ratelimitutil.NewLimiter(newLimit, burst))
			updated = true
		}
		if updated {
			log.Debug("RateLimiter register for rateType",
				zap.String("rateType", internalpb.RateType_name[(int32(rt))]),
				zap.String("rateLimit", newLimit.String()),
				zap.String("burst", fmt.Sprintf("%v", burst)))
		}
	}
}

// newClusterLimiter init limiter of cluster level for all rate types and rate scopes.
// Cluster rate limiter doesn't support to accumulate metrics dynamically, it only uses
// configurations as limit values.
func newClusterLimiter() *rlinternal.RateLimiterNode {
	clusterRateLimiters := rlinternal.NewRateLimiterNode(internalpb.RateScope_Cluster)
	clusterLimiterConfigs := getDefaultLimiterConfig(internalpb.RateScope_Cluster)
	initLimiter(clusterRateLimiters, clusterLimiterConfigs)
	return clusterRateLimiters
}

func newDatabaseLimiter() *rlinternal.RateLimiterNode {
	dbRateLimiters := rlinternal.NewRateLimiterNode(internalpb.RateScope_Database)
	databaseLimiterConfigs := getDefaultLimiterConfig(internalpb.RateScope_Database)
	initLimiter(dbRateLimiters, databaseLimiterConfigs)
	return dbRateLimiters
}

func newCollectionLimiters() *rlinternal.RateLimiterNode {
	collectionRateLimiters := rlinternal.NewRateLimiterNode(internalpb.RateScope_Collection)
	collectionLimiterConfigs := getDefaultLimiterConfig(internalpb.RateScope_Collection)
	initLimiter(collectionRateLimiters, collectionLimiterConfigs)
	return collectionRateLimiters
}

func newPartitionLimiters() *rlinternal.RateLimiterNode {
	partRateLimiters := rlinternal.NewRateLimiterNode(internalpb.RateScope_Partition)
	partitionLimiterConfigs := getDefaultLimiterConfig(internalpb.RateScope_Partition)
	initLimiter(partRateLimiters, partitionLimiterConfigs)
	return partRateLimiters
}

func (m *SimpleLimiter) updateLimiterNode(req *proxypb.Limiter, node *rlinternal.RateLimiterNode, sourceID string) error {
	curLimiters := node.GetLimiters()
	for _, rate := range req.GetRates() {
		limit, ok := curLimiters.Get(rate.GetRt())
		if !ok {
			return fmt.Errorf("unregister rateLimiter for rateType %s", rate.GetRt().String())
		}
		limit.SetLimit(ratelimitutil.Limit(rate.GetR()))
		setRateGaugeByRateType(rate.GetRt(), paramtable.GetNodeID(), sourceID, rate.GetR())
	}
	quotaStates := typeutil.NewConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode]()
	states := req.GetStates()
	codes := req.GetCodes()
	for i, state := range states {
		quotaStates.Insert(state, codes[i])
	}
	node.SetQuotaStates(quotaStates)
	return nil
}

func (m *SimpleLimiter) updateRateLimiter(reqRootLimiterNode *proxypb.LimiterNode) error {
	reqClusterLimiter := reqRootLimiterNode.GetLimiter()
	clusterLimiter := m.rateLimiter.GetRootLimiters()
	err := m.updateLimiterNode(reqClusterLimiter, clusterLimiter, "cluster")
	if err != nil {
		log.Warn("update cluster rate limiters failed", zap.Error(err))
		return err
	}

	getDBSourceID := func(dbID int64) string {
		return fmt.Sprintf("db.%d", dbID)
	}
	getCollectionSourceID := func(collectionID int64) string {
		return fmt.Sprintf("collection.%d", collectionID)
	}
	getPartitionSourceID := func(partitionID int64) string {
		return fmt.Sprintf("partition.%d", partitionID)
	}

	for dbID, reqDBRateLimiters := range reqRootLimiterNode.GetChildren() {
		// update database rate limiters
		dbRateLimiters := m.rateLimiter.GetOrCreateDatabaseLimiters(dbID, newDatabaseLimiter)
		err := m.updateLimiterNode(reqDBRateLimiters.GetLimiter(), dbRateLimiters, getDBSourceID(dbID))
		if err != nil {
			log.Warn("update database rate limiters failed", zap.Error(err))
			return err
		}

		// update collection rate limiters
		for collectionID, reqCollectionRateLimiter := range reqDBRateLimiters.GetChildren() {
			collectionRateLimiter := m.rateLimiter.GetOrCreateCollectionLimiters(dbID, collectionID,
				newDatabaseLimiter, newCollectionLimiters)
			err := m.updateLimiterNode(reqCollectionRateLimiter.GetLimiter(), collectionRateLimiter,
				getCollectionSourceID(collectionID))
			if err != nil {
				log.Warn("update collection rate limiters failed", zap.Error(err))
				return err
			}

			// update partition rate limiters
			for partitionID, reqPartitionRateLimiters := range reqCollectionRateLimiter.GetChildren() {
				partitionRateLimiter := m.rateLimiter.GetOrCreatePartitionLimiters(dbID, collectionID, partitionID,
					newDatabaseLimiter, newCollectionLimiters, newPartitionLimiters)

				err := m.updateLimiterNode(reqPartitionRateLimiters.GetLimiter(), partitionRateLimiter,
					getPartitionSourceID(partitionID))
				if err != nil {
					log.Warn("update partition rate limiters failed", zap.Error(err))
					return err
				}
			}
		}
	}

	return nil
}

// setRateGaugeByRateType sets ProxyLimiterRate metrics.
func setRateGaugeByRateType(rateType internalpb.RateType, nodeID int64, sourceID string, rate float64) {
	if ratelimitutil.Limit(rate) == ratelimitutil.Inf {
		return
	}
	nodeIDStr := strconv.FormatInt(nodeID, 10)
	metrics.ProxyLimiterRate.WithLabelValues(nodeIDStr, sourceID, rateType.String()).Set(rate)
}

func getDefaultLimiterConfig(scope internalpb.RateScope) map[internalpb.RateType]*paramtable.ParamItem {
	return quota.GetQuotaConfigMap(scope)
}

func IsDDLRequest(rt internalpb.RateType) bool {
	switch rt {
	case internalpb.RateType_DDLCollection,
		internalpb.RateType_DDLPartition,
		internalpb.RateType_DDLIndex,
		internalpb.RateType_DDLFlush,
		internalpb.RateType_DDLCompaction:
		return true
	default:
		return false
	}
}
