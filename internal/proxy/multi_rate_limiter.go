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
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var QuotaErrorString = map[commonpb.ErrorCode]string{
	commonpb.ErrorCode_ForceDeny:            "manually force deny",
	commonpb.ErrorCode_MemoryQuotaExhausted: "memory quota exhausted, please allocate more resources",
	commonpb.ErrorCode_DiskQuotaExhausted:   "disk quota exhausted, please allocate more resources",
	commonpb.ErrorCode_TimeTickLongDelay:    "time tick long delay",
}

func GetQuotaErrorString(errCode commonpb.ErrorCode) string {
	return QuotaErrorString[errCode]
}

// MultiRateLimiter includes multilevel rate limiters, such as global rateLimiter,
// collection level rateLimiter and so on. It also implements Limiter interface.
type MultiRateLimiter struct {
	quotaStatesMu sync.RWMutex
	// for DML and DQL
	collectionLimiters map[int64]*rateLimiter
	// for DDL
	globalDDLLimiter *rateLimiter
}

// NewMultiRateLimiter returns a new MultiRateLimiter.
func NewMultiRateLimiter() *MultiRateLimiter {
	m := &MultiRateLimiter{
		collectionLimiters: make(map[int64]*rateLimiter, 0),
		globalDDLLimiter:   newRateLimiter(true),
	}
	return m
}

// Check checks if request would be limited or denied.
func (m *MultiRateLimiter) Check(collectionID int64, rt internalpb.RateType, n int) commonpb.ErrorCode {
	if !Params.QuotaConfig.QuotaAndLimitsEnabled.GetAsBool() {
		return commonpb.ErrorCode_Success
	}

	m.quotaStatesMu.RLock()
	defer m.quotaStatesMu.RUnlock()

	checkFunc := func(limiter *rateLimiter) commonpb.ErrorCode {
		if limiter == nil {
			return commonpb.ErrorCode_Success
		}

		limit, rate := limiter.limit(rt, n)
		if rate == 0 {
			return limiter.getErrorCode(rt)
		}
		if limit {
			return commonpb.ErrorCode_RateLimit
		}
		return commonpb.ErrorCode_Success
	}

	// first, check global level rate limits
	ret := checkFunc(m.globalDDLLimiter)

	// second check collection level rate limits
	if ret == commonpb.ErrorCode_Success && !IsDDLRequest(rt) {
		// only dml and dql have collection level rate limits
		ret = checkFunc(m.collectionLimiters[collectionID])
		if ret != commonpb.ErrorCode_Success {
			m.globalDDLLimiter.cancel(rt, n)
		}
	}

	return ret
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

// GetQuotaStates returns quota states.
func (m *MultiRateLimiter) GetQuotaStates() ([]milvuspb.QuotaState, []string) {
	m.quotaStatesMu.RLock()
	defer m.quotaStatesMu.RUnlock()
	serviceStates := make(map[milvuspb.QuotaState]typeutil.Set[commonpb.ErrorCode])

	// deduplicate same (state, code) pair from different collection
	for _, limiter := range m.collectionLimiters {
		limiter.quotaStates.Range(func(state milvuspb.QuotaState, errCode commonpb.ErrorCode) bool {
			if serviceStates[state] == nil {
				serviceStates[state] = typeutil.NewSet[commonpb.ErrorCode]()
			}
			serviceStates[state].Insert(errCode)
			return true
		})
	}

	states := make([]milvuspb.QuotaState, 0)
	reasons := make([]string, 0)
	for state, errCodes := range serviceStates {
		for errCode := range errCodes {
			states = append(states, state)
			reasons = append(reasons, GetQuotaErrorString(errCode))
		}
	}

	return states, reasons
}

// SetQuotaStates sets quota states for MultiRateLimiter.
func (m *MultiRateLimiter) SetRates(rates []*proxypb.CollectionRate) error {
	m.quotaStatesMu.Lock()
	defer m.quotaStatesMu.Unlock()
	collectionSet := typeutil.NewUniqueSet()
	for _, collectionRates := range rates {
		collectionSet.Insert(collectionRates.Collection)
		rateLimiter, ok := m.collectionLimiters[collectionRates.GetCollection()]
		if !ok {
			rateLimiter = newRateLimiter(false)
		}
		err := rateLimiter.setRates(collectionRates)
		if err != nil {
			return err
		}
		m.collectionLimiters[collectionRates.GetCollection()] = rateLimiter
	}

	// remove dropped collection's rate limiter
	for collectionID := range m.collectionLimiters {
		if !collectionSet.Contain(collectionID) {
			delete(m.collectionLimiters, collectionID)
		}
	}
	return nil
}

// rateLimiter implements Limiter.
type rateLimiter struct {
	limiters    *typeutil.ConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter]
	quotaStates *typeutil.ConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode]
}

// newRateLimiter returns a new RateLimiter.
func newRateLimiter(globalLevel bool) *rateLimiter {
	rl := &rateLimiter{
		limiters:    typeutil.NewConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter](),
		quotaStates: typeutil.NewConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode](),
	}
	rl.registerLimiters(globalLevel)
	return rl
}

// limit returns true, the request will be rejected.
// Otherwise, the request will pass.
func (rl *rateLimiter) limit(rt internalpb.RateType, n int) (bool, float64) {
	limit, ok := rl.limiters.Get(rt)
	if !ok {
		return false, -1
	}
	return !limit.AllowN(time.Now(), n), float64(limit.Limit())
}

func (rl *rateLimiter) cancel(rt internalpb.RateType, n int) {
	limit, ok := rl.limiters.Get(rt)
	if !ok {
		return
	}
	limit.Cancel(n)
}

func (rl *rateLimiter) setRates(collectionRate *proxypb.CollectionRate) error {
	log := log.Ctx(context.TODO()).WithRateGroup("proxy.rateLimiter", 1.0, 60.0).With(
		zap.Int64("proxyNodeID", paramtable.GetNodeID()),
		zap.Int64("CollectionID", collectionRate.Collection),
	)
	for _, r := range collectionRate.GetRates() {
		if limit, ok := rl.limiters.Get(r.GetRt()); ok {
			limit.SetLimit(ratelimitutil.Limit(r.GetR()))
			setRateGaugeByRateType(r.GetRt(), paramtable.GetNodeID(), collectionRate.Collection, r.GetR())
		} else {
			return fmt.Errorf("unregister rateLimiter for rateType %s", r.GetRt().String())
		}
		log.RatedDebug(30, "current collection rates in proxy",
			zap.String("rateType", r.Rt.String()),
			zap.String("rateLimit", ratelimitutil.Limit(r.GetR()).String()),
		)
	}

	// clear old quota states
	rl.quotaStates = typeutil.NewConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode]()
	for i := 0; i < len(collectionRate.GetStates()); i++ {
		rl.quotaStates.Insert(collectionRate.States[i], collectionRate.Codes[i])
		log.RatedWarn(30, "Proxy set collection quota states",
			zap.String("state", collectionRate.GetStates()[i].String()),
			zap.String("reason", collectionRate.GetCodes()[i].String()),
		)
	}

	return nil
}

func (rl *rateLimiter) getErrorCode(rt internalpb.RateType) commonpb.ErrorCode {
	switch rt {
	case internalpb.RateType_DMLInsert, internalpb.RateType_DMLUpsert, internalpb.RateType_DMLDelete, internalpb.RateType_DMLBulkLoad:
		if errCode, ok := rl.quotaStates.Get(milvuspb.QuotaState_DenyToWrite); ok {
			return errCode
		}
	case internalpb.RateType_DQLSearch, internalpb.RateType_DQLQuery:
		if errCode, ok := rl.quotaStates.Get(milvuspb.QuotaState_DenyToRead); ok {
			return errCode
		}
	}
	return commonpb.ErrorCode_Success
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

// registerLimiters register limiter for all rate types.
func (rl *rateLimiter) registerLimiters(globalLevel bool) {
	log := log.Ctx(context.TODO()).WithRateGroup("proxy.rateLimiter", 1.0, 60.0)
	quotaConfig := &Params.QuotaConfig
	for rt := range internalpb.RateType_name {
		var r *paramtable.ParamItem
		switch internalpb.RateType(rt) {
		case internalpb.RateType_DDLCollection:
			r = &quotaConfig.DDLCollectionRate
		case internalpb.RateType_DDLPartition:
			r = &quotaConfig.DDLPartitionRate
		case internalpb.RateType_DDLIndex:
			r = &quotaConfig.MaxIndexRate
		case internalpb.RateType_DDLFlush:
			r = &quotaConfig.MaxFlushRate
		case internalpb.RateType_DDLCompaction:
			r = &quotaConfig.MaxCompactionRate
		case internalpb.RateType_DMLInsert:
			if globalLevel {
				r = &quotaConfig.DMLMaxInsertRate
			} else {
				r = &quotaConfig.DMLMaxInsertRatePerCollection
			}
		case internalpb.RateType_DMLUpsert:
			if globalLevel {
				r = &quotaConfig.DMLMaxUpsertRate
			} else {
				r = &quotaConfig.DMLMaxUpsertRatePerCollection
			}
		case internalpb.RateType_DMLDelete:
			if globalLevel {
				r = &quotaConfig.DMLMaxDeleteRate
			} else {
				r = &quotaConfig.DMLMaxDeleteRatePerCollection
			}
		case internalpb.RateType_DMLBulkLoad:
			if globalLevel {
				r = &quotaConfig.DMLMaxBulkLoadRate
			} else {
				r = &quotaConfig.DMLMaxBulkLoadRatePerCollection
			}
		case internalpb.RateType_DQLSearch:
			if globalLevel {
				r = &quotaConfig.DQLMaxSearchRate
			} else {
				r = &quotaConfig.DQLMaxSearchRatePerCollection
			}
		case internalpb.RateType_DQLQuery:
			if globalLevel {
				r = &quotaConfig.DQLMaxQueryRate
			} else {
				r = &quotaConfig.DQLMaxQueryRatePerCollection
			}
		}
		limit := ratelimitutil.Limit(r.GetAsFloat())
		burst := r.GetAsFloat() // use rate as burst, because Limiter is with punishment mechanism, burst is insignificant.
		rl.limiters.GetOrInsert(internalpb.RateType(rt), ratelimitutil.NewLimiter(limit, burst))
		onEvent := func(rateType internalpb.RateType) func(*config.Event) {
			return func(event *config.Event) {
				f, err := strconv.ParseFloat(event.Value, 64)
				if err != nil {
					log.Info("Error format for rateLimit",
						zap.String("rateType", rateType.String()),
						zap.String("key", event.Key),
						zap.String("value", event.Value),
						zap.Error(err))
					return
				}
				limit, ok := rl.limiters.Get(rateType)
				if !ok {
					return
				}
				limit.SetLimit(ratelimitutil.Limit(f))
			}
		}(internalpb.RateType(rt))
		paramtable.Get().Watch(r.Key, config.NewHandler(fmt.Sprintf("rateLimiter-%d", rt), onEvent))
		log.RatedDebug(30, "RateLimiter register for rateType",
			zap.String("rateType", internalpb.RateType_name[rt]),
			zap.String("rateLimit", ratelimitutil.Limit(r.GetAsFloat()).String()),
			zap.String("burst", fmt.Sprintf("%v", burst)))
	}
}
