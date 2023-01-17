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
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/config"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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
	globalRateLimiter *rateLimiter
	// TODO: add collection level rateLimiter
	quotaStatesMu sync.RWMutex
	quotaStates   map[milvuspb.QuotaState]commonpb.ErrorCode
}

// NewMultiRateLimiter returns a new MultiRateLimiter.
func NewMultiRateLimiter() *MultiRateLimiter {
	m := &MultiRateLimiter{}
	m.globalRateLimiter = newRateLimiter()
	return m
}

// Check checks if request would be limited or denied.
func (m *MultiRateLimiter) Check(rt internalpb.RateType, n int) commonpb.ErrorCode {
	if !Params.QuotaConfig.QuotaAndLimitsEnabled.GetAsBool() {
		return commonpb.ErrorCode_Success
	}
	limit, rate := m.globalRateLimiter.limit(rt, n)
	if rate == 0 {
		return m.GetErrorCode(rt)
	}
	if limit {
		return commonpb.ErrorCode_RateLimit
	}
	return commonpb.ErrorCode_Success
}

func (m *MultiRateLimiter) GetErrorCode(rt internalpb.RateType) commonpb.ErrorCode {
	switch rt {
	case internalpb.RateType_DMLInsert, internalpb.RateType_DMLDelete, internalpb.RateType_DMLBulkLoad:
		m.quotaStatesMu.RLock()
		defer m.quotaStatesMu.RUnlock()
		return m.quotaStates[milvuspb.QuotaState_DenyToWrite]
	case internalpb.RateType_DQLSearch, internalpb.RateType_DQLQuery:
		m.quotaStatesMu.RLock()
		defer m.quotaStatesMu.RUnlock()
		return m.quotaStates[milvuspb.QuotaState_DenyToRead]
	}
	return commonpb.ErrorCode_Success
}

// GetQuotaStates returns quota states.
func (m *MultiRateLimiter) GetQuotaStates() ([]milvuspb.QuotaState, []string) {
	m.quotaStatesMu.RLock()
	defer m.quotaStatesMu.RUnlock()
	states := make([]milvuspb.QuotaState, 0, len(m.quotaStates))
	reasons := make([]string, 0, len(m.quotaStates))
	for k, v := range m.quotaStates {
		states = append(states, k)
		reasons = append(reasons, GetQuotaErrorString(v))
	}
	return states, reasons
}

// SetQuotaStates sets quota states for MultiRateLimiter.
func (m *MultiRateLimiter) SetQuotaStates(states []milvuspb.QuotaState, codes []commonpb.ErrorCode) {
	m.quotaStatesMu.Lock()
	defer m.quotaStatesMu.Unlock()
	m.quotaStates = make(map[milvuspb.QuotaState]commonpb.ErrorCode, len(states))
	for i := 0; i < len(states); i++ {
		m.quotaStates[states[i]] = codes[i]
	}
}

// rateLimiter implements Limiter.
type rateLimiter struct {
	limiters *typeutil.ConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter]
}

// newRateLimiter returns a new RateLimiter.
func newRateLimiter() *rateLimiter {
	rl := &rateLimiter{
		limiters: typeutil.NewConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter](),
	}
	rl.registerLimiters()
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

// setRates sets new rates for the limiters.
func (rl *rateLimiter) setRates(rates []*internalpb.Rate) error {
	for _, r := range rates {
		if limit, ok := rl.limiters.Get(r.GetRt()); ok {
			limit.SetLimit(ratelimitutil.Limit(r.GetR()))
			metrics.SetRateGaugeByRateType(r.GetRt(), paramtable.GetNodeID(), r.GetR())
		} else {
			return fmt.Errorf("unregister rateLimiter for rateType %s", r.GetRt().String())
		}
	}
	// rl.printRates(rates)
	return nil
}

// printRates logs the rate info.
func (rl *rateLimiter) printRates(rates []*internalpb.Rate) {
	//fmt.Printf("RateLimiter set rates:\n---------------------------------\n")
	//for _, r := range rates {
	//	fmt.Printf("%s -> %v\n", r.GetRt().String(), r.GetR())
	//}
	//fmt.Printf("---------------------------------\n")
	log.Debug("RateLimiter setRates", zap.Any("rates", rates))
}

// registerLimiters register limiter for all rate types.
func (rl *rateLimiter) registerLimiters() {
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
			r = &quotaConfig.DMLMaxInsertRate
		case internalpb.RateType_DMLDelete:
			r = &quotaConfig.DMLMaxDeleteRate
		case internalpb.RateType_DMLBulkLoad:
			r = &quotaConfig.DMLMaxBulkLoadRate
		case internalpb.RateType_DQLSearch:
			r = &quotaConfig.DQLMaxSearchRate
		case internalpb.RateType_DQLQuery:
			r = &quotaConfig.DQLMaxQueryRate
		}
		limit := ratelimitutil.Limit(r.GetAsFloat())
		burst := r.GetAsFloat() // use rate as burst, because Limiter is with punishment mechanism, burst is insignificant.
		rl.limiters.InsertIfNotPresent(internalpb.RateType(rt), ratelimitutil.NewLimiter(limit, burst))
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
		log.Info("RateLimiter register for rateType",
			zap.String("rateType", internalpb.RateType_name[rt]),
			zap.String("rate", ratelimitutil.Limit(r.GetAsFloat()).String()),
			zap.String("burst", fmt.Sprintf("%v", burst)))
	}
}
