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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/ratelimitutil"
)

// MultiRateLimiter includes multilevel rate limiters, such as global rateLimiter,
// collection level rateLimiter and so on. It also implements Limiter interface.
type MultiRateLimiter struct {
	globalRateLimiter *rateLimiter
	// TODO: add collection level rateLimiter
}

// NewMultiRateLimiter returns a new MultiRateLimiter.
func NewMultiRateLimiter() *MultiRateLimiter {
	m := &MultiRateLimiter{}
	m.globalRateLimiter = newRateLimiter()
	return m
}

// Limit returns true, the request will be rejected.
// Otherwise, the request will pass. Limit also returns limit of limiter.
func (m *MultiRateLimiter) Limit(rt internalpb.RateType, n int) (bool, float64) {
	if !Params.QuotaConfig.EnableQuotaAndLimits {
		return false, 1 // no limit
	}
	// TODO: call other rate limiters
	return m.globalRateLimiter.limit(rt, n)
}

// rateLimiter implements Limiter.
type rateLimiter struct {
	limiters map[internalpb.RateType]*ratelimitutil.Limiter
}

// newRateLimiter returns a new RateLimiter.
func newRateLimiter() *rateLimiter {
	rl := &rateLimiter{
		limiters: make(map[internalpb.RateType]*ratelimitutil.Limiter),
	}
	rl.registerLimiters()
	return rl
}

// limit returns true, the request will be rejected.
// Otherwise, the request will pass.
func (rl *rateLimiter) limit(rt internalpb.RateType, n int) (bool, float64) {
	return !rl.limiters[rt].AllowN(time.Now(), n), float64(rl.limiters[rt].Limit())
}

// setRates sets new rates for the limiters.
func (rl *rateLimiter) setRates(rates []*internalpb.Rate) error {
	for _, r := range rates {
		if _, ok := rl.limiters[r.GetRt()]; ok {
			rl.limiters[r.GetRt()].SetLimit(ratelimitutil.Limit(r.GetR()))
			metrics.SetRateGaugeByRateType(r.GetRt(), Params.ProxyCfg.GetNodeID(), r.GetR())
		} else {
			return fmt.Errorf("unregister rateLimiter for rateType %s", r.GetRt().String())
		}
	}
	rl.printRates(rates)
	return nil
}

// printRates logs the rate info.
func (rl *rateLimiter) printRates(rates []*internalpb.Rate) {
	//fmt.Printf("RateLimiter set rates:\n---------------------------------\n")
	//for _, r := range rates {
	//	fmt.Printf("%s -> %f\n", r.GetRt().String(), r.GetR())
	//}
	//fmt.Printf("---------------------------------\n")
	log.Debug("RateLimiter setRates", zap.Any("rates", rates))
}

// registerLimiters register limiter for all rate types.
func (rl *rateLimiter) registerLimiters() {
	for rt := range internalpb.RateType_name {
		var r float64
		switch internalpb.RateType(rt) {
		case internalpb.RateType_DDLCollection:
			r = Params.QuotaConfig.DDLCollectionRate
		case internalpb.RateType_DDLPartition:
			r = Params.QuotaConfig.DDLPartitionRate
		case internalpb.RateType_DDLIndex:
			r = Params.QuotaConfig.DDLIndexRate
		case internalpb.RateType_DDLFlush:
			r = Params.QuotaConfig.DDLFlushRate
		case internalpb.RateType_DDLCompaction:
			r = Params.QuotaConfig.DDLCompactionRate
		case internalpb.RateType_DMLInsert:
			r = Params.QuotaConfig.DMLMaxInsertRate
		case internalpb.RateType_DMLDelete:
			r = Params.QuotaConfig.DMLMaxDeleteRate
		case internalpb.RateType_DMLBulkLoad:
			r = Params.QuotaConfig.DMLMaxBulkLoadRate
		case internalpb.RateType_DQLSearch:
			r = Params.QuotaConfig.DQLMaxSearchRate
		case internalpb.RateType_DQLQuery:
			r = Params.QuotaConfig.DQLMaxQueryRate
		}
		log.Info("RateLimiter register for rateType",
			zap.String("rateType", internalpb.RateType_name[rt]),
			zap.Float64("rate", r))
		limit := ratelimitutil.Limit(r)
		if limit < 0 {
			limit = ratelimitutil.Inf
		}
		burst := int(r) // use rate as burst, because Limiter is with punishment mechanism, burst is insignificant.
		rl.limiters[internalpb.RateType(rt)] = ratelimitutil.NewLimiter(limit, burst)
	}
}
