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
	"math"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/stretchr/testify/assert"
)

func TestMultiRateLimiter(t *testing.T) {
	Params.Init()
	t.Run("test multiRateLimiter", func(t *testing.T) {
		bak := Params.QuotaConfig.EnableQuotaAndLimits
		Params.QuotaConfig.EnableQuotaAndLimits = true
		multiLimiter := NewMultiRateLimiter()
		for _, rt := range internalpb.RateType_value {
			multiLimiter.globalRateLimiter.limiters[internalpb.RateType(rt)] = ratelimitutil.NewLimiter(ratelimitutil.Limit(1000), 1)
		}
		for _, rt := range internalpb.RateType_value {
			ok, _ := multiLimiter.Limit(internalpb.RateType(rt), 1)
			assert.False(t, ok)
			ok, _ = multiLimiter.Limit(internalpb.RateType(rt), math.MaxInt)
			assert.False(t, ok)
			ok, _ = multiLimiter.Limit(internalpb.RateType(rt), math.MaxInt)
			assert.True(t, ok)
		}
		Params.QuotaConfig.EnableQuotaAndLimits = bak
	})

	t.Run("not enable quotaAndLimit", func(t *testing.T) {
		multiLimiter := NewMultiRateLimiter()
		bak := Params.QuotaConfig.EnableQuotaAndLimits
		Params.QuotaConfig.EnableQuotaAndLimits = false
		ok, r := multiLimiter.Limit(internalpb.RateType(0), 1)
		assert.False(t, ok)
		assert.NotEqual(t, float64(0), r)
		Params.QuotaConfig.EnableQuotaAndLimits = bak
	})
}

func TestRateLimiter(t *testing.T) {
	Params.Init()
	t.Run("test limit", func(t *testing.T) {
		limiter := newRateLimiter()
		limiter.registerLimiters()
		for _, rt := range internalpb.RateType_value {
			limiter.limiters[internalpb.RateType(rt)] = ratelimitutil.NewLimiter(ratelimitutil.Limit(1000), 1)
		}
		for _, rt := range internalpb.RateType_value {
			ok, _ := limiter.limit(internalpb.RateType(rt), 1)
			assert.False(t, ok)
			ok, _ = limiter.limit(internalpb.RateType(rt), math.MaxInt)
			assert.False(t, ok)
			ok, _ = limiter.limit(internalpb.RateType(rt), math.MaxInt)
			assert.True(t, ok)
		}
	})

	t.Run("test setRates", func(t *testing.T) {
		limiter := newRateLimiter()
		for _, rt := range internalpb.RateType_value {
			limiter.limiters[internalpb.RateType(rt)] = ratelimitutil.NewLimiter(ratelimitutil.Limit(1000), 1)
		}

		zeroRates := make([]*internalpb.Rate, 0, len(internalpb.RateType_value))
		for _, rt := range internalpb.RateType_value {
			zeroRates = append(zeroRates, &internalpb.Rate{
				Rt: internalpb.RateType(rt), R: 0,
			})
		}
		err := limiter.setRates(zeroRates)
		assert.NoError(t, err)
		for _, rt := range internalpb.RateType_value {
			for i := 0; i < 100; i++ {
				if i == 0 {
					// Consume token initialed in bucket
					ok, _ := limiter.limit(internalpb.RateType(rt), 1)
					assert.False(t, ok)
				} else {
					ok, _ := limiter.limit(internalpb.RateType(rt), 1)
					assert.True(t, ok)
				}
			}
		}
	})
}
