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

package rate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestAdaptiveRateLimitControllerConfigFetcher(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()

	channel := types.PChannelInfo{Name: "test-channel"}
	sourceName := SourceNodeMemory
	fetcher := newAdaptiveRateLimitControllerConfigFetcher(channel, sourceName)

	t.Run("test normal fetch", func(t *testing.T) {
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryHWM.Key, "100mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryLWM.Key, "10mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryIncremental.Key, "1mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryNormalDelayInterval.Key, "10s")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryIncreaseInterval.Key, "1s")

		recoveryConfig := fetcher.FetchRecoveryConfig()
		assert.Equal(t, int64(100*1024*1024), recoveryConfig.HWM)
		assert.Equal(t, int64(10*1024*1024), recoveryConfig.LWM)
		assert.Equal(t, int64(1*1024*1024), recoveryConfig.Incremental)
		assert.Equal(t, 10*time.Second, recoveryConfig.NormalDelayInterval)
		assert.Equal(t, 1*time.Second, recoveryConfig.IncreaseInterval)

		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownHWM.Key, "50mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownLWM.Key, "5mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownDecreaseInterval.Key, "5s")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownDecreaseRatio.Key, "0.8")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownRejectDelayInterval.Key, "1m")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownStartupDelayInterval.Key, "10s")

		slowdownConfig := fetcher.FetchSlowdownConfig()
		assert.Equal(t, int64(50*1024*1024), slowdownConfig.HWM)
		assert.Equal(t, int64(5*1024*1024), slowdownConfig.LWM)
		assert.Equal(t, 5*time.Second, slowdownConfig.DecreaseInterval)
		assert.Equal(t, 0.8, slowdownConfig.DecreaseRatio)
		assert.Equal(t, 1*time.Minute, slowdownConfig.RejectDelayInterval)
	})

	t.Run("test illegal recovery fetch fallback", func(t *testing.T) {
		// Save a valid config first to have something to fallback to.
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryHWM.Key, "100mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryLWM.Key, "10mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryIncremental.Key, "1mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryNormalDelayInterval.Key, "10s")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryIncreaseInterval.Key, "1s")
		lastValid := fetcher.FetchRecoveryConfig()

		// Set an illegal config: HWM < LWM
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryHWM.Key, "5mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryLWM.Key, "10mb")

		recoveryConfig := fetcher.FetchRecoveryConfig()
		assert.Equal(t, lastValid, recoveryConfig)

		// Set an illegal config: Incremental <= 0
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryHWM.Key, "100mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryIncremental.Key, "0")

		recoveryConfig = fetcher.FetchRecoveryConfig()
		assert.Equal(t, lastValid, recoveryConfig)
	})

	t.Run("test illegal slowdown fetch fallback", func(t *testing.T) {
		// Save a valid config first.
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownHWM.Key, "50mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownLWM.Key, "5mb")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownDecreaseInterval.Key, "5s")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownDecreaseRatio.Key, "0.8")
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownRejectDelayInterval.Key, "1m")
		lastValid := fetcher.FetchSlowdownConfig()

		// Set an illegal config: DecreaseRatio >= 1
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownDecreaseRatio.Key, "1.0")

		slowdownConfig := fetcher.FetchSlowdownConfig()
		assert.Equal(t, lastValid, slowdownConfig)

		// Set an illegal config: DecreaseRatio <= 0
		params.Save(params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownDecreaseRatio.Key, "0")

		slowdownConfig = fetcher.FetchSlowdownConfig()
		assert.Equal(t, lastValid, slowdownConfig)
	})
}
