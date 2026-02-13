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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var _ ratelimit.AdaptiveRateLimitControllerConfigFetcher = (*adaptiveRateLimitControllerConfigFetcher)(nil)

type adaptiveRateLimitControllerConfigFetcher struct {
	channel    types.PChannelInfo
	sourceName string
	config     *paramtable.AdaptiveRateLimitConfig

	mu           sync.Mutex
	lastRecovery ratelimit.RecoveryConfig
	lastSlowdown ratelimit.SlowdownConfig
}

func (f *adaptiveRateLimitControllerConfigFetcher) FetchRecoveryConfig() ratelimit.RecoveryConfig {
	f.mu.Lock()
	defer f.mu.Unlock()

	newConfig := ratelimit.RecoveryConfig{
		HWM:                 f.config.RecoveryHWM.GetAsSize(),
		LWM:                 f.config.RecoveryLWM.GetAsSize(),
		NormalDelayInterval: f.config.RecoveryNormalDelayInterval.GetAsDurationByParse(),
		Incremental:         f.config.RecoveryIncremental.GetAsSize(),
		IncreaseInterval:    f.config.RecoveryIncreaseInterval.GetAsDurationByParse(),
	}

	if newConfig.HWM < newConfig.LWM || newConfig.Incremental <= 0 || newConfig.NormalDelayInterval < 0 || newConfig.IncreaseInterval < 0 {
		log.Warn("illegal recovery config, fallback to previous one",
			zap.String("sourceName", f.sourceName),
			zap.Int64("hwm", newConfig.HWM),
			zap.Int64("lwm", newConfig.LWM),
			zap.Int64("incremental", newConfig.Incremental),
			zap.Duration("normalInterval", newConfig.NormalDelayInterval),
			zap.Duration("increaseDelayInterval", newConfig.IncreaseInterval))
		return f.lastRecovery
	}
	if f.lastRecovery != newConfig {
		f.lastRecovery = newConfig
		log.Info("recovery config changed",
			zap.String("sourceName", f.sourceName),
			zap.Int64("hwm", newConfig.HWM),
			zap.Int64("lwm", newConfig.LWM),
			zap.Duration("normalInterval", newConfig.NormalDelayInterval),
			zap.Int64("incremental", newConfig.Incremental),
			zap.Duration("increaseDelayInterval", newConfig.IncreaseInterval))
	}
	f.lastRecovery = newConfig
	f.reportRecoveryConfigMetrics(newConfig)
	return newConfig
}

// reportRecoveryConfigMetrics reports the recovery config metrics.
func (f *adaptiveRateLimitControllerConfigFetcher) reportRecoveryConfigMetrics(config ratelimit.RecoveryConfig) {
	metrics.WALRateLimitConfigRecoveryHWM.WithLabelValues(
		paramtable.GetStringNodeID(), f.channel.Name, f.sourceName,
	).Set(float64(config.HWM))
	metrics.WALRateLimitConfigRecoveryLWM.WithLabelValues(
		paramtable.GetStringNodeID(), f.channel.Name, f.sourceName,
	).Set(float64(config.LWM))
}

func (f *adaptiveRateLimitControllerConfigFetcher) FetchSlowdownConfig() ratelimit.SlowdownConfig {
	f.mu.Lock()
	defer f.mu.Unlock()

	newConfig := ratelimit.SlowdownConfig{
		FirstSlowdownDelay:  f.config.SlowdownStartupDelayInterval.GetAsDurationByParse(),
		HWM:                 f.config.SlowdownHWM.GetAsSize(),
		LWM:                 f.config.SlowdownLWM.GetAsSize(),
		DecreaseInterval:    f.config.SlowdownDecreaseInterval.GetAsDurationByParse(),
		DecreaseRatio:       f.config.SlowdownDecreaseRatio.GetAsFloat(),
		RejectDelayInterval: f.config.SlowdownRejectDelayInterval.GetAsDurationByParse(),
	}

	if newConfig.FirstSlowdownDelay < 0 || newConfig.HWM < newConfig.LWM || newConfig.DecreaseInterval < 0 || newConfig.DecreaseRatio <= 0 || newConfig.DecreaseRatio >= 1 || newConfig.RejectDelayInterval < 0 {
		log.Warn("illegal slowdown config, fallback to previous one",
			zap.String("sourceName", f.sourceName),
			zap.Duration("firstSlowdownDelay", newConfig.FirstSlowdownDelay),
			zap.Int64("hwm", newConfig.HWM),
			zap.Int64("lwm", newConfig.LWM),
			zap.Duration("decreaseInterval", newConfig.DecreaseInterval),
			zap.Float64("decreaseRatio", newConfig.DecreaseRatio),
			zap.Duration("rejectDelayInterval", newConfig.RejectDelayInterval))
		return f.lastSlowdown
	}
	if f.lastSlowdown != newConfig {
		f.lastSlowdown = newConfig
		log.Info("slowdown config changed",
			zap.String("sourceName", f.sourceName),
			zap.Duration("firstSlowdownDelay", newConfig.FirstSlowdownDelay),
			zap.Int64("hwm", newConfig.HWM),
			zap.Int64("lwm", newConfig.LWM),
			zap.Duration("decreaseInterval", newConfig.DecreaseInterval),
			zap.Float64("decreaseRatio", newConfig.DecreaseRatio),
			zap.Duration("rejectDelayInterval", newConfig.RejectDelayInterval))
	}
	f.reportSlowdownConfigMetrics(newConfig)
	return newConfig
}

// reportSlowdownConfigMetrics reports the slowdown config metrics.
func (f *adaptiveRateLimitControllerConfigFetcher) reportSlowdownConfigMetrics(config ratelimit.SlowdownConfig) {
	metrics.WALRateLimitConfigSlowdownHWM.WithLabelValues(
		paramtable.GetStringNodeID(), f.channel.Name, f.sourceName,
	).Set(float64(config.HWM))
	metrics.WALRateLimitConfigSlowdownLWM.WithLabelValues(
		paramtable.GetStringNodeID(), f.channel.Name, f.sourceName,
	).Set(float64(config.LWM))
}

// Close closes the adaptive rate limit controller config fetcher.
func (f *adaptiveRateLimitControllerConfigFetcher) Close() {
	metrics.WALRateLimitConfigRecoveryHWM.DeleteLabelValues(
		paramtable.GetStringNodeID(), f.channel.Name, f.sourceName,
	)
	metrics.WALRateLimitConfigRecoveryLWM.DeleteLabelValues(
		paramtable.GetStringNodeID(), f.channel.Name, f.sourceName,
	)
	metrics.WALRateLimitConfigSlowdownHWM.DeleteLabelValues(
		paramtable.GetStringNodeID(), f.channel.Name, f.sourceName,
	)
	metrics.WALRateLimitConfigSlowdownLWM.DeleteLabelValues(
		paramtable.GetStringNodeID(), f.channel.Name, f.sourceName,
	)
}

func newAdaptiveRateLimitControllerConfigFetcher(channel types.PChannelInfo, sourceName string) ratelimit.AdaptiveRateLimitControllerConfigFetcher {
	var config *paramtable.AdaptiveRateLimitConfig
	switch sourceName {
	case SourceNodeMemory:
		config = &paramtable.Get().StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit
	case SourceFlusherRecovering:
		config = &paramtable.Get().StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit
	case SourceRecoveryStorage:
		config = &paramtable.Get().StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit
	case SourceAppendRate:
		config = &paramtable.Get().StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit
	default:
		panic("unknown source name")
	}
	defaultFetcher := ratelimit.DefaultAdaptiveRateLimitControllerConfigFetcher{}
	f := &adaptiveRateLimitControllerConfigFetcher{
		channel:      channel,
		sourceName:   sourceName,
		config:       config,
		lastRecovery: defaultFetcher.FetchRecoveryConfig(),
		lastSlowdown: defaultFetcher.FetchSlowdownConfig(),
	}
	// Initialize last valid configs.
	f.lastRecovery = f.FetchRecoveryConfig()
	f.lastSlowdown = f.FetchSlowdownConfig()
	return f
}
