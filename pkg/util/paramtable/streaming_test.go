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

package paramtable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStreamingParam(t *testing.T) {
	Init()
	params := Get()

	assert.Equal(t, false, params.StreamingCfg.WALScannerPauseConsumption.GetAsBool())
	assert.Equal(t, 1*time.Minute, params.StreamingCfg.WALBalancerTriggerInterval.GetAsDurationByParse())
	assert.Equal(t, 10*time.Millisecond, params.StreamingCfg.WALBalancerBackoffInitialInterval.GetAsDurationByParse())
	assert.Equal(t, 5*time.Second, params.StreamingCfg.WALBalancerBackoffMaxInterval.GetAsDurationByParse())
	assert.Equal(t, 2.0, params.StreamingCfg.WALBalancerBackoffMultiplier.GetAsFloat())
	assert.Equal(t, "vchannelFair", params.StreamingCfg.WALBalancerPolicyName.GetValue())
	assert.Equal(t, true, params.StreamingCfg.WALBalancerPolicyAllowRebalance.GetAsBool())
	assert.Equal(t, 5*time.Minute, params.StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.GetAsDurationByParse())
	assert.Equal(t, 1*time.Second, params.StreamingCfg.WALBalancerPolicyAllowRebalanceRecoveryLagThreshold.GetAsDurationByParse())
	assert.Equal(t, 0.4, params.StreamingCfg.WALBalancerPolicyVChannelFairPChannelWeight.GetAsFloat())
	assert.Equal(t, 0.3, params.StreamingCfg.WALBalancerPolicyVChannelFairVChannelWeight.GetAsFloat())
	assert.Equal(t, 0.01, params.StreamingCfg.WALBalancerPolicyVChannelFairAntiAffinityWeight.GetAsFloat())
	assert.Equal(t, 0.01, params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceTolerance.GetAsFloat())
	assert.Equal(t, 3, params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceMaxStep.GetAsInt())
	assert.Equal(t, 5*time.Minute, params.StreamingCfg.WALBalancerOperationTimeout.GetAsDurationByParse())
	assert.Equal(t, 4.0, params.StreamingCfg.WALBroadcasterConcurrencyRatio.GetAsFloat())
	assert.Equal(t, 5*time.Minute, params.StreamingCfg.WALBroadcasterTombstoneCheckInternal.GetAsDurationByParse())
	assert.Equal(t, 8192, params.StreamingCfg.WALBroadcasterTombstoneMaxCount.GetAsInt())
	assert.Equal(t, 24*time.Hour, params.StreamingCfg.WALBroadcasterTombstoneMaxLifetime.GetAsDurationByParse())
	assert.Equal(t, 10*time.Second, params.StreamingCfg.TxnDefaultKeepaliveTimeout.GetAsDurationByParse())
	assert.Equal(t, 30*time.Second, params.StreamingCfg.WALWriteAheadBufferKeepalive.GetAsDurationByParse())
	assert.Equal(t, int64(64*1024*1024), params.StreamingCfg.WALWriteAheadBufferCapacity.GetAsSize())
	assert.Equal(t, 128, params.StreamingCfg.WALReadAheadBufferLength.GetAsInt())
	assert.Equal(t, 1*time.Second, params.StreamingCfg.LoggingAppendSlowThreshold.GetAsDurationByParse())
	assert.Equal(t, 3*time.Second, params.StreamingCfg.WALRecoveryGracefulCloseTimeout.GetAsDurationByParse())
	assert.Equal(t, 24*time.Hour, params.StreamingCfg.WALRecoverySchemaExpirationTolerance.GetAsDurationByParse())
	assert.Equal(t, 100, params.StreamingCfg.WALRecoveryMaxDirtyMessage.GetAsInt())
	assert.Equal(t, 10*time.Second, params.StreamingCfg.WALRecoveryPersistInterval.GetAsDurationByParse())
	assert.Equal(t, float64(0.6), params.StreamingCfg.FlushMemoryThreshold.GetAsFloat())
	assert.Equal(t, float64(0.2), params.StreamingCfg.FlushGrowingSegmentBytesHwmThreshold.GetAsFloat())
	assert.Equal(t, float64(0.1), params.StreamingCfg.FlushGrowingSegmentBytesLwmThreshold.GetAsFloat())
	assert.Equal(t, 10*time.Minute, params.StreamingCfg.FlushL0MaxLifetime.GetAsDurationByParse())
	assert.Equal(t, 500000, params.StreamingCfg.FlushL0MaxRowNum.GetAsInt())
	assert.Equal(t, int64(32*1024*1024), params.StreamingCfg.FlushL0MaxSize.GetAsSize())
	assert.Equal(t, 1*time.Minute, params.StreamingCfg.DelegatorEmptyTimeTickMaxFilterInterval.GetAsDurationByParse())
	assert.Equal(t, 1*time.Second, params.StreamingCfg.FlushEmptyTimeTickMaxFilterInterval.GetAsDurationByParse())
	assert.Equal(t, 0, params.StreamingCfg.WALBalancerExpectedInitialStreamingNodeNum.GetAsInt())

	assert.Equal(t, int64(20*1024*1024), params.StreamingCfg.WALRateLimitDefaultBurst.GetAsSize())
	assert.Equal(t, 0.9, params.StreamingCfg.WALRateLimitNodeMemorySlowdownThreshold.GetAsFloat())
	assert.Equal(t, 0.95, params.StreamingCfg.WALRateLimitNodeMemoryRejectThreshold.GetAsFloat())
	assert.Equal(t, 0.85, params.StreamingCfg.WALRateLimitNodeMemoryRecoverThreshold.GetAsFloat())

	// append rate limit
	assert.Equal(t, false, params.StreamingCfg.WALRateLimitAppendRateEnabled.GetAsBool())
	assert.Equal(t, int64(32*1024*1024), params.StreamingCfg.WALRateLimitAppendRateSlowdownThreshold.GetAsSize())
	assert.Equal(t, int64(28*1024*1024), params.StreamingCfg.WALRateLimitAppendRateRecoverThreshold.GetAsSize())

	// append rate adaptive rate limit
	assert.Equal(t, 0*time.Second, params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.SlowdownStartupDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(32*1024*1024), params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.SlowdownHWM.GetAsSize())
	assert.Equal(t, int64(2*1024*1024), params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.SlowdownLWM.GetAsSize())
	assert.Equal(t, 10*time.Second, params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.SlowdownDecreaseInterval.GetAsDurationByParse())
	assert.Equal(t, 0.9, params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.SlowdownDecreaseRatio.GetAsFloat())
	assert.Equal(t, 0*time.Second, params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.SlowdownRejectDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(32*1024*1024), params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.RecoveryHWM.GetAsSize())
	assert.Equal(t, int64(4*1024*1024), params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.RecoveryLWM.GetAsSize())
	assert.Equal(t, 10*time.Second, params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.RecoveryNormalDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(512*1024), params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.RecoveryIncremental.GetAsSize())
	assert.Equal(t, 1*time.Second, params.StreamingCfg.WALRateLimitAppendRateAdaptiveRateLimit.RecoveryIncreaseInterval.GetAsDurationByParse())

	// node memory adaptive rate limit
	assert.Equal(t, 0*time.Second, params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownStartupDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(8*1024*1024), params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownHWM.GetAsSize())
	assert.Equal(t, int64(512*1024), params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownLWM.GetAsSize())
	assert.Equal(t, 30*time.Second, params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownDecreaseInterval.GetAsDurationByParse())
	assert.Equal(t, 0.8, params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownDecreaseRatio.GetAsFloat())
	assert.Equal(t, 0*time.Second, params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.SlowdownRejectDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(32*1024*1024), params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryHWM.GetAsSize())
	assert.Equal(t, int64(2*1024*1024), params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryLWM.GetAsSize())
	assert.Equal(t, 10*time.Second, params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryNormalDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(512*1024), params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryIncremental.GetAsSize())
	assert.Equal(t, 1*time.Second, params.StreamingCfg.WALRateLimitNodeMemoryAdaptiveRateLimit.RecoveryIncreaseInterval.GetAsDurationByParse())

	// recovery storage adaptive rate limit
	assert.Equal(t, 30*time.Second, params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.SlowdownStartupDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(16*1024*1024), params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.SlowdownHWM.GetAsSize())
	assert.Equal(t, int64(1*1024*1024), params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.SlowdownLWM.GetAsSize())
	assert.Equal(t, 30*time.Second, params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.SlowdownDecreaseInterval.GetAsDurationByParse())
	assert.Equal(t, 0.8, params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.SlowdownDecreaseRatio.GetAsFloat())
	assert.Equal(t, 2*time.Minute, params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.SlowdownRejectDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(32*1024*1024), params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.RecoveryHWM.GetAsSize())
	assert.Equal(t, int64(2*1024*1024), params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.RecoveryLWM.GetAsSize())
	assert.Equal(t, 1*time.Minute, params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.RecoveryNormalDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(512*1024), params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.RecoveryIncremental.GetAsSize())
	assert.Equal(t, 1*time.Second, params.StreamingCfg.WALRateLimitRecoveryStorageAdaptiveRateLimit.RecoveryIncreaseInterval.GetAsDurationByParse())

	// flusher adaptive rate limit
	assert.Equal(t, 10*time.Second, params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.SlowdownStartupDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(16*1024*1024), params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.SlowdownHWM.GetAsSize())
	assert.Equal(t, int64(2*1024*1024), params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.SlowdownLWM.GetAsSize())
	assert.Equal(t, 30*time.Second, params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.SlowdownDecreaseInterval.GetAsDurationByParse())
	assert.Equal(t, 0.8, params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.SlowdownDecreaseRatio.GetAsFloat())
	assert.Equal(t, 2*time.Minute, params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.SlowdownRejectDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(64*1024*1024), params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.RecoveryHWM.GetAsSize())
	assert.Equal(t, int64(16*1024*1024), params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.RecoveryLWM.GetAsSize())
	assert.Equal(t, 5*time.Second, params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.RecoveryNormalDelayInterval.GetAsDurationByParse())
	assert.Equal(t, int64(5*1024*1024), params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.RecoveryIncremental.GetAsSize())
	assert.Equal(t, 1*time.Second, params.StreamingCfg.WALRateLimitFlusherAdaptiveRateLimit.RecoveryIncreaseInterval.GetAsDurationByParse())

	params.Save(params.StreamingCfg.WALBalancerTriggerInterval.Key, "50s")
	params.Save(params.StreamingCfg.WALBalancerBackoffInitialInterval.Key, "50s")
	params.Save(params.StreamingCfg.WALBalancerBackoffMultiplier.Key, "3.5")
	params.Save(params.StreamingCfg.WALBroadcasterConcurrencyRatio.Key, "1.5")
	params.Save(params.StreamingCfg.TxnDefaultKeepaliveTimeout.Key, "3500ms")
	params.Save(params.StreamingCfg.WALWriteAheadBufferKeepalive.Key, "10s")
	params.Save(params.StreamingCfg.WALWriteAheadBufferCapacity.Key, "128k")
	params.Save(params.StreamingCfg.WALBalancerPolicyName.Key, "pchannelFair")
	params.Save(params.StreamingCfg.WALBalancerPolicyVChannelFairPChannelWeight.Key, "0.5")
	params.Save(params.StreamingCfg.WALBalancerPolicyVChannelFairVChannelWeight.Key, "0.4")
	params.Save(params.StreamingCfg.WALBalancerPolicyVChannelFairAntiAffinityWeight.Key, "0.02")
	params.Save(params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceTolerance.Key, "0.02")
	params.Save(params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceMaxStep.Key, "4")
	params.Save(params.StreamingCfg.WALBalancerPolicyAllowRebalance.Key, "false")
	params.Save(params.StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.Key, "10s")
	params.Save(params.StreamingCfg.WALBalancerPolicyAllowRebalanceRecoveryLagThreshold.Key, "1s")
	params.Save(params.StreamingCfg.LoggingAppendSlowThreshold.Key, "3s")
	params.Save(params.StreamingCfg.WALRecoveryGracefulCloseTimeout.Key, "4s")
	params.Save(params.StreamingCfg.WALRecoveryMaxDirtyMessage.Key, "200")
	params.Save(params.StreamingCfg.WALRecoveryPersistInterval.Key, "20s")
	params.Save(params.StreamingCfg.FlushMemoryThreshold.Key, "0.7")
	params.Save(params.StreamingCfg.FlushGrowingSegmentBytesHwmThreshold.Key, "0.25")
	params.Save(params.StreamingCfg.FlushGrowingSegmentBytesLwmThreshold.Key, "0.15")
	params.Save(params.StreamingCfg.WALRateLimitDefaultBurst.Key, "10485760") // 10MB
	params.Save(params.StreamingCfg.WALRateLimitAppendRateEnabled.Key, "true")
	params.Save(params.StreamingCfg.WALRateLimitAppendRateSlowdownThreshold.Key, "128m")
	params.Save(params.StreamingCfg.WALRateLimitAppendRateRecoverThreshold.Key, "100m")
	assert.Equal(t, 50*time.Second, params.StreamingCfg.WALBalancerTriggerInterval.GetAsDurationByParse())
	assert.Equal(t, 50*time.Second, params.StreamingCfg.WALBalancerBackoffInitialInterval.GetAsDurationByParse())
	assert.Equal(t, 3.5, params.StreamingCfg.WALBalancerBackoffMultiplier.GetAsFloat())
	assert.Equal(t, 1.5, params.StreamingCfg.WALBroadcasterConcurrencyRatio.GetAsFloat())
	assert.Equal(t, "pchannelFair", params.StreamingCfg.WALBalancerPolicyName.GetValue())
	assert.Equal(t, false, params.StreamingCfg.WALBalancerPolicyAllowRebalance.GetAsBool())
	assert.Equal(t, 10*time.Second, params.StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.GetAsDurationByParse())
	assert.Equal(t, 1*time.Second, params.StreamingCfg.WALBalancerPolicyAllowRebalanceRecoveryLagThreshold.GetAsDurationByParse())
	assert.Equal(t, 0.5, params.StreamingCfg.WALBalancerPolicyVChannelFairPChannelWeight.GetAsFloat())
	assert.Equal(t, 0.4, params.StreamingCfg.WALBalancerPolicyVChannelFairVChannelWeight.GetAsFloat())
	assert.Equal(t, 0.02, params.StreamingCfg.WALBalancerPolicyVChannelFairAntiAffinityWeight.GetAsFloat())
	assert.Equal(t, 0.02, params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceTolerance.GetAsFloat())
	assert.Equal(t, 4, params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceMaxStep.GetAsInt())
	assert.Equal(t, 3500*time.Millisecond, params.StreamingCfg.TxnDefaultKeepaliveTimeout.GetAsDurationByParse())
	assert.Equal(t, 10*time.Second, params.StreamingCfg.WALWriteAheadBufferKeepalive.GetAsDurationByParse())
	assert.Equal(t, int64(128*1024), params.StreamingCfg.WALWriteAheadBufferCapacity.GetAsSize())
	assert.Equal(t, 3*time.Second, params.StreamingCfg.LoggingAppendSlowThreshold.GetAsDurationByParse())
	assert.Equal(t, 4*time.Second, params.StreamingCfg.WALRecoveryGracefulCloseTimeout.GetAsDurationByParse())
	assert.Equal(t, 200, params.StreamingCfg.WALRecoveryMaxDirtyMessage.GetAsInt())
	assert.Equal(t, 20*time.Second, params.StreamingCfg.WALRecoveryPersistInterval.GetAsDurationByParse())
	assert.Equal(t, float64(0.7), params.StreamingCfg.FlushMemoryThreshold.GetAsFloat())
	assert.Equal(t, float64(0.25), params.StreamingCfg.FlushGrowingSegmentBytesHwmThreshold.GetAsFloat())
	assert.Equal(t, float64(0.15), params.StreamingCfg.FlushGrowingSegmentBytesLwmThreshold.GetAsFloat())
	assert.Equal(t, 10*1024*1024, params.StreamingCfg.WALRateLimitDefaultBurst.GetAsInt())
	assert.Equal(t, true, params.StreamingCfg.WALRateLimitAppendRateEnabled.GetAsBool())
	assert.Equal(t, int64(128*1024*1024), params.StreamingCfg.WALRateLimitAppendRateSlowdownThreshold.GetAsSize())
	assert.Equal(t, int64(100*1024*1024), params.StreamingCfg.WALRateLimitAppendRateRecoverThreshold.GetAsSize())
}
