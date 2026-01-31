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

type streamingConfig struct {
	// scanner
	WALScannerPauseConsumption ParamItem `refreshable:"true"`

	// balancer
	WALBalancerTriggerInterval        ParamItem `refreshable:"true"`
	WALBalancerBackoffInitialInterval ParamItem `refreshable:"true"`
	WALBalancerBackoffMultiplier      ParamItem `refreshable:"true"`
	WALBalancerBackoffMaxInterval     ParamItem `refreshable:"true"`
	WALBalancerOperationTimeout       ParamItem `refreshable:"true"`

	// balancer Policy
	WALBalancerPolicyName                               ParamItem `refreshable:"true"`
	WALBalancerPolicyAllowRebalance                     ParamItem `refreshable:"true"`
	WALBalancerPolicyMinRebalanceIntervalThreshold      ParamItem `refreshable:"true"`
	WALBalancerPolicyAllowRebalanceRecoveryLagThreshold ParamItem `refreshable:"true"`
	WALBalancerPolicyVChannelFairPChannelWeight         ParamItem `refreshable:"true"`
	WALBalancerPolicyVChannelFairVChannelWeight         ParamItem `refreshable:"true"`
	WALBalancerPolicyVChannelFairAntiAffinityWeight     ParamItem `refreshable:"true"`
	WALBalancerPolicyVChannelFairRebalanceTolerance     ParamItem `refreshable:"true"`
	WALBalancerPolicyVChannelFairRebalanceMaxStep       ParamItem `refreshable:"true"`
	WALBalancerExpectedInitialStreamingNodeNum          ParamItem `refreshable:"true"`

	// broadcaster
	WALBroadcasterConcurrencyRatio       ParamItem `refreshable:"false"`
	WALBroadcasterTombstoneCheckInternal ParamItem `refreshable:"true"`
	WALBroadcasterTombstoneMaxCount      ParamItem `refreshable:"true"`
	WALBroadcasterTombstoneMaxLifetime   ParamItem `refreshable:"true"`

	// txn
	TxnDefaultKeepaliveTimeout ParamItem `refreshable:"true"`

	// write ahead buffer
	WALWriteAheadBufferCapacity  ParamItem `refreshable:"true"`
	WALWriteAheadBufferKeepalive ParamItem `refreshable:"true"`

	// read ahead buffer size
	WALReadAheadBufferLength ParamItem `refreshable:"true"`

	// logging
	LoggingAppendSlowThreshold ParamItem `refreshable:"true"`

	// memory usage control
	FlushMemoryThreshold                 ParamItem `refreshable:"true"`
	FlushGrowingSegmentBytesHwmThreshold ParamItem `refreshable:"true"`
	FlushGrowingSegmentBytesLwmThreshold ParamItem `refreshable:"true"`

	// Flush control
	FlushL0MaxLifetime ParamItem `refreshable:"true"`
	FlushL0MaxRowNum   ParamItem `refreshable:"true"`
	FlushL0MaxSize     ParamItem `refreshable:"true"`

	// recovery configuration.
	WALRecoveryPersistInterval           ParamItem `refreshable:"true"`
	WALRecoveryMaxDirtyMessage           ParamItem `refreshable:"true"`
	WALRecoveryGracefulCloseTimeout      ParamItem `refreshable:"true"`
	WALRecoverySchemaExpirationTolerance ParamItem `refreshable:"true"`

	// wal rate limit
	WALRateLimitDefaultBurst                     ParamItem `refreshable:"true"`
	WALRateLimitNodeMemorySlowdownThreshold      ParamItem `refreshable:"true"`
	WALRateLimitNodeMemoryRejectThreshold        ParamItem `refreshable:"true"`
	WALRateLimitNodeMemoryRecoverThreshold       ParamItem `refreshable:"true"`
	WALRateLimitNodeMemoryAdaptiveRateLimit      AdaptiveRateLimitConfig
	WALRateLimitFlusherAdaptiveRateLimit         AdaptiveRateLimitConfig
	WALRateLimitRecoveryStorageAdaptiveRateLimit AdaptiveRateLimitConfig
	WALRateLimitAppendRateEnabled                ParamItem `refreshable:"true"`
	WALRateLimitAppendRateSlowdownThreshold      ParamItem `refreshable:"true"`
	WALRateLimitAppendRateRecoverThreshold       ParamItem `refreshable:"true"`
	WALRateLimitAppendRateAdaptiveRateLimit      AdaptiveRateLimitConfig

	// Empty TimeTick Filtering configration
	DelegatorEmptyTimeTickMaxFilterInterval ParamItem `refreshable:"true"`
	FlushEmptyTimeTickMaxFilterInterval     ParamItem `refreshable:"true"`
}

func (p *streamingConfig) init(base *BaseTable) {
	// scanner
	p.WALScannerPauseConsumption = ParamItem{
		Key:     "streaming.walScanner.pauseConsumption",
		Version: "2.6.8",
		Doc: `Whether to pause the scanner from consuming WAL messages, false by default.

This parameter can be used as a temporary mitigation when a StreamingNode
enters a crash loop and fails to execute UpdateReplicateConfigure (Primary-Secondary switchover) 
due to bugs. Pausing scanner consumption prevents the crash from being 
repeatedly triggered during the node's startup and recovery phase.

In such recovery scenarios, set this value to "true". After UpdateReplicateConfigure
completes successfully or the StreamingNode returns to a healthy state, 
this value should be set back to "false" to resume normal message processing.

This configuration is applied dynamically and does not require restarting the cluster.`,
		DefaultValue: "false",
		Export:       false,
	}
	p.WALScannerPauseConsumption.Init(base.mgr)

	// balancer
	p.WALBalancerTriggerInterval = ParamItem{
		Key:     "streaming.walBalancer.triggerInterval",
		Version: "2.6.0",
		Doc: `The interval of balance task trigger at background, 1 min by default.
It's ok to set it into duration string, such as 30s or 1m30s, see time.ParseDuration`,
		DefaultValue: "1m",
		Export:       true,
	}
	p.WALBalancerTriggerInterval.Init(base.mgr)
	p.WALBalancerBackoffInitialInterval = ParamItem{
		Key:     "streaming.walBalancer.backoffInitialInterval",
		Version: "2.6.0",
		Doc: `The initial interval of balance task trigger backoff, 10 ms by default.
It's ok to set it into duration string, such as 30s or 1m30s, see time.ParseDuration`,
		DefaultValue: "10ms",
		Export:       true,
	}
	p.WALBalancerBackoffInitialInterval.Init(base.mgr)
	p.WALBalancerBackoffMultiplier = ParamItem{
		Key:          "streaming.walBalancer.backoffMultiplier",
		Version:      "2.6.0",
		Doc:          "The multiplier of balance task trigger backoff, 2 by default",
		DefaultValue: "2",
		Export:       true,
	}
	p.WALBalancerBackoffMultiplier.Init(base.mgr)
	p.WALBalancerBackoffMaxInterval = ParamItem{
		Key:     "streaming.walBalancer.backoffMaxInterval",
		Version: "2.6.0",
		Doc: `The max interval of balance task trigger backoff, 5s by default.
It's ok to set it into duration string, such as 30s or 1m30s, see time.ParseDuration`,
		DefaultValue: "5s",
		Export:       true,
	}
	p.WALBalancerBackoffMaxInterval.Init(base.mgr)
	p.WALBalancerOperationTimeout = ParamItem{
		Key:     "streaming.walBalancer.operationTimeout",
		Version: "2.6.0",
		Doc: `The timeout of wal balancer operation, 5m by default.
If the operation exceeds this timeout, it will be canceled.`,
		DefaultValue: "5m",
		Export:       true,
	}
	p.WALBalancerOperationTimeout.Init(base.mgr)

	p.WALBalancerPolicyName = ParamItem{
		Key:          "streaming.walBalancer.balancePolicy.name",
		Version:      "2.6.0",
		Doc:          "The multiplier of balance task trigger backoff, 2 by default",
		DefaultValue: "vchannelFair",
		Export:       true,
	}
	p.WALBalancerPolicyName.Init(base.mgr)

	p.WALBalancerPolicyAllowRebalance = ParamItem{
		Key:     "streaming.walBalancer.balancePolicy.allowRebalance",
		Version: "2.6.0",
		Doc: `Whether to allow rebalance, true by default.
If the rebalance is not allowed, only the lost wal recovery will be executed, the rebalance (move a pchannel from one node to another node) will be skipped.`,
		DefaultValue: "true",
		Export:       true,
	}
	p.WALBalancerPolicyAllowRebalance.Init(base.mgr)

	p.WALBalancerPolicyMinRebalanceIntervalThreshold = ParamItem{
		Key:          "streaming.walBalancer.balancePolicy.minRebalanceIntervalThreshold",
		Version:      "2.6.0",
		Doc:          `The max interval of rebalance for each wal, 5m by default.`,
		DefaultValue: "5m",
		Export:       true,
	}
	p.WALBalancerPolicyMinRebalanceIntervalThreshold.Init(base.mgr)

	p.WALBalancerPolicyAllowRebalanceRecoveryLagThreshold = ParamItem{
		Key:     "streaming.walBalancer.balancePolicy.allowRebalanceRecoveryLagThreshold",
		Version: "2.6.0",
		Doc: `The threshold of recovery lag for rebalance, 1s by default.
If the recovery lag is greater than this threshold, the rebalance of current pchannel is not allowed.`,
		DefaultValue: "1s",
		Export:       true,
	}
	p.WALBalancerPolicyAllowRebalanceRecoveryLagThreshold.Init(base.mgr)

	p.WALBalancerPolicyVChannelFairPChannelWeight = ParamItem{
		Key:     "streaming.walBalancer.balancePolicy.vchannelFair.pchannelWeight",
		Version: "2.6.0",
		Doc: `The weight of pchannel count in vchannelFair balance policy,
the pchannel count will more evenly distributed if the weight is greater, 0.4 by default`,
		DefaultValue: "0.4",
		Export:       true,
	}
	p.WALBalancerPolicyVChannelFairPChannelWeight.Init(base.mgr)

	p.WALBalancerPolicyVChannelFairVChannelWeight = ParamItem{
		Key:     "streaming.walBalancer.balancePolicy.vchannelFair.vchannelWeight",
		Version: "2.6.0",
		Doc: `The weight of vchannel count in vchannelFair balance policy,
the vchannel count will more evenly distributed if the weight is greater, 0.3 by default`,
		DefaultValue: "0.3",
		Export:       true,
	}
	p.WALBalancerPolicyVChannelFairVChannelWeight.Init(base.mgr)

	p.WALBalancerPolicyVChannelFairAntiAffinityWeight = ParamItem{
		Key:     "streaming.walBalancer.balancePolicy.vchannelFair.antiAffinityWeight",
		Version: "2.6.0",
		Doc: `The weight of affinity in vchannelFair balance policy,
the fewer VChannels belonging to the same Collection between two PChannels, the higher the affinity,
the vchannel of one collection will more evenly distributed if the weight is greater, 0.01 by default`,
		DefaultValue: "0.01",
		Export:       true,
	}
	p.WALBalancerPolicyVChannelFairAntiAffinityWeight.Init(base.mgr)

	p.WALBalancerPolicyVChannelFairRebalanceTolerance = ParamItem{
		Key:     "streaming.walBalancer.balancePolicy.vchannelFair.rebalanceTolerance",
		Version: "2.6.0",
		Doc: `The tolerance of vchannelFair balance policy, if the score of two balance result is less than the tolerance,
the balance result will be ignored, the lower tolerance, the sensitive rebalance, 0.01 by default`,
		DefaultValue: "0.01",
		Export:       true,
	}
	p.WALBalancerPolicyVChannelFairRebalanceTolerance.Init(base.mgr)

	p.WALBalancerPolicyVChannelFairRebalanceMaxStep = ParamItem{
		Key:     "streaming.walBalancer.balancePolicy.vchannelFair.rebalanceMaxStep",
		Version: "2.6.0",
		Doc: `Indicates how many pchannels will be considered as a batch for rebalancing,
the larger step, more aggressive and accurate rebalance,
it also determine the depth of depth first search method that is used to find the best balance result, 3 by default`,
		DefaultValue: "3",
		Export:       true,
	}
	p.WALBalancerPolicyVChannelFairRebalanceMaxStep.Init(base.mgr)

	p.WALBalancerExpectedInitialStreamingNodeNum = ParamItem{
		Key:     "streaming.walBalancer.expectedInitialStreamingNodeNum",
		Version: "2.6.9",
		Doc: `The expected initial streaming node number, 0 by default, means no expected initial streaming node number.
When the milvus is upgrading from 2.5 -> 2.6.9, the mixcoord will check if the expected initial streaming node number is reached,
then open the streaming service to continue the upgrade process.`,
		DefaultValue: "0",
		Export:       false,
	}
	p.WALBalancerExpectedInitialStreamingNodeNum.Init(base.mgr)

	p.WALBroadcasterConcurrencyRatio = ParamItem{
		Key:          "streaming.walBroadcaster.concurrencyRatio",
		Version:      "2.5.4",
		Doc:          `The concurrency ratio based on number of CPU for wal broadcaster, 4 by default.`,
		DefaultValue: "4",
		Export:       true,
	}
	p.WALBroadcasterConcurrencyRatio.Init(base.mgr)

	p.WALBroadcasterTombstoneCheckInternal = ParamItem{
		Key:     "streaming.walBroadcaster.tombstone.checkInternal",
		Version: "2.6.0",
		Doc: `The interval of garbage collection of tombstone, 5m by default.
Tombstone is used to reject duplicate submissions of DDL messages,
too few tombstones may lead to ABA issues in the state of milvus cluster.`,
		DefaultValue: "5m",
		Export:       false,
	}
	p.WALBroadcasterTombstoneCheckInternal.Init(base.mgr)

	p.WALBroadcasterTombstoneMaxCount = ParamItem{
		Key:     "streaming.walBroadcaster.tombstone.maxCount",
		Version: "2.6.0",
		Doc: `The max count of tombstone, 8192 by default. 
Tombstone is used to reject duplicate submissions of DDL messages,
too few tombstones may lead to ABA issues in the state of milvus cluster.`,
		DefaultValue: "8192",
		Export:       false,
	}
	p.WALBroadcasterTombstoneMaxCount.Init(base.mgr)

	p.WALBroadcasterTombstoneMaxLifetime = ParamItem{
		Key:     "streaming.walBroadcaster.tombstone.maxLifetime",
		Version: "2.6.0",
		Doc: `The max lifetime of tombstone, 24h by default.
Tombstone is used to reject duplicate submissions of DDL messages,
too few tombstones may lead to ABA issues in the state of milvus cluster.`,
		DefaultValue: "24h",
		Export:       false,
	}
	p.WALBroadcasterTombstoneMaxLifetime.Init(base.mgr)

	// txn
	p.TxnDefaultKeepaliveTimeout = ParamItem{
		Key:          "streaming.txn.defaultKeepaliveTimeout",
		Version:      "2.5.0",
		Doc:          "The default keepalive timeout for wal txn, 10s by default",
		DefaultValue: "10s",
		Export:       true,
	}
	p.TxnDefaultKeepaliveTimeout.Init(base.mgr)

	p.WALWriteAheadBufferCapacity = ParamItem{
		Key:          "streaming.walWriteAheadBuffer.capacity",
		Version:      "2.6.0",
		Doc:          "The capacity of write ahead buffer of each wal, 64M by default",
		DefaultValue: "64m",
		Export:       true,
	}
	p.WALWriteAheadBufferCapacity.Init(base.mgr)
	p.WALWriteAheadBufferKeepalive = ParamItem{
		Key:          "streaming.walWriteAheadBuffer.keepalive",
		Version:      "2.6.0",
		Doc:          "The keepalive duration for entries in write ahead buffer of each wal, 30s by default",
		DefaultValue: "30s",
		Export:       true,
	}
	p.WALWriteAheadBufferKeepalive.Init(base.mgr)

	p.WALReadAheadBufferLength = ParamItem{
		Key:     "streaming.walReadAheadBuffer.length",
		Version: "2.6.0",
		Doc: `The buffer length (pending message count) of read ahead buffer of each wal scanner can be used, 128 by default.
Higher one will increase the throughput of wal message handling, but introduce higher memory utilization.
Use the underlying wal default value if 0 is given.`,
		DefaultValue: "128",
		Export:       true,
	}
	p.WALReadAheadBufferLength.Init(base.mgr)

	p.LoggingAppendSlowThreshold = ParamItem{
		Key:     "streaming.logging.appendSlowThreshold",
		Version: "2.6.0",
		Doc: `The threshold of slow log, 1s by default. 
If the wal implementation is woodpecker, the minimum threshold is 3s`,
		DefaultValue: "1s",
		Export:       true,
	}
	p.LoggingAppendSlowThreshold.Init(base.mgr)

	p.FlushMemoryThreshold = ParamItem{
		Key:     "streaming.flush.memoryThreshold",
		Version: "2.6.0",
		Doc: `The threshold of memory usage for one streaming node,
If the memory usage is higher than this threshold, the node will try to trigger flush action to decrease the total of growing segment until growingSegmentBytesLwmThreshold,
the value should be in the range of (0, 1), 0.6 by default.`,
		DefaultValue: "0.6",
		Export:       true,
	}
	p.FlushMemoryThreshold.Init(base.mgr)

	p.FlushGrowingSegmentBytesHwmThreshold = ParamItem{
		Key:     "streaming.flush.growingSegmentBytesHwmThreshold",
		Version: "2.6.0",
		Doc: `The high watermark of total growing segment bytes for one streaming node,
If the total bytes of growing segment is greater than this threshold,
a flush process will be triggered to decrease total bytes of growing segment until growingSegmentBytesLwmThreshold, 0.2 by default`,
		DefaultValue: "0.2",
		Export:       true,
	}
	p.FlushGrowingSegmentBytesHwmThreshold.Init(base.mgr)

	p.FlushGrowingSegmentBytesLwmThreshold = ParamItem{
		Key:     "streaming.flush.growingSegmentBytesLwmThreshold",
		Version: "2.6.0",
		Doc: `The lower watermark of total growing segment bytes for one streaming node,
growing segment flush process will try to flush some growing segment into sealed 
until the total bytes of growing segment is less than this threshold, 0.1 by default.`,
		DefaultValue: "0.1",
		Export:       true,
	}
	p.FlushGrowingSegmentBytesLwmThreshold.Init(base.mgr)

	p.FlushL0MaxLifetime = ParamItem{
		Key:     "streaming.flush.l0.maxLifetime",
		Version: "2.6.0",
		Doc: `The max lifetime of l0 segment, 10 minutes by default.
If the l0 segment is older than this time, it will be flushed.`,
		DefaultValue: "10m",
		Export:       true,
	}
	p.FlushL0MaxLifetime.Init(base.mgr)

	p.FlushL0MaxRowNum = ParamItem{
		Key:     "streaming.flush.l0.maxRowNum",
		Version: "2.6.0",
		Doc: `The max row num of l0 segment, 500000 by default.
If the row num of l0 segment is greater than this num, it will be flushed.`,
		DefaultValue: "500000",
		Export:       true,
	}
	p.FlushL0MaxRowNum.Init(base.mgr)

	p.FlushL0MaxSize = ParamItem{
		Key:     "streaming.flush.l0.maxSize",
		Version: "2.6.0",
		Doc: `The max size of l0 segment, 32m by default.
If the binary size of l0 segment is greater than this size, it will be flushed.`,
		DefaultValue: "32m",
		Export:       true,
	}
	p.FlushL0MaxSize.Init(base.mgr)

	p.WALRecoveryPersistInterval = ParamItem{
		Key:     "streaming.walRecovery.persistInterval",
		Version: "2.6.0",
		Doc: `The interval of persist recovery info, 10s by default. 
Every the interval, the recovery info of wal will try to persist, and the checkpoint of wal can be advanced.
Currently it only affect the recovery of wal, but not affect the recovery of data flush into object storage`,
		DefaultValue: "10s",
		Export:       true,
	}
	p.WALRecoveryPersistInterval.Init(base.mgr)

	p.WALRecoveryMaxDirtyMessage = ParamItem{
		Key:     "streaming.walRecovery.maxDirtyMessage",
		Version: "2.6.0",
		Doc: `The max dirty message count of wal recovery, 100 by default.
If there are more than this count of dirty message in wal recovery info, it will be persisted immediately, 
but not wait for the persist interval.`,
		DefaultValue: "100",
		Export:       true,
	}
	p.WALRecoveryMaxDirtyMessage.Init(base.mgr)

	p.WALRecoveryGracefulCloseTimeout = ParamItem{
		Key:     "streaming.walRecovery.gracefulCloseTimeout",
		Version: "2.6.0",
		Doc: `The graceful close timeout for wal recovery, 3s by default.
When the wal is on-closing, the recovery module will try to persist the recovery info for wal to make next recovery operation more fast.
If that persist operation exceeds this timeout, the wal recovery module will close right now.`,
		DefaultValue: "3s",
		Export:       true,
	}
	p.WALRecoveryGracefulCloseTimeout.Init(base.mgr)

	p.WALRecoverySchemaExpirationTolerance = ParamItem{
		Key:     "streaming.walRecovery.schemaExpirationTolerance",
		Version: "2.6.8",
		Doc: `The tolerance of schema expiration for wal recovery, 24h by default.
If the schema is older than (the channel checkpoint - tolerance), it will be removed forever.`,
		DefaultValue: "24h",
		Export:       false,
	}
	p.WALRecoverySchemaExpirationTolerance.Init(base.mgr)

	p.WALRateLimitDefaultBurst = ParamItem{
		Key:          "streaming.walRateLimit.defaultBurst",
		Version:      "2.6.9",
		Doc:          "The default burst size for the WAL rate limiter, 20MB by default. The burst size determines the maximum number of bytes that can be consumed at once.",
		DefaultValue: "20m",
		Export:       true,
	}
	p.WALRateLimitDefaultBurst.Init(base.mgr)

	p.WALRateLimitNodeMemorySlowdownThreshold = ParamItem{
		Key:          "streaming.walRateLimit.nodeMemory.slowdownThreshold",
		Version:      "2.6.9",
		Doc:          "When the memory usage is greater than this threshold, the node memory rate limiter will enter slowdown mode, 0.9 by default.",
		DefaultValue: "0.9",
		Export:       true,
	}
	p.WALRateLimitNodeMemorySlowdownThreshold.Init(base.mgr)

	p.WALRateLimitNodeMemoryRejectThreshold = ParamItem{
		Key:          "streaming.walRateLimit.nodeMemory.rejectThreshold",
		Version:      "2.6.9",
		Doc:          "When the memory usage is greater than this threshold, the node memory rate limiter will enter reject mode, 0.95 by default.",
		DefaultValue: "0.95",
		Export:       true,
	}
	p.WALRateLimitNodeMemoryRejectThreshold.Init(base.mgr)

	p.WALRateLimitNodeMemoryRecoverThreshold = ParamItem{
		Key:          "streaming.walRateLimit.nodeMemory.recoverThreshold",
		Version:      "2.6.9",
		Doc:          "When the memory usage is less than this threshold, the node memory rate limiter will enter recovery mode, 0.85 by default.",
		DefaultValue: "0.85",
		Export:       true,
	}
	p.WALRateLimitNodeMemoryRecoverThreshold.Init(base.mgr)

	p.WALRateLimitNodeMemoryAdaptiveRateLimit.init(base, "streaming.walRateLimit.nodeMemory.adaptiveRateLimit", AdaptiveRateLimitConfigDefaultValue{
		SlowdownStartupDelayInterval: "0",
		SlowdownHWM:                  "8mb",
		SlowdownLWM:                  "512kb",
		SlowdownDecreaseInterval:     "30s",
		SlowdownDecreaseRatio:        "0.8",
		SlowdownRejectDelayInterval:  "0",
		RecoveryHWM:                  "32mb",
		RecoveryLWM:                  "2mb",
		RecoveryNormalDelayInterval:  "10s",
		RecoveryIncremental:          "512kb",
		RecoveryIncreaseInterval:     "1s",
		Export:                       true,
	})

	p.WALRateLimitRecoveryStorageAdaptiveRateLimit.init(base, "streaming.walRateLimit.recoveryStorage.adaptiveRateLimit", AdaptiveRateLimitConfigDefaultValue{
		SlowdownStartupDelayInterval: "30s",
		SlowdownHWM:                  "16mb",
		SlowdownLWM:                  "1mb",
		SlowdownDecreaseInterval:     "30s",
		SlowdownDecreaseRatio:        "0.8",
		SlowdownRejectDelayInterval:  "2m",
		RecoveryHWM:                  "32mb",
		RecoveryLWM:                  "2mb",
		RecoveryNormalDelayInterval:  "1m",
		RecoveryIncremental:          "512kb",
		RecoveryIncreaseInterval:     "1s",
		Export:                       true,
	})

	p.WALRateLimitFlusherAdaptiveRateLimit.init(base, "streaming.walRateLimit.flusher.adaptiveRateLimit", AdaptiveRateLimitConfigDefaultValue{
		SlowdownStartupDelayInterval: "10s",
		SlowdownHWM:                  "16mb",
		SlowdownLWM:                  "2mb",
		SlowdownDecreaseInterval:     "30s",
		SlowdownDecreaseRatio:        "0.8",
		SlowdownRejectDelayInterval:  "2m",
		RecoveryHWM:                  "64mb",
		RecoveryLWM:                  "16mb",
		RecoveryNormalDelayInterval:  "5s",
		RecoveryIncremental:          "5mb",
		RecoveryIncreaseInterval:     "1s",
		Export:                       false,
	})

	p.WALRateLimitAppendRateEnabled = ParamItem{
		Key:          "streaming.walRateLimit.appendRate.enabled",
		Version:      "2.6.10",
		Doc:          "Whether to enable the append rate limiter, true by default. When enabled, the rate limiter will throttle writes based on append rate thresholds.",
		DefaultValue: "false",
		Export:       true,
	}
	p.WALRateLimitAppendRateEnabled.Init(base.mgr)

	p.WALRateLimitAppendRateSlowdownThreshold = ParamItem{
		Key:          "streaming.walRateLimit.appendRate.slowdownThreshold",
		Version:      "2.6.10",
		Doc:          "When the append rate (bytes/sec) is greater than this threshold, the append rate limiter will enter slowdown mode, 32MB/s by default. This protects each WAL from being overloaded.",
		DefaultValue: "32m",
		Export:       true,
	}
	p.WALRateLimitAppendRateSlowdownThreshold.Init(base.mgr)

	p.WALRateLimitAppendRateRecoverThreshold = ParamItem{
		Key:          "streaming.walRateLimit.appendRate.recoverThreshold",
		Version:      "2.6.10",
		Doc:          "When the append rate (bytes/sec) is less than this threshold, the append rate limiter will enter recovery mode, 28MB/s by default. The gap between slowdown and recover threshold prevents oscillation.",
		DefaultValue: "28m",
		Export:       true,
	}
	p.WALRateLimitAppendRateRecoverThreshold.Init(base.mgr)

	p.WALRateLimitAppendRateAdaptiveRateLimit.init(base, "streaming.walRateLimit.appendRate.adaptiveRateLimit", AdaptiveRateLimitConfigDefaultValue{
		SlowdownStartupDelayInterval: "0",
		SlowdownHWM:                  "32mb",
		SlowdownLWM:                  "2mb",
		SlowdownDecreaseInterval:     "10s",
		SlowdownDecreaseRatio:        "0.9",
		SlowdownRejectDelayInterval:  "0",
		RecoveryHWM:                  "32mb",
		RecoveryLWM:                  "4mb",
		RecoveryNormalDelayInterval:  "10s",
		RecoveryIncremental:          "512kb",
		RecoveryIncreaseInterval:     "1s",
		Export:                       true,
	})

	p.DelegatorEmptyTimeTickMaxFilterInterval = ParamItem{
		Key:     "streaming.delegator.emptyTimeTick.maxFilterInterval",
		Version: "2.6.9",
		Doc: `The max filter interval for empty time tick of delegator, 1m by default.
If the interval since last timetick is less than this config, the empty time tick will be filtered.`,
		DefaultValue: "1m",
		Export:       false,
	}
	p.DelegatorEmptyTimeTickMaxFilterInterval.Init(base.mgr)

	p.FlushEmptyTimeTickMaxFilterInterval = ParamItem{
		Key:     "streaming.flush.emptyTimeTick.maxFilterInterval",
		Version: "2.6.9",
		Doc: `The max filter interval for empty time tick of flush, 1s by default.
If the interval since last timetick is less than this config, the empty time tick will be filtered.
Because current flusher need the empty time tick to trigger the cp update,
too huge threshold will block the GetFlushState operation,
so we set 1 second here as a threshold.`,
		DefaultValue: "1s",
		Export:       false,
	}
	p.FlushEmptyTimeTickMaxFilterInterval.Init(base.mgr)
}

type AdaptiveRateLimitConfigDefaultValue struct {
	SlowdownStartupDelayInterval string
	SlowdownHWM                  string
	SlowdownLWM                  string
	SlowdownDecreaseInterval     string
	SlowdownDecreaseRatio        string
	SlowdownRejectDelayInterval  string
	RecoveryHWM                  string
	RecoveryLWM                  string
	RecoveryNormalDelayInterval  string
	RecoveryIncremental          string
	RecoveryIncreaseInterval     string

	Export bool
}

type AdaptiveRateLimitConfig struct {
	SlowdownStartupDelayInterval ParamItem `refreshable:"true"`
	SlowdownHWM                  ParamItem `refreshable:"true"`
	SlowdownLWM                  ParamItem `refreshable:"true"`
	SlowdownDecreaseInterval     ParamItem `refreshable:"true"`
	SlowdownDecreaseRatio        ParamItem `refreshable:"true"`
	SlowdownRejectDelayInterval  ParamItem `refreshable:"true"`
	RecoveryHWM                  ParamItem `refreshable:"true"`
	RecoveryLWM                  ParamItem `refreshable:"true"`
	RecoveryIncremental          ParamItem `refreshable:"true"`
	RecoveryIncreaseInterval     ParamItem `refreshable:"true"`
	RecoveryNormalDelayInterval  ParamItem `refreshable:"true"`
}

func (p *AdaptiveRateLimitConfig) init(
	base *BaseTable,
	prefix string,
	defaults AdaptiveRateLimitConfigDefaultValue,
) {
	p.SlowdownStartupDelayInterval = ParamItem{
		Key:          prefix + ".slowdown.startupDelayInterval",
		Version:      "2.6.10",
		Doc:          "The startup delay interval for adaptive rate limit slowdown, when the first time the rate limit enters slowdown mode, it will wait for this interval to take effect, " + defaults.SlowdownStartupDelayInterval + " by default.",
		DefaultValue: defaults.SlowdownStartupDelayInterval,
		Export:       defaults.Export,
	}
	p.SlowdownStartupDelayInterval.Init(base.mgr)

	p.SlowdownHWM = ParamItem{
		Key:          prefix + ".slowdown.hwm",
		Version:      "2.6.10",
		Doc:          "The high watermark of adaptive rate limit slowdown, the rate limit will be set to this value when slowdown mode is triggered, " + defaults.SlowdownHWM + "/s by default.",
		DefaultValue: defaults.SlowdownHWM,
		Export:       defaults.Export,
	}
	p.SlowdownHWM.Init(base.mgr)

	p.SlowdownLWM = ParamItem{
		Key:          prefix + ".slowdown.lwm",
		Version:      "2.6.10",
		Doc:          "The low watermark of adaptive rate limit slowdown, the rate limit will decrease until this value if slowdown mode is kept, " + defaults.SlowdownLWM + "/s by default.",
		DefaultValue: defaults.SlowdownLWM,
		Export:       defaults.Export,
	}
	p.SlowdownLWM.Init(base.mgr)

	p.SlowdownDecreaseInterval = ParamItem{
		Key:          prefix + ".slowdown.decreaseInterval",
		Version:      "2.6.10",
		Doc:          "The interval of adaptive rate limit slowdown decrease, the rate limit will decrease every this interval, " + defaults.SlowdownDecreaseInterval + " by default.",
		DefaultValue: defaults.SlowdownDecreaseInterval,
		Export:       defaults.Export,
	}
	p.SlowdownDecreaseInterval.Init(base.mgr)

	p.SlowdownDecreaseRatio = ParamItem{
		Key:          prefix + ".slowdown.decreaseRatio",
		Version:      "2.6.10",
		Doc:          "The ratio of adaptive rate limit slowdown decrease, the rate limit will decrease by this ratio every decrease interval, " + defaults.SlowdownDecreaseRatio + " by default.",
		DefaultValue: defaults.SlowdownDecreaseRatio,
		Export:       defaults.Export,
	}
	p.SlowdownDecreaseRatio.Init(base.mgr)

	p.SlowdownRejectDelayInterval = ParamItem{
		Key:          prefix + ".slowdown.rejectDelayInterval",
		Version:      "2.6.10",
		Doc:          "The delay interval of adaptive rate limit slowdown to reject mode after slowdown reaches low watermark; 0 means no reject operation will be triggered, " + defaults.SlowdownRejectDelayInterval + " by default.",
		DefaultValue: defaults.SlowdownRejectDelayInterval,
		Export:       defaults.Export,
	}
	p.SlowdownRejectDelayInterval.Init(base.mgr)

	p.RecoveryHWM = ParamItem{
		Key:          prefix + ".recovery.hwm",
		Version:      "2.6.10",
		Doc:          "The high watermark of adaptive rate limit recovery, the rate limit will increase until this value if recovery mode is kept, " + defaults.RecoveryHWM + "/s by default.",
		DefaultValue: defaults.RecoveryHWM,
		Export:       defaults.Export,
	}
	p.RecoveryHWM.Init(base.mgr)

	p.RecoveryLWM = ParamItem{
		Key:          prefix + ".recovery.lwm",
		Version:      "2.6.10",
		Doc:          "The low watermark of adaptive rate limit recovery, the rate limit will be set to this value when recovery mode is triggered, " + defaults.RecoveryLWM + "/s by default.",
		DefaultValue: defaults.RecoveryLWM,
		Export:       defaults.Export,
	}
	p.RecoveryLWM.Init(base.mgr)

	p.RecoveryIncreaseInterval = ParamItem{
		Key:          prefix + ".recovery.increaseInterval",
		Version:      "2.6.10",
		Doc:          "The delay interval of adaptive rate limit recovery increase, the rate limit will increase every this interval, " + defaults.RecoveryIncreaseInterval + " by default.",
		DefaultValue: defaults.RecoveryIncreaseInterval,
		Export:       defaults.Export,
	}
	p.RecoveryIncreaseInterval.Init(base.mgr)

	p.RecoveryIncremental = ParamItem{
		Key:          prefix + ".recovery.incremental",
		Version:      "2.6.10",
		Doc:          "The incremental of adaptive rate limit recovery, the rate limit will increase by this value every increase interval, " + defaults.RecoveryIncremental + "/s by default.",
		DefaultValue: defaults.RecoveryIncremental,
		Export:       defaults.Export,
	}
	p.RecoveryIncremental.Init(base.mgr)

	p.RecoveryNormalDelayInterval = ParamItem{
		Key:          prefix + ".recovery.normalDelayInterval",
		Version:      "2.6.10",
		Doc:          "The delay interval of adaptive rate limit recovery to normal mode after recovery reaches high watermark, " + defaults.RecoveryNormalDelayInterval + " by default.",
		DefaultValue: defaults.RecoveryNormalDelayInterval,
		Export:       defaults.Export,
	}
	p.RecoveryNormalDelayInterval.Init(base.mgr)
}
