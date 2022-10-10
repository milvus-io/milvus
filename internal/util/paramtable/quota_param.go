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
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

const (
	// defaultMax is the default unlimited rate or threshold.
	defaultMax = float64(math.MaxFloat64)
	// GB used to convert gigabytes and bytes.
	GB = 1024.0 * 1024.0 * 1024.0
	// defaultDiskQuotaInGB is the default disk quota in gigabytes.
	defaultDiskQuotaInGB = defaultMax / GB
	// defaultMin is the default minimal rate.
	defaultMin = float64(0)
	// defaultLowWaterLevel is the default memory low water level.
	defaultLowWaterLevel = float64(0.85)
	// defaultHighWaterLevel is the default memory low water level.
	defaultHighWaterLevel = float64(0.95)
)

// quotaConfig is configuration for quota and limitations.
type quotaConfig struct {
	Base *BaseTable
	once sync.Once

	QuotaAndLimitsEnabled      bool
	QuotaCenterCollectInterval float64

	// ddl
	DDLLimitEnabled   bool
	DDLCollectionRate float64
	DDLPartitionRate  float64

	IndexLimitEnabled      bool
	MaxIndexRate           float64
	FlushLimitEnabled      bool
	MaxFlushRate           float64
	CompactionLimitEnabled bool
	MaxCompactionRate      float64

	// dml
	DMLLimitEnabled    bool
	DMLMaxInsertRate   float64
	DMLMinInsertRate   float64
	DMLMaxDeleteRate   float64
	DMLMinDeleteRate   float64
	DMLMaxBulkLoadRate float64
	DMLMinBulkLoadRate float64

	// dql
	DQLLimitEnabled  bool
	DQLMaxSearchRate float64
	DQLMinSearchRate float64
	DQLMaxQueryRate  float64
	DQLMinQueryRate  float64

	// limits
	MaxCollectionNum int

	// limit writing
	ForceDenyWriting              bool
	TtProtectionEnabled           bool
	MaxTimeTickDelay              time.Duration
	MemProtectionEnabled          bool
	DataNodeMemoryLowWaterLevel   float64
	DataNodeMemoryHighWaterLevel  float64
	QueryNodeMemoryLowWaterLevel  float64
	QueryNodeMemoryHighWaterLevel float64
	DiskProtectionEnabled         bool
	DiskQuota                     float64

	// limit reading
	ForceDenyReading       bool
	QueueProtectionEnabled bool
	NQInQueueThreshold     int64
	QueueLatencyThreshold  float64
	CoolOffSpeed           float64
}

func (p *quotaConfig) init(base *BaseTable) {
	p.Base = base

	p.initQuotaAndLimitsEnabled()
	p.initQuotaCenterCollectInterval()

	// ddl
	p.initDDLLimitEnabled()
	p.initDDLCollectionRate()
	p.initDDLPartitionRate()

	p.initIndexLimitEnabled()
	p.initMaxIndexRate()
	p.initFlushLimitEnabled()
	p.initMaxFlushRate()
	p.initCompactionLimitEnabled()
	p.initMaxCompactionRate()

	// dml
	p.initDMLLimitEnabled()
	p.initDMLMaxInsertRate()
	p.initDMLMinInsertRate()
	p.initDMLMaxDeleteRate()
	p.initDMLMinDeleteRate()
	p.initDMLMaxBulkLoadRate()
	p.initDMLMinBulkLoadRate()

	// dql
	p.initDQLLimitEnabled()
	p.initDQLMaxSearchRate()
	p.initDQLMinSearchRate()
	p.initDQLMaxQueryRate()
	p.initDQLMinQueryRate()

	// limits
	p.initMaxCollectionNum()

	// limit writing
	p.initForceDenyWriting()
	p.initTtProtectionEnabled()
	p.initMaxTimeTickDelay()
	p.initMemProtectionEnabled()
	p.initDataNodeMemoryLowWaterLevel()
	p.initDataNodeMemoryHighWaterLevel()
	p.initQueryNodeMemoryLowWaterLevel()
	p.initQueryNodeMemoryHighWaterLevel()
	p.initDiskProtectionEnabled()
	p.initDiskQuota()

	// limit reading
	p.initForceDenyReading()
	p.initQueueProtectionEnabled()
	p.initNQInQueueThreshold()
	p.initQueueLatencyThreshold()
	p.initCoolOffSpeed()
}

func (p *quotaConfig) initQuotaAndLimitsEnabled() {
	p.QuotaAndLimitsEnabled = p.Base.ParseBool("quotaAndLimits.enabled", false)
}

func (p *quotaConfig) initQuotaCenterCollectInterval() {
	const defaultInterval = 3.0
	p.QuotaCenterCollectInterval = p.Base.ParseFloatWithDefault("quotaAndLimits.quotaCenterCollectInterval", defaultInterval)
	// (0 ~ 65536)
	if p.QuotaCenterCollectInterval <= 0 || p.QuotaCenterCollectInterval >= 65536 {
		p.QuotaCenterCollectInterval = defaultInterval
	}
}

func (p *quotaConfig) initDDLLimitEnabled() {
	p.DDLLimitEnabled = p.Base.ParseBool("quotaAndLimits.ddl.enabled", false)
}

func (p *quotaConfig) initDDLCollectionRate() {
	if !p.DDLLimitEnabled {
		p.DDLCollectionRate = defaultMax
		return
	}
	p.DDLCollectionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.collectionRate", defaultMax)
	// [0 ~ Inf)
	if p.DDLCollectionRate < 0 {
		p.DDLCollectionRate = defaultMax
	}
}

func (p *quotaConfig) initDDLPartitionRate() {
	if !p.DDLLimitEnabled {
		p.DDLPartitionRate = defaultMax
		return
	}
	p.DDLPartitionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.partitionRate", defaultMax)
	// [0 ~ Inf)
	if p.DDLPartitionRate < 0 {
		p.DDLPartitionRate = defaultMax
	}
}

func (p *quotaConfig) initIndexLimitEnabled() {
	p.IndexLimitEnabled = p.Base.ParseBool("quotaAndLimits.indexRate.enabled", false)
}

func (p *quotaConfig) initMaxIndexRate() {
	if !p.IndexLimitEnabled {
		p.MaxIndexRate = defaultMax
		return
	}
	p.MaxIndexRate = p.Base.ParseFloatWithDefault("quotaAndLimits.indexRate.max", defaultMax)
	// [0 ~ Inf)
	if p.MaxIndexRate < 0 {
		p.MaxIndexRate = defaultMax
	}
}

func (p *quotaConfig) initFlushLimitEnabled() {
	p.FlushLimitEnabled = p.Base.ParseBool("quotaAndLimits.flushRate.enabled", false)
}

func (p *quotaConfig) initMaxFlushRate() {
	if !p.FlushLimitEnabled {
		p.MaxFlushRate = defaultMax
		return
	}
	p.MaxFlushRate = p.Base.ParseFloatWithDefault("quotaAndLimits.flushRate.max", defaultMax)
	// [0 ~ Inf)
	if p.MaxFlushRate < 0 {
		p.MaxFlushRate = defaultMax
	}
}

func (p *quotaConfig) initCompactionLimitEnabled() {
	p.CompactionLimitEnabled = p.Base.ParseBool("quotaAndLimits.compactionRate.enabled", false)
}

func (p *quotaConfig) initMaxCompactionRate() {
	if !p.CompactionLimitEnabled {
		p.MaxCompactionRate = defaultMax
		return
	}
	p.MaxCompactionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.compactionRate.max", defaultMax)
	// [0 ~ Inf)
	if p.MaxCompactionRate < 0 {
		p.MaxCompactionRate = defaultMax
	}
}

func megaBytesRate2Bytes(f float64) float64 {
	return f * 1024 * 1024
}

func (p *quotaConfig) checkMinMaxLegal(min, max float64) bool {
	if min > max {
		log.Warn("init QuotaConfig failed, max/high must be greater than or equal to min/low, use default values",
			zap.Float64("min", min), zap.Float64("max", max), zap.Float64("defaultMin", defaultMin), zap.Float64("defaultMax", defaultMax))
		return false
	}
	return true
}

func (p *quotaConfig) initDMLLimitEnabled() {
	p.DMLLimitEnabled = p.Base.ParseBool("quotaAndLimits.dml.enabled", false)
}

func (p *quotaConfig) initDMLMaxInsertRate() {
	if !p.DMLLimitEnabled {
		p.DMLMaxInsertRate = defaultMax
		return
	}
	p.DMLMaxInsertRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.insertRate.max", defaultMax)
	if math.Abs(p.DMLMaxInsertRate-defaultMax) > 0.001 { // maxRate != defaultMax
		p.DMLMaxInsertRate = megaBytesRate2Bytes(p.DMLMaxInsertRate)
	}
	// [0, inf)
	if p.DMLMaxInsertRate < 0 {
		p.DMLMaxInsertRate = defaultMax
	}
}

func (p *quotaConfig) initDMLMinInsertRate() {
	if !p.DMLLimitEnabled {
		p.DMLMinInsertRate = defaultMin
		return
	}
	p.DMLMinInsertRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.insertRate.min", defaultMin)
	p.DMLMinInsertRate = megaBytesRate2Bytes(p.DMLMinInsertRate)
	// [0, inf)
	if p.DMLMinInsertRate < 0 {
		p.DMLMinInsertRate = defaultMin
	}
	if !p.checkMinMaxLegal(p.DMLMinInsertRate, p.DMLMaxInsertRate) {
		p.DMLMinInsertRate = defaultMin
		p.DMLMaxInsertRate = defaultMax
	}
}

func (p *quotaConfig) initDMLMaxDeleteRate() {
	if !p.DMLLimitEnabled {
		p.DMLMaxDeleteRate = defaultMax
		return
	}
	p.DMLMaxDeleteRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.deleteRate.max", defaultMax)
	if math.Abs(p.DMLMaxDeleteRate-defaultMax) > 0.001 { // maxRate != defaultMax
		p.DMLMaxDeleteRate = megaBytesRate2Bytes(p.DMLMaxDeleteRate)
	}
	// [0, inf)
	if p.DMLMaxDeleteRate < 0 {
		p.DMLMaxDeleteRate = defaultMax
	}
}

func (p *quotaConfig) initDMLMinDeleteRate() {
	if !p.DMLLimitEnabled {
		p.DMLMinDeleteRate = defaultMin
		return
	}
	p.DMLMinDeleteRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.deleteRate.min", defaultMin)
	p.DMLMinDeleteRate = megaBytesRate2Bytes(p.DMLMinDeleteRate)
	// [0, inf)
	if p.DMLMinDeleteRate < 0 {
		p.DMLMinDeleteRate = defaultMin
	}
	if !p.checkMinMaxLegal(p.DMLMinDeleteRate, p.DMLMaxDeleteRate) {
		p.DMLMinDeleteRate = defaultMin
		p.DMLMaxDeleteRate = defaultMax
	}
}

func (p *quotaConfig) initDMLMaxBulkLoadRate() {
	if !p.DMLLimitEnabled {
		p.DMLMaxBulkLoadRate = defaultMax
		return
	}
	p.DMLMaxBulkLoadRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.bulkLoadRate.max", defaultMax)
	if math.Abs(p.DMLMaxBulkLoadRate-defaultMax) > 0.001 { // maxRate != defaultMax
		p.DMLMaxBulkLoadRate = megaBytesRate2Bytes(p.DMLMaxBulkLoadRate)
	}
	// [0, inf)
	if p.DMLMaxBulkLoadRate < 0 {
		p.DMLMaxBulkLoadRate = defaultMax
	}
}

func (p *quotaConfig) initDMLMinBulkLoadRate() {
	if !p.DMLLimitEnabled {
		p.DMLMinBulkLoadRate = defaultMin
		return
	}
	p.DMLMinBulkLoadRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.bulkLoadRate.min", defaultMin)
	p.DMLMinBulkLoadRate = megaBytesRate2Bytes(p.DMLMinBulkLoadRate)
	// [0, inf)
	if p.DMLMinBulkLoadRate < 0 {
		p.DMLMinBulkLoadRate = defaultMin
	}
	if !p.checkMinMaxLegal(p.DMLMinBulkLoadRate, p.DMLMaxBulkLoadRate) {
		p.DMLMinBulkLoadRate = defaultMin
		p.DMLMaxBulkLoadRate = defaultMax
	}
}

func (p *quotaConfig) initDQLLimitEnabled() {
	p.DQLLimitEnabled = p.Base.ParseBool("quotaAndLimits.dql.enabled", false)
}

func (p *quotaConfig) initDQLMaxSearchRate() {
	if !p.DQLLimitEnabled {
		p.DQLMaxSearchRate = defaultMax
		return
	}
	p.DQLMaxSearchRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.searchRate.max", defaultMax)
	// [0, inf)
	if p.DQLMaxSearchRate < 0 {
		p.DQLMaxSearchRate = defaultMax
	}
}

func (p *quotaConfig) initDQLMinSearchRate() {
	if !p.DQLLimitEnabled {
		p.DQLMinSearchRate = defaultMin
		return
	}
	p.DQLMinSearchRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.searchRate.min", defaultMin)
	// [0, inf)
	if p.DQLMinSearchRate < 0 {
		p.DQLMinSearchRate = defaultMin
	}
	if !p.checkMinMaxLegal(p.DQLMinSearchRate, p.DQLMaxSearchRate) {
		p.DQLMinSearchRate = defaultMax
		p.DQLMaxSearchRate = defaultMax
	}
}

func (p *quotaConfig) initDQLMaxQueryRate() {
	if !p.DQLLimitEnabled {
		p.DQLMaxQueryRate = defaultMax
		return
	}
	p.DQLMaxQueryRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.queryRate.max", defaultMax)
	// [0, inf)
	if p.DQLMaxQueryRate < 0 {
		p.DQLMaxQueryRate = defaultMax
	}
}

func (p *quotaConfig) initDQLMinQueryRate() {
	if !p.DQLLimitEnabled {
		p.DQLMinQueryRate = defaultMin
		return
	}
	p.DQLMinQueryRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.queryRate.min", defaultMin)
	// [0, inf)
	if p.DQLMinQueryRate < 0 {
		p.DQLMinQueryRate = defaultMin
	}
	if !p.checkMinMaxLegal(p.DQLMinQueryRate, p.DQLMaxQueryRate) {
		p.DQLMinQueryRate = defaultMin
		p.DQLMaxQueryRate = defaultMax
	}
}

func (p *quotaConfig) initMaxCollectionNum() {
	p.MaxCollectionNum = p.Base.ParseIntWithDefault("quotaAndLimits.limits.collection.maxNum", 64)
}

func (p *quotaConfig) initForceDenyWriting() {
	p.ForceDenyWriting = p.Base.ParseBool("quotaAndLimits.limitWriting.forceDeny", false)
}

func (p *quotaConfig) initTtProtectionEnabled() {
	p.TtProtectionEnabled = p.Base.ParseBool("quotaAndLimits.limitWriting.ttProtection", true)
}

func (p *quotaConfig) initMaxTimeTickDelay() {
	if !p.TtProtectionEnabled {
		return
	}
	const defaultMaxTtDelay = 30.0
	delay := p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.ttProtection.maxTimeTickDelay", defaultMaxTtDelay)
	// (0, 65536)
	if delay <= 0 || delay >= 65536 {
		delay = defaultMaxTtDelay
	}
	p.MaxTimeTickDelay = time.Duration(delay * float64(time.Second))
}

func (p *quotaConfig) initMemProtectionEnabled() {
	p.MemProtectionEnabled = p.Base.ParseBool("quotaAndLimits.limitWriting.memProtection.enabled", true)
}

func (p *quotaConfig) initDataNodeMemoryLowWaterLevel() {
	if !p.MemProtectionEnabled {
		return
	}
	p.DataNodeMemoryLowWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.memProtection.dataNodeMemoryLowWaterLevel", defaultLowWaterLevel)
	// (0, 1]
	if p.DataNodeMemoryLowWaterLevel <= 0 || p.DataNodeMemoryLowWaterLevel > 1 {
		log.Warn("MemoryLowWaterLevel must in the range of `(0, 1]`, use default value", zap.Float64("low", p.DataNodeMemoryLowWaterLevel), zap.Float64("default", defaultLowWaterLevel))
		p.DataNodeMemoryLowWaterLevel = defaultLowWaterLevel
	}
}

func (p *quotaConfig) initDataNodeMemoryHighWaterLevel() {
	if !p.MemProtectionEnabled {
		return
	}
	p.DataNodeMemoryHighWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.memProtection.dataNodeMemoryHighWaterLevel", defaultHighWaterLevel)
	// (0, 1]
	if p.DataNodeMemoryHighWaterLevel <= 0 || p.DataNodeMemoryHighWaterLevel > 1 {
		log.Warn("MemoryLowWaterLevel must in the range of `(0, 1]`, use default value", zap.Float64("low", p.DataNodeMemoryHighWaterLevel), zap.Float64("default", defaultHighWaterLevel))
		p.DataNodeMemoryHighWaterLevel = defaultHighWaterLevel
	}
	if !p.checkMinMaxLegal(p.DataNodeMemoryLowWaterLevel, p.DataNodeMemoryHighWaterLevel) {
		p.DataNodeMemoryHighWaterLevel = defaultHighWaterLevel
		p.DataNodeMemoryLowWaterLevel = defaultLowWaterLevel
	}
}

func (p *quotaConfig) initQueryNodeMemoryLowWaterLevel() {
	if !p.MemProtectionEnabled {
		return
	}
	p.QueryNodeMemoryLowWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.memProtection.queryNodeMemoryLowWaterLevel", defaultLowWaterLevel)
	// (0, 1]
	if p.QueryNodeMemoryLowWaterLevel <= 0 || p.QueryNodeMemoryLowWaterLevel > 1 {
		log.Warn("MemoryLowWaterLevel must in the range of `(0, 1]`, use default value", zap.Float64("low", p.QueryNodeMemoryLowWaterLevel), zap.Float64("default", defaultLowWaterLevel))
		p.QueryNodeMemoryLowWaterLevel = defaultLowWaterLevel
	}
}

func (p *quotaConfig) initQueryNodeMemoryHighWaterLevel() {
	if !p.MemProtectionEnabled {
		return
	}
	p.QueryNodeMemoryHighWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.memProtection.queryNodeMemoryHighWaterLevel", defaultHighWaterLevel)
	// (0, 1]
	if p.QueryNodeMemoryHighWaterLevel <= 0 || p.QueryNodeMemoryHighWaterLevel > 1 {
		log.Warn("MemoryLowWaterLevel must in the range of `(0, 1]`, use default value", zap.Float64("low", p.QueryNodeMemoryHighWaterLevel), zap.Float64("default", defaultHighWaterLevel))
		p.QueryNodeMemoryHighWaterLevel = defaultHighWaterLevel
	}
	if !p.checkMinMaxLegal(p.QueryNodeMemoryLowWaterLevel, p.QueryNodeMemoryHighWaterLevel) {
		p.QueryNodeMemoryHighWaterLevel = defaultHighWaterLevel
		p.QueryNodeMemoryLowWaterLevel = defaultLowWaterLevel
	}
}

func (p *quotaConfig) initDiskProtectionEnabled() {
	p.DiskProtectionEnabled = p.Base.ParseBool("quotaAndLimits.limitWriting.diskProtection.enabled", true)
}

func (p *quotaConfig) initDiskQuota() {
	if !p.DiskProtectionEnabled {
		p.DiskQuota = defaultMax
		return
	}
	p.DiskQuota = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.diskProtection.diskQuota", defaultDiskQuotaInGB)
	// (0, +inf)
	if p.DiskQuota <= 0 {
		log.Warn("DiskQuota must in the range of `(0, +inf)`, use default +inf", zap.Float64("DiskQuota", p.DiskQuota))
		p.DiskQuota = defaultDiskQuotaInGB
	}
	// gigabytes to bytes
	p.DiskQuota = p.DiskQuota * GB
}

func (p *quotaConfig) initForceDenyReading() {
	p.ForceDenyReading = p.Base.ParseBool("quotaAndLimits.limitReading.forceDeny", false)
}

func (p *quotaConfig) initQueueProtectionEnabled() {
	p.QueueProtectionEnabled = p.Base.ParseBool("quotaAndLimits.limitReading.queueProtection.enabled", false)
}

func (p *quotaConfig) initNQInQueueThreshold() {
	if !p.QueueProtectionEnabled {
		return
	}
	p.NQInQueueThreshold = p.Base.ParseInt64WithDefault("quotaAndLimits.limitReading.queueProtection.nqInQueueThreshold", math.MaxInt64)
	// [0, inf)
	if p.NQInQueueThreshold < 0 {
		p.NQInQueueThreshold = math.MaxInt64
	}
}

func (p *quotaConfig) initQueueLatencyThreshold() {
	if !p.QueueProtectionEnabled {
		return
	}
	p.QueueLatencyThreshold = p.Base.ParseFloatWithDefault("quotaAndLimits.limitReading.queueProtection.queueLatencyThreshold", defaultMax)
	// [0, inf)
	if p.QueueLatencyThreshold < 0 {
		p.QueueLatencyThreshold = defaultMax
	}
}

func (p *quotaConfig) initCoolOffSpeed() {
	const defaultSpeed = 0.9
	p.CoolOffSpeed = defaultSpeed
	if !p.QueueProtectionEnabled {
		return
	}
	p.CoolOffSpeed = p.Base.ParseFloatWithDefault("quotaAndLimits.limitReading.queueProtection.coolOffSpeed", defaultSpeed)
	// (0, 1]
	if p.CoolOffSpeed <= 0 || p.CoolOffSpeed > 1 {
		log.Warn("CoolOffSpeed must in the range of `(0, 1]`, use default value", zap.Float64("speed", p.CoolOffSpeed), zap.Float64("default", defaultSpeed))
		p.CoolOffSpeed = defaultSpeed
	}
}
