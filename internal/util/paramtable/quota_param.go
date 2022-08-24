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
	// defaultMax is the default minimal rate.
	defaultMin = float64(0)
	// defaultLowWaterLevel is the default memory low water level.
	defaultLowWaterLevel = float64(0.8)
	// defaultHighWaterLevel is the default memory low water level.
	defaultHighWaterLevel = float64(0.9)
)

// quotaConfig is configuration for quota and limitations.
type quotaConfig struct {
	Base *BaseTable
	once sync.Once

	EnableQuotaAndLimits bool

	QuotaCenterCollectInterval float64

	// ddl
	DDLCollectionRate float64
	DDLPartitionRate  float64
	DDLIndexRate      float64
	DDLFlushRate      float64
	DDLCompactionRate float64

	// dml
	DMLMaxInsertRate   float64
	DMLMinInsertRate   float64
	DMLMaxDeleteRate   float64
	DMLMinDeleteRate   float64
	DMLMaxBulkLoadRate float64
	DMLMinBulkLoadRate float64

	// dql
	DQLMaxSearchRate float64
	DQLMinSearchRate float64
	DQLMaxQueryRate  float64
	DQLMinQueryRate  float64

	// limits
	MaxCollectionNum int

	ForceDenyWriting              bool
	MaxTimeTickDelay              time.Duration
	DataNodeMemoryLowWaterLevel   float64
	DataNodeMemoryHighWaterLevel  float64
	QueryNodeMemoryLowWaterLevel  float64
	QueryNodeMemoryHighWaterLevel float64

	ForceDenyReading      bool
	NQInQueueThreshold    int64
	QueueLatencyThreshold float64
	CoolOffSpeed          float64
}

func (p *quotaConfig) init(base *BaseTable) {
	p.Base = base

	p.initEnableQuotaAndLimits()
	p.initQuotaCenterCollectInterval()

	p.initDDLCollectionRate()
	p.initDDLPartitionRate()
	p.initDDLIndexRate()
	p.initDDLFlushRate()
	p.initDDLCompactionRate()

	p.initDMLMaxInsertRate()
	p.initDMLMinInsertRate()
	p.initDMLMaxDeleteRate()
	p.initDMLMinDeleteRate()
	p.initDMLMaxBulkLoadRate()
	p.initDMLMinBulkLoadRate()

	p.initDQLMaxSearchRate()
	p.initDQLMinSearchRate()
	p.initDQLMaxQueryRate()
	p.initDQLMinQueryRate()

	p.initMaxCollectionNum()

	p.initForceDenyWriting()
	p.initMaxTimeTickDelay()
	p.initDataNodeMemoryLowWaterLevel()
	p.initDataNodeMemoryHighWaterLevel()
	p.initQueryNodeMemoryLowWaterLevel()
	p.initQueryNodeMemoryHighWaterLevel()

	p.initForceDenyReading()
	p.initNQInQueueThreshold()
	p.initQueueLatencyThreshold()
	p.initCoolOffSpeed()
}

func (p *quotaConfig) initEnableQuotaAndLimits() {
	p.EnableQuotaAndLimits = p.Base.ParseBool("quotaAndLimits.enable", false)
}

func (p *quotaConfig) initQuotaCenterCollectInterval() {
	const defaultInterval = 3.0
	p.QuotaCenterCollectInterval = p.Base.ParseFloatWithDefault("quotaAndLimits.quotaCenterCollectInterval", defaultInterval)
	// (0 ~ 65536)
	if p.QuotaCenterCollectInterval <= 0 || p.QuotaCenterCollectInterval >= 65536 {
		p.QuotaCenterCollectInterval = defaultInterval
	}
}

func (p *quotaConfig) initDDLCollectionRate() {
	p.DDLCollectionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.collectionRate", defaultMax)
	// [0 ~ Inf)
	if p.DDLCollectionRate < 0 {
		p.DDLCollectionRate = defaultMax
	}
}

func (p *quotaConfig) initDDLPartitionRate() {
	p.DDLPartitionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.partitionRate", defaultMax)
	// [0 ~ Inf)
	if p.DDLPartitionRate < 0 {
		p.DDLPartitionRate = defaultMax
	}
}

func (p *quotaConfig) initDDLIndexRate() {
	p.DDLIndexRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.indexRate", defaultMax)
	// [0 ~ Inf)
	if p.DDLIndexRate < 0 {
		p.DDLIndexRate = defaultMax
	}
}

func (p *quotaConfig) initDDLFlushRate() {
	p.DDLFlushRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.flushRate", defaultMax)
	// [0 ~ Inf)
	if p.DDLFlushRate < 0 {
		p.DDLFlushRate = defaultMax
	}
}

func (p *quotaConfig) initDDLCompactionRate() {
	p.DDLCompactionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.compactionRate", defaultMax)
	// [0 ~ Inf)
	if p.DDLCompactionRate < 0 {
		p.DDLCompactionRate = defaultMax
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

func (p *quotaConfig) initDMLMaxInsertRate() {
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

func (p *quotaConfig) initDQLMaxSearchRate() {
	p.DQLMaxSearchRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.searchRate.max", defaultMax)
	// [0, inf)
	if p.DQLMaxSearchRate < 0 {
		p.DQLMaxSearchRate = defaultMax
	}
}

func (p *quotaConfig) initDQLMinSearchRate() {
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
	p.DQLMaxQueryRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.queryRate.max", defaultMax)
	// [0, inf)
	if p.DQLMaxQueryRate < 0 {
		p.DQLMaxQueryRate = defaultMax
	}
}

func (p *quotaConfig) initDQLMinQueryRate() {
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

func (p *quotaConfig) initMaxTimeTickDelay() {
	const defaultMaxTtDelay = 30.0
	delay := p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.maxTimeTickDelay", defaultMaxTtDelay)
	// (0, 65536)
	if delay <= 0 || delay >= 65536 {
		delay = defaultMaxTtDelay
	}
	p.MaxTimeTickDelay = time.Duration(delay * float64(time.Second))
}

func (p *quotaConfig) initDataNodeMemoryLowWaterLevel() {
	p.DataNodeMemoryLowWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.dataNodeMemoryLowWaterLevel", defaultLowWaterLevel)
	// (0, 1]
	if p.DataNodeMemoryLowWaterLevel <= 0 || p.DataNodeMemoryLowWaterLevel > 1 {
		log.Warn("MemoryLowWaterLevel must in the range of `(0, 1]`, use default value", zap.Float64("low", p.DataNodeMemoryLowWaterLevel), zap.Float64("default", defaultLowWaterLevel))
		p.DataNodeMemoryLowWaterLevel = defaultLowWaterLevel
	}
}

func (p *quotaConfig) initDataNodeMemoryHighWaterLevel() {
	p.DataNodeMemoryHighWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.dataNodeMemoryHighWaterLevel", defaultHighWaterLevel)
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
	p.QueryNodeMemoryLowWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.queryNodeMemoryLowWaterLevel", defaultLowWaterLevel)
	// (0, 1]
	if p.QueryNodeMemoryLowWaterLevel <= 0 || p.QueryNodeMemoryLowWaterLevel > 1 {
		log.Warn("MemoryLowWaterLevel must in the range of `(0, 1]`, use default value", zap.Float64("low", p.QueryNodeMemoryLowWaterLevel), zap.Float64("default", defaultLowWaterLevel))
		p.QueryNodeMemoryLowWaterLevel = defaultLowWaterLevel
	}
}

func (p *quotaConfig) initQueryNodeMemoryHighWaterLevel() {
	p.QueryNodeMemoryHighWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.queryNodeMemoryHighWaterLevel", defaultHighWaterLevel)
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

func (p *quotaConfig) initForceDenyReading() {
	p.ForceDenyReading = p.Base.ParseBool("quotaAndLimits.limitReading.forceDeny", false)
}

func (p *quotaConfig) initNQInQueueThreshold() {
	p.NQInQueueThreshold = p.Base.ParseInt64WithDefault("quotaAndLimits.limitReading.NQInQueueThreshold", math.MaxInt64)
	// [0, inf)
	if p.NQInQueueThreshold < 0 {
		p.NQInQueueThreshold = math.MaxInt64
	}
}

func (p *quotaConfig) initQueueLatencyThreshold() {
	p.QueueLatencyThreshold = p.Base.ParseFloatWithDefault("quotaAndLimits.limitReading.queueLatencyThreshold", defaultMax)
	// [0, inf)
	if p.QueueLatencyThreshold < 0 {
		p.QueueLatencyThreshold = defaultMax
	}
}

func (p *quotaConfig) initCoolOffSpeed() {
	const defaultSpeed = 0.9
	p.CoolOffSpeed = p.Base.ParseFloatWithDefault("quotaAndLimits.limitReading.coolOffSpeed", defaultSpeed)
	// (0, 1]
	if p.CoolOffSpeed <= 0 || p.CoolOffSpeed > 1 {
		log.Warn("CoolOffSpeed must in the range of `(0, 1]`, use default value", zap.Float64("speed", p.CoolOffSpeed), zap.Float64("default", defaultSpeed))
		p.CoolOffSpeed = defaultSpeed
	}
}
