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
	"fmt"
	"math"
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

const (
	// defaultMax is the default unlimited rate or threshold.
	defaultMax = float64(math.MaxFloat64)
	// MBSize used to convert megabytes and bytes.
	MBSize = 1024.0 * 1024.0
	// defaultDiskQuotaInMB is the default disk quota in megabytes.
	defaultDiskQuotaInMB = defaultMax / MBSize
	// defaultMin is the default minimal rate.
	defaultMin = float64(0)
	// defaultLowWaterLevel is the default memory low water level.
	defaultLowWaterLevel = float64(0.85)
	// defaultHighWaterLevel is the default memory low water level.
	defaultHighWaterLevel = float64(0.95)
)

// quotaConfig is configuration for quota and limitations.
type quotaConfig struct {
	QuotaAndLimitsEnabled      ParamItem `refreshable:"false"`
	QuotaCenterCollectInterval ParamItem `refreshable:"false"`

	// ddl
	DDLLimitEnabled   ParamItem `refreshable:"true"`
	DDLCollectionRate ParamItem `refreshable:"true"`
	DDLPartitionRate  ParamItem `refreshable:"true"`

	IndexLimitEnabled ParamItem `refreshable:"true"`
	MaxIndexRate      ParamItem `refreshable:"true"`

	FlushLimitEnabled ParamItem `refreshable:"true"`
	MaxFlushRate      ParamItem `refreshable:"true"`

	CompactionLimitEnabled ParamItem `refreshable:"true"`
	MaxCompactionRate      ParamItem `refreshable:"true"`

	// dml
	DMLLimitEnabled    ParamItem `refreshable:"true"`
	DMLMaxInsertRate   ParamItem `refreshable:"true"`
	DMLMinInsertRate   ParamItem `refreshable:"true"`
	DMLMaxDeleteRate   ParamItem `refreshable:"true"`
	DMLMinDeleteRate   ParamItem `refreshable:"true"`
	DMLMaxBulkLoadRate ParamItem `refreshable:"true"`
	DMLMinBulkLoadRate ParamItem `refreshable:"true"`

	// dql
	DQLLimitEnabled  ParamItem `refreshable:"true"`
	DQLMaxSearchRate ParamItem `refreshable:"true"`
	DQLMinSearchRate ParamItem `refreshable:"true"`
	DQLMaxQueryRate  ParamItem `refreshable:"true"`
	DQLMinQueryRate  ParamItem `refreshable:"true"`

	// limits
	MaxCollectionNum ParamItem `refreshable:"true"`

	// limit writing
	ForceDenyWriting              ParamItem `refreshable:"true"`
	TtProtectionEnabled           ParamItem `refreshable:"true"`
	MaxTimeTickDelay              ParamItem `refreshable:"true"`
	MemProtectionEnabled          ParamItem `refreshable:"true"`
	DataNodeMemoryLowWaterLevel   ParamItem `refreshable:"true"`
	DataNodeMemoryHighWaterLevel  ParamItem `refreshable:"true"`
	QueryNodeMemoryLowWaterLevel  ParamItem `refreshable:"true"`
	QueryNodeMemoryHighWaterLevel ParamItem `refreshable:"true"`
	DiskProtectionEnabled         ParamItem `refreshable:"true"`
	DiskQuota                     ParamItem `refreshable:"true"`

	// limit reading
	ForceDenyReading        ParamItem `refreshable:"true"`
	QueueProtectionEnabled  ParamItem `refreshable:"true"`
	NQInQueueThreshold      ParamItem `refreshable:"true"`
	QueueLatencyThreshold   ParamItem `refreshable:"true"`
	ResultProtectionEnabled ParamItem `refreshable:"true"`
	MaxReadResultRate       ParamItem `refreshable:"true"`
	CoolOffSpeed            ParamItem `refreshable:"true"`
}

func (p *quotaConfig) init(base *BaseTable) {
	p.QuotaAndLimitsEnabled = ParamItem{
		Key:          "quotaAndLimits.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.QuotaAndLimitsEnabled.Init(base.mgr)

	const defaultInterval = "3.0"
	p.QuotaCenterCollectInterval = ParamItem{
		Key:          "quotaAndLimits.quotaCenterCollectInterval",
		Version:      "2.2.0",
		DefaultValue: defaultInterval,
		Formatter: func(v string) string {
			// (0 ~ 65536)
			if getAsInt(v) <= 0 || getAsInt(v) >= 65536 {
				return defaultInterval
			}
			return v
		},
	}
	p.QuotaCenterCollectInterval.Init(base.mgr)

	// ddl
	max := fmt.Sprintf("%f", defaultMax)
	min := fmt.Sprintf("%f", defaultMin)
	p.DDLLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.ddl.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.DDLLimitEnabled.Init(base.mgr)

	p.DDLCollectionRate = ParamItem{
		Key:          "quotaAndLimits.ddl.collectionRate",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DDLLimitEnabled.GetAsBool() {
				return max
			}
			// [0 ~ Inf)
			if getAsInt(v) < 0 {
				return max
			}
			return v
		},
	}
	p.DDLCollectionRate.Init(base.mgr)

	p.DDLPartitionRate = ParamItem{
		Key:          "quotaAndLimits.ddl.partitionRate",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DDLLimitEnabled.GetAsBool() {
				return max
			}
			// [0 ~ Inf)
			if getAsInt(v) < 0 {
				return max
			}
			return v
		},
	}
	p.DDLPartitionRate.Init(base.mgr)

	p.IndexLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.indexRate.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.IndexLimitEnabled.Init(base.mgr)

	p.MaxIndexRate = ParamItem{
		Key:          "quotaAndLimits.indexRate.max",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.IndexLimitEnabled.GetAsBool() {
				return max
			}
			// [0 ~ Inf)
			if getAsFloat(v) < 0 {
				return max
			}
			return v
		},
	}
	p.MaxIndexRate.Init(base.mgr)

	p.FlushLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.flushRate.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.FlushLimitEnabled.Init(base.mgr)

	p.MaxFlushRate = ParamItem{
		Key:          "quotaAndLimits.flushRate.max",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.FlushLimitEnabled.GetAsBool() {
				return max
			}
			// [0 ~ Inf)
			if getAsInt(v) < 0 {
				return max
			}
			return v
		},
	}
	p.MaxFlushRate.Init(base.mgr)

	p.CompactionLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.compactionRate.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.CompactionLimitEnabled.Init(base.mgr)

	p.MaxCompactionRate = ParamItem{
		Key:          "quotaAndLimits.compactionRate.max",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.CompactionLimitEnabled.GetAsBool() {
				return max
			}
			// [0 ~ Inf)
			if getAsInt(v) < 0 {
				return max
			}
			return v
		},
	}
	p.MaxCompactionRate.Init(base.mgr)

	// dml
	p.DMLLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.dml.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.DMLLimitEnabled.Init(base.mgr)

	p.DMLMaxInsertRate = ParamItem{
		Key:          "quotaAndLimits.dml.insertRate.max",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return max
			}
			if math.Abs(getAsFloat(v)-defaultMax) > 0.001 { // maxRate != defaultMax
				return fmt.Sprintf("%f", megaBytes2Bytes(getAsFloat(v)))
			}
			// [0, inf)
			if getAsInt(v) < 0 {
				return max
			}
			return v
		},
	}
	p.DMLMaxInsertRate.Init(base.mgr)

	p.DMLMinInsertRate = ParamItem{
		Key:          "quotaAndLimits.dml.insertRate.min",
		Version:      "2.2.0",
		DefaultValue: min,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return min
			}
			rate := megaBytes2Bytes(getAsFloat(v))
			// [0, inf)
			if rate < 0 {
				return min
			}
			if !p.checkMinMaxLegal(rate, getAsFloat(v)) {
				return min
			}
			return v
		},
	}
	p.DMLMinInsertRate.Init(base.mgr)

	p.DMLMaxDeleteRate = ParamItem{
		Key:          "quotaAndLimits.dml.deleteRate.max",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return max
			}
			rate := getAsFloat(v)
			if math.Abs(rate-defaultMax) > 0.001 { // maxRate != defaultMax
				return fmt.Sprintf("%f", megaBytes2Bytes(rate))
			}
			// [0, inf)
			if rate < 0 {
				return max
			}
			return v
		},
	}
	p.DMLMaxDeleteRate.Init(base.mgr)

	p.DMLMinDeleteRate = ParamItem{
		Key:          "quotaAndLimits.dml.deleteRate.min",
		Version:      "2.2.0",
		DefaultValue: min,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return min
			}
			rate := megaBytes2Bytes(getAsFloat(v))
			// [0, inf)
			if rate < 0 {
				return min
			}
			if !p.checkMinMaxLegal(rate, getAsFloat(v)) {
				return min
			}
			return v
		},
	}
	p.DMLMinDeleteRate.Init(base.mgr)

	p.DMLMaxBulkLoadRate = ParamItem{
		Key:          "quotaAndLimits.dml.bulkLoadRate.max",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return max
			}
			rate := getAsFloat(v)
			if math.Abs(rate-defaultMax) > 0.001 { // maxRate != defaultMax
				return fmt.Sprintf("%f", megaBytes2Bytes(rate))
			}
			// [0, inf)
			if rate < 0 {
				return max
			}
			return v
		},
	}
	p.DMLMaxBulkLoadRate.Init(base.mgr)

	p.DMLMinBulkLoadRate = ParamItem{
		Key:          "quotaAndLimits.dml.bulkLoadRate.min",
		Version:      "2.2.0",
		DefaultValue: min,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return min
			}
			rate := megaBytes2Bytes(getAsFloat(v))
			// [0, inf)
			if rate < 0 {
				return min
			}
			if !p.checkMinMaxLegal(rate, getAsFloat(v)) {
				return min
			}
			return v
		},
	}
	p.DMLMinBulkLoadRate.Init(base.mgr)

	// dql
	p.DQLLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.dql.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.DQLLimitEnabled.Init(base.mgr)

	p.DQLMaxSearchRate = ParamItem{
		Key:          "quotaAndLimits.dql.searchRate.max",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DQLLimitEnabled.GetAsBool() {
				return max
			}
			// [0, inf)
			if getAsFloat(v) < 0 {
				return max
			}
			return v
		},
	}
	p.DQLMaxSearchRate.Init(base.mgr)

	p.DQLMinSearchRate = ParamItem{
		Key:          "quotaAndLimits.dql.searchRate.min",
		Version:      "2.2.0",
		DefaultValue: min,
		Formatter: func(v string) string {
			if !p.DQLLimitEnabled.GetAsBool() {
				return min
			}
			rate := getAsFloat(v)
			// [0, inf)
			if rate < 0 {
				return min
			}
			if !p.checkMinMaxLegal(rate, getAsFloat(v)) {
				return min
			}
			return v
		},
	}
	p.DQLMinSearchRate.Init(base.mgr)

	p.DQLMaxQueryRate = ParamItem{
		Key:          "quotaAndLimits.dql.queryRate.max",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DQLLimitEnabled.GetAsBool() {
				return max
			}
			// [0, inf)
			if getAsFloat(v) < 0 {
				return max
			}
			return v
		},
	}
	p.DQLMaxQueryRate.Init(base.mgr)

	p.DQLMinQueryRate = ParamItem{
		Key:          "quotaAndLimits.dql.queryRate.min",
		Version:      "2.2.0",
		DefaultValue: min,
		Formatter: func(v string) string {
			if !p.DQLLimitEnabled.GetAsBool() {
				return min
			}
			rate := getAsFloat(v)
			// [0, inf)
			if rate < 0 {
				return min
			}
			if !p.checkMinMaxLegal(rate, getAsFloat(v)) {
				return min
			}
			return v
		},
	}
	p.DQLMinQueryRate.Init(base.mgr)

	// limits
	p.MaxCollectionNum = ParamItem{
		Key:          "quotaAndLimits.limits.collection.maxNum",
		Version:      "2.2.0",
		DefaultValue: "64",
	}
	p.MaxCollectionNum.Init(base.mgr)

	// limit writing
	p.ForceDenyWriting = ParamItem{
		Key:          "quotaAndLimits.limitWriting.forceDeny",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.ForceDenyWriting.Init(base.mgr)

	p.TtProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitWriting.ttProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "true",
	}
	p.TtProtectionEnabled.Init(base.mgr)

	const defaultMaxTtDelay = "300.0"
	p.MaxTimeTickDelay = ParamItem{
		Key:          "quotaAndLimits.limitWriting.ttProtection.maxTimeTickDelay",
		Version:      "2.2.0",
		DefaultValue: defaultMaxTtDelay,
		Formatter: func(v string) string {
			if !p.TtProtectionEnabled.GetAsBool() {
				return fmt.Sprintf("%d", math.MaxInt64)
			}
			delay := getAsFloat(v)
			// (0, 65536)
			if delay <= 0 || delay >= 65536 {
				return defaultMaxTtDelay
			}
			return fmt.Sprintf("%f", delay)
		},
	}
	p.MaxTimeTickDelay.Init(base.mgr)

	p.MemProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitWriting.memProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "true",
	}
	p.MemProtectionEnabled.Init(base.mgr)

	lowWaterLevel := fmt.Sprintf("%f", defaultLowWaterLevel)
	highWaterLevel := fmt.Sprintf("%f", defaultHighWaterLevel)

	p.DataNodeMemoryLowWaterLevel = ParamItem{
		Key:          "quotaAndLimits.limitWriting.memProtection.dataNodeMemoryLowWaterLevel",
		Version:      "2.2.0",
		DefaultValue: lowWaterLevel,
		Formatter: func(v string) string {
			if !p.MemProtectionEnabled.GetAsBool() {
				return lowWaterLevel
			}
			level := getAsFloat(v)
			// (0, 1]
			if level <= 0 || level > 1 {
				return lowWaterLevel
			}
			return v
		},
	}
	p.DataNodeMemoryLowWaterLevel.Init(base.mgr)

	p.DataNodeMemoryHighWaterLevel = ParamItem{
		Key:          "quotaAndLimits.limitWriting.memProtection.dataNodeMemoryHighWaterLevel",
		Version:      "2.2.0",
		DefaultValue: highWaterLevel,
		Formatter: func(v string) string {
			if !p.MemProtectionEnabled.GetAsBool() {
				return "1"
			}
			level := getAsFloat(v)
			// (0, 1]
			if level <= 0 || level > 1 {
				// log.Warn("MemoryLowWaterLevel must in the range of `(0, 1]`, use default value", zap.Float64("low", p.DataNodeMemoryHighWaterLevel), zap.Float64("default", defaultHighWaterLevel))
				return highWaterLevel
			}
			if !p.checkMinMaxLegal(p.DataNodeMemoryLowWaterLevel.GetAsFloat(), getAsFloat(v)) {
				return highWaterLevel
			}
			return v
		},
	}
	p.DataNodeMemoryHighWaterLevel.Init(base.mgr)

	p.QueryNodeMemoryLowWaterLevel = ParamItem{
		Key:          "quotaAndLimits.limitWriting.memProtection.queryNodeMemoryLowWaterLevel",
		Version:      "2.2.0",
		DefaultValue: lowWaterLevel,
		Formatter: func(v string) string {
			if !p.MemProtectionEnabled.GetAsBool() {
				return lowWaterLevel
			}
			level := getAsFloat(v)
			// (0, 1]
			if level <= 0 || level > 1 {
				// log.Warn("MemoryLowWaterLevel must in the range of `(0, 1]`, use default value", zap.Float64("low", p.QueryNodeMemoryLowWaterLevel), zap.Float64("default", defaultLowWaterLevel))
				return lowWaterLevel
			}
			return v
		},
	}
	p.QueryNodeMemoryLowWaterLevel.Init(base.mgr)

	p.QueryNodeMemoryHighWaterLevel = ParamItem{
		Key:          "quotaAndLimits.limitWriting.memProtection.queryNodeMemoryHighWaterLevel",
		Version:      "2.2.0",
		DefaultValue: highWaterLevel,
		Formatter: func(v string) string {
			if !p.MemProtectionEnabled.GetAsBool() {
				return highWaterLevel
			}
			level := getAsFloat(v)
			// (0, 1]
			if level <= 0 || level > 1 {
				// log.Warn("MemoryLowWaterLevel must in the range of `(0, 1]`, use default value", zap.Float64("low", p.QueryNodeMemoryHighWaterLevel), zap.Float64("default", defaultHighWaterLevel))
				return highWaterLevel
			}
			if !p.checkMinMaxLegal(p.QueryNodeMemoryLowWaterLevel.GetAsFloat(), getAsFloat(v)) {
				return highWaterLevel
			}
			return v
		},
	}
	p.QueryNodeMemoryHighWaterLevel.Init(base.mgr)

	p.DiskProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitWriting.diskProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "true",
	}
	p.DiskProtectionEnabled.Init(base.mgr)

	quota := fmt.Sprintf("%f", defaultDiskQuotaInMB)
	p.DiskQuota = ParamItem{
		Key:          "quotaAndLimits.limitWriting.diskProtection.diskQuota",
		Version:      "2.2.0",
		DefaultValue: quota,
		Formatter: func(v string) string {
			if !p.DiskProtectionEnabled.GetAsBool() {
				return max
			}
			level := getAsFloat(v)
			// (0, +inf)
			if level <= 0 {
				level = getAsFloat(quota)
			}
			// megabytes to bytes
			return fmt.Sprintf("%f", megaBytes2Bytes(level))
		},
	}
	p.DiskQuota.Init(base.mgr)

	// limit reading
	p.ForceDenyReading = ParamItem{
		Key:          "quotaAndLimits.limitReading.forceDeny",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.ForceDenyReading.Init(base.mgr)

	p.QueueProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitReading.queueProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.QueueProtectionEnabled.Init(base.mgr)

	p.NQInQueueThreshold = ParamItem{
		Key:          "quotaAndLimits.limitReading.queueProtection.nqInQueueThreshold",
		Version:      "2.2.0",
		DefaultValue: strconv.FormatInt(math.MaxInt64, 10),
		Formatter: func(v string) string {
			if !p.QueueProtectionEnabled.GetAsBool() {
				return strconv.FormatInt(math.MaxInt64, 10)
			}
			threshold := getAsFloat(v)
			// [0, inf)
			if threshold < 0 {
				return strconv.FormatInt(math.MaxInt64, 10)
			}
			return v
		},
	}
	p.NQInQueueThreshold.Init(base.mgr)

	p.QueueLatencyThreshold = ParamItem{
		Key:          "quotaAndLimits.limitReading.queueProtection.queueLatencyThreshold",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.QueueProtectionEnabled.GetAsBool() {
				return max
			}
			level := getAsFloat(v)
			// [0, inf)
			if level < 0 {
				return max
			}
			return v
		},
	}
	p.QueueLatencyThreshold.Init(base.mgr)

	p.ResultProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitReading.resultProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.ResultProtectionEnabled.Init(base.mgr)

	p.MaxReadResultRate = ParamItem{
		Key:          "quotaAndLimits.limitReading.resultProtection.maxReadResultRate",
		Version:      "2.2.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.ResultProtectionEnabled.GetAsBool() {
				return max
			}
			rate := getAsFloat(v)
			if math.Abs(rate-defaultMax) > 0.001 { // maxRate != defaultMax
				return fmt.Sprintf("%f", megaBytes2Bytes(rate))
			}
			// [0, inf)
			if rate < 0 {
				return max
			}
			return v
		},
	}
	p.MaxReadResultRate.Init(base.mgr)

	const defaultSpeed = "0.9"
	p.CoolOffSpeed = ParamItem{
		Key:          "quotaAndLimits.limitReading.coolOffSpeed",
		Version:      "2.2.0",
		DefaultValue: defaultSpeed,
		Formatter: func(v string) string {
			// (0, 1]
			speed := getAsFloat(v)
			if speed <= 0 || speed > 1 {
				// log.Warn("CoolOffSpeed must in the range of `(0, 1]`, use default value", zap.Float64("speed", p.CoolOffSpeed), zap.Float64("default", defaultSpeed))
				return defaultSpeed
			}
			return v
		},
	}
	p.CoolOffSpeed.Init(base.mgr)

}

func megaBytes2Bytes(f float64) float64 {
	return f * MBSize
}

func (p *quotaConfig) checkMinMaxLegal(min, max float64) bool {
	if min > max {
		log.Warn("init QuotaConfig failed, max/high must be greater than or equal to min/low, use default values",
			zap.String("msg", fmt.Sprintf("min: %v, max: %v, defaultMin: %v, defaultMax: %v", min, max, defaultMin, defaultMax)))
		return false
	}
	return true
}
