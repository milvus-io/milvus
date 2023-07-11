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

	"github.com/milvus-io/milvus/pkg/log"
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
	DMLLimitEnabled                 ParamItem `refreshable:"true"`
	DMLMaxInsertRate                ParamItem `refreshable:"true"`
	DMLMinInsertRate                ParamItem `refreshable:"true"`
	DMLMaxUpsertRate                ParamItem `refreshable:"true"`
	DMLMinUpsertRate                ParamItem `refreshable:"true"`
	DMLMaxDeleteRate                ParamItem `refreshable:"true"`
	DMLMinDeleteRate                ParamItem `refreshable:"true"`
	DMLMaxBulkLoadRate              ParamItem `refreshable:"true"`
	DMLMinBulkLoadRate              ParamItem `refreshable:"true"`
	DMLMaxInsertRatePerCollection   ParamItem `refreshable:"true"`
	DMLMinInsertRatePerCollection   ParamItem `refreshable:"true"`
	DMLMaxUpsertRatePerCollection   ParamItem `refreshable:"true"`
	DMLMinUpsertRatePerCollection   ParamItem `refreshable:"true"`
	DMLMaxDeleteRatePerCollection   ParamItem `refreshable:"true"`
	DMLMinDeleteRatePerCollection   ParamItem `refreshable:"true"`
	DMLMaxBulkLoadRatePerCollection ParamItem `refreshable:"true"`
	DMLMinBulkLoadRatePerCollection ParamItem `refreshable:"true"`

	// dql
	DQLLimitEnabled               ParamItem `refreshable:"true"`
	DQLMaxSearchRate              ParamItem `refreshable:"true"`
	DQLMinSearchRate              ParamItem `refreshable:"true"`
	DQLMaxQueryRate               ParamItem `refreshable:"true"`
	DQLMinQueryRate               ParamItem `refreshable:"true"`
	DQLMaxSearchRatePerCollection ParamItem `refreshable:"true"`
	DQLMinSearchRatePerCollection ParamItem `refreshable:"true"`
	DQLMaxQueryRatePerCollection  ParamItem `refreshable:"true"`
	DQLMinQueryRatePerCollection  ParamItem `refreshable:"true"`

	// limits
	MaxCollectionNum      ParamItem `refreshable:"true"`
	MaxCollectionNumPerDB ParamItem `refreshable:"true"`
	TopKLimit             ParamItem `refreshable:"true"`
	NQLimit               ParamItem `refreshable:"true"`
	MaxQueryResultWindow  ParamItem `refreshable:"true"`

	// limit writing
	ForceDenyWriting                     ParamItem `refreshable:"true"`
	TtProtectionEnabled                  ParamItem `refreshable:"true"`
	MaxTimeTickDelay                     ParamItem `refreshable:"true"`
	MemProtectionEnabled                 ParamItem `refreshable:"true"`
	DataNodeMemoryLowWaterLevel          ParamItem `refreshable:"true"`
	DataNodeMemoryHighWaterLevel         ParamItem `refreshable:"true"`
	QueryNodeMemoryLowWaterLevel         ParamItem `refreshable:"true"`
	QueryNodeMemoryHighWaterLevel        ParamItem `refreshable:"true"`
	GrowingSegmentsSizeProtectionEnabled ParamItem `refreshable:"true"`
	GrowingSegmentsSizeLowWaterLevel     ParamItem `refreshable:"true"`
	GrowingSegmentsSizeHighWaterLevel    ParamItem `refreshable:"true"`
	DiskProtectionEnabled                ParamItem `refreshable:"true"`
	DiskQuota                            ParamItem `refreshable:"true"`
	DiskQuotaPerCollection               ParamItem `refreshable:"true"`

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
		Doc:          "`true` to enable quota and limits, `false` to disable.",
		Export:       true,
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
		Doc: `quotaCenterCollectInterval is the time interval that quotaCenter
collects metrics from Proxies, Query cluster and Data cluster.
seconds, (0 ~ 65536)`,
		Export: true,
	}
	p.QuotaCenterCollectInterval.Init(base.mgr)

	// ddl
	max := fmt.Sprintf("%f", defaultMax)
	min := fmt.Sprintf("%f", defaultMin)
	p.DDLLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.ddl.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
		Export:       true,
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
		Doc:    "qps, default no limit, rate for CreateCollection, DropCollection, LoadCollection, ReleaseCollection",
		Export: true,
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
		Doc:    "qps, default no limit, rate for CreatePartition, DropPartition, LoadPartition, ReleasePartition",
		Export: true,
	}
	p.DDLPartitionRate.Init(base.mgr)

	p.IndexLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.indexRate.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
		Export:       true,
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
		Doc:    "qps, default no limit, rate for CreateIndex, DropIndex",
		Export: true,
	}
	p.MaxIndexRate.Init(base.mgr)

	p.FlushLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.flushRate.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
		Export:       true,
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
		Doc:    "qps, default no limit, rate for flush",
		Export: true,
	}
	p.MaxFlushRate.Init(base.mgr)

	p.CompactionLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.compactionRate.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
		Export:       true,
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
		Doc:    "qps, default no limit, rate for manualCompaction",
		Export: true,
	}
	p.MaxCompactionRate.Init(base.mgr)

	// dml
	p.DMLLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.dml.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
		Doc: `dml limit rates, default no limit.
The maximum rate will not be greater than ` + "max" + `.`,
		Export: true,
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
			rate := getAsFloat(v)
			if math.Abs(rate-defaultMax) > 0.001 { // maxRate != defaultMax
				rate = megaBytes2Bytes(rate)
			}
			// [0, inf)
			if rate < 0 {
				return max
			}
			return fmt.Sprintf("%f", rate)
		},
		Doc:    "MB/s, default no limit",
		Export: true,
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
			if !p.checkMinMaxLegal(rate, p.DMLMaxInsertRate.GetAsFloat()) {
				return min
			}
			return fmt.Sprintf("%f", rate)
		},
	}
	p.DMLMinInsertRate.Init(base.mgr)

	p.DMLMaxInsertRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dml.insertRate.collection.max",
		Version:      "2.2.9",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return max
			}
			rate := getAsFloat(v)
			if math.Abs(rate-defaultMax) > 0.001 { // maxRate != defaultMax
				rate = megaBytes2Bytes(rate)
			}
			// [0, inf)
			if rate < 0 {
				return p.DMLMaxInsertRate.GetValue()
			}
			return fmt.Sprintf("%f", rate)
		},
		Doc:    "MB/s, default no limit",
		Export: true,
	}
	p.DMLMaxInsertRatePerCollection.Init(base.mgr)

	p.DMLMinInsertRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dml.insertRate.collection.min",
		Version:      "2.2.9",
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
			if !p.checkMinMaxLegal(rate, p.DMLMaxInsertRatePerCollection.GetAsFloat()) {
				return min
			}
			return fmt.Sprintf("%f", rate)
		},
	}
	p.DMLMinInsertRatePerCollection.Init(base.mgr)

	p.DMLMaxUpsertRate = ParamItem{
		Key:          "quotaAndLimits.dml.upsertRate.max",
		Version:      "2.3.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return max
			}
			rate := getAsFloat(v)
			if math.Abs(rate-defaultMax) > 0.001 { // maxRate != defaultMax
				rate = megaBytes2Bytes(rate)
			}
			// [0, inf)
			if rate < 0 {
				return max
			}
			return fmt.Sprintf("%f", rate)
		},
		Doc:    "MB/s, default no limit",
		Export: true,
	}
	p.DMLMaxUpsertRate.Init(base.mgr)

	p.DMLMinUpsertRate = ParamItem{
		Key:          "quotaAndLimits.dml.UpsertRate.min",
		Version:      "2.3.0",
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
			if !p.checkMinMaxLegal(rate, p.DMLMaxUpsertRate.GetAsFloat()) {
				return min
			}
			return fmt.Sprintf("%f", rate)
		},
	}
	p.DMLMinUpsertRate.Init(base.mgr)

	p.DMLMaxUpsertRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dml.upsertRate.collection.max",
		Version:      "2.3.0",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return max
			}
			rate := getAsFloat(v)
			if math.Abs(rate-defaultMax) > 0.001 { // maxRate != defaultMax
				rate = megaBytes2Bytes(rate)
			}
			// [0, inf)
			if rate < 0 {
				return p.DMLMaxUpsertRate.GetValue()
			}
			return fmt.Sprintf("%f", rate)
		},
		Doc:    "MB/s, default no limit",
		Export: true,
	}
	p.DMLMaxUpsertRatePerCollection.Init(base.mgr)

	p.DMLMinUpsertRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dml.upsertRate.collection.min",
		Version:      "2.3.0",
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
			if !p.checkMinMaxLegal(rate, p.DMLMaxUpsertRatePerCollection.GetAsFloat()) {
				return min
			}
			return fmt.Sprintf("%f", rate)
		},
	}
	p.DMLMinUpsertRatePerCollection.Init(base.mgr)

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
				rate = megaBytes2Bytes(rate)
			}
			// [0, inf)
			if rate < 0 {
				return max
			}
			return fmt.Sprintf("%f", rate)
		},
		Doc:    "MB/s, default no limit",
		Export: true,
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
			if !p.checkMinMaxLegal(rate, p.DMLMaxDeleteRate.GetAsFloat()) {
				return min
			}
			return fmt.Sprintf("%f", rate)
		},
	}
	p.DMLMinDeleteRate.Init(base.mgr)

	p.DMLMaxDeleteRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dml.deleteRate.collection.max",
		Version:      "2.2.9",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return max
			}
			rate := getAsFloat(v)
			if math.Abs(rate-defaultMax) > 0.001 { // maxRate != defaultMax
				rate = megaBytes2Bytes(rate)
			}
			// [0, inf)
			if rate < 0 {
				return p.DMLMaxDeleteRate.GetValue()
			}
			return fmt.Sprintf("%f", rate)
		},
		Doc:    "MB/s, default no limit",
		Export: true,
	}
	p.DMLMaxDeleteRatePerCollection.Init(base.mgr)

	p.DMLMinDeleteRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dml.deleteRate.collection.min",
		Version:      "2.2.9",
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
			if !p.checkMinMaxLegal(rate, p.DMLMaxDeleteRatePerCollection.GetAsFloat()) {
				return min
			}
			return fmt.Sprintf("%f", rate)
		},
	}
	p.DMLMinDeleteRatePerCollection.Init(base.mgr)

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
				rate = megaBytes2Bytes(rate)
			}
			// [0, inf)
			if rate < 0 {
				return max
			}
			return fmt.Sprintf("%f", rate)
		},
		Doc:    "MB/s, default no limit, not support yet. TODO: limit bulkLoad rate",
		Export: true,
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
			if !p.checkMinMaxLegal(rate, p.DMLMaxBulkLoadRate.GetAsFloat()) {
				return min
			}
			return fmt.Sprintf("%f", rate)
		},
	}
	p.DMLMinBulkLoadRate.Init(base.mgr)

	p.DMLMaxBulkLoadRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dml.bulkLoadRate.collection.max",
		Version:      "2.2.9",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DMLLimitEnabled.GetAsBool() {
				return max
			}
			rate := getAsFloat(v)
			if math.Abs(rate-defaultMax) > 0.001 { // maxRate != defaultMax
				rate = megaBytes2Bytes(rate)
			}
			// [0, inf)
			if rate < 0 {
				return p.DMLMaxBulkLoadRate.GetValue()
			}
			return fmt.Sprintf("%f", rate)
		},
		Doc:    "MB/s, default no limit, not support yet. TODO: limit collection bulkLoad rate",
		Export: true,
	}
	p.DMLMaxBulkLoadRatePerCollection.Init(base.mgr)

	p.DMLMinBulkLoadRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dml.bulkLoadRate.collection.min",
		Version:      "2.2.9",
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
			if !p.checkMinMaxLegal(rate, p.DMLMaxBulkLoadRatePerCollection.GetAsFloat()) {
				return min
			}
			return fmt.Sprintf("%f", rate)
		},
	}
	p.DMLMinBulkLoadRatePerCollection.Init(base.mgr)

	// dql
	p.DQLLimitEnabled = ParamItem{
		Key:          "quotaAndLimits.dql.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
		Doc: `dql limit rates, default no limit.
The maximum rate will not be greater than ` + "max" + `.`,
		Export: true,
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
		Doc:    "vps (vectors per second), default no limit",
		Export: true,
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
			if !p.checkMinMaxLegal(rate, p.DQLMaxSearchRate.GetAsFloat()) {
				return min
			}
			return v
		},
	}
	p.DQLMinSearchRate.Init(base.mgr)

	p.DQLMaxSearchRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dql.searchRate.collection.max",
		Version:      "2.2.9",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DQLLimitEnabled.GetAsBool() {
				return max
			}
			// [0, inf)
			if getAsFloat(v) < 0 {
				return p.DQLMaxSearchRate.GetValue()
			}
			return v
		},
		Doc:    "vps (vectors per second), default no limit",
		Export: true,
	}
	p.DQLMaxSearchRatePerCollection.Init(base.mgr)

	p.DQLMinSearchRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dql.searchRate.collection.min",
		Version:      "2.2.9",
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
			if !p.checkMinMaxLegal(rate, p.DQLMaxSearchRatePerCollection.GetAsFloat()) {
				return min
			}
			return v
		},
	}
	p.DQLMinSearchRatePerCollection.Init(base.mgr)

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
		Doc:    "qps, default no limit",
		Export: true,
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
			if !p.checkMinMaxLegal(rate, p.DQLMaxQueryRate.GetAsFloat()) {
				return min
			}
			return v
		},
	}
	p.DQLMinQueryRate.Init(base.mgr)

	p.DQLMaxQueryRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dql.queryRate.collection.max",
		Version:      "2.2.9",
		DefaultValue: max,
		Formatter: func(v string) string {
			if !p.DQLLimitEnabled.GetAsBool() {
				return max
			}
			// [0, inf)
			if getAsFloat(v) < 0 {
				return p.DQLMaxQueryRate.GetValue()
			}
			return v
		},
		Doc:    "qps, default no limit",
		Export: true,
	}
	p.DQLMaxQueryRatePerCollection.Init(base.mgr)

	p.DQLMinQueryRatePerCollection = ParamItem{
		Key:          "quotaAndLimits.dql.queryRate.collection.min",
		Version:      "2.2.9",
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
			if !p.checkMinMaxLegal(rate, p.DQLMaxQueryRatePerCollection.GetAsFloat()) {
				return min
			}
			return v
		},
	}
	p.DQLMinQueryRatePerCollection.Init(base.mgr)

	// limits
	p.MaxCollectionNum = ParamItem{
		Key:          "quotaAndLimits.limits.maxCollectionNum",
		Version:      "2.2.0",
		DefaultValue: "65536",
	}
	p.MaxCollectionNum.Init(base.mgr)

	p.MaxCollectionNumPerDB = ParamItem{
		Key:          "quotaAndLimits.limits.maxCollectionNumPerDB",
		Version:      "2.2.0",
		DefaultValue: "64",
	}
	p.MaxCollectionNumPerDB.Init(base.mgr)

	p.TopKLimit = ParamItem{
		Key:          "quotaAndLimits.limits.topK",
		Version:      "2.2.1",
		DefaultValue: "16384",
		FallbackKeys: []string{
			"common.topKLimit",
		},
		Doc: `Search limit, which applies on:
maximum # of results to return (topK).
Check https://milvus.io/docs/limitations.md for more details.`,
	}
	p.TopKLimit.Init(base.mgr)

	p.NQLimit = ParamItem{
		Key:          "quotaAndLimits.limits.nq",
		Version:      "2.3.0",
		DefaultValue: "16384",
		FallbackKeys: []string{},
		Doc: `Search limit, which applies on:
maximum # of search requests (nq).
Check https://milvus.io/docs/limitations.md for more details.`,
	}
	p.NQLimit.Init(base.mgr)

	p.MaxQueryResultWindow = ParamItem{
		Key:          "quotaAndLimits.limits.maxQueryResultWindow",
		Version:      "2.3.0",
		DefaultValue: "16384",
		FallbackKeys: []string{},
		Doc:          `Query limit, which applies on: maximum of offset + limit`,
	}
	p.MaxQueryResultWindow.Init(base.mgr)

	// limit writing
	p.ForceDenyWriting = ParamItem{
		Key:          "quotaAndLimits.limitWriting.forceDeny",
		Version:      "2.2.0",
		DefaultValue: "false",
		Doc: `forceDeny ` + "false" + ` means dml requests are allowed (except for some
specific conditions, such as memory of nodes to water marker), ` + "true" + ` means always reject all dml requests.`,
		Export: true,
	}
	p.ForceDenyWriting.Init(base.mgr)

	p.TtProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitWriting.ttProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
		Export:       true,
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
		Doc: `maxTimeTickDelay indicates the backpressure for DML Operations.
DML rates would be reduced according to the ratio of time tick delay to maxTimeTickDelay,
if time tick delay is greater than maxTimeTickDelay, all DML requests would be rejected.
seconds`,
		Export: true,
	}
	p.MaxTimeTickDelay.Init(base.mgr)

	p.MemProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitWriting.memProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "true",
		Doc: `When memory usage > memoryHighWaterLevel, all dml requests would be rejected;
When memoryLowWaterLevel < memory usage < memoryHighWaterLevel, reduce the dml rate;
When memory usage < memoryLowWaterLevel, no action.`,
		Export: true,
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
		Doc:    "(0, 1], memoryLowWaterLevel in DataNodes",
		Export: true,
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
		Doc:    "(0, 1], memoryHighWaterLevel in DataNodes",
		Export: true,
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
		Doc:    "(0, 1], memoryLowWaterLevel in QueryNodes",
		Export: true,
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
		Doc:    "(0, 1], memoryHighWaterLevel in QueryNodes",
		Export: true,
	}
	p.QueryNodeMemoryHighWaterLevel.Init(base.mgr)

	p.GrowingSegmentsSizeProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitWriting.growingSegmentsSizeProtection.enabled",
		Version:      "2.2.9",
		DefaultValue: "false",
		Doc: `1. No action will be taken if the ratio of growing segments size is less than the low water level.
2. The DML rate will be reduced if the ratio of growing segments size is greater than the low water level and less than the high water level.
3. All DML requests will be rejected if the ratio of growing segments size is greater than the high water level.`,
		Export: true,
	}
	p.GrowingSegmentsSizeProtectionEnabled.Init(base.mgr)

	defaultGrowingSegSizeLowWaterLevel := "0.2"
	p.GrowingSegmentsSizeLowWaterLevel = ParamItem{
		Key:          "quotaAndLimits.limitWriting.growingSegmentsSizeProtection.lowWaterLevel",
		Version:      "2.2.9",
		DefaultValue: defaultGrowingSegSizeLowWaterLevel,
		Formatter: func(v string) string {
			level := getAsFloat(v)
			if level <= 0 || level > 1 {
				return defaultGrowingSegSizeLowWaterLevel
			}
			return v
		},
		Export: true,
	}
	p.GrowingSegmentsSizeLowWaterLevel.Init(base.mgr)

	defaultGrowingSegSizeHighWaterLevel := "0.4"
	p.GrowingSegmentsSizeHighWaterLevel = ParamItem{
		Key:          "quotaAndLimits.limitWriting.growingSegmentsSizeProtection.highWaterLevel",
		Version:      "2.2.9",
		DefaultValue: defaultGrowingSegSizeHighWaterLevel,
		Formatter: func(v string) string {
			level := getAsFloat(v)
			if level <= 0 || level > 1 {
				return defaultGrowingSegSizeHighWaterLevel
			}
			if !p.checkMinMaxLegal(p.GrowingSegmentsSizeLowWaterLevel.GetAsFloat(), getAsFloat(v)) {
				return defaultGrowingSegSizeHighWaterLevel
			}
			return v
		},
		Export: true,
	}
	p.GrowingSegmentsSizeHighWaterLevel.Init(base.mgr)

	p.DiskProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitWriting.diskProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "true",
		Doc:          "When the total file size of object storage is greater than `diskQuota`, all dml requests would be rejected;",
		Export:       true,
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
				return max
			}
			// megabytes to bytes
			return fmt.Sprintf("%f", megaBytes2Bytes(level))
		},
		Doc:    "MB, (0, +inf), default no limit",
		Export: true,
	}
	p.DiskQuota.Init(base.mgr)

	p.DiskQuotaPerCollection = ParamItem{
		Key:          "quotaAndLimits.limitWriting.diskProtection.diskQuotaPerCollection",
		Version:      "2.2.8",
		DefaultValue: quota,
		Formatter: func(v string) string {
			if !p.DiskProtectionEnabled.GetAsBool() {
				return max
			}
			level := getAsFloat(v)
			// (0, +inf)
			if level <= 0 {
				return p.DiskQuota.GetValue()
			}
			// megabytes to bytes
			return fmt.Sprintf("%f", megaBytes2Bytes(level))
		},
		Doc:    "MB, (0, +inf), default no limit",
		Export: true,
	}
	p.DiskQuotaPerCollection.Init(base.mgr)

	// limit reading
	p.ForceDenyReading = ParamItem{
		Key:          "quotaAndLimits.limitReading.forceDeny",
		Version:      "2.2.0",
		DefaultValue: "false",
		Doc: `forceDeny ` + "false" + ` means dql requests are allowed (except for some
specific conditions, such as collection has been dropped), ` + "true" + ` means always reject all dql requests.`,
		Export: true,
	}
	p.ForceDenyReading.Init(base.mgr)

	p.QueueProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitReading.queueProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
		Export:       true,
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
		Doc: `nqInQueueThreshold indicated that the system was under backpressure for Search/Query path.
If NQ in any QueryNode's queue is greater than nqInQueueThreshold, search&query rates would gradually cool off
until the NQ in queue no longer exceeds nqInQueueThreshold. We think of the NQ of query request as 1.
int, default no limit`,
		Export: true,
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
		Doc: `queueLatencyThreshold indicated that the system was under backpressure for Search/Query path.
If dql latency of queuing is greater than queueLatencyThreshold, search&query rates would gradually cool off
until the latency of queuing no longer exceeds queueLatencyThreshold.
The latency here refers to the averaged latency over a period of time.
milliseconds, default no limit`,
		Export: true,
	}
	p.QueueLatencyThreshold.Init(base.mgr)

	p.ResultProtectionEnabled = ParamItem{
		Key:          "quotaAndLimits.limitReading.resultProtection.enabled",
		Version:      "2.2.0",
		DefaultValue: "false",
		Export:       true,
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
		Doc: `maxReadResultRate indicated that the system was under backpressure for Search/Query path.
If dql result rate is greater than maxReadResultRate, search&query rates would gradually cool off
until the read result rate no longer exceeds maxReadResultRate.
MB/s, default no limit`,
		Export: true,
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
		Doc: `colOffSpeed is the speed of search&query rates cool off.
(0, 1]`,
		Export: true,
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
