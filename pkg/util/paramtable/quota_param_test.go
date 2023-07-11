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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuotaParam(t *testing.T) {
	qc := quotaConfig{}
	qc.init(&baseParams)

	t.Run("test quota", func(t *testing.T) {
		assert.True(t, qc.QuotaAndLimitsEnabled.GetAsBool())
		assert.Equal(t, float64(3), qc.QuotaCenterCollectInterval.GetAsFloat())
	})

	t.Run("test ddl", func(t *testing.T) {
		assert.Equal(t, false, qc.DDLLimitEnabled.GetAsBool())
		assert.Equal(t, defaultMax, qc.DDLCollectionRate.GetAsFloat())
		assert.Equal(t, defaultMax, qc.DDLPartitionRate.GetAsFloat())
	})

	t.Run("test functional params", func(t *testing.T) {
		assert.Equal(t, false, qc.IndexLimitEnabled.GetAsBool())
		assert.Equal(t, defaultMax, qc.MaxIndexRate.GetAsFloat())
		assert.Equal(t, false, qc.FlushLimitEnabled.GetAsBool())
		assert.Equal(t, defaultMax, qc.MaxFlushRate.GetAsFloat())
		assert.Equal(t, false, qc.CompactionLimitEnabled.GetAsBool())
		assert.Equal(t, defaultMax, qc.MaxCompactionRate.GetAsFloat())
	})

	t.Run("test dml", func(t *testing.T) {
		params.Init()
		params.Save(params.QuotaConfig.DMLLimitEnabled.Key, "true")
		params.Save(params.QuotaConfig.DMLMaxInsertRate.Key, "10")
		params.Save(params.QuotaConfig.DMLMinInsertRate.Key, "1")
		params.Save(params.QuotaConfig.DMLMaxUpsertRate.Key, "10")
		params.Save(params.QuotaConfig.DMLMinUpsertRate.Key, "1")
		params.Save(params.QuotaConfig.DMLMaxDeleteRate.Key, "10")
		params.Save(params.QuotaConfig.DMLMinDeleteRate.Key, "1")
		params.Save(params.QuotaConfig.DMLMaxBulkLoadRate.Key, "10")
		params.Save(params.QuotaConfig.DMLMinBulkLoadRate.Key, "1")
		assert.Equal(t, true, params.QuotaConfig.DMLLimitEnabled.GetAsBool())
		assert.Equal(t, float64(10)*1024*1024, params.QuotaConfig.DMLMaxInsertRate.GetAsFloat())
		assert.Equal(t, float64(1)*1024*1024, params.QuotaConfig.DMLMinInsertRate.GetAsFloat())
		assert.Equal(t, float64(10)*1024*1024, params.QuotaConfig.DMLMaxUpsertRate.GetAsFloat())
		assert.Equal(t, float64(1)*1024*1024, params.QuotaConfig.DMLMinUpsertRate.GetAsFloat())
		assert.Equal(t, float64(10)*1024*1024, params.QuotaConfig.DMLMaxDeleteRate.GetAsFloat())
		assert.Equal(t, float64(1)*1024*1024, params.QuotaConfig.DMLMinDeleteRate.GetAsFloat())
		assert.Equal(t, float64(10)*1024*1024, params.QuotaConfig.DMLMaxBulkLoadRate.GetAsFloat())
		assert.Equal(t, float64(1)*1024*1024, params.QuotaConfig.DMLMinBulkLoadRate.GetAsFloat())
	})

	t.Run("test collection dml", func(t *testing.T) {
		params.Init()
		params.Save(params.QuotaConfig.DMLLimitEnabled.Key, "true")
		params.Save(params.QuotaConfig.DMLMaxInsertRatePerCollection.Key, "10")
		params.Save(params.QuotaConfig.DMLMinInsertRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DMLMaxUpsertRatePerCollection.Key, "10")
		params.Save(params.QuotaConfig.DMLMinUpsertRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DMLMaxDeleteRatePerCollection.Key, "10")
		params.Save(params.QuotaConfig.DMLMinDeleteRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DMLMaxBulkLoadRatePerCollection.Key, "10")
		params.Save(params.QuotaConfig.DMLMinBulkLoadRatePerCollection.Key, "1")
		assert.Equal(t, true, params.QuotaConfig.DMLLimitEnabled.GetAsBool())
		assert.Equal(t, float64(10)*1024*1024, params.QuotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1)*1024*1024, params.QuotaConfig.DMLMinInsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(10)*1024*1024, params.QuotaConfig.DMLMaxUpsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1)*1024*1024, params.QuotaConfig.DMLMinUpsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(10)*1024*1024, params.QuotaConfig.DMLMaxDeleteRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1)*1024*1024, params.QuotaConfig.DMLMinDeleteRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(10)*1024*1024, params.QuotaConfig.DMLMaxBulkLoadRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1)*1024*1024, params.QuotaConfig.DMLMinBulkLoadRatePerCollection.GetAsFloat())

		// test only set global rate limit
		params.Save(params.QuotaConfig.DMLMaxInsertRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DMLMinInsertRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DMLMaxUpsertRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DMLMinUpsertRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DMLMaxDeleteRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DMLMinDeleteRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DMLMaxBulkLoadRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DMLMinBulkLoadRatePerCollection.Key, "-1")
		assert.Equal(t, params.QuotaConfig.DMLMaxInsertRate.GetAsFloat(), params.QuotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DMLMinInsertRatePerCollection.GetAsFloat())
		assert.Equal(t, params.QuotaConfig.DMLMaxUpsertRate.GetAsFloat(), params.QuotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DMLMinUpsertRatePerCollection.GetAsFloat())
		assert.Equal(t, params.QuotaConfig.DMLMaxDeleteRate.GetAsFloat(), params.QuotaConfig.DMLMaxDeleteRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DMLMinDeleteRatePerCollection.GetAsFloat())
		assert.Equal(t, params.QuotaConfig.DMLMaxBulkLoadRate.GetAsFloat(), params.QuotaConfig.DMLMaxBulkLoadRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DMLMinBulkLoadRatePerCollection.GetAsFloat())

		// test invalid config value
		params.Save(params.QuotaConfig.DMLMaxInsertRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DMLMinInsertRatePerCollection.Key, "5")
		params.Save(params.QuotaConfig.DMLMaxUpsertRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DMLMinUpsertRatePerCollection.Key, "5")
		params.Save(params.QuotaConfig.DMLMaxDeleteRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DMLMinDeleteRatePerCollection.Key, "5")
		params.Save(params.QuotaConfig.DMLMaxBulkLoadRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DMLMinBulkLoadRatePerCollection.Key, "5")
		assert.Equal(t, float64(1*1024*1024), params.QuotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DMLMinInsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1*1024*1024), params.QuotaConfig.DMLMaxUpsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DMLMinUpsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1*1024*1024), params.QuotaConfig.DMLMaxDeleteRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DMLMinDeleteRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1*1024*1024), params.QuotaConfig.DMLMaxBulkLoadRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DMLMinBulkLoadRatePerCollection.GetAsFloat())
	})

	t.Run("test dql", func(t *testing.T) {
		params.Init()
		params.Save(params.QuotaConfig.DQLLimitEnabled.Key, "true")
		params.Save(params.QuotaConfig.DQLMaxSearchRate.Key, "10")
		params.Save(params.QuotaConfig.DQLMinSearchRate.Key, "1")
		params.Save(params.QuotaConfig.DQLMaxQueryRate.Key, "10")
		params.Save(params.QuotaConfig.DQLMinQueryRate.Key, "1")
		assert.Equal(t, float64(10), params.QuotaConfig.DQLMaxSearchRate.GetAsFloat())
		assert.Equal(t, float64(1), params.QuotaConfig.DQLMinSearchRate.GetAsFloat())
		assert.Equal(t, float64(10), params.QuotaConfig.DQLMaxQueryRate.GetAsFloat())
		assert.Equal(t, float64(1), params.QuotaConfig.DQLMinQueryRate.GetAsFloat())
	})

	t.Run("test collection dql", func(t *testing.T) {
		params.Init()
		params.Save(params.QuotaConfig.DQLLimitEnabled.Key, "true")
		params.Save(params.QuotaConfig.DQLMaxSearchRatePerCollection.Key, "10")
		params.Save(params.QuotaConfig.DQLMinSearchRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DQLMaxQueryRatePerCollection.Key, "10")
		params.Save(params.QuotaConfig.DQLMinQueryRatePerCollection.Key, "1")
		assert.Equal(t, float64(10), params.QuotaConfig.DQLMaxSearchRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1), params.QuotaConfig.DQLMinSearchRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(10), params.QuotaConfig.DQLMaxQueryRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1), params.QuotaConfig.DQLMinQueryRatePerCollection.GetAsFloat())

		// test only config global value
		params.Save(params.QuotaConfig.DQLMaxSearchRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DQLMinSearchRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DQLMaxQueryRatePerCollection.Key, "-1")
		params.Save(params.QuotaConfig.DQLMinQueryRatePerCollection.Key, "-1")
		assert.Equal(t, params.QuotaConfig.DQLMaxSearchRate.GetAsFloat(), params.QuotaConfig.DQLMaxSearchRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DQLMinSearchRatePerCollection.GetAsFloat())
		assert.Equal(t, params.QuotaConfig.DQLMaxQueryRate.GetAsFloat(), params.QuotaConfig.DQLMaxQueryRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DQLMinQueryRatePerCollection.GetAsFloat())

		// test invalid config
		params.Save(params.QuotaConfig.DQLMaxSearchRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DQLMinSearchRatePerCollection.Key, "5")
		params.Save(params.QuotaConfig.DQLMaxQueryRatePerCollection.Key, "1")
		params.Save(params.QuotaConfig.DQLMinQueryRatePerCollection.Key, "5")
		assert.Equal(t, float64(1), params.QuotaConfig.DQLMaxSearchRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DQLMinSearchRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(1), params.QuotaConfig.DQLMaxQueryRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(0), params.QuotaConfig.DQLMinQueryRatePerCollection.GetAsFloat())
	})

	t.Run("test limits", func(t *testing.T) {
		assert.Equal(t, 65536, qc.MaxCollectionNum.GetAsInt())
		assert.Equal(t, 65536, qc.MaxCollectionNumPerDB.GetAsInt())
	})

	t.Run("test limit writing", func(t *testing.T) {
		assert.False(t, qc.ForceDenyWriting.GetAsBool())
		assert.Equal(t, false, qc.TtProtectionEnabled.GetAsBool())
		assert.Equal(t, math.MaxInt64, qc.MaxTimeTickDelay.GetAsInt())
		assert.Equal(t, defaultLowWaterLevel, qc.DataNodeMemoryLowWaterLevel.GetAsFloat())
		assert.Equal(t, defaultHighWaterLevel, qc.DataNodeMemoryHighWaterLevel.GetAsFloat())
		assert.Equal(t, defaultLowWaterLevel, qc.QueryNodeMemoryLowWaterLevel.GetAsFloat())
		assert.Equal(t, defaultHighWaterLevel, qc.QueryNodeMemoryHighWaterLevel.GetAsFloat())
		assert.Equal(t, false, qc.GrowingSegmentsSizeProtectionEnabled.GetAsBool())
		assert.Equal(t, 0.2, qc.GrowingSegmentsSizeLowWaterLevel.GetAsFloat())
		assert.Equal(t, 0.4, qc.GrowingSegmentsSizeHighWaterLevel.GetAsFloat())
		assert.Equal(t, true, qc.DiskProtectionEnabled.GetAsBool())
		assert.Equal(t, defaultMax, qc.DiskQuota.GetAsFloat())
		assert.Equal(t, defaultMax, qc.DiskQuotaPerCollection.GetAsFloat())
	})

	t.Run("test limit reading", func(t *testing.T) {
		assert.False(t, qc.ForceDenyReading.GetAsBool())
		assert.Equal(t, false, qc.QueueProtectionEnabled.GetAsBool())
		assert.Equal(t, int64(math.MaxInt64), qc.NQInQueueThreshold.GetAsInt64())
		assert.Equal(t, defaultMax, qc.QueueLatencyThreshold.GetAsFloat())
		assert.Equal(t, false, qc.ResultProtectionEnabled.GetAsBool())
		assert.Equal(t, defaultMax, qc.MaxReadResultRate.GetAsFloat())
		assert.Equal(t, 0.9, qc.CoolOffSpeed.GetAsFloat())
	})

	t.Run("test disk quota", func(t *testing.T) {
		assert.Equal(t, defaultMax, qc.DiskQuota.GetAsFloat())
		assert.Equal(t, defaultMax, qc.DiskQuotaPerCollection.GetAsFloat())

		// test invalid config
		params.Save(params.QuotaConfig.DiskQuotaPerCollection.Key, "-1")
		assert.Equal(t, qc.DiskQuota.GetAsFloat(), qc.DiskQuotaPerCollection.GetAsFloat())

	})
}
