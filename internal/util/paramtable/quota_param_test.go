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
		assert.True(t, qc.QuotaAndLimitsEnabled)
		assert.Equal(t, float64(3), qc.QuotaCenterCollectInterval)
	})

	t.Run("test ddl", func(t *testing.T) {
		assert.Equal(t, false, qc.DDLLimitEnabled)
		assert.Equal(t, defaultMax, qc.DDLCollectionRate)
		assert.Equal(t, defaultMax, qc.DDLPartitionRate)
	})

	t.Run("test functional params", func(t *testing.T) {
		assert.Equal(t, false, qc.IndexLimitEnabled)
		assert.Equal(t, defaultMax, qc.MaxIndexRate)
		assert.Equal(t, false, qc.FlushLimitEnabled)
		assert.Equal(t, defaultMax, qc.MaxFlushRate)
		assert.Equal(t, false, qc.CompactionLimitEnabled)
		assert.Equal(t, defaultMax, qc.MaxCompactionRate)
	})

	t.Run("test dml", func(t *testing.T) {
		qc.Base.Save("quotaAndLimits.dml.enabled", "true")
		qc.Base.Save("quotaAndLimits.dml.insertRate.max", "10")
		qc.Base.Save("quotaAndLimits.dml.insertRate.min", "1")
		qc.Base.Save("quotaAndLimits.dml.deleteRate.max", "10")
		qc.Base.Save("quotaAndLimits.dml.deleteRate.min", "1")
		qc.Base.Save("quotaAndLimits.dml.bulkLoadRate.max", "10")
		qc.Base.Save("quotaAndLimits.dml.bulkLoadRate.min", "1")
		qc.init(&baseParams)
		assert.Equal(t, float64(10*1024*1024), qc.DMLMaxInsertRate)
		assert.Equal(t, float64(1*1024*1024), qc.DMLMinInsertRate)
		assert.Equal(t, float64(10*1024*1024), qc.DMLMaxDeleteRate)
		assert.Equal(t, float64(1*1024*1024), qc.DMLMinDeleteRate)
		assert.Equal(t, float64(10*1024*1024), qc.DMLMaxBulkLoadRate)
		assert.Equal(t, float64(1*1024*1024), qc.DMLMinBulkLoadRate)
	})

	t.Run("test collection dml", func(t *testing.T) {
		qc.Base.Save("quotaAndLimits.dml.enabled", "true")
		qc.Base.Save("quotaAndLimits.dml.insertRate.collection.max", "10")
		qc.Base.Save("quotaAndLimits.dml.insertRate.collection.min", "1")
		qc.Base.Save("quotaAndLimits.dml.deleteRate.collection.max", "10")
		qc.Base.Save("quotaAndLimits.dml.deleteRate.collection.min", "1")
		qc.Base.Save("quotaAndLimits.dml.bulkLoadRate.collection.max", "10")
		qc.Base.Save("quotaAndLimits.dml.bulkLoadRate.collection.min", "1")
		qc.init(&baseParams)
		assert.Equal(t, float64(10*1024*1024), qc.DMLMaxInsertRatePerCollection)
		assert.Equal(t, float64(1*1024*1024), qc.DMLMinInsertRatePerCollection)
		assert.Equal(t, float64(10*1024*1024), qc.DMLMaxDeleteRatePerCollection)
		assert.Equal(t, float64(1*1024*1024), qc.DMLMinDeleteRatePerCollection)
		assert.Equal(t, float64(10*1024*1024), qc.DMLMaxBulkLoadRatePerCollection)
		assert.Equal(t, float64(1*1024*1024), qc.DMLMinBulkLoadRatePerCollection)
	})

	t.Run("test dql", func(t *testing.T) {
		qc.Base.Save("quotaAndLimits.dql.enabled", "true")
		qc.Base.Save("quotaAndLimits.dql.searchRate.max", "10")
		qc.Base.Save("quotaAndLimits.dql.searchRate.min", "1")
		qc.Base.Save("quotaAndLimits.dql.queryRate.max", "10")
		qc.Base.Save("quotaAndLimits.dql.queryRate.min", "1")
		qc.init(&baseParams)
		assert.Equal(t, float64(10), qc.DQLMaxSearchRate)
		assert.Equal(t, float64(1), qc.DQLMinSearchRate)
		assert.Equal(t, float64(10), qc.DQLMaxQueryRate)
		assert.Equal(t, float64(1), qc.DQLMinQueryRate)
	})

	t.Run("test collection dql", func(t *testing.T) {
		qc.Base.Save("quotaAndLimits.dql.enabled", "true")
		qc.Base.Save("quotaAndLimits.dql.searchRate.collection.max", "10")
		qc.Base.Save("quotaAndLimits.dql.searchRate.collection.min", "1")
		qc.Base.Save("quotaAndLimits.dql.queryRate.collection.max", "10")
		qc.Base.Save("quotaAndLimits.dql.queryRate.collection.min", "1")
		qc.init(&baseParams)
		assert.Equal(t, float64(10), qc.DQLMaxSearchRatePerCollection)
		assert.Equal(t, float64(1), qc.DQLMinSearchRatePerCollection)
		assert.Equal(t, float64(10), qc.DQLMaxQueryRatePerCollection)
		assert.Equal(t, float64(1), qc.DQLMinQueryRatePerCollection)
	})

	t.Run("test limits", func(t *testing.T) {
		assert.Equal(t, 65536, qc.MaxCollectionNum)
		assert.Equal(t, 64, qc.MaxCollectionNumPerDB)
	})

	t.Run("test limit writing", func(t *testing.T) {
		assert.False(t, qc.ForceDenyWriting)
		assert.Equal(t, false, qc.TtProtectionEnabled)
		assert.Equal(t, math.MaxInt64, int(qc.MaxTimeTickDelay))
		assert.Equal(t, defaultLowWaterLevel, qc.DataNodeMemoryLowWaterLevel)
		assert.Equal(t, defaultHighWaterLevel, qc.DataNodeMemoryHighWaterLevel)
		assert.Equal(t, defaultLowWaterLevel, qc.QueryNodeMemoryLowWaterLevel)
		assert.Equal(t, defaultHighWaterLevel, qc.QueryNodeMemoryHighWaterLevel)
		assert.Equal(t, true, qc.DiskProtectionEnabled)
		assert.Equal(t, defaultMax, qc.DiskQuota)
		assert.Equal(t, defaultMax, qc.DiskQuotaPerCollection)
	})

	t.Run("test limit reading", func(t *testing.T) {
		assert.False(t, qc.ForceDenyReading)
		assert.Equal(t, false, qc.QueueProtectionEnabled)
		assert.Equal(t, int64(math.MaxInt64), qc.NQInQueueThreshold)
		assert.Equal(t, defaultMax, qc.QueueLatencyThreshold)
		assert.Equal(t, false, qc.ResultProtectionEnabled)
		assert.Equal(t, defaultMax, qc.MaxReadResultRate)
		assert.Equal(t, 0.9, qc.CoolOffSpeed)
	})
}
