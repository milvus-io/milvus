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
		assert.Equal(t, false, qc.DMLLimitEnabled.GetAsBool())
		assert.Equal(t, defaultMax, qc.DMLMaxInsertRate.GetAsFloat())
		assert.Equal(t, defaultMin, qc.DMLMinInsertRate.GetAsFloat())
		assert.Equal(t, defaultMax, qc.DMLMaxDeleteRate.GetAsFloat())
		assert.Equal(t, defaultMin, qc.DMLMinDeleteRate.GetAsFloat())
		assert.Equal(t, defaultMax, qc.DMLMaxBulkLoadRate.GetAsFloat())
		assert.Equal(t, defaultMin, qc.DMLMinBulkLoadRate.GetAsFloat())
	})

	t.Run("test dql", func(t *testing.T) {
		assert.Equal(t, false, qc.DQLLimitEnabled.GetAsBool())
		assert.Equal(t, defaultMax, qc.DQLMaxSearchRate.GetAsFloat())
		assert.Equal(t, defaultMin, qc.DQLMinSearchRate.GetAsFloat())
		assert.Equal(t, defaultMax, qc.DQLMaxQueryRate.GetAsFloat())
		assert.Equal(t, defaultMin, qc.DQLMinQueryRate.GetAsFloat())
	})

	t.Run("test limits", func(t *testing.T) {
		assert.Equal(t, 64, qc.MaxCollectionNum.GetAsInt())
	})

	t.Run("test limit writing", func(t *testing.T) {
		assert.False(t, qc.ForceDenyWriting.GetAsBool())
		assert.Equal(t, false, qc.TtProtectionEnabled.GetAsBool())
		assert.Equal(t, math.MaxInt64, qc.MaxTimeTickDelay.GetAsInt())
		assert.Equal(t, defaultLowWaterLevel, qc.DataNodeMemoryLowWaterLevel.GetAsFloat())
		assert.Equal(t, defaultHighWaterLevel, qc.DataNodeMemoryHighWaterLevel.GetAsFloat())
		assert.Equal(t, defaultLowWaterLevel, qc.QueryNodeMemoryLowWaterLevel.GetAsFloat())
		assert.Equal(t, defaultHighWaterLevel, qc.QueryNodeMemoryHighWaterLevel.GetAsFloat())
		assert.Equal(t, true, qc.DiskProtectionEnabled.GetAsBool())
		assert.Equal(t, defaultMax, qc.DiskQuota.GetAsFloat())
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
}
