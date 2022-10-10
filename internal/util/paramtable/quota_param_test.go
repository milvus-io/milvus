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

func TestQuotaParam(t *testing.T) {
	qc := quotaConfig{}
	qc.init(&baseParams)

	t.Run("test quota", func(t *testing.T) {
		assert.False(t, qc.QuotaAndLimitsEnabled)
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
		assert.Equal(t, false, qc.DMLLimitEnabled)
		assert.Equal(t, defaultMax, qc.DMLMaxInsertRate)
		assert.Equal(t, defaultMin, qc.DMLMinInsertRate)
		assert.Equal(t, defaultMax, qc.DMLMaxDeleteRate)
		assert.Equal(t, defaultMin, qc.DMLMinDeleteRate)
		assert.Equal(t, defaultMax, qc.DMLMaxBulkLoadRate)
		assert.Equal(t, defaultMin, qc.DMLMinBulkLoadRate)
	})

	t.Run("test dql", func(t *testing.T) {
		assert.Equal(t, false, qc.DQLLimitEnabled)
		assert.Equal(t, defaultMax, qc.DQLMaxSearchRate)
		assert.Equal(t, defaultMin, qc.DQLMinSearchRate)
		assert.Equal(t, defaultMax, qc.DQLMaxQueryRate)
		assert.Equal(t, defaultMin, qc.DQLMinQueryRate)
	})

	t.Run("test limits", func(t *testing.T) {
		assert.Equal(t, 64, qc.MaxCollectionNum)
	})

	t.Run("test limit writing", func(t *testing.T) {
		assert.False(t, qc.ForceDenyWriting)
		assert.Equal(t, true, qc.TtProtectionEnabled)
		assert.Equal(t, 30*time.Second, qc.MaxTimeTickDelay)
		assert.Equal(t, defaultLowWaterLevel, qc.DataNodeMemoryLowWaterLevel)
		assert.Equal(t, defaultHighWaterLevel, qc.DataNodeMemoryHighWaterLevel)
		assert.Equal(t, defaultLowWaterLevel, qc.QueryNodeMemoryLowWaterLevel)
		assert.Equal(t, defaultHighWaterLevel, qc.QueryNodeMemoryHighWaterLevel)
		assert.Equal(t, true, qc.DiskProtectionEnabled)
		assert.Equal(t, defaultMax, qc.DiskQuota)
	})

	t.Run("test limit reading", func(t *testing.T) {
		assert.False(t, qc.ForceDenyReading)
		assert.Equal(t, false, qc.QueueProtectionEnabled)
		assert.Equal(t, int64(0), qc.NQInQueueThreshold)
		assert.Equal(t, float64(0), qc.QueueLatencyThreshold)
		assert.Equal(t, 0.9, qc.CoolOffSpeed)
	})
}
