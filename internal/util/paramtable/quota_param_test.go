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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQuotaParam(t *testing.T) {
	qc := quotaConfig{}
	qc.init(&baseParams)

	t.Run("test quota", func(t *testing.T) {
		assert.True(t, qc.EnableQuotaAndLimits)
		assert.Equal(t, float64(3), qc.QuotaCenterCollectInterval)
	})

	t.Run("test ddl", func(t *testing.T) {
		assert.Equal(t, defaultMax, qc.DDLCollectionRate)
		assert.Equal(t, defaultMax, qc.DDLPartitionRate)
		assert.Equal(t, defaultMax, qc.DDLIndexRate)
		assert.Equal(t, defaultMax, qc.DDLFlushRate)
		assert.Equal(t, defaultMax, qc.DDLCompactionRate)
	})

	t.Run("test dml", func(t *testing.T) {
		assert.Equal(t, defaultMax, qc.DMLMaxInsertRate)
		assert.Equal(t, defaultMin, qc.DMLMinInsertRate)
		assert.Equal(t, defaultMax, qc.DMLMaxDeleteRate)
		assert.Equal(t, defaultMin, qc.DMLMinDeleteRate)
		assert.Equal(t, defaultMax, qc.DMLMaxBulkLoadRate)
		assert.Equal(t, defaultMin, qc.DMLMinBulkLoadRate)
	})

	t.Run("test dql", func(t *testing.T) {
		assert.Equal(t, defaultMax, qc.DQLMaxSearchRate)
		assert.Equal(t, defaultMin, qc.DQLMinSearchRate)
		assert.Equal(t, defaultMax, qc.DQLMaxQueryRate)
		assert.Equal(t, defaultMin, qc.DQLMinQueryRate)
	})

	t.Run("test limits", func(t *testing.T) {
		assert.Equal(t, 64, qc.MaxCollectionNum)
	})

	t.Run("test force deny writing", func(t *testing.T) {
		assert.False(t, qc.ForceDenyWriting)
		assert.Equal(t, 30*time.Second, qc.MaxTimeTickDelay)
		assert.Equal(t, defaultLowWaterLevel, qc.DataNodeMemoryLowWaterLevel)
		assert.Equal(t, defaultHighWaterLevel, qc.DataNodeMemoryHighWaterLevel)
		assert.Equal(t, defaultLowWaterLevel, qc.QueryNodeMemoryLowWaterLevel)
		assert.Equal(t, defaultHighWaterLevel, qc.QueryNodeMemoryHighWaterLevel)
	})

	t.Run("test force deny reading", func(t *testing.T) {
		assert.False(t, qc.ForceDenyReading)
		assert.Equal(t, int64(math.MaxInt64), qc.NQInQueueThreshold)
		assert.Equal(t, float64(defaultMax), qc.QueueLatencyThreshold)
		assert.Equal(t, 0.9, qc.CoolOffSpeed)
	})
}
