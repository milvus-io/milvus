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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestPartitionStats(t *testing.T) {
	partStats := NewPartitionStatsSnapshot()
	{
		fieldStats := make([]FieldStats, 0)
		fieldStat1 := FieldStats{
			FieldID: 1,
			Type:    schemapb.DataType_Int64,
			Max:     NewInt64FieldValue(200),
			Min:     NewInt64FieldValue(100),
		}
		fieldStat2 := FieldStats{
			FieldID: 2,
			Type:    schemapb.DataType_Int64,
			Max:     NewInt64FieldValue(200),
			Min:     NewInt64FieldValue(100),
		}
		fieldStats = append(fieldStats, fieldStat1)
		fieldStats = append(fieldStats, fieldStat2)

		partStats.UpdateSegmentStats(1, SegmentStats{
			FieldStats: fieldStats,
		})
	}
	{
		fieldStat1 := FieldStats{
			FieldID: 1,
			Type:    schemapb.DataType_Int64,
			Max:     NewInt64FieldValue(200),
			Min:     NewInt64FieldValue(100),
		}
		fieldStat2 := FieldStats{
			FieldID: 2,
			Type:    schemapb.DataType_Int64,
			Max:     NewInt64FieldValue(200),
			Min:     NewInt64FieldValue(100),
		}
		partStats.UpdateSegmentStats(1, SegmentStats{
			FieldStats: []FieldStats{fieldStat1, fieldStat2},
		})
	}
	partStats.SetVersion(100)
	assert.Equal(t, int64(100), partStats.GetVersion())
	partBytes, err := SerializePartitionStatsSnapshot(partStats)
	assert.NoError(t, err)
	assert.NotNil(t, partBytes)
	desPartStats, err := DeserializePartitionsStatsSnapshot(partBytes)
	assert.NoError(t, err)
	assert.NotNil(t, desPartStats)
	assert.Equal(t, 1, len(desPartStats.SegmentStats))
	assert.Equal(t, 2, len(desPartStats.SegmentStats[1].FieldStats))
}
