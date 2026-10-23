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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestFloatingFieldValueScalarOrder(t *testing.T) {
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Float, schemapb.DataType_Double} {
		t.Run(dataType.String(), func(t *testing.T) {
			makeValue := func(v float64) ScalarFieldValue {
				if dataType == schemapb.DataType_Float {
					return NewFloatFieldValue(float32(v))
				}
				return NewDoubleFieldValue(v)
			}
			// Two NaN signs/payloads and both zeros are distinct representations of
			// the same SQL value. NULL is not a floating-point representation.
			values := []float64{math.Inf(-1), -1, math.Copysign(0, -1), 0, 1, math.Inf(1), math.NaN(), math.Float64frombits(0xfff8000000000042)}
			ranks := []int{0, 1, 2, 2, 3, 4, 5, 5}
			for i, a := range values {
				for j, b := range values {
					left, right := makeValue(a), makeValue(b)
					require.Equal(t, ranks[i] > ranks[j], left.GT(right), "GT %v %v", a, b)
					require.Equal(t, ranks[i] >= ranks[j], left.GE(right), "GE %v %v", a, b)
					require.Equal(t, ranks[i] < ranks[j], left.LT(right), "LT %v %v", a, b)
					require.Equal(t, ranks[i] <= ranks[j], left.LE(right), "LE %v %v", a, b)
					require.Equal(t, ranks[i] == ranks[j], left.EQ(right), "EQ %v %v", a, b)
				}
			}
			for _, input := range [][]float64{{math.NaN(), 1, -1}, {-1, 1, math.NaN()}} {
				stats := FieldStats{Type: dataType}
				for _, v := range input {
					stats.UpdateMinMax(makeValue(v))
				}
				require.True(t, stats.Min.EQ(makeValue(-1)))
				require.True(t, stats.Max.EQ(makeValue(math.NaN())))
			}
		})
	}
}

func TestPartitionStatsFloatingBoundsCompatibility(t *testing.T) {
	paramtable.Init()
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Float, schemapb.DataType_Double} {
		t.Run(dataType.String(), func(t *testing.T) {
			makeStats := func(values ...float64) FieldStats {
				stats, err := NewFieldStats(101, dataType, 0)
				require.NoError(t, err)
				for _, value := range values {
					if dataType == schemapb.DataType_Float {
						stats.UpdateMinMax(NewFloatFieldValue(float32(value)))
					} else {
						stats.UpdateMinMax(NewDoubleFieldValue(value))
					}
				}
				return stats.Clone()
			}
			snapshot := NewPartitionStatsSnapshot()
			snapshot.SetVersion(42)
			for id, values := range [][]float64{{-1, 1}, {1, math.NaN()}, {math.Inf(-1), 1}, {1, math.Inf(1)}, {math.NaN()}} {
				snapshot.UpdateSegmentStats(int64(id+1), SegmentStats{FieldStats: []FieldStats{makeStats(values...)}, NumRows: len(values)})
			}
			// Older DataCoord remapping may materialize a missing stats entry as zero.
			snapshot.UpdateSegmentStats(6, SegmentStats{})
			encoded, err := SerializePartitionStatsSnapshot(snapshot)
			require.NoError(t, err)
			require.Len(t, snapshot.SegmentStats, 6, "serialization must not mutate the producer's exact stats")
			decoded, err := DeserializePartitionsStatsSnapshot(encoded)
			require.NoError(t, err)
			require.Len(t, decoded.SegmentStats, 1)
			require.True(t, decoded.SegmentStats[1].FieldStats[0].FloatBoundsComplete)
			require.True(t, decoded.SegmentStats[1].FieldStats[0].HasUsableScalarBounds())

			// The old JSON shape has plain numeric bounds and no knowledge of the new
			// marker. It must decode the new snapshot without strings or NaN-as-null.
			var legacy struct {
				SegmentStats map[int64]struct {
					FieldStats []struct {
						FieldID  int64
						Type     schemapb.DataType
						Min, Max float64
					}
					NumRows int
				} `json:"segmentStats"`
			}
			require.NoError(t, json.Unmarshal(encoded, &legacy))
			require.Len(t, legacy.SegmentStats, 1)
			require.Equal(t, -1.0, legacy.SegmentStats[1].FieldStats[0].Min)
			require.Equal(t, 1.0, legacy.SegmentStats[1].FieldStats[0].Max)
			require.Equal(t, 2, legacy.SegmentStats[1].NumRows)

			// A legacy writer could silently ignore a NaN after finite values. Neither
			// reloading nor extending those bounds is proof that they are complete.
			old := makeStats(-1, 1)
			old.FloatBoundsComplete = false
			oldJSON, err := json.Marshal(old)
			require.NoError(t, err)
			var restored FieldStats
			require.NoError(t, json.Unmarshal(oldJSON, &restored))
			require.False(t, restored.HasUsableScalarBounds())
			restored.UpdateMinMax(restored.Max)
			require.False(t, restored.Clone().FloatBoundsComplete)
		})
	}
}
