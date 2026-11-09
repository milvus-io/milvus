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

package agg

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestNewAggregationFieldMap_GroupByInvalidField(t *testing.T) {
	// GROUP BY query with an output field that's neither group_by column nor aggregate
	countAggs, err := NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggs := make([]AggregateBase, len(countAggs))
	copy(aggs, countAggs)

	_, err = NewAggregationFieldMap(
		[]string{"category", "count(*)", "invalid_field"}, // "invalid_field" is not groupBy or agg
		[]string{"category"},                              // groupBy
		aggs,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_field")
	assert.Contains(t, err.Error(), "GROUP BY")
	assert.Contains(t, err.Error(), "category") // should list valid targets
}

func TestNewAggregationFieldMap_GlobalAggInvalidField(t *testing.T) {
	// Global aggregation (no GROUP BY) with a regular column mixed in
	countAggs, err := NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggs := make([]AggregateBase, len(countAggs))
	copy(aggs, countAggs)

	_, err = NewAggregationFieldMap(
		[]string{"count(*)", "int64"}, // "int64" is not an aggregate
		[]string{},                    // no groupBy (global aggregation)
		aggs,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "int64")
	assert.Contains(t, err.Error(), "aggregation functions")
	assert.NotContains(t, err.Error(), "GROUP BY") // should NOT mention GROUP BY
}

func TestNewAggregationFieldMap_ValidGroupBy(t *testing.T) {
	countAggs, err := NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggs := make([]AggregateBase, len(countAggs))
	copy(aggs, countAggs)

	aggMap, err := NewAggregationFieldMap(
		[]string{"category", "count(*)"},
		[]string{"category"},
		aggs,
	)
	require.NoError(t, err)
	assert.Equal(t, 2, aggMap.Count())
	assert.Equal(t, "category", aggMap.NameAt(0))
	assert.Equal(t, "count(*)", aggMap.NameAt(1))
}

func TestNewAggregationFieldMap_ValidGlobalAgg(t *testing.T) {
	countAggs, err := NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggs := make([]AggregateBase, len(countAggs))
	copy(aggs, countAggs)

	aggMap, err := NewAggregationFieldMap(
		[]string{"count(*)"},
		[]string{},
		aggs,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, aggMap.Count())
	assert.Equal(t, "count(*)", aggMap.NameAt(0))
}

// TestComputeVarianceFromCountAndM2 covers [1, 2, 3, NULL] (count 3, M2 2),
// an empty or all-NULL group (count 0), a singleton, 2 values and an M2 that
// overflowed to +Inf.
func TestComputeVarianceFromCountAndM2(t *testing.T) {
	count := genEmptyLongFieldData(schemapb.DataType_Int64, []int64{3, 0, 1, 2, 2})
	m2, err := genEmptyFieldDataByType(schemapb.DataType_Double)
	require.NoError(t, err)
	inf := math.Inf(1)
	m2.GetScalars().GetDoubleData().Data = []float64{2, 0, 0, 0.5, inf}
	m2.ValidData = []bool{true, false, true, true, true}

	nan := math.NaN()
	for op, want := range map[string][]float64{
		"var_pop":     {2.0 / 3, nan, 0, 0.25, inf},
		"var_samp":    {1, nan, nan, 0.5, inf},
		"stddev_pop":  {math.Sqrt(2.0 / 3), nan, 0, 0.5, inf},
		"stddev_samp": {1, nan, nan, math.Sqrt(0.5), inf},
	} {
		res, err := ComputeVarianceFromCountAndM2(op, count, m2)
		require.NoError(t, err)
		require.Equal(t, schemapb.DataType_Double, res.GetType())
		got := res.GetScalars().GetDoubleData().GetData()
		require.Len(t, got, len(want))
		for i, w := range want {
			require.Equal(t, !math.IsNaN(w), res.GetValidData()[i], "%s row %d", op, i)
			if !math.IsNaN(w) {
				require.InDelta(t, w, got[i], 1e-15, "%s row %d", op, i)
			}
		}
	}

	for _, bad := range []float64{-1, nan} {
		m2.GetScalars().GetDoubleData().Data[0] = bad
		_, err = ComputeVarianceFromCountAndM2("var_pop", count, m2)
		require.ErrorContains(t, err, "invalid variance state", "m2 %v", bad)
	}
	m2.GetScalars().GetDoubleData().Data[0] = 2
	m2.ValidData[0] = false
	_, err = ComputeVarianceFromCountAndM2("var_pop", count, m2)
	require.ErrorContains(t, err, "invalid variance state")
	_, err = ComputeVarianceFromCountAndM2("var_pop", count, count)
	require.ErrorContains(t, err, "invalid variance state")
}

// TestAggregationFieldMapFinalize finalizes repeated avg and var_pop fields,
// which share their state columns, and rejects a field whose column count
// does not match its function.
func TestAggregationFieldMapFinalize(t *testing.T) {
	avg, err := NewAggregate(kAvg, 101, "avg(x)", schemapb.DataType_Double)
	require.NoError(t, err)
	vp, err := NewAggregate(kVarPop, 101, "var_pop(x)", schemapb.DataType_Double)
	require.NoError(t, err)
	fields := []string{"var_pop(x)", "avg(x)", "var_pop(x)", "avg(x)"}
	fieldMap, err := NewAggregationFieldMap(fields, nil, append(vp, avg...))
	require.NoError(t, err)
	double := func(v float64) *schemapb.FieldData {
		fd, err := genEmptyFieldDataByType(schemapb.DataType_Double)
		require.NoError(t, err)
		fd.GetScalars().GetDoubleData().Data = []float64{v}
		return fd
	}
	long := func(v int64) *schemapb.FieldData { return genEmptyLongFieldData(schemapb.DataType_Int64, []int64{v}) }
	// var_pop of {1, 3}: mean 2, count 2, m2 2. avg: sum 4, count 2.
	cols := []*schemapb.FieldData{double(2), long(2), double(2), double(4), long(2)}
	out, err := fieldMap.Finalize(cols)
	require.NoError(t, err)
	require.Len(t, out, 4)
	for i, want := range []float64{1, 2, 1, 2} {
		require.Equal(t, fields[i], out[i].GetFieldName())
		require.InDelta(t, want, out[i].GetScalars().GetDoubleData().GetData()[0], 1e-15, fields[i])
	}

	// A plan that was not deduplicated gives var_pop(x) 6 columns.
	fieldMap, err = NewAggregationFieldMap([]string{"var_pop(x)"}, nil, append(vp, vp...))
	require.NoError(t, err)
	_, err = fieldMap.Finalize(append(cols[:3:3], cols[:3]...))
	require.ErrorContains(t, err, "has 6 state columns")
}
