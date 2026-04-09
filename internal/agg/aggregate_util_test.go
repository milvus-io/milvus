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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
