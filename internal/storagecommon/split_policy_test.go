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

package storagecommon

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func AssertSplitEqual(t *testing.T, expect, actual *currentSplit) {
	if expect == nil && actual == nil {
		return
	}
	assert.Equal(t, expect.processFields.Len(), actual.processFields.Len())
	for _, field := range expect.processFields.Collect() {
		assert.True(t, actual.processFields.Contain(field))
	}

	assert.Equal(t, len(expect.outputGroups), len(actual.outputGroups))
	for i := range expect.outputGroups {
		assert.Equal(t, expect.outputGroups[i].GroupID, actual.outputGroups[i].GroupID)
		assert.Equal(t, expect.outputGroups[i].Columns, actual.outputGroups[i].Columns)
		assert.Equal(t, expect.outputGroups[i].Fields, actual.outputGroups[i].Fields)
	}
}

func TestWideDataTypePolicy(t *testing.T) {
	type testCase struct {
		tag    string
		input  *currentSplit
		expect *currentSplit
	}

	cases := []testCase{
		{
			tag: "float_vector",
			input: newCurrentSplit([]*schemapb.FieldSchema{
				{
					FieldID:  0,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  1,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  100,
					DataType: schemapb.DataType_FloatVector,
				},
			}, nil),
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](100),
				outputGroups: []ColumnGroup{
					{
						GroupID: 100,
						Columns: []int{2},
						Fields:  []int64{100},
					},
				},
			},
		},
		{
			tag: "text_with_processed_group",
			input: &currentSplit{
				fields: []*schemapb.FieldSchema{
					{
						FieldID:  0,
						DataType: schemapb.DataType_Int64,
					},
					{
						FieldID:  1,
						DataType: schemapb.DataType_Int64,
					},
					{
						FieldID:      100,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						FieldID:      101,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Text,
					},
				},
				processFields: typeutil.NewSet[int64](0, 1, 100),
				outputGroups: []ColumnGroup{
					{
						GroupID: 0,
						Columns: []int{0, 1, 2},
						Fields:  []int64{0, 1, 100},
					},
				},
			},
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](0, 1, 100, 101),
				outputGroups: []ColumnGroup{
					{
						GroupID: 0,
						Columns: []int{0, 1, 2},
						Fields:  []int64{0, 1, 100},
					},
					{
						GroupID: 101,
						Columns: []int{3},
						Fields:  []int64{101},
					},
				},
			},
		},
	}

	policy := selectedDataTypePolicy{}
	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			result := policy.Split(tc.input)

			AssertSplitEqual(t, tc.expect, result)
		})
	}
}

func TestSystemColumnPolicy(t *testing.T) {
	type testCase struct {
		tag                  string
		includePK            bool
		includePartKey       bool
		includeClusteringKey bool
		input                *currentSplit
		expect               *currentSplit
	}

	cases := []testCase{
		{
			tag: "normal_include_pk",
			input: newCurrentSplit([]*schemapb.FieldSchema{
				{
					FieldID:  0,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  1,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:      100,
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
				{
					FieldID:  101,
					DataType: schemapb.DataType_FloatVector,
				},
			}, nil),
			includePK: true,
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](0, 1, 100),
				outputGroups: []ColumnGroup{
					{
						GroupID: 0,
						Columns: []int{0, 1, 2},
						Fields:  []int64{0, 1, 100},
					},
				},
			},
		},
		{
			tag: "normal_include_partition_key",
			input: newCurrentSplit([]*schemapb.FieldSchema{
				{
					FieldID:  0,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  1,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:      100,
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
				{
					FieldID:  101,
					DataType: schemapb.DataType_FloatVector,
				},
				{
					FieldID:        102,
					DataType:       schemapb.DataType_Int64,
					IsPartitionKey: true,
				},
			}, nil),
			includePK:      true,
			includePartKey: true,
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](0, 1, 100, 102),
				outputGroups: []ColumnGroup{
					{
						GroupID: 0,
						Columns: []int{0, 1, 2, 4},
						Fields:  []int64{0, 1, 100, 102},
					},
				},
			},
		},
		{
			tag: "normal_include_clustering_key",
			input: newCurrentSplit([]*schemapb.FieldSchema{
				{
					FieldID:  0,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  1,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:      100,
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
				{
					FieldID:  101,
					DataType: schemapb.DataType_FloatVector,
				},
				{
					FieldID:         102,
					DataType:        schemapb.DataType_Int64,
					IsClusteringKey: true,
				},
			}, nil),
			includePK:            true,
			includeClusteringKey: true,
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](0, 1, 100, 102),
				outputGroups: []ColumnGroup{
					{
						GroupID: 0,
						Columns: []int{0, 1, 2, 4},
						Fields:  []int64{0, 1, 100, 102},
					},
				},
			},
		},
		{
			tag: "normal_with_processed_not_include_pk",
			input: &currentSplit{
				fields: []*schemapb.FieldSchema{
					{
						FieldID:  0,
						DataType: schemapb.DataType_Int64,
					},
					{
						FieldID:  1,
						DataType: schemapb.DataType_Int64,
					},
					{
						FieldID:      100,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						FieldID:  101,
						DataType: schemapb.DataType_SparseFloatVector,
					},
				},
				processFields: typeutil.NewSet[int64](101),
				outputGroups: []ColumnGroup{
					{
						GroupID: 101,
						Columns: []int{3},
						Fields:  []int64{101},
					},
				},
			},
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](0, 1, 101),
				outputGroups: []ColumnGroup{
					{
						GroupID: 101,
						Columns: []int{3},
						Fields:  []int64{101},
					},
					{
						GroupID: 0,
						Columns: []int{0, 1},
						Fields:  []int64{0, 1},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			policy := &systemColumnPolicy{
				includePrimaryKey:    tc.includePK,
				includePartitionKey:  tc.includePartKey,
				includeClusteringKey: tc.includeClusteringKey,
			}
			result := policy.Split(tc.input)

			AssertSplitEqual(t, tc.expect, result)
		})
	}
}

func TestRemanentShortPolicy(t *testing.T) {
	type testCase struct {
		tag          string
		maxGroupSize int
		input        *currentSplit
		expect       *currentSplit
	}

	cases := []testCase{
		{
			tag: "normal_remanent_nolimit",
			input: &currentSplit{
				fields: []*schemapb.FieldSchema{
					{
						FieldID:  0,
						DataType: schemapb.DataType_Int64,
					},
					{
						FieldID:  1,
						DataType: schemapb.DataType_Int64,
					},
					{
						FieldID:      100,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
					},
					{
						FieldID:  101,
						DataType: schemapb.DataType_FloatVector,
					},
					{
						FieldID:  102,
						DataType: schemapb.DataType_VarChar,
					},
					{
						FieldID:  103,
						DataType: schemapb.DataType_Float,
					},
					{
						FieldID:  104,
						DataType: schemapb.DataType_Bool,
					},
				},
				processFields: typeutil.NewSet[int64](0, 1, 100, 101),
				outputGroups: []ColumnGroup{
					{
						GroupID: 101,
						Columns: []int{3},
						Fields:  []int64{101},
					},
					{
						GroupID: 0,
						Columns: []int{0, 1, 2},
						Fields:  []int64{0, 1, 100},
					},
				},
				nextGroupID: 1,
			},
			maxGroupSize: -1,
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](0, 1, 100, 101, 102, 103, 104),
				outputGroups: []ColumnGroup{
					{
						GroupID: 101,
						Columns: []int{3},
						Fields:  []int64{101},
					},
					{
						GroupID: 0,
						Columns: []int{0, 1, 2},
						Fields:  []int64{0, 1, 100},
					},
					{
						GroupID: 1,
						Columns: []int{4, 5, 6},
						Fields:  []int64{102, 103, 104},
					},
				},
			},
		},
		{
			tag: "with_group_size=2",
			input: &currentSplit{
				fields: []*schemapb.FieldSchema{
					{
						FieldID:  0,
						DataType: schemapb.DataType_Int64,
					},
					{
						FieldID:  1,
						DataType: schemapb.DataType_Int64,
					},
					{
						FieldID:      100,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
					},
					{
						FieldID:  101,
						DataType: schemapb.DataType_FloatVector,
					},
					{
						FieldID:  102,
						DataType: schemapb.DataType_VarChar,
					},
					{
						FieldID:  103,
						DataType: schemapb.DataType_Float,
					},
					{
						FieldID:  104,
						DataType: schemapb.DataType_Bool,
					},
				},
				processFields: typeutil.NewSet[int64](0, 1, 101),
				outputGroups: []ColumnGroup{
					{
						GroupID: 101,
						Columns: []int{3},
						Fields:  []int64{101},
					},
					{
						GroupID: 0,
						Columns: []int{0, 1},
						Fields:  []int64{0, 1},
					},
				},
				nextGroupID: 1,
			},
			maxGroupSize: 2,
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](0, 1, 100, 101, 102, 103, 104),
				outputGroups: []ColumnGroup{
					{
						GroupID: 101,
						Columns: []int{3},
						Fields:  []int64{101},
					},
					{
						GroupID: 0,
						Columns: []int{0, 1},
						Fields:  []int64{0, 1},
					},
					{
						GroupID: 1,
						Columns: []int{2, 4},
						Fields:  []int64{100, 102},
					},
					{
						GroupID: 2,
						Columns: []int{5, 6},
						Fields:  []int64{103, 104},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			policy := NewRemanentShortPolicy(tc.maxGroupSize)
			result := policy.Split(tc.input)

			AssertSplitEqual(t, tc.expect, result)
		})
	}
}

func TestAvgSizePolicy(t *testing.T) {
	type testCase struct {
		tag           string
		sizeThreshold int64
		input         *currentSplit
		expect        *currentSplit
	}

	cases := []testCase{
		{
			tag: "over_threshold",
			input: newCurrentSplit([]*schemapb.FieldSchema{
				{
					FieldID:  0,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  1,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:      100,
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					DataType: schemapb.DataType_VarChar,
				},
			}, map[int64]ColumnStats{
				101: {
					AvgSize: 512,
					MaxSize: 1024,
				},
			}),
			sizeThreshold: 500,
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](101),
				outputGroups: []ColumnGroup{
					{
						GroupID: 101,
						Columns: []int{3},
						Fields:  []int64{101},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			policy := NewAvgSizePolicy(tc.sizeThreshold)
			result := policy.Split(tc.input)

			AssertSplitEqual(t, tc.expect, result)
		})
	}
}
