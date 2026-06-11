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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
		assert.Equal(t, expect.outputGroups[i].Format, actual.outputGroups[i].Format)
	}
}

func AssertPendingGroupsEqual(t *testing.T, expect []ColumnGroup, actual *currentSplit) {
	groups := actual.RangeGroups(nil)
	assert.Equal(t, len(expect), len(groups))
	for i := range expect {
		assert.Equal(t, expect[i].Columns, groups[i].indices)
		assert.Equal(t, expect[i].Fields, groups[i].fields)
		assert.Equal(t, expect[i].Format, storageFormatForLocalFormat(groups[i].localFormat))
	}
}

func TestWideDataTypePolicy(t *testing.T) {
	type testCase struct {
		tag    string
		input  *currentSplit
		expect *currentSplit
	}

	localFormatParam := func(format string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{
			{
				Key:   common.LocalFormatKey,
				Value: format,
			},
		}
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
			tag: "text_with_vortex_local_format",
			input: newCurrentSplit([]*schemapb.FieldSchema{
				{
					FieldID:  100,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:    101,
					DataType:   schemapb.DataType_Text,
					TypeParams: localFormatParam(common.LocalFormatVortex),
				},
			}, nil),
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](101),
				outputGroups: []ColumnGroup{
					{
						GroupID: 101,
						Columns: []int{1},
						Fields:  []int64{101},
						Format:  common.LocalFormatVortex,
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

func TestLocalFormatPolicy(t *testing.T) {
	type testCase struct {
		tag    string
		input  *currentSplit
		expect *currentSplit
	}

	localFormatParam := func(format string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{
			{
				Key:   common.LocalFormatKey,
				Value: format,
			},
		}
	}

	cases := []testCase{
		{
			tag: "mixed_local_formats",
			input: newCurrentSplit([]*schemapb.FieldSchema{
				{
					FieldID:  100,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:    101,
					DataType:   schemapb.DataType_VarChar,
					TypeParams: localFormatParam(common.LocalFormatVortex),
				},
				{
					FieldID:  102,
					DataType: schemapb.DataType_Double,
				},
				{
					FieldID:    103,
					DataType:   schemapb.DataType_Int64,
					TypeParams: localFormatParam(common.LocalFormatVortex),
				},
			}, nil),
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](),
			},
		},
		{
			tag: "single_vortex_local_format_partitions_without_output",
			input: newCurrentSplit([]*schemapb.FieldSchema{
				{
					FieldID:    100,
					DataType:   schemapb.DataType_Int64,
					TypeParams: localFormatParam(common.LocalFormatVortex),
				},
				{
					FieldID:    101,
					DataType:   schemapb.DataType_Double,
					TypeParams: localFormatParam(common.LocalFormatVortex),
				},
			}, nil),
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](),
			},
		},
	}

	policy := NewLocalFormatPolicy()
	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			result := policy.Split(tc.input)

			AssertSplitEqual(t, tc.expect, result)
			switch tc.tag {
			case "mixed_local_formats":
				AssertPendingGroupsEqual(t, []ColumnGroup{
					{
						Columns: []int{0, 2},
						Fields:  []int64{100, 102},
					},
					{
						Columns: []int{1, 3},
						Fields:  []int64{101, 103},
						Format:  common.LocalFormatVortex,
					},
				}, result)
			case "single_vortex_local_format_partitions_without_output":
				AssertPendingGroupsEqual(t, []ColumnGroup{
					{
						Columns: []int{0, 1},
						Fields:  []int64{100, 101},
						Format:  common.LocalFormatVortex,
					},
				}, result)
			}
		})
	}
}

func TestSplitColumnsSeparatesLocalFormatsBeforeRemanent(t *testing.T) {
	localFormatParam := func(format string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{
			{
				Key:   common.LocalFormatKey,
				Value: format,
			},
		}
	}

	fields := []*schemapb.FieldSchema{
		{
			FieldID:  100,
			DataType: schemapb.DataType_Int64,
		},
		{
			FieldID:    101,
			DataType:   schemapb.DataType_Int64,
			TypeParams: localFormatParam(common.LocalFormatVortex),
		},
		{
			FieldID:  102,
			DataType: schemapb.DataType_FloatVector,
		},
		{
			FieldID:  103,
			DataType: schemapb.DataType_Double,
		},
		{
			FieldID:    104,
			DataType:   schemapb.DataType_Int64,
			TypeParams: localFormatParam(common.LocalFormatVortex),
		},
	}

	result := SplitColumns(fields,
		map[int64]ColumnStats{},
		NewLocalFormatPolicy(),
		NewSelectedDataTypePolicy(),
		NewRemanentShortPolicy(-1))

	assert.Equal(t, []ColumnGroup{
		{
			GroupID: 0,
			Columns: []int{0, 3},
			Fields:  []int64{100, 103},
		},
		{
			GroupID: 1,
			Columns: []int{1, 4},
			Fields:  []int64{101, 104},
			Format:  common.LocalFormatVortex,
		},
		{
			GroupID: 102,
			Columns: []int{2},
			Fields:  []int64{102},
		},
	}, result)
}

func TestLocalFormatPolicyKeepsLaterSplitsWithinFormat(t *testing.T) {
	localFormatParam := func(format string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{
			{
				Key:   common.LocalFormatKey,
				Value: format,
			},
		}
	}

	fields := []*schemapb.FieldSchema{
		{
			FieldID:  100,
			DataType: schemapb.DataType_Int64,
		},
		{
			FieldID:    101,
			DataType:   schemapb.DataType_Int64,
			TypeParams: localFormatParam(common.LocalFormatVortex),
		},
		{
			FieldID:  102,
			DataType: schemapb.DataType_Double,
		},
		{
			FieldID:    103,
			DataType:   schemapb.DataType_Double,
			TypeParams: localFormatParam(common.LocalFormatVortex),
		},
	}

	result := SplitColumns(fields,
		map[int64]ColumnStats{},
		NewLocalFormatPolicy(),
		NewRemanentShortPolicy(1))

	assert.Equal(t, []ColumnGroup{
		{
			GroupID: 0,
			Columns: []int{0},
			Fields:  []int64{100},
		},
		{
			GroupID: 1,
			Columns: []int{2},
			Fields:  []int64{102},
		},
		{
			GroupID: 2,
			Columns: []int{1},
			Fields:  []int64{101},
			Format:  common.LocalFormatVortex,
		},
		{
			GroupID: 3,
			Columns: []int{3},
			Fields:  []int64{103},
			Format:  common.LocalFormatVortex,
		},
	}, result)
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

	localFormatParam := func(format string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{
			{
				Key:   common.LocalFormatKey,
				Value: format,
			},
		}
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
			tag: "include_pk_respects_local_format_partitions",
			input: func() *currentSplit {
				split := newCurrentSplit([]*schemapb.FieldSchema{
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
						TypeParams:   localFormatParam(common.LocalFormatVortex),
					},
					{
						FieldID:  101,
						DataType: schemapb.DataType_FloatVector,
					},
				}, nil)
				split.PartitionRemainingByLocalFormat()
				return split
			}(),
			includePK: true,
			expect: &currentSplit{
				processFields: typeutil.NewSet[int64](0, 1, 100),
				outputGroups: []ColumnGroup{
					{
						GroupID: 0,
						Columns: []int{0, 1},
						Fields:  []int64{0, 1},
					},
					{
						GroupID: 1,
						Columns: []int{2},
						Fields:  []int64{100},
						Format:  common.LocalFormatVortex,
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
		{
			tag: "over_threshold_preserves_vortex_local_format",
			input: newCurrentSplit([]*schemapb.FieldSchema{
				{
					FieldID:  100,
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.LocalFormatKey,
							Value: common.LocalFormatVortex,
						},
					},
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
						Columns: []int{1},
						Fields:  []int64{101},
						Format:  common.LocalFormatVortex,
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
