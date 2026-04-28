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

package queryutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestReduceByPKOperator_Name(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	assert.Equal(t, OpReduceByPK, op.Name())
}

func TestReduceByPKOperator_EmptyInput(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	ctx := context.Background()

	// Test with nil input
	results := []*internalpb.RetrieveResults{nil, nil}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)
	assert.NotNil(t, outputs[0])
}

func TestReduceByPKOperator_SingleResult(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "id",
				FieldId:   1,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
						},
					},
				},
			},
		},
	}

	results := []*internalpb.RetrieveResults{result}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)
	require.NotNil(t, outputs[0])

	merged := outputs[0].(*internalpb.RetrieveResults)
	assert.Equal(t, int64(1), merged.GetIds().GetIntId().GetData()[0])
	assert.Equal(t, int64(2), merged.GetIds().GetIntId().GetData()[1])
	assert.Equal(t, int64(3), merged.GetIds().GetIntId().GetData()[2])
}

func TestReduceByPKOperator_MultipleResults(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	ctx := context.Background()

	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 3, 5}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 300, 500}},
						},
					},
				},
			},
		},
	}

	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{2, 4, 6}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{200, 400, 600}},
						},
					},
				},
			},
		},
	}

	results := []*internalpb.RetrieveResults{result1, result2}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)
	require.NotNil(t, outputs[0])

	merged := outputs[0].(*internalpb.RetrieveResults)
	// Should be merged by PK order: 1, 2, 3, 4, 5, 6
	ids := merged.GetIds().GetIntId().GetData()
	assert.Equal(t, 6, len(ids))
	assert.Equal(t, int64(1), ids[0])
	assert.Equal(t, int64(2), ids[1])
	assert.Equal(t, int64(3), ids[2])
	assert.Equal(t, int64(4), ids[3])
	assert.Equal(t, int64(5), ids[4])
	assert.Equal(t, int64(6), ids[5])
}

func TestReduceByPKOperator_StringPK(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	ctx := context.Background()

	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{Data: []string{"a", "c", "e"}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"Alice", "Charlie", "Eve"}},
						},
					},
				},
			},
		},
	}

	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{Data: []string{"b", "d"}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"Bob", "David"}},
						},
					},
				},
			},
		},
	}

	results := []*internalpb.RetrieveResults{result1, result2}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)

	merged := outputs[0].(*internalpb.RetrieveResults)
	ids := merged.GetIds().GetStrId().GetData()
	assert.Equal(t, 5, len(ids))
	assert.Equal(t, "a", ids[0])
	assert.Equal(t, "b", ids[1])
	assert.Equal(t, "c", ids[2])
	assert.Equal(t, "d", ids[3])
	assert.Equal(t, "e", ids[4])
}

func TestReduceByPKOperator_DuplicatePK(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	ctx := context.Background()

	// Two results with overlapping PKs (2, 3 are duplicated)
	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 200, 300}},
						},
					},
				},
			},
		},
	}

	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{2, 3, 4}}, // 2, 3 duplicate
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{201, 301, 400}},
						},
					},
				},
			},
		},
	}

	// Proxy level always fails on duplicate PK (cross-shard duplication = data corruption)
	results := []*internalpb.RetrieveResults{result1, result2}
	_, err := op.Run(ctx, nil, results)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate PK")
}

func TestReduceByPKOperator_HasMoreResult(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	ctx := context.Background()

	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 300}},
						},
					},
				},
			},
		},
		HasMoreResult: true, // Has more results
	}

	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{2, 4}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{200, 400}},
						},
					},
				},
			},
		},
		HasMoreResult: false,
	}

	results := []*internalpb.RetrieveResults{result1, result2}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)

	merged := outputs[0].(*internalpb.RetrieveResults)
	// HasMoreResult should be propagated
	assert.True(t, merged.GetHasMoreResult())
}

func TestSortAndCheckPKOperator_NoDuplicate(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	ctx := context.Background()

	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 300}},
						},
					},
				},
			},
		},
	}

	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{2, 4}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{200, 400}},
						},
					},
				},
			},
		},
	}

	results := []*internalpb.RetrieveResults{result1, result2}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)

	merged := outputs[0].(*internalpb.RetrieveResults)
	ids := merged.GetIds().GetIntId().GetData()
	assert.Equal(t, []int64{1, 2, 3, 4}, ids)
}

func TestSortAndCheckPKOperator_FailOnDuplicate(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	ctx := context.Background()

	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 300}},
						},
					},
				},
			},
		},
	}

	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{2, 3}}, // PK=3 is duplicate
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{200, 301}},
						},
					},
				},
			},
		},
	}

	results := []*internalpb.RetrieveResults{result1, result2}
	_, err := op.Run(ctx, nil, results)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate PK")
	assert.Contains(t, err.Error(), "data integrity")
}

func TestReduceByPKOperator_DrainResultWithInOrder(t *testing.T) {
	// IReduceInOrder should stop when one result is drained but has HasMoreResult=true
	op := NewSortAndCheckPKOperator(reduce.IReduceInOrder, nil)
	ctx := context.Background()

	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100}},
						},
					},
				},
			},
		},
		HasMoreResult: true,
	}

	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{2, 3, 4}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{200, 300, 400}},
						},
					},
				},
			},
		},
		HasMoreResult: false,
	}

	results := []*internalpb.RetrieveResults{result1, result2}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)

	merged := outputs[0].(*internalpb.RetrieveResults)
	ids := merged.GetIds().GetIntId().GetData()
	// With IReduceInOrder, should stop when result1 is drained (HasMoreResult=true)
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, int64(1), ids[0])
}

func TestReduceByPKOperator_DrainResultWithNoOrder(t *testing.T) {
	// Use IReduceNoOrder which should NOT stop when one result is drained
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil) // Default is IReduceNoOrder
	ctx := context.Background()

	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100}},
						},
					},
				},
			},
		},
		HasMoreResult: true,
	}

	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{2, 3, 4}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{200, 300, 400}},
						},
					},
				},
			},
		},
		HasMoreResult: false,
	}

	results := []*internalpb.RetrieveResults{result1, result2}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)

	merged := outputs[0].(*internalpb.RetrieveResults)
	ids := merged.GetIds().GetIntId().GetData()
	// With IReduceNoOrder, should continue processing all results
	assert.Equal(t, 4, len(ids))
}

func TestReduceByPKWithTimestampOperator_Name(t *testing.T) {
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 0, 0, nil)
	assert.Equal(t, OpReduceByPKTS, op.Name())
}

func TestReduceByPKWithTimestampOperator_DuplicatePKWithHigherTimestamp(t *testing.T) {
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 0, 0, nil)
	ctx := context.Background()

	const timestampFieldID int64 = 1 // common.TimeStampField

	// Result1 has PK=1 with timestamp=100
	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: timestampFieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 200}}, // timestamps
						},
					},
				},
			},
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1000, 2000}}, // values
						},
					},
				},
			},
		},
	}

	// Result2 has PK=1 with timestamp=150 (higher), PK=3 with timestamp=300
	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: timestampFieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{150, 300}}, // timestamps
						},
					},
				},
			},
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1001, 3000}}, // values (1001 is newer)
						},
					},
				},
			},
		},
	}

	results := []*internalpb.RetrieveResults{result1, result2}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)

	merged := outputs[0].(*internalpb.RetrieveResults)
	ids := merged.GetIds().GetIntId().GetData()

	// Should have PKs: 1, 2, 3
	assert.Equal(t, 3, len(ids))
	assert.Equal(t, int64(1), ids[0])
	assert.Equal(t, int64(2), ids[1])
	assert.Equal(t, int64(3), ids[2])

	// For PK=1, should use result2's value (1001) because timestamp is higher (150 > 100)
	values := merged.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	assert.Equal(t, int64(1001), values[0]) // PK=1 from result2 (higher timestamp)
	assert.Equal(t, int64(2000), values[1]) // PK=2 from result1
	assert.Equal(t, int64(3000), values[2]) // PK=3 from result2
}

func TestReduceByPKWithTimestampOperator_NoTimestampField(t *testing.T) {
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 0, 0, nil)
	ctx := context.Background()

	// Results without timestamp field - should still work (fallback to first seen)
	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 200}},
						},
					},
				},
			},
		},
	}

	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{101, 300}},
						},
					},
				},
			},
		},
	}

	results := []*internalpb.RetrieveResults{result1, result2}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)

	merged := outputs[0].(*internalpb.RetrieveResults)
	ids := merged.GetIds().GetIntId().GetData()

	// Should deduplicate by PK, keeping first seen
	assert.Equal(t, 3, len(ids))
}

// =========================================================================
// Element-Level Tests
// =========================================================================

// makeElementLevelResult is a helper to create element-level RetrieveResults for tests.
func makeElementLevelResult(pks []int64, timestamps []int64, elemIndices [][]int32) *internalpb.RetrieveResults {
	const timestampFieldID int64 = 1

	fieldsData := []*schemapb.FieldData{
		{
			Type:    schemapb.DataType_Int64,
			FieldId: timestampFieldID,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: timestamps},
					},
				},
			},
		},
	}

	elemIdxList := make([]*internalpb.ElementIndices, len(elemIndices))
	for i, indices := range elemIndices {
		elemIdxList[i] = &internalpb.ElementIndices{Indices: indices}
	}

	return &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: pks},
			},
		},
		FieldsData:     fieldsData,
		ElementLevel:   true,
		ElementIndices: elemIdxList,
	}
}

func TestReduceByPKWithTimestamp_ElementLevel_MergeAndDedup(t *testing.T) {
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 0, 0, nil)
	ctx := context.Background()

	// r1: pk=1 (ts=100, elements [0,1]), pk=3 (ts=100, elements [2])
	r1 := makeElementLevelResult(
		[]int64{1, 3},
		[]int64{100, 100},
		[][]int32{{0, 1}, {2}},
	)
	// r2: pk=1 (ts=200, elements [0,1,2] — newer), pk=2 (ts=100, elements [0])
	r2 := makeElementLevelResult(
		[]int64{1, 2},
		[]int64{200, 100},
		[][]int32{{0, 1, 2}, {0}},
	)

	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)

	result := outputs[0].(*internalpb.RetrieveResults)
	assert.True(t, result.GetElementLevel())

	ids := result.GetIds().GetIntId().GetData()
	assert.Equal(t, []int64{1, 2, 3}, ids)

	// pk=1 should have r2's indices (newer ts=200): [0,1,2]
	assert.Equal(t, []int32{0, 1, 2}, result.GetElementIndices()[0].GetIndices())
	// pk=2: [0]
	assert.Equal(t, []int32{0}, result.GetElementIndices()[1].GetIndices())
	// pk=3: [2]
	assert.Equal(t, []int32{2}, result.GetElementIndices()[2].GetIndices())
}

func TestReduceByPKOperator_ElementLevel_Propagation(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	ctx := context.Background()

	// Two element-level results with no overlapping PKs
	r1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 300}},
						},
					},
				},
			},
		},
		ElementLevel: true,
		ElementIndices: []*internalpb.ElementIndices{
			{Indices: []int32{0, 1}},
			{Indices: []int32{2}},
		},
	}

	r2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{2}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{200}},
						},
					},
				},
			},
		},
		ElementLevel: true,
		ElementIndices: []*internalpb.ElementIndices{
			{Indices: []int32{0, 1, 2}},
		},
	}

	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)

	result := outputs[0].(*internalpb.RetrieveResults)
	assert.True(t, result.GetElementLevel())

	ids := result.GetIds().GetIntId().GetData()
	assert.Equal(t, []int64{1, 2, 3}, ids)

	// Verify element indices are preserved in PK order
	assert.Equal(t, []int32{0, 1}, result.GetElementIndices()[0].GetIndices())    // pk=1
	assert.Equal(t, []int32{0, 1, 2}, result.GetElementIndices()[1].GetIndices()) // pk=2
	assert.Equal(t, []int32{2}, result.GetElementIndices()[2].GetIndices())       // pk=3
}

// =========================================================================
// maxOutputSize edge cases for ReduceByPKWithTimestampOperator
// =========================================================================

// TestReduceByPKWithTimestampOperator_MaxOutputSize_ExactlyAtLimit verifies that a total
// size equal to maxOutputSize is accepted (check is strict >).
// Each Int64 field row = 8 bytes; 3 rows × 8 bytes = 24 bytes.
func TestReduceByPKWithTimestampOperator_MaxOutputSize_ExactlyAtLimit(t *testing.T) {
	const rowSize = 8 // Int64 field bytes per row
	const rowCount = 3
	const exactLimit int64 = rowSize * rowCount // 24 bytes

	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, exactLimit, 0, nil)
	ctx := context.Background()

	r := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}}},
					},
				},
			},
		},
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.Equal(t, 3, len(result.GetIds().GetIntId().GetData()))
}

// TestReduceByPKWithTimestampOperator_MaxOutputSize_Exceeded verifies that exceeding
// maxOutputSize by one byte returns an error.
func TestReduceByPKWithTimestampOperator_MaxOutputSize_Exceeded(t *testing.T) {
	const rowSize = 8 // Int64 field bytes per row
	const rowCount = 3
	const limitOneByte int64 = rowSize*rowCount - 1 // 23 bytes

	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, limitOneByte, 0, nil)
	ctx := context.Background()

	r := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}}},
					},
				},
			},
		},
	}
	_, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "maxOutputSize")
}
