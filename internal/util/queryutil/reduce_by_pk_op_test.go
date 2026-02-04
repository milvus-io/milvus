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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func TestReduceByPKOperator_Name(t *testing.T) {
	op := NewReduceByPKOperator()
	assert.Equal(t, OpReduceByPK, op.Name())
}

func TestReduceByPKOperator_EmptyInput(t *testing.T) {
	op := NewReduceByPKOperator()
	ctx := context.Background()

	// Test with nil input
	results := []*internalpb.RetrieveResults{nil, nil}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)
	assert.NotNil(t, outputs[0])
}

func TestReduceByPKOperator_SingleResult(t *testing.T) {
	op := NewReduceByPKOperator()
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
	op := NewReduceByPKOperator()
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
	op := NewReduceByPKOperator()
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
	op := NewReduceByPKOperator()
	ctx := context.Background()

	// Two results with overlapping PKs (1, 2, 3)
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

	results := []*internalpb.RetrieveResults{result1, result2}
	outputs, err := op.Run(ctx, nil, results)
	require.NoError(t, err)

	merged := outputs[0].(*internalpb.RetrieveResults)
	ids := merged.GetIds().GetIntId().GetData()
	// Should be deduplicated: 1, 2, 3, 4
	assert.Equal(t, 4, len(ids))
	assert.Equal(t, int64(1), ids[0])
	assert.Equal(t, int64(2), ids[1])
	assert.Equal(t, int64(3), ids[2])
	assert.Equal(t, int64(4), ids[3])

	// Values should come from the first seen (result1 for 2, 3)
	values := merged.GetFieldsData()[0].GetScalars().GetLongData().GetData()
	assert.Equal(t, int64(100), values[0]) // PK=1 from result1
	assert.Equal(t, int64(200), values[1]) // PK=2 from result1 (first seen)
	assert.Equal(t, int64(300), values[2]) // PK=3 from result1 (first seen)
	assert.Equal(t, int64(400), values[3]) // PK=4 from result2
}

func TestReduceByPKOperator_HasMoreResult(t *testing.T) {
	op := NewReduceByPKOperator()
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

func TestReduceByPKOperator_DrainResultWithInOrder(t *testing.T) {
	// Use IReduceInOrder which should stop when one result is drained but has more
	op := NewReduceByPKOperatorWithType(reduce.IReduceInOrder)
	ctx := context.Background()

	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1}}, // Only 1 row
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
		HasMoreResult: true, // But has more on server
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
	// With IReduceInOrder, should stop when result1 is drained (has HasMoreResult=true)
	// So only PK=1 should be included
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, int64(1), ids[0])
}

func TestReduceByPKOperator_DrainResultWithNoOrder(t *testing.T) {
	// Use IReduceNoOrder which should NOT stop when one result is drained
	op := NewReduceByPKOperator() // Default is IReduceNoOrder
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
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder)
	assert.Equal(t, OpReduceByPKTS, op.Name())
}

func TestReduceByPKWithTimestampOperator_DuplicatePKWithHigherTimestamp(t *testing.T) {
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder)
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
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder)
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
