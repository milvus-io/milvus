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
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// makePKField creates a PK field (position 0 in segcore layout).
func makePKField(ids []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "pk",
		FieldId:   1,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: ids},
				},
			},
		},
	}
}

func TestOrderByLimitOperator_Name(t *testing.T) {
	op := NewOrderByLimitOperator(nil, 0)
	assert.Equal(t, OpOrderByLimit, op.Name())
}

func TestOrderByLimitOperator_EmptyInput(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{}
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	assert.NotNil(t, outputs[0])
}

func TestOrderByLimitOperator_SingleRow(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField([]int64{1}),
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	assert.Equal(t, int64(1), sorted.GetIds().GetIntId().GetData()[0])
}

func TestOrderByLimitOperator_SortAscending(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{3, 1, 2}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField([]int64{3, 1, 2}),
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{300, 100, 200}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	values := sorted.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	assert.Equal(t, int64(100), values[0])
	assert.Equal(t, int64(200), values[1])
	assert.Equal(t, int64(300), values[2])
}

func TestOrderByLimitOperator_SortDescending(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: false, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 3, 2}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField([]int64{1, 3, 2}),
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 300, 200}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	values := sorted.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	assert.Equal(t, int64(300), values[0])
	assert.Equal(t, int64(200), values[1])
	assert.Equal(t, int64(100), values[2])
}

func TestOrderByLimitOperator_SortStrings(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "name", Ascending: true, DataType: schemapb.DataType_VarChar},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{3, 1, 2, 4}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField([]int64{3, 1, 2, 4}),
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"Charlie", "Alice", "Bob", "David"}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	names := sorted.GetFieldsData()[1].GetScalars().GetStringData().GetData()
	assert.Equal(t, "Alice", names[0])
	assert.Equal(t, "Bob", names[1])
	assert.Equal(t, "Charlie", names[2])
	assert.Equal(t, "David", names[3])
}

func TestOrderByLimitOperator_MultipleFields(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "category", Ascending: true, DataType: schemapb.DataType_Int32},
		{FieldID: 3, FieldName: "value", Ascending: false, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField([]int64{1, 2, 3, 4}),
			{
				Type:      schemapb.DataType_Int32,
				FieldName: "category",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{2, 1, 1, 2}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   3,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 200, 300, 50}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	categories := sorted.GetFieldsData()[1].GetScalars().GetIntData().GetData()
	values := sorted.GetFieldsData()[2].GetScalars().GetLongData().GetData()

	// Sorted by category ASC, then value DESC:
	// (cat=1, val=300), (cat=1, val=200), (cat=2, val=100), (cat=2, val=50)
	assert.Equal(t, int32(1), categories[0])
	assert.Equal(t, int64(300), values[0])
	assert.Equal(t, int32(1), categories[1])
	assert.Equal(t, int64(200), values[1])
	assert.Equal(t, int32(2), categories[2])
	assert.Equal(t, int64(100), values[2])
	assert.Equal(t, int32(2), categories[3])
	assert.Equal(t, int64(50), values[3])
}

func TestOrderByLimitOperator_PartialSort(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, DataType: schemapb.DataType_Int64},
	}
	// topK=2 → only need top-2 rows out of 5
	op := NewOrderByLimitOperator(orderByFields, 2)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{5, 1, 4, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField([]int64{5, 1, 4, 2, 3}),
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{500, 100, 400, 200, 300}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	values := sorted.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	ids := sorted.GetIds().GetIntId().GetData()

	// Partial sort returns top-2 sorted: 100, 200
	require.Len(t, values, 2)
	assert.Equal(t, int64(100), values[0])
	assert.Equal(t, int64(200), values[1])
	assert.Equal(t, int64(1), ids[0])
	assert.Equal(t, int64(2), ids[1])
}

func TestOrderByLimitOperator_TopKWithOffset(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, DataType: schemapb.DataType_Int64},
	}
	// topK=3 (offset=1 + limit=2, caller passes offset+limit as topK)
	op := NewOrderByLimitOperator(orderByFields, 3)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{5, 1, 4, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField([]int64{5, 1, 4, 2, 3}),
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{500, 100, 400, 200, 300}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	values := sorted.GetFieldsData()[1].GetScalars().GetLongData().GetData()

	// Returns top-3 sorted: 100, 200, 300
	require.Len(t, values, 3)
	assert.Equal(t, int64(100), values[0])
	assert.Equal(t, int64(200), values[1])
	assert.Equal(t, int64(300), values[2])
}

func TestOrderByLimitOperator_PartialSortDescending(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: false, DataType: schemapb.DataType_Int64},
	}
	// topK=2 → top-2 in DESC order
	op := NewOrderByLimitOperator(orderByFields, 2)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 5, 3, 2, 4}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField([]int64{1, 5, 3, 2, 4}),
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 500, 300, 200, 400}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	values := sorted.GetFieldsData()[1].GetScalars().GetLongData().GetData()

	// Top-2 descending: 500, 400
	require.Len(t, values, 2)
	assert.Equal(t, int64(500), values[0])
	assert.Equal(t, int64(400), values[1])
}

func TestOrderByLimitOperator_TopKExceedsRows(t *testing.T) {
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, DataType: schemapb.DataType_Int64},
	}
	// topK=100 exceeds row count → should fall back to full sort
	op := NewOrderByLimitOperator(orderByFields, 100)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{3, 1, 2}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField([]int64{3, 1, 2}),
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{300, 100, 200}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	values := sorted.GetFieldsData()[1].GetScalars().GetLongData().GetData()

	// Full sort: all 3 rows returned sorted
	require.Len(t, values, 3)
	assert.Equal(t, int64(100), values[0])
	assert.Equal(t, int64(200), values[1])
	assert.Equal(t, int64(300), values[2])
}

func TestOrderByLimitOperator_WithFieldPositions(t *testing.T) {
	// Simulate GROUP BY + ORDER BY: layout [group_col, agg_col]
	// ORDER BY agg_col (at position 1)
	orderByFields := []*orderby.OrderByField{
		{FieldID: 0, FieldName: "count", Ascending: false, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperatorWithPositions(orderByFields, []int{1}, 2)
	ctx := context.Background()

	// Layout: [group_col(category), agg_col(count)]
	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "category",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"A", "B", "C"}},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "count",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{10, 30, 20}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	counts := sorted.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	categories := sorted.GetFieldsData()[0].GetScalars().GetStringData().GetData()

	// Top-2 DESC by count: B(30), C(20)
	require.Len(t, counts, 2)
	assert.Equal(t, int64(30), counts[0])
	assert.Equal(t, "B", categories[0])
	assert.Equal(t, int64(20), counts[1])
	assert.Equal(t, "C", categories[1])
}

// makeNullableInt64Field creates a nullable Int64 field where ValidData[i]=false marks row i as NULL.
// Values at null positions are placeholder zeros (ignored by getFieldValue).
func makeNullableInt64Field(fieldID int64, name string, vals []int64, validData []bool) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: name,
		FieldId:   fieldID,
		ValidData: validData,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: vals},
				},
			},
		},
	}
}

// makeOrderByResultWithIDs builds a RetrieveResults with IDs and a single nullable field.
func makeOrderByResultWithIDs(ids []int64, fieldID int64, name string, vals []int64, validData []bool) *internalpb.RetrieveResults {
	return &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: ids},
			},
		},
		FieldsData: []*schemapb.FieldData{
			makePKField(ids),
			makeNullableInt64Field(fieldID, name, vals, validData),
		},
	}
}

// TestOrderByLimitOperator_NullsFirst_AscDefault verifies the PostgreSQL default for ASC:
// NULLs sort last (NullsFirst=false).
func TestOrderByLimitOperator_NullsFirst_AscDefault(t *testing.T) {
	// Rows: pk=[1,2,3], value=[200, NULL, 100]
	// ASC default (NullsFirst=false) → expected order: 100(pk=3), 200(pk=1), NULL(pk=2)
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, NullsFirst: false, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := makeOrderByResultWithIDs(
		[]int64{1, 2, 3},
		2, "value",
		[]int64{200, 0, 100}, // 0 is placeholder for NULL at index 1
		[]bool{true, false, true},
	)

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	ids := sorted.GetIds().GetIntId().GetData()
	require.Len(t, ids, 3)
	// Non-null values first (ASC), then NULL at end
	assert.Equal(t, int64(3), ids[0]) // value=100
	assert.Equal(t, int64(1), ids[1]) // value=200
	assert.Equal(t, int64(2), ids[2]) // NULL last
}

// TestOrderByLimitOperator_NullsFirst_AscNullsFirst verifies ASC NULLS FIRST:
// NULLs sort before all non-null values.
func TestOrderByLimitOperator_NullsFirst_AscNullsFirst(t *testing.T) {
	// Rows: pk=[1,2,3], value=[200, NULL, 100]
	// ASC NULLS FIRST → expected order: NULL(pk=2), 100(pk=3), 200(pk=1)
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, NullsFirst: true, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := makeOrderByResultWithIDs(
		[]int64{1, 2, 3},
		2, "value",
		[]int64{200, 0, 100},
		[]bool{true, false, true},
	)

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	ids := sorted.GetIds().GetIntId().GetData()
	require.Len(t, ids, 3)
	// NULL first, then non-null values ASC
	assert.Equal(t, int64(2), ids[0]) // NULL first
	assert.Equal(t, int64(3), ids[1]) // value=100
	assert.Equal(t, int64(1), ids[2]) // value=200
}

// TestOrderByLimitOperator_NullsFirst_DescDefault verifies the PostgreSQL default for DESC:
// NULLs sort first (NullsFirst=true is the default for DESC).
func TestOrderByLimitOperator_NullsFirst_DescDefault(t *testing.T) {
	// Rows: pk=[1,2,3], value=[200, NULL, 100]
	// DESC default (NullsFirst=true) → expected order: NULL(pk=2), 200(pk=1), 100(pk=3)
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: false, NullsFirst: true, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := makeOrderByResultWithIDs(
		[]int64{1, 2, 3},
		2, "value",
		[]int64{200, 0, 100},
		[]bool{true, false, true},
	)

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	ids := sorted.GetIds().GetIntId().GetData()
	require.Len(t, ids, 3)
	// NULL first, then non-null values DESC
	assert.Equal(t, int64(2), ids[0]) // NULL first
	assert.Equal(t, int64(1), ids[1]) // value=200
	assert.Equal(t, int64(3), ids[2]) // value=100
}

// TestOrderByLimitOperator_NullsFirst_DescNullsLast verifies DESC NULLS LAST override:
// NULLs sort after all non-null values despite DESC direction.
func TestOrderByLimitOperator_NullsFirst_DescNullsLast(t *testing.T) {
	// Rows: pk=[1,2,3], value=[200, NULL, 100]
	// DESC NULLS LAST → expected order: 200(pk=1), 100(pk=3), NULL(pk=2)
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: false, NullsFirst: false, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := makeOrderByResultWithIDs(
		[]int64{1, 2, 3},
		2, "value",
		[]int64{200, 0, 100},
		[]bool{true, false, true},
	)

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	ids := sorted.GetIds().GetIntId().GetData()
	require.Len(t, ids, 3)
	// Non-null DESC first, NULL at end
	assert.Equal(t, int64(1), ids[0]) // value=200
	assert.Equal(t, int64(3), ids[1]) // value=100
	assert.Equal(t, int64(2), ids[2]) // NULL last
}

// TestOrderByLimitOperator_NullsFirst_AllNull verifies that all-null inputs sort stably.
func TestOrderByLimitOperator_NullsFirst_AllNull(t *testing.T) {
	// Rows: pk=[1,2,3], all values NULL
	// With NullsFirst=true ASC: all equal → stable sort preserves original order
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, NullsFirst: true, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 0)
	ctx := context.Background()

	result := makeOrderByResultWithIDs(
		[]int64{1, 2, 3},
		2, "value",
		[]int64{0, 0, 0},
		[]bool{false, false, false},
	)

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	ids := sorted.GetIds().GetIntId().GetData()
	// All null → stable sort preserves PK tie-breaker order
	require.Len(t, ids, 3)
	assert.Equal(t, int64(1), ids[0])
	assert.Equal(t, int64(2), ids[1])
	assert.Equal(t, int64(3), ids[2])
}

// TestOrderByLimitOperator_NullsFirst_PartialSort verifies NullsFirst with topK < rowCount.
// Ensures the heap-based partial sort also respects NullsFirst semantics.
func TestOrderByLimitOperator_NullsFirst_PartialSort(t *testing.T) {
	// Rows: pk=[1,2,3,4,5], value=[300, NULL, 100, NULL, 200]
	// ASC NULLS FIRST, topK=3 → top-3: NULL(pk=2), NULL(pk=4), 100(pk=3)
	orderByFields := []*orderby.OrderByField{
		{FieldID: 2, FieldName: "value", Ascending: true, NullsFirst: true, DataType: schemapb.DataType_Int64},
	}
	op := NewOrderByLimitOperator(orderByFields, 3)
	ctx := context.Background()

	result := makeOrderByResultWithIDs(
		[]int64{1, 2, 3, 4, 5},
		2, "value",
		[]int64{300, 0, 100, 0, 200},
		[]bool{true, false, true, false, true},
	)

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sorted := outputs[0].(*internalpb.RetrieveResults)
	ids := sorted.GetIds().GetIntId().GetData()
	require.Len(t, ids, 3)
	// Two NULLs (sorted by PK tie-breaker: pk=2 < pk=4), then value=100
	assert.Equal(t, int64(2), ids[0]) // NULL, pk=2 (tie-breaker)
	assert.Equal(t, int64(4), ids[1]) // NULL, pk=4 (tie-breaker)
	assert.Equal(t, int64(3), ids[2]) // value=100
}
