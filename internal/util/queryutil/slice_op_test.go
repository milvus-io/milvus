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
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func TestSliceOperator_Name(t *testing.T) {
	op := NewSliceOperator(10, 0)
	assert.Equal(t, OpSlice, op.Name())
}

func TestSliceOperator_LimitOnly(t *testing.T) {
	op := NewSliceOperator(3, 0)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
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
							LongData: &schemapb.LongArray{Data: []int64{10, 20, 30, 40, 50}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sliced := outputs[0].(*internalpb.RetrieveResults)
	ids := sliced.GetIds().GetIntId().GetData()
	assert.Equal(t, 3, len(ids))
	assert.Equal(t, int64(1), ids[0])
	assert.Equal(t, int64(2), ids[1])
	assert.Equal(t, int64(3), ids[2])
}

func TestSliceOperator_OffsetOnly(t *testing.T) {
	op := NewSliceOperator(-1, 2) // -1 means no limit
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
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
							LongData: &schemapb.LongArray{Data: []int64{10, 20, 30, 40, 50}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sliced := outputs[0].(*internalpb.RetrieveResults)
	ids := sliced.GetIds().GetIntId().GetData()
	assert.Equal(t, 3, len(ids))
	assert.Equal(t, int64(3), ids[0]) // Skipped 1, 2
	assert.Equal(t, int64(4), ids[1])
	assert.Equal(t, int64(5), ids[2])
}

func TestSliceOperator_LimitAndOffset(t *testing.T) {
	op := NewSliceOperator(2, 2)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
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
							LongData: &schemapb.LongArray{Data: []int64{10, 20, 30, 40, 50}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sliced := outputs[0].(*internalpb.RetrieveResults)
	ids := sliced.GetIds().GetIntId().GetData()
	assert.Equal(t, 2, len(ids))
	assert.Equal(t, int64(3), ids[0]) // Skipped 1, 2, took 3, 4
	assert.Equal(t, int64(4), ids[1])
}

func TestSliceOperator_OffsetBeyondLength(t *testing.T) {
	op := NewSliceOperator(10, 10)
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
				FieldName: "value",
				FieldId:   2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
						},
					},
				},
			},
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sliced := outputs[0].(*internalpb.RetrieveResults)
	// Should return empty when offset > length
	assert.Nil(t, sliced.GetIds())
}

func TestSliceOperator_EmptyInput(t *testing.T) {
	op := NewSliceOperator(10, 0)
	ctx := context.Background()

	result := &internalpb.RetrieveResults{}
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	assert.NotNil(t, outputs[0])
}

// =========================================================================
// Element-Level Tests
// =========================================================================

// makeElementLevelSliceResult creates an element-level result for slice tests.
// Each doc has the given element indices and a single int64 field with the PK values.
func makeElementLevelSliceResult(pks []int64, elemIndices [][]int32) *internalpb.RetrieveResults {
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
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 2,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: pks},
						},
					},
				},
			},
		},
		ElementLevel:   true,
		ElementIndices: elemIdxList,
	}
}

func TestSliceOperator_ElementLevel_LimitOnly(t *testing.T) {
	ctx := context.Background()

	// doc0: 3 elements [0,1,2], doc1: 2 elements [0,1], doc2: 1 element [0]
	// Total: 6 elements
	result := makeElementLevelSliceResult(
		[]int64{10, 20, 30},
		[][]int32{{0, 1, 2}, {0, 1}, {0}},
	)

	// limit=4 → doc0 (3 elem) + doc1 partial (1 of 2 elem) = 4
	op := NewSliceOperator(4, 0)
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sliced := outputs[0].(*internalpb.RetrieveResults)
	assert.True(t, sliced.GetElementLevel())

	ids := sliced.GetIds().GetIntId().GetData()
	assert.Equal(t, 2, len(ids)) // doc0 + doc1
	assert.Equal(t, int64(10), ids[0])
	assert.Equal(t, int64(20), ids[1])

	// doc0: full [0,1,2], doc1: trimmed to [0]
	assert.Equal(t, []int32{0, 1, 2}, sliced.GetElementIndices()[0].GetIndices())
	assert.Equal(t, []int32{0}, sliced.GetElementIndices()[1].GetIndices())
}

func TestSliceOperator_ElementLevel_OffsetOnly(t *testing.T) {
	ctx := context.Background()

	// doc0: 3 elements [0,1,2], doc1: 2 elements [0,1], doc2: 1 element [0]
	result := makeElementLevelSliceResult(
		[]int64{10, 20, 30},
		[][]int32{{0, 1, 2}, {0, 1}, {0}},
	)

	// offset=2 → skip 2 elements from doc0, remaining: doc0[2], doc1[0,1], doc2[0]
	op := NewSliceOperator(-1, 2)
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sliced := outputs[0].(*internalpb.RetrieveResults)
	assert.True(t, sliced.GetElementLevel())

	ids := sliced.GetIds().GetIntId().GetData()
	assert.Equal(t, 3, len(ids)) // doc0 (trimmed) + doc1 + doc2

	// doc0: trimmed to [2], doc1: full, doc2: full
	assert.Equal(t, []int32{2}, sliced.GetElementIndices()[0].GetIndices())
	assert.Equal(t, []int32{0, 1}, sliced.GetElementIndices()[1].GetIndices())
	assert.Equal(t, []int32{0}, sliced.GetElementIndices()[2].GetIndices())
}

func TestSliceOperator_ElementLevel_OffsetAndLimit(t *testing.T) {
	ctx := context.Background()

	// doc0: 3 elements [0,1,2], doc1: 2 elements [0,1], doc2: 1 element [0]
	result := makeElementLevelSliceResult(
		[]int64{10, 20, 30},
		[][]int32{{0, 1, 2}, {0, 1}, {0}},
	)

	// offset=2, limit=3 → skip 2 from doc0, take 3: doc0[2] + doc1[0,1] = 3
	op := NewSliceOperator(3, 2)
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sliced := outputs[0].(*internalpb.RetrieveResults)
	ids := sliced.GetIds().GetIntId().GetData()
	assert.Equal(t, 2, len(ids)) // doc0 (trimmed) + doc1

	assert.Equal(t, []int32{2}, sliced.GetElementIndices()[0].GetIndices())    // doc0 trimmed
	assert.Equal(t, []int32{0, 1}, sliced.GetElementIndices()[1].GetIndices()) // doc1 full
}

func TestSliceOperator_ElementLevel_OffsetSkipsEntireDoc(t *testing.T) {
	ctx := context.Background()

	// doc0: 3 elements, doc1: 2 elements
	result := makeElementLevelSliceResult(
		[]int64{10, 20},
		[][]int32{{0, 1, 2}, {0, 1}},
	)

	// offset=3 → skip all of doc0 (3 elem), start at doc1
	op := NewSliceOperator(-1, 3)
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sliced := outputs[0].(*internalpb.RetrieveResults)
	ids := sliced.GetIds().GetIntId().GetData()
	assert.Equal(t, 1, len(ids)) // only doc1
	assert.Equal(t, int64(20), ids[0])
	assert.Equal(t, []int32{0, 1}, sliced.GetElementIndices()[0].GetIndices())
}

func TestSliceOperator_ElementLevel_OffsetBeyondAll(t *testing.T) {
	ctx := context.Background()

	// doc0: 2 elements, doc1: 1 element → total 3
	result := makeElementLevelSliceResult(
		[]int64{10, 20},
		[][]int32{{0, 1}, {0}},
	)

	// offset=5 → beyond all elements
	op := NewSliceOperator(-1, 5)
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)

	sliced := outputs[0].(*internalpb.RetrieveResults)
	assert.True(t, sliced.GetElementLevel())
	assert.Nil(t, sliced.GetIds())
}
