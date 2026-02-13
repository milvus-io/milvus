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
