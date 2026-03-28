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

// makeRemapField creates a minimal FieldData with just FieldId and FieldName set.
func makeRemapField(fieldID int64, name string) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   fieldID,
		FieldName: name,
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}},
			},
		},
	}
}

func TestRemapOperator_Name(t *testing.T) {
	op := NewRemapOperator([]int{0, 1})
	assert.Equal(t, OpRemap, op.Name())
}

func TestRemapOperator_Reorder(t *testing.T) {
	// Segcore layout: [pk(0), age(1), color(2)]
	// User wants: [color, pk, age]
	// outputIndices = [2, 0, 1]
	op := NewRemapOperator([]int{2, 0, 1})
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeInt64Field(100, "pk", []int64{1, 2}),
			makeInt64Field(101, "age", []int64{20, 30}),
			makeStringField(200, "color", []string{"red", "blue"}),
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	out := outputs[0].(*internalpb.RetrieveResults)

	require.Len(t, out.GetFieldsData(), 3)
	assert.Equal(t, "color", out.GetFieldsData()[0].GetFieldName())
	assert.Equal(t, "pk", out.GetFieldsData()[1].GetFieldName())
	assert.Equal(t, "age", out.GetFieldsData()[2].GetFieldName())
}

func TestRemapOperator_SubsetSelection(t *testing.T) {
	// Segcore layout: [pk(0), orderby_age(1), name(2)]
	// User only wants: [name, age] (pk is sort-only, not in user output)
	// outputIndices = [2, 1]
	op := NewRemapOperator([]int{2, 1})
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeInt64Field(100, "pk", []int64{1, 2}),
			makeInt64Field(101, "age", []int64{20, 30}),
			makeStringField(102, "name", []string{"Alice", "Bob"}),
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	out := outputs[0].(*internalpb.RetrieveResults)

	require.Len(t, out.GetFieldsData(), 2)
	assert.Equal(t, "name", out.GetFieldsData()[0].GetFieldName())
	assert.Equal(t, "age", out.GetFieldsData()[1].GetFieldName())
}

func TestRemapOperator_EmptyInput(t *testing.T) {
	op := NewRemapOperator([]int{0, 1})
	ctx := context.Background()

	// nil result
	outputs, err := op.Run(ctx, nil, (*internalpb.RetrieveResults)(nil))
	require.NoError(t, err)
	assert.Nil(t, outputs[0])

	// empty FieldsData
	result := &internalpb.RetrieveResults{}
	outputs, err = op.Run(ctx, nil, result)
	require.NoError(t, err)
	assert.Empty(t, outputs[0].(*internalpb.RetrieveResults).GetFieldsData())
}

func TestRemapOperator_EmptyIndices(t *testing.T) {
	op := NewRemapOperator([]int{})
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeInt64Field(100, "pk", []int64{1}),
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	// Empty indices → pass through unchanged
	assert.Len(t, outputs[0].(*internalpb.RetrieveResults).GetFieldsData(), 1)
}

func TestFieldIDRemapOperator_Basic(t *testing.T) {
	ctx := context.Background()
	// FieldsData from segcore: [pk(100), orderby(101), meta(102)]
	// User wants: [meta(102), orderby(101)]
	op := NewFieldIDRemapOperator([]int64{102, 101})
	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeRemapField(100, "pk"),
			makeRemapField(101, "val"),
			makeRemapField(102, "$meta"),
		},
	}
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	out := outputs[0].(*internalpb.RetrieveResults)
	require.Len(t, out.GetFieldsData(), 2)
	assert.Equal(t, int64(102), out.GetFieldsData()[0].GetFieldId())
	assert.Equal(t, int64(101), out.GetFieldsData()[1].GetFieldId())
}

func TestFieldIDRemapOperator_DynamicField(t *testing.T) {
	ctx := context.Background()
	// User requests output_fields=["price", "x", "y"] where x,y are dynamic subkeys.
	// outputFieldIDs = [price_id(101), meta_id(102)]
	// Segcore returns: [pk(100), price(101), $meta(102)]
	// Remap should keep price and $meta (dynamic field data for x,y in JSON)
	op := NewFieldIDRemapOperator([]int64{101, 102})
	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeRemapField(100, "pk"),
			makeRemapField(101, "price"),
			makeRemapField(102, "$meta"),
		},
	}
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	out := outputs[0].(*internalpb.RetrieveResults)
	require.Len(t, out.GetFieldsData(), 2)
	assert.Equal(t, int64(101), out.GetFieldsData()[0].GetFieldId())
	assert.Equal(t, int64(102), out.GetFieldsData()[1].GetFieldId())
}

func TestFieldIDRemapOperator_EmptyInput(t *testing.T) {
	ctx := context.Background()
	op := NewFieldIDRemapOperator([]int64{100, 101})

	result := &internalpb.RetrieveResults{}
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	assert.Empty(t, outputs[0].(*internalpb.RetrieveResults).GetFieldsData())
}

func TestFieldIDRemapOperator_FieldNotFound(t *testing.T) {
	ctx := context.Background()
	// Desired field 999 doesn't exist in result — silently skipped (schema evolution)
	op := NewFieldIDRemapOperator([]int64{101, 999})
	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeRemapField(100, "pk"),
			makeRemapField(101, "val"),
		},
	}
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	out := outputs[0].(*internalpb.RetrieveResults)
	// Only field 101 found, 999 skipped
	require.Len(t, out.GetFieldsData(), 1)
	assert.Equal(t, int64(101), out.GetFieldsData()[0].GetFieldId())
}

func TestFieldIDRemapOperator_ExcludesPKAndTimestamp(t *testing.T) {
	ctx := context.Background()
	// outputFieldIDs from filterSystemFields doesn't include PK or timestamp
	// Only user-requested fields: [101]
	op := NewFieldIDRemapOperator([]int64{101})
	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeRemapField(100, "pk"),
			makeRemapField(101, "val"),
			makeRemapField(1, "timestamp"),
		},
	}
	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	out := outputs[0].(*internalpb.RetrieveResults)
	require.Len(t, out.GetFieldsData(), 1)
	assert.Equal(t, int64(101), out.GetFieldsData()[0].GetFieldId())
}
