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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// makeTimestampField creates a timestamp field (FieldID=1).
func makeTimestampField(timestamps []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "timestamp",
		FieldId:   common.TimeStampField,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: timestamps},
				},
			},
		},
	}
}

func makeDedupIntIDs(ids []int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
}

func makeDedupInt64Field(fieldID int64, name string, vals []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   fieldID,
		FieldName: name,
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: vals}},
			},
		},
	}
}

func TestDeduplicatePKOperator_Name(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	assert.Equal(t, OpDeduplicatePK, op.Name())
}

func TestDeduplicatePKOperator_EmptyInput(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	ctx := context.Background()

	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.NotNil(t, result)
}

func TestDeduplicatePKOperator_NilResults(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	ctx := context.Background()

	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{nil, nil})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.NotNil(t, result)
}

func TestDeduplicatePKOperator_SingleResult(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	ctx := context.Background()

	r := &internalpb.RetrieveResults{
		Ids:        makeDedupIntIDs([]int64{1, 2, 3}),
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20, 30})},
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.Equal(t, []int64{1, 2, 3}, result.GetIds().GetIntId().GetData())
	assert.Equal(t, []int64{10, 20, 30}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func TestDeduplicatePKOperator_MultipleResults_NoDuplicates(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	ctx := context.Background()

	r1 := &internalpb.RetrieveResults{
		Ids:        makeDedupIntIDs([]int64{1, 2}),
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20})},
	}
	r2 := &internalpb.RetrieveResults{
		Ids:        makeDedupIntIDs([]int64{3, 4}),
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{30, 40})},
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.Equal(t, 4, len(result.GetIds().GetIntId().GetData()))
}

func TestDeduplicatePKOperator_DuplicatePK_HigherTimestampWins(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	ctx := context.Background()

	// r1: PK=1 with ts=100, val=10
	r1 := &internalpb.RetrieveResults{
		Ids: makeDedupIntIDs([]int64{1}),
		FieldsData: []*schemapb.FieldData{
			makeDedupInt64Field(100, "val", []int64{10}),
			makeTimestampField([]int64{100}),
		},
	}
	// r2: PK=1 with ts=200, val=99 (should win)
	r2 := &internalpb.RetrieveResults{
		Ids: makeDedupIntIDs([]int64{1}),
		FieldsData: []*schemapb.FieldData{
			makeDedupInt64Field(100, "val", []int64{99}),
			makeTimestampField([]int64{200}),
		},
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.Equal(t, []int64{1}, result.GetIds().GetIntId().GetData())
	assert.Equal(t, []int64{99}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func TestDeduplicatePKOperator_DuplicatePK_LowerTimestampLoses(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	ctx := context.Background()

	// r1: PK=1 with ts=200, val=10 (should win)
	r1 := &internalpb.RetrieveResults{
		Ids: makeDedupIntIDs([]int64{1}),
		FieldsData: []*schemapb.FieldData{
			makeDedupInt64Field(100, "val", []int64{10}),
			makeTimestampField([]int64{200}),
		},
	}
	// r2: PK=1 with ts=100, val=99
	r2 := &internalpb.RetrieveResults{
		Ids: makeDedupIntIDs([]int64{1}),
		FieldsData: []*schemapb.FieldData{
			makeDedupInt64Field(100, "val", []int64{99}),
			makeTimestampField([]int64{100}),
		},
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.Equal(t, []int64{1}, result.GetIds().GetIntId().GetData())
	assert.Equal(t, []int64{10}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func TestDeduplicatePKOperator_DuplicateWithinSingleResult(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	ctx := context.Background()

	// Single result with duplicate PK=1 (ts=100 then ts=200)
	r := &internalpb.RetrieveResults{
		Ids: makeDedupIntIDs([]int64{1, 2, 1}),
		FieldsData: []*schemapb.FieldData{
			makeDedupInt64Field(100, "val", []int64{10, 20, 99}),
			makeTimestampField([]int64{100, 150, 200}),
		},
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	// Should have 2 unique PKs
	assert.Equal(t, 2, len(result.GetIds().GetIntId().GetData()))
}

func TestDeduplicatePKOperator_MaxOutputSize(t *testing.T) {
	// Very small maxOutputSize should trigger error
	op := NewDeduplicatePKOperator(1) // 1 byte limit
	ctx := context.Background()

	r := &internalpb.RetrieveResults{
		Ids:        makeDedupIntIDs([]int64{1, 2, 3}),
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20, 30})},
	}
	_, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "maxOutputSize")
}

func TestDeduplicatePKOperator_HasMoreResult(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	ctx := context.Background()

	r1 := &internalpb.RetrieveResults{
		Ids:           makeDedupIntIDs([]int64{1}),
		FieldsData:    []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10})},
		HasMoreResult: true,
	}
	r2 := &internalpb.RetrieveResults{
		Ids:           makeDedupIntIDs([]int64{2}),
		FieldsData:    []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{20})},
		HasMoreResult: false,
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.True(t, result.GetHasMoreResult())
}

func TestDeduplicatePKOperator_StringPK(t *testing.T) {
	op := NewDeduplicatePKOperator(0)
	ctx := context.Background()

	r1 := &internalpb.RetrieveResults{
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}}},
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20})},
	}
	r2 := &internalpb.RetrieveResults{
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"b", "c"}}}},
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{99, 30})},
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	// "b" is duplicated, should be deduped to 3 unique PKs
	assert.Equal(t, 3, len(result.GetIds().GetStrId().GetData()))
}

// =========================================================================
// ConcatAndCheckPKOperator
// =========================================================================

func TestConcatAndCheckPKOperator_DuplicatePK_ReturnsError(t *testing.T) {
	op := NewConcatAndCheckPKOperator()
	ctx := context.Background()

	r1 := &internalpb.RetrieveResults{
		Ids:        makeDedupIntIDs([]int64{1, 2}),
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20})},
	}
	r2 := &internalpb.RetrieveResults{
		Ids:        makeDedupIntIDs([]int64{2, 3}),
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{99, 30})},
	}
	_, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate PK")
}

func TestConcatAndCheckPKOperator_SingleResult(t *testing.T) {
	op := NewConcatAndCheckPKOperator()
	ctx := context.Background()

	r := &internalpb.RetrieveResults{
		Ids:        makeDedupIntIDs([]int64{1, 2, 3}),
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20, 30})},
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.Equal(t, []int64{1, 2, 3}, result.GetIds().GetIntId().GetData())
}

// TestConcatAndCheckPKOperator_HasMoreResult_MultiResult verifies HasMoreResult is propagated
// when multiple non-empty results are concatenated.
func TestConcatAndCheckPKOperator_HasMoreResult_MultiResult(t *testing.T) {
	op := NewConcatAndCheckPKOperator()
	ctx := context.Background()

	r1 := &internalpb.RetrieveResults{
		Ids:           makeDedupIntIDs([]int64{1, 2}),
		FieldsData:    []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20})},
		HasMoreResult: true,
	}
	r2 := &internalpb.RetrieveResults{
		Ids:           makeDedupIntIDs([]int64{3, 4}),
		FieldsData:    []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{30, 40})},
		HasMoreResult: false,
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.True(t, result.GetHasMoreResult())
}

// TestConcatAndCheckPKOperator_HasMoreResult_EmptyResultPropagates verifies that HasMoreResult
// from an empty (filtered-out) result is still propagated to the merged output.
func TestConcatAndCheckPKOperator_HasMoreResult_EmptyResultPropagates(t *testing.T) {
	op := NewConcatAndCheckPKOperator()
	ctx := context.Background()

	// r1 is empty (no rows) but has HasMoreResult=true
	r1 := &internalpb.RetrieveResults{
		HasMoreResult: true,
	}
	// r2 has rows but HasMoreResult=false
	r2 := &internalpb.RetrieveResults{
		Ids:           makeDedupIntIDs([]int64{1, 2}),
		FieldsData:    []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20})},
		HasMoreResult: false,
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	// HasMoreResult from the empty result must be ORed into the output
	assert.True(t, result.GetHasMoreResult())
}

// TestConcatAndCheckPKOperator_HasMoreResult_AllEmpty verifies HasMoreResult is set on
// the empty output when all inputs are empty.
func TestConcatAndCheckPKOperator_HasMoreResult_AllEmpty(t *testing.T) {
	op := NewConcatAndCheckPKOperator()
	ctx := context.Background()

	r1 := &internalpb.RetrieveResults{HasMoreResult: true}
	r2 := &internalpb.RetrieveResults{HasMoreResult: false}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r1, r2})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.True(t, result.GetHasMoreResult())
}

// TestDeduplicatePKOperator_MaxOutputSize_ExactlyAtLimit verifies that a result whose
// total size equals maxOutputSize is accepted (check is strict >, not >=).
// Each Int64 row = 8 bytes; 3 rows × 8 bytes = 24 bytes.
func TestDeduplicatePKOperator_MaxOutputSize_ExactlyAtLimit(t *testing.T) {
	const rowSize = 8 // Int64 field bytes per row
	const rowCount = 3
	const exactLimit = rowSize * rowCount // 24 bytes

	op := NewDeduplicatePKOperator(exactLimit)
	ctx := context.Background()

	r := &internalpb.RetrieveResults{
		Ids:        makeDedupIntIDs([]int64{1, 2, 3}),
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20, 30})},
	}
	outputs, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r})
	require.NoError(t, err)
	result := outputs[0].(*internalpb.RetrieveResults)
	assert.Equal(t, 3, len(result.GetIds().GetIntId().GetData()))
}

// TestDeduplicatePKOperator_MaxOutputSize_OneBelowLimit verifies that a result whose
// total size exceeds maxOutputSize by one byte returns an error.
func TestDeduplicatePKOperator_MaxOutputSize_OneBelowLimit(t *testing.T) {
	const rowSize = 8 // Int64 field bytes per row
	const rowCount = 3
	const limitOneByte = rowSize*rowCount - 1 // 23 bytes — one below total

	op := NewDeduplicatePKOperator(limitOneByte)
	ctx := context.Background()

	r := &internalpb.RetrieveResults{
		Ids:        makeDedupIntIDs([]int64{1, 2, 3}),
		FieldsData: []*schemapb.FieldData{makeDedupInt64Field(100, "val", []int64{10, 20, 30})},
	}
	_, err := op.Run(ctx, nil, []*internalpb.RetrieveResults{r})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "maxOutputSize")
}
