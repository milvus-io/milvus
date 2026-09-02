package segcore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func marshalInputPlanForTest(t *testing.T, plan *chain.DataFrameInputPlan) []byte {
	t.Helper()
	blob, err := MarshalFunctionChainInputPlan(plan)
	require.NoError(t, err)
	return blob
}

func TestFunctionChainInputPlanMarshalling(t *testing.T) {
	path := []string{"a/b", "~c", "0", "", "中文", "nul\x00key"}
	inputPlan := &chain.DataFrameInputPlan{Inputs: []chain.ResolvedChainInput{
		{LogicalName: "scalar", SourceFieldID: 100, DataType: schemapb.DataType_Int8},
		{
			LogicalName: `metadata["a/b"]`, SourceFieldID: 101,
			DataType: schemapb.DataType_JSON, NestedPath: path, DataTypeHint: schemapb.DataType_VarChar,
		},
	}}
	blob := marshalInputPlanForTest(t, inputPlan)
	var decoded cgopb.FunctionChainInputPlan
	require.NoError(t, proto.Unmarshal(blob, &decoded))
	require.Len(t, decoded.GetInputs(), 2)
	scalar, jsonPath := decoded.Inputs[0], decoded.Inputs[1]
	assert.Equal(t, int64(100), scalar.GetSourceFieldId())
	assert.Equal(t, "scalar", scalar.GetLogicalName())
	assert.Equal(t, schemapb.DataType_Int8, scalar.GetTargetDataType())
	assert.False(t, scalar.GetIsJsonPath())
	assert.Empty(t, scalar.GetNestedPath())
	assert.Equal(t, int64(101), jsonPath.GetSourceFieldId())
	assert.Equal(t, inputPlan.Inputs[1].LogicalName, jsonPath.GetLogicalName())
	assert.Equal(t, schemapb.DataType_VarChar, jsonPath.GetTargetDataType())
	assert.True(t, jsonPath.GetIsJsonPath())
	assert.Equal(t, path, jsonPath.GetNestedPath())
	for _, emptyPlan := range []*chain.DataFrameInputPlan{nil, {}} {
		assert.Empty(t, marshalInputPlanForTest(t, emptyPlan))
	}
}

func TestFunctionChainInputPlanMarshallingRejectsInvalidPlan(t *testing.T) {
	for _, input := range []chain.ResolvedChainInput{
		{SourceFieldID: 100, DataType: schemapb.DataType_Int64},
		{LogicalName: "metadata", SourceFieldID: 100, DataType: schemapb.DataType_JSON},
		{LogicalName: "scalar", SourceFieldID: 100, DataType: schemapb.DataType_Int64, NestedPath: []string{"key"}},
	} {
		blob, err := MarshalFunctionChainInputPlan(&chain.DataFrameInputPlan{Inputs: []chain.ResolvedChainInput{input}})
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.Nil(t, blob)
	}
	blob, err := MarshalFunctionChainInputPlan(&chain.DataFrameInputPlan{Inputs: []chain.ResolvedChainInput{{
		LogicalName: "invalid\xff", DataType: schemapb.DataType_Int64,
	}}})
	require.ErrorIs(t, err, merr.ErrSerializationFailed)
	assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
	assert.Nil(t, blob)
}

func TestFunctionChainMalformedInputPlanIsSystemError(t *testing.T) {
	plan := NewDummySearchPlanForTest(t)
	blob := []byte{0xff}
	record, chunks, err := ExportSearchResultAsArrowRecordBatchWithInputPlan(t.Context(), &SearchResult{}, plan, blob)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.Nil(t, chunks)
	assert.Contains(t, err.Error(), "failed to parse function chain input plan")
	assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
	assert.False(t, merr.IsSegcoreDataFormatBroken(err))
	record, err = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(t.Context(), []*SearchResult{{}}, plan, blob, []int32{0}, []int64{0})
	require.Error(t, err)
	assert.Nil(t, record)
	assert.Contains(t, err.Error(), "failed to parse function chain input plan")
	assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
	assert.False(t, merr.IsSegcoreDataFormatBroken(err))
}

func TestClassifyFunctionChainProjectionError(t *testing.T) {
	corrupt := merr.SegcoreError(2024, "malformed persisted JSON")
	err := classifyFunctionChainProjectionError(corrupt)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.True(t, merr.IsSegcoreDataFormatBroken(err))

	other := merr.SegcoreError(2001, "unexpected")
	assert.Same(t, other, classifyFunctionChainProjectionError(other))

	for _, code := range []int32{2034, 2043} {
		resourceErr := merr.SegcoreError(code, "simdjson resource failure")
		classified := classifyFunctionChainProjectionError(resourceErr)
		assert.Same(t, resourceErr, classified)
		assert.Equal(t, merr.SystemError, merr.GetErrorType(classified))
		assert.True(t, merr.Status(classified).GetRetriable())
	}
}

func TestExportSearchResultAsArrowRecordBatchWithInputPlanValidation(t *testing.T) {
	record, chunkSizes, err := ExportSearchResultAsArrowRecordBatchWithInputPlan(context.Background(), nil, NewDummySearchPlanForTest(t), nil)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.Nil(t, chunkSizes)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "nil search result")

	record, chunkSizes, err = ExportSearchResultAsArrowRecordBatchWithInputPlan(context.Background(), &SearchResult{}, nil, nil)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.Nil(t, chunkSizes)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "nil search plan")
}

func TestFillFieldsOrderedAsArrowRecordBatchWithInputPlanValidation(t *testing.T) {
	inputPlan := &chain.DataFrameInputPlan{Inputs: []chain.ResolvedChainInput{{
		LogicalName: "scalar", SourceFieldID: 100, DataType: schemapb.DataType_Int64,
	}}}
	record, err := FillFieldsOrderedAsArrowRecordBatchWithInputPlan(context.Background(), []*SearchResult{{}}, nil, marshalInputPlanForTest(t, inputPlan), nil, nil)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "nil search plan")

	plan := NewDummySearchPlanForTest(t)
	record, err = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(context.Background(), nil, plan, marshalInputPlanForTest(t, inputPlan), nil, nil)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "empty search results")

	record, err = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(context.Background(), []*SearchResult{{}}, plan, nil, nil, nil)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "empty function chain input plan")

	record, err = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(context.Background(), []*SearchResult{{}}, plan, marshalInputPlanForTest(t, inputPlan), []int32{0}, nil)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "unaligned segment indices")

	record, err = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(context.Background(), []*SearchResult{nil}, plan, marshalInputPlanForTest(t, inputPlan), nil, nil)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "nil search result at index 0")
}

func TestFillOutputFieldsOrderedValidation(t *testing.T) {
	blob, err := FillOutputFieldsOrdered(context.Background(), []*SearchResult{{}}, nil, nil, nil)
	require.Error(t, err)
	assert.Nil(t, blob)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "nil search plan")

	plan := NewDummySearchPlanForTest(t)
	blob, err = FillOutputFieldsOrdered(context.Background(), nil, plan, nil, nil)
	require.Error(t, err)
	assert.Nil(t, blob)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "empty search results")

	blob, err = FillOutputFieldsOrdered(context.Background(), []*SearchResult{{}}, plan, []int32{0}, nil)
	require.Error(t, err)
	assert.Nil(t, blob)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "unaligned segment indices")

	blob, err = FillOutputFieldsOrdered(context.Background(), []*SearchResult{nil}, plan, nil, nil)
	require.Error(t, err)
	assert.Nil(t, blob)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "nil search result at index 0")
}

func TestExportSearchResultWithEmptyInputPlan(t *testing.T) {
	plan := NewDummySearchPlanForTest(t)
	for _, inputPlan := range []*chain.DataFrameInputPlan{nil, {}} {
		blob := marshalInputPlanForTest(t, inputPlan)
		record, chunks, err := ExportSearchResultAsArrowRecordBatchWithInputPlan(t.Context(), &SearchResult{}, plan, blob)
		require.Error(t, err)
		assert.Nil(t, record)
		assert.Nil(t, chunks)
		assert.Contains(t, err.Error(), "null search result")
		assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
	}
}

func TestExportSearchResultWithInputPlanCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, blob := range [][]byte{nil, {}, {0xff}} {
		record, chunks, err := ExportSearchResultAsArrowRecordBatchWithInputPlan(ctx, nil, nil, blob)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, record)
		assert.Nil(t, chunks)
	}
}
