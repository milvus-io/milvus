package segcore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestExportSearchResultAsArrowRecordBatchValidation(t *testing.T) {
	record, chunkSizes, err := ExportSearchResultAsArrowRecordBatch(context.Background(), nil, NewDummySearchPlanForTest(t), nil)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.Nil(t, chunkSizes)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "nil search result")

	record, chunkSizes, err = ExportSearchResultAsArrowRecordBatch(context.Background(), &SearchResult{}, nil, nil)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.Nil(t, chunkSizes)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "nil search plan")
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
