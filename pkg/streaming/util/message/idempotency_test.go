package message

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestMergeIdempotentInsertResults(t *testing.T) {
	merged, hadAny, err := MergeIdempotentInsertResults(
		&messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{0},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
			},
		},
		nil, // nil elements are skipped
		&messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{2, 1},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{12, 11}}},
			},
		},
	)
	require.NoError(t, err)
	require.True(t, hadAny)
	require.Equal(t, []uint32{0, 2, 1}, merged.GetRowOffsets())
	require.Equal(t, []int64{10, 12, 11}, merged.GetIds().GetIntId().GetData())
}

func TestMergeIdempotentInsertResultsEmpty(t *testing.T) {
	merged, hadAny, err := MergeIdempotentInsertResults()
	require.NoError(t, err)
	require.False(t, hadAny)
	require.Nil(t, merged)

	merged, hadAny, err = MergeIdempotentInsertResults(nil, nil)
	require.NoError(t, err)
	require.False(t, hadAny)
	require.Nil(t, merged)
}

func TestMergeIdempotentInsertResultsRejectsMixedIDTypes(t *testing.T) {
	_, hadAny, err := MergeIdempotentInsertResults(
		&messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{0},
			Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}}},
		},
		&messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{1},
			Ids:        &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk"}}}},
		},
	)
	require.Error(t, err)
	require.False(t, hadAny)
}

func TestValidateIdempotentInsertResult(t *testing.T) {
	require.NoError(t, ValidateIdempotentInsertResult(nil))
	require.NoError(t, ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{}))

	// row offsets but no ids
	require.Error(t, ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
	}))
	// ids but no row offsets
	require.Error(t, ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}}},
	}))
	// ids present but neither int nor string field populated
	require.Error(t, ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids:        &schemapb.IDs{},
	}))
	// length mismatch
	require.Error(t, ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0, 1},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}}},
	}))
}

func TestInsertHeaderIdempotentInsertResult(t *testing.T) {
	header := &InsertMessageHeader{}
	result := &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{1, 0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-1", "pk-0"}}},
		},
	}
	SetInsertHeaderIdempotentInsertResult(header, result)
	require.NotNil(t, header.GetIdempotentResult())

	roundTrip, ok := IdempotentInsertResultFromInsertHeader(header)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 0}, roundTrip.GetRowOffsets())
	require.Equal(t, []string{"pk-1", "pk-0"}, roundTrip.GetIds().GetStrId().GetData())
	require.NoError(t, ValidateIdempotentInsertResult(roundTrip))
}
