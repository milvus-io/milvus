package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// buildPKAscFieldsData builds a fields-data slice that mimics the output of
// the query/retrieve reduce step: rows are laid out in primary key ascending
// order, with a compact float-vector representation that skips rows where
// validData[i] is false.
func buildPKAscFieldsData(pkType schemapb.DataType, pkData any, vec [][]float32, dim int64, validData []bool) []*schemapb.FieldData {
	pkField := &schemapb.FieldData{
		Type:      pkType,
		FieldName: "id",
		FieldId:   100,
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}},
	}
	switch pkType {
	case schemapb.DataType_Int64:
		pkField.GetScalars().Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pkData.([]int64)}}
	case schemapb.DataType_VarChar:
		pkField.GetScalars().Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: pkData.([]string)}}
	}

	flat := make([]float32, 0, int64(len(vec))*dim)
	for i, v := range vec {
		if validData != nil && !validData[i] {
			continue
		}
		flat = append(flat, v...)
	}
	vecField := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: "vec",
		FieldId:   101,
		ValidData: validData,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: flat}},
			},
		},
	}
	return []*schemapb.FieldData{pkField, vecField}
}

func TestReorderFieldDataByInputIDs_Int64PK(t *testing.T) {
	// Query result is in PK ASC order [pk=1 vec=[1,0], pk=2 vec=[0,1]];
	// caller asked for ids=[2, 1] and must get rows back in that order.
	src := buildPKAscFieldsData(
		schemapb.DataType_Int64,
		[]int64{1, 2},
		[][]float32{{1, 0}, {0, 1}},
		2,
		nil,
	)
	inputIDs := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2, 1}}}}

	dst, err := ReorderFieldDataByInputIDs(src, src[0], inputIDs)
	require.NoError(t, err)
	require.Len(t, dst, 2)

	assert.Equal(t, []int64{2, 1}, dst[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float32{0, 1, 1, 0}, dst[1].GetVectors().GetFloatVector().GetData())
}

func TestReorderFieldDataByInputIDs_VarCharPK(t *testing.T) {
	src := buildPKAscFieldsData(
		schemapb.DataType_VarChar,
		[]string{"a", "b", "c"},
		[][]float32{{1, 0}, {0, 1}, {1, 1}},
		2,
		nil,
	)
	inputIDs := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"c", "a", "b"}}}}

	dst, err := ReorderFieldDataByInputIDs(src, src[0], inputIDs)
	require.NoError(t, err)
	require.Len(t, dst, 2)

	assert.Equal(t, []string{"c", "a", "b"}, dst[0].GetScalars().GetStringData().GetData())
	assert.Equal(t, []float32{1, 1, 1, 0, 0, 1}, dst[1].GetVectors().GetFloatVector().GetData())
}

func TestReorderFieldDataByInputIDs_NullableVector(t *testing.T) {
	// pk=2 is null. Input order [3, 2, 1] → validData and the compact vector
	// slice must both follow the input order.
	src := buildPKAscFieldsData(
		schemapb.DataType_Int64,
		[]int64{1, 2, 3},
		[][]float32{{1, 0}, {9, 9}, {0, 1}}, // {9,9} placeholder, not actually stored
		2,
		[]bool{true, false, true},
	)
	inputIDs := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3, 2, 1}}}}

	dst, err := ReorderFieldDataByInputIDs(src, src[0], inputIDs)
	require.NoError(t, err)
	require.Len(t, dst, 2)

	assert.Equal(t, []int64{3, 2, 1}, dst[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []bool{true, false, true}, dst[1].GetValidData())
	assert.Equal(t, []float32{0, 1, 1, 0}, dst[1].GetVectors().GetFloatVector().GetData())
}

func TestReorderFieldDataByInputIDs_PlaceholderBytesPreserveOrder(t *testing.T) {
	// End-to-end check matching what handleIfSearchByPK does: reorder the
	// reduced query result by input IDs, then pack the vector field into a
	// PlaceholderGroup. Decode the bytes back to confirm vectors are in
	// input-ID order, not PK-ASC order.
	src := buildPKAscFieldsData(
		schemapb.DataType_Int64,
		[]int64{1, 2},
		[][]float32{{1, 0}, {0, 1}},
		2,
		nil,
	)
	inputIDs := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2, 1}}}}

	dst, err := ReorderFieldDataByInputIDs(src, src[0], inputIDs)
	require.NoError(t, err)

	bytes, count, err := FieldDataToPlaceholderGroupBytesWithCount(dst[1])
	require.NoError(t, err)
	require.Equal(t, 2, count)

	pg := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(bytes, pg))
	require.Len(t, pg.GetPlaceholders(), 1)
	values := pg.GetPlaceholders()[0].GetValues()
	require.Len(t, values, 2)
	// First placeholder vector must be vec of pk=2 = [0,1] (8 little-endian bytes).
	require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0x80, 0x3f}, values[0])
	// Second is vec of pk=1 = [1,0].
	require.Equal(t, []byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0}, values[1])
}

func TestReorderFieldDataByInputIDs_MissingPKReturnsError(t *testing.T) {
	src := buildPKAscFieldsData(
		schemapb.DataType_Int64,
		[]int64{1, 2},
		[][]float32{{1, 0}, {0, 1}},
		2,
		nil,
	)
	inputIDs := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2, 99}}}}

	_, err := ReorderFieldDataByInputIDs(src, src[0], inputIDs)
	assert.Error(t, err)
}
