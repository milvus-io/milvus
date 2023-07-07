package querynode

import (
	"sort"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"

	"github.com/stretchr/testify/assert"
)

func TestResultSorter_ByIntPK(t *testing.T) {
	result := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{5, 4, 3, 2, 9, 8, 7, 6},
				}},
		},
		Offset: []int64{5, 4, 3, 2, 9, 8, 7, 6},
		FieldsData: []*schemapb.FieldData{
			genFieldData("int64 field", 100, schemapb.DataType_Int64,
				[]int64{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("double field", 101, schemapb.DataType_Double,
				[]float64{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("string field", 102, schemapb.DataType_VarChar,
				[]string{"5", "4", "3", "2", "9", "8", "7", "6"}, 1),
			genFieldData("bool field", 103, schemapb.DataType_Bool,
				[]bool{false, true, false, true, false, true, false, true}, 1),
			genFieldData("float field", 104, schemapb.DataType_Float,
				[]float32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("int field", 105, schemapb.DataType_Int32,
				[]int32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("float vector field", 106, schemapb.DataType_FloatVector,
				[]float32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("binary vector field", 107, schemapb.DataType_BinaryVector,
				[]byte{5, 4, 3, 2, 9, 8, 7, 6}, 8),
			genFieldData("json field", 108, schemapb.DataType_JSON,
				[][]byte{[]byte("{\"5\": 5}"), []byte("{\"4\": 4}"), []byte("{\"3\": 3}"), []byte("{\"2\": 2}"),
					[]byte("{\"9\": 9}"), []byte("{\"8\": 8}"), []byte("{\"7\": 7}"), []byte("{\"6\": 6}")}, 1),
			genFieldData("json field", 108, schemapb.DataType_Array,
				[]*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{5, 6, 7}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{4, 5, 6}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3, 4, 5}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{2, 3, 4}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{9, 10, 11}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{8, 9, 10}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{7, 8, 9}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{6, 7, 8}}}},
				}, 1),
		},
	}

	sort.Sort(&byPK{result})

	assert.Equal(t, []int64{2, 3, 4, 5, 6, 7, 8, 9}, result.GetIds().GetIntId().GetData())
	assert.Equal(t, []int64{2, 3, 4, 5, 6, 7, 8, 9}, result.GetOffset())
	assert.Equal(t, []int64{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[0].GetScalars().GetLongData().Data)
	assert.InDeltaSlice(t, []float64{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[1].GetScalars().GetDoubleData().Data, 10e-10)
	assert.Equal(t, []string{"2", "3", "4", "5", "6", "7", "8", "9"}, result.FieldsData[2].GetScalars().GetStringData().Data)
	assert.Equal(t, []bool{true, false, true, false, true, false, true, false}, result.FieldsData[3].GetScalars().GetBoolData().Data)
	assert.InDeltaSlice(t, []float32{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[4].GetScalars().GetFloatData().Data, 10e-10)
	assert.Equal(t, []int32{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[5].GetScalars().GetIntData().Data)
	assert.InDeltaSlice(t, []float32{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[6].GetVectors().GetFloatVector().GetData(), 10e-10)
	assert.Equal(t, []byte{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[7].GetVectors().GetBinaryVector())
	assert.Equal(t, [][]byte{[]byte("{\"2\": 2}"), []byte("{\"3\": 3}"), []byte("{\"4\": 4}"), []byte("{\"5\": 5}"),
		[]byte("{\"6\": 6}"), []byte("{\"7\": 7}"), []byte("{\"8\": 8}"), []byte("{\"9\": 9}")}, result.FieldsData[8].GetScalars().GetJsonData().GetData())
	assert.Equal(t, []*schemapb.ScalarField{
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{2, 3, 4}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3, 4, 5}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{4, 5, 6}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{5, 6, 7}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{6, 7, 8}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{7, 8, 9}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{8, 9, 10}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{9, 10, 11}}}},
	}, result.FieldsData[9].GetScalars().GetArrayData().GetData())
}
