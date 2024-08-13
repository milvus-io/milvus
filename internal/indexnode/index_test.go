package indexnode

import (
	"fmt"
	"math/rand"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func generateFloatVectors(nb, dim int) []float32 {
	vectors := make([]float32, 0)
	for i := 0; i < nb; i++ {
		for j := 0; j < dim; j++ {
			vectors = append(vectors, rand.Float32())
		}
	}
	return vectors
}

func generateTestSchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, Name: "ts", DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, Name: "rowid", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "bool", DataType: schemapb.DataType_Bool},
		{FieldID: 101, Name: "int8", DataType: schemapb.DataType_Int8},
		{FieldID: 102, Name: "int16", DataType: schemapb.DataType_Int16},
		{FieldID: 103, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 104, Name: "float", DataType: schemapb.DataType_Float},
		{FieldID: 105, Name: "double", DataType: schemapb.DataType_Double},
		{FieldID: 106, Name: "varchar", DataType: schemapb.DataType_VarChar},
		{FieldID: 107, Name: "string", DataType: schemapb.DataType_String},
		{FieldID: 108, Name: "array", DataType: schemapb.DataType_Array},
		{FieldID: 109, Name: "json", DataType: schemapb.DataType_JSON},
		{FieldID: 110, Name: "int32", DataType: schemapb.DataType_Int32},
		{FieldID: 111, Name: "floatVector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 112, Name: "binaryVector", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 113, Name: "float16Vector", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 114, Name: "bf16Vector", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 115, Name: "sparseFloatVector", DataType: schemapb.DataType_SparseFloatVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "28433"},
		}},
	}}

	return schema
}

func generateTestData(collID, partID, segID int64, num int) ([]*Blob, error) {
	insertCodec := storage.NewInsertCodecWithSchema(&etcdpb.CollectionMeta{ID: collID, Schema: generateTestSchema()})

	var (
		field0 []int64
		field1 []int64

		field10 []bool
		field11 []int8
		field12 []int16
		field13 []int64
		field14 []float32
		field15 []float64
		field16 []string
		field17 []string
		field18 []*schemapb.ScalarField
		field19 [][]byte

		field101 []int32
		field102 []float32
		field103 []byte

		field104 []byte
		field105 []byte
		field106 [][]byte
	)

	for i := 1; i <= num; i++ {
		field0 = append(field0, int64(i))
		field1 = append(field1, int64(i))
		field10 = append(field10, true)
		field11 = append(field11, int8(i))
		field12 = append(field12, int16(i))
		field13 = append(field13, int64(i))
		field14 = append(field14, float32(i))
		field15 = append(field15, float64(i))
		field16 = append(field16, fmt.Sprint(i))
		field17 = append(field17, fmt.Sprint(i))

		arr := &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{int32(i), int32(i), int32(i)}},
			},
		}
		field18 = append(field18, arr)

		field19 = append(field19, []byte{byte(i)})
		field101 = append(field101, int32(i))

		f102 := make([]float32, 8)
		for j := range f102 {
			f102[j] = float32(i)
		}

		field102 = append(field102, f102...)
		field103 = append(field103, 0xff)

		f104 := make([]byte, 16)
		for j := range f104 {
			f104[j] = byte(i)
		}
		field104 = append(field104, f104...)
		field105 = append(field105, f104...)

		field106 = append(field106, typeutil.CreateSparseFloatRow([]uint32{0, uint32(18 * i), uint32(284 * i)}, []float32{1.1, 0.3, 2.4}))
	}

	data := &storage.InsertData{Data: map[int64]storage.FieldData{
		common.RowIDField:     &storage.Int64FieldData{Data: field0},
		common.TimeStampField: &storage.Int64FieldData{Data: field1},

		100: &storage.BoolFieldData{Data: field10},
		101: &storage.Int8FieldData{Data: field11},
		102: &storage.Int16FieldData{Data: field12},
		103: &storage.Int64FieldData{Data: field13},
		104: &storage.FloatFieldData{Data: field14},
		105: &storage.DoubleFieldData{Data: field15},
		106: &storage.StringFieldData{Data: field16},
		107: &storage.StringFieldData{Data: field17},
		108: &storage.ArrayFieldData{Data: field18},
		109: &storage.JSONFieldData{Data: field19},
		110: &storage.Int32FieldData{Data: field101},
		111: &storage.FloatVectorFieldData{
			Data: field102,
			Dim:  8,
		},
		112: &storage.BinaryVectorFieldData{
			Data: field103,
			Dim:  8,
		},
		113: &storage.Float16VectorFieldData{
			Data: field104,
			Dim:  8,
		},
		114: &storage.BFloat16VectorFieldData{
			Data: field105,
			Dim:  8,
		},
		115: &storage.SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Dim:      28433,
				Contents: field106,
			},
		},
	}}

	blobs, err := insertCodec.Serialize(partID, segID, data)
	return blobs, err
}
