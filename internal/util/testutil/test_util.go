package testutil

import (
	rand2 "crypto/rand"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/testutils"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	testMaxVarCharLength = 100
)

func ConstructCollectionSchemaWithKeys(collectionName string,
	fieldName2DataType map[string]schemapb.DataType,
	primaryFieldName string,
	partitionKeyFieldName string,
	clusteringKeyFieldName string,
	autoID bool,
	dim int,
) *schemapb.CollectionSchema {
	schema := ConstructCollectionSchemaByDataType(collectionName,
		fieldName2DataType,
		primaryFieldName,
		autoID,
		dim)
	for _, field := range schema.Fields {
		if field.Name == partitionKeyFieldName {
			field.IsPartitionKey = true
		}
		if field.Name == clusteringKeyFieldName {
			field.IsClusteringKey = true
		}
	}

	return schema
}

func ConstructCollectionSchemaByDataType(collectionName string,
	fieldName2DataType map[string]schemapb.DataType,
	primaryFieldName string,
	autoID bool,
	dim int,
) *schemapb.CollectionSchema {
	fieldsSchema := make([]*schemapb.FieldSchema, 0)
	fieldIdx := int64(0)
	for fieldName, dataType := range fieldName2DataType {
		fieldSchema := &schemapb.FieldSchema{
			Name:     fieldName,
			DataType: dataType,
			FieldID:  fieldIdx,
		}
		fieldIdx += 1
		if typeutil.IsVectorType(dataType) {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			}
		}
		if dataType == schemapb.DataType_VarChar {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: strconv.Itoa(testMaxVarCharLength),
				},
			}
		}
		if fieldName == primaryFieldName {
			fieldSchema.IsPrimaryKey = true
			fieldSchema.AutoID = autoID
		}

		fieldsSchema = append(fieldsSchema, fieldSchema)
	}

	return &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: fieldsSchema,
	}
}

func randomString(length int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func CreateInsertData(schema *schemapb.CollectionSchema, rows int) (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(schema)
	if err != nil {
		return nil, err
	}
	for _, field := range schema.GetFields() {
		if field.GetAutoID() {
			continue
		}
		switch field.GetDataType() {
		case schemapb.DataType_Bool:
			boolData := make([]bool, 0)
			for i := 0; i < rows; i++ {
				boolData = append(boolData, i%3 != 0)
			}
			insertData.Data[field.GetFieldID()] = &storage.BoolFieldData{Data: boolData}
		case schemapb.DataType_Float:
			floatData := make([]float32, 0)
			for i := 0; i < rows; i++ {
				floatData = append(floatData, float32(i/2))
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatFieldData{Data: floatData}
		case schemapb.DataType_Double:
			doubleData := make([]float64, 0)
			for i := 0; i < rows; i++ {
				doubleData = append(doubleData, float64(i/5))
			}
			insertData.Data[field.GetFieldID()] = &storage.DoubleFieldData{Data: doubleData}
		case schemapb.DataType_Int8:
			int8Data := make([]int8, 0)
			for i := 0; i < rows; i++ {
				int8Data = append(int8Data, int8(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int8FieldData{Data: int8Data}
		case schemapb.DataType_Int16:
			int16Data := make([]int16, 0)
			for i := 0; i < rows; i++ {
				int16Data = append(int16Data, int16(i%65536))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int16FieldData{Data: int16Data}
		case schemapb.DataType_Int32:
			int32Data := make([]int32, 0)
			for i := 0; i < rows; i++ {
				int32Data = append(int32Data, int32(i%1000))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int32FieldData{Data: int32Data}
		case schemapb.DataType_Int64:
			int64Data := make([]int64, 0)
			for i := 0; i < rows; i++ {
				int64Data = append(int64Data, int64(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int64FieldData{Data: int64Data}
		case schemapb.DataType_BinaryVector:
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			binVecData := make([]byte, 0)
			total := rows * int(dim) / 8
			for i := 0; i < total; i++ {
				binVecData = append(binVecData, byte(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.BinaryVectorFieldData{Data: binVecData, Dim: int(dim)}
		case schemapb.DataType_FloatVector:
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			floatVecData := make([]float32, 0)
			total := rows * int(dim)
			for i := 0; i < total; i++ {
				floatVecData = append(floatVecData, rand.Float32())
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatVectorFieldData{Data: floatVecData, Dim: int(dim)}
		case schemapb.DataType_Float16Vector:
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			total := int64(rows) * dim * 2
			float16VecData := make([]byte, total)
			_, err = rand2.Read(float16VecData)
			if err != nil {
				return nil, err
			}
			insertData.Data[field.GetFieldID()] = &storage.Float16VectorFieldData{Data: float16VecData, Dim: int(dim)}
		case schemapb.DataType_BFloat16Vector:
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			total := int64(rows) * dim * 2
			bfloat16VecData := make([]byte, total)
			_, err = rand2.Read(bfloat16VecData)
			if err != nil {
				return nil, err
			}
			insertData.Data[field.GetFieldID()] = &storage.BFloat16VectorFieldData{Data: bfloat16VecData, Dim: int(dim)}
		case schemapb.DataType_SparseFloatVector:
			sparseFloatVecData := testutils.GenerateSparseFloatVectors(rows)
			insertData.Data[field.GetFieldID()] = &storage.SparseFloatVectorFieldData{
				SparseFloatArray: *sparseFloatVecData,
			}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			varcharData := make([]string, 0)
			for i := 0; i < rows; i++ {
				varcharData = append(varcharData, strconv.Itoa(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.StringFieldData{Data: varcharData}
		case schemapb.DataType_JSON:
			jsonData := make([][]byte, 0)
			for i := 0; i < rows; i++ {
				if i%4 == 0 {
					v, _ := json.Marshal("{\"a\": \"%s\", \"b\": %d}")
					jsonData = append(jsonData, v)
				} else if i%4 == 1 {
					v, _ := json.Marshal(i)
					jsonData = append(jsonData, v)
				} else if i%4 == 2 {
					v, _ := json.Marshal(float32(i) * 0.1)
					jsonData = append(jsonData, v)
				} else if i%4 == 3 {
					v, _ := json.Marshal(strconv.Itoa(i))
					jsonData = append(jsonData, v)
				}
			}
			insertData.Data[field.GetFieldID()] = &storage.JSONFieldData{Data: jsonData}
		case schemapb.DataType_Array:
			arrayData := make([]*schemapb.ScalarField, 0)
			switch field.GetElementType() {
			case schemapb.DataType_Bool:
				for i := 0; i < rows; i++ {
					data := []bool{i%2 == 0, i%3 == 0, i%4 == 0}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: data,
							},
						},
					})
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
			case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
				for i := 0; i < rows; i++ {
					data := []int32{int32(i), int32(i + 1), int32(i + 2)}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: data,
							},
						},
					})
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
			case schemapb.DataType_Int64:
				for i := 0; i < rows; i++ {
					data := []int64{int64(i), int64(i + 1), int64(i + 2)}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: data,
							},
						},
					})
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
			case schemapb.DataType_Float:
				for i := 0; i < rows; i++ {
					data := []float32{float32(i) * 0.1, float32(i+1) * 0.1, float32(i+2) * 0.1}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: data,
							},
						},
					})
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
			case schemapb.DataType_Double:
				for i := 0; i < rows; i++ {
					data := []float64{float64(i) * 0.02, float64(i+1) * 0.02, float64(i+2) * 0.02}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: data,
							},
						},
					})
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
			case schemapb.DataType_String, schemapb.DataType_VarChar:
				for i := 0; i < rows; i++ {
					data := []string{
						randomString(5) + "-" + fmt.Sprintf("%d", i),
						randomString(5) + "-" + fmt.Sprintf("%d", i),
						randomString(5) + "-" + fmt.Sprintf("%d", i),
					}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: data,
							},
						},
					})
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
			}
		default:
			panic(fmt.Sprintf("unexpected data type: %s", field.GetDataType().String()))
		}
	}
	return insertData, nil
}

func BuildArrayData(schema *schemapb.CollectionSchema, insertData *storage.InsertData) ([]arrow.Array, error) {
	mem := memory.NewGoAllocator()
	columns := make([]arrow.Array, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		if field.GetIsPrimaryKey() && field.GetAutoID() {
			continue
		}
		fieldID := field.GetFieldID()
		dataType := field.GetDataType()
		elementType := field.GetElementType()
		switch dataType {
		case schemapb.DataType_Bool:
			builder := array.NewBooleanBuilder(mem)
			boolData := insertData.Data[fieldID].(*storage.BoolFieldData).Data
			builder.AppendValues(boolData, nil)
			columns = append(columns, builder.NewBooleanArray())
		case schemapb.DataType_Int8:
			builder := array.NewInt8Builder(mem)
			int8Data := insertData.Data[fieldID].(*storage.Int8FieldData).Data
			builder.AppendValues(int8Data, nil)
			columns = append(columns, builder.NewInt8Array())
		case schemapb.DataType_Int16:
			builder := array.NewInt16Builder(mem)
			int16Data := insertData.Data[fieldID].(*storage.Int16FieldData).Data
			builder.AppendValues(int16Data, nil)
			columns = append(columns, builder.NewInt16Array())
		case schemapb.DataType_Int32:
			builder := array.NewInt32Builder(mem)
			int32Data := insertData.Data[fieldID].(*storage.Int32FieldData).Data
			builder.AppendValues(int32Data, nil)
			columns = append(columns, builder.NewInt32Array())
		case schemapb.DataType_Int64:
			builder := array.NewInt64Builder(mem)
			int64Data := insertData.Data[fieldID].(*storage.Int64FieldData).Data
			builder.AppendValues(int64Data, nil)
			columns = append(columns, builder.NewInt64Array())
		case schemapb.DataType_Float:
			builder := array.NewFloat32Builder(mem)
			floatData := insertData.Data[fieldID].(*storage.FloatFieldData).Data
			builder.AppendValues(floatData, nil)
			columns = append(columns, builder.NewFloat32Array())
		case schemapb.DataType_Double:
			builder := array.NewFloat64Builder(mem)
			doubleData := insertData.Data[fieldID].(*storage.DoubleFieldData).Data
			builder.AppendValues(doubleData, nil)
			columns = append(columns, builder.NewFloat64Array())
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			builder := array.NewStringBuilder(mem)
			stringData := insertData.Data[fieldID].(*storage.StringFieldData).Data
			builder.AppendValues(stringData, nil)
			columns = append(columns, builder.NewStringArray())
		case schemapb.DataType_BinaryVector:
			builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
			dim := insertData.Data[fieldID].(*storage.BinaryVectorFieldData).Dim
			binVecData := insertData.Data[fieldID].(*storage.BinaryVectorFieldData).Data
			rowBytes := dim / 8
			rows := len(binVecData) / rowBytes
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0)
			for i := 0; i < rows; i++ {
				offsets = append(offsets, int32(i*rowBytes))
				valid = append(valid, true)
			}
			builder.ValueBuilder().(*array.Uint8Builder).AppendValues(binVecData, nil)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_FloatVector:
			builder := array.NewListBuilder(mem, &arrow.Float32Type{})
			dim := insertData.Data[fieldID].(*storage.FloatVectorFieldData).Dim
			floatVecData := insertData.Data[fieldID].(*storage.FloatVectorFieldData).Data
			rows := len(floatVecData) / dim
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			for i := 0; i < rows; i++ {
				offsets = append(offsets, int32(i*dim))
				valid = append(valid, true)
			}
			builder.ValueBuilder().(*array.Float32Builder).AppendValues(floatVecData, nil)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_Float16Vector:
			builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
			dim := insertData.Data[fieldID].(*storage.Float16VectorFieldData).Dim
			float16VecData := insertData.Data[fieldID].(*storage.Float16VectorFieldData).Data
			rowBytes := dim * 2
			rows := len(float16VecData) / rowBytes
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			for i := 0; i < rows; i++ {
				offsets = append(offsets, int32(i*rowBytes))
				valid = append(valid, true)
			}
			builder.ValueBuilder().(*array.Uint8Builder).AppendValues(float16VecData, nil)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_BFloat16Vector:
			builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
			dim := insertData.Data[fieldID].(*storage.BFloat16VectorFieldData).Dim
			bfloat16VecData := insertData.Data[fieldID].(*storage.BFloat16VectorFieldData).Data
			rowBytes := dim * 2
			rows := len(bfloat16VecData) / rowBytes
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			for i := 0; i < rows; i++ {
				offsets = append(offsets, int32(i*rowBytes))
				valid = append(valid, true)
			}
			builder.ValueBuilder().(*array.Uint8Builder).AppendValues(bfloat16VecData, nil)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_SparseFloatVector:
			sparseFloatVecData := make([]byte, 0)
			builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
			contents := insertData.Data[fieldID].(*storage.SparseFloatVectorFieldData).GetContents()
			rows := len(contents)
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			currOffset := int32(0)
			for i := 0; i < rows; i++ {
				rowVecData := contents[i]
				sparseFloatVecData = append(sparseFloatVecData, rowVecData...)
				offsets = append(offsets, currOffset)
				currOffset = currOffset + int32(len(rowVecData))
				valid = append(valid, true)
			}
			builder.ValueBuilder().(*array.Uint8Builder).AppendValues(sparseFloatVecData, nil)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_JSON:
			builder := array.NewStringBuilder(mem)
			jsonData := insertData.Data[fieldID].(*storage.JSONFieldData).Data
			builder.AppendValues(lo.Map(jsonData, func(bs []byte, _ int) string {
				return string(bs)
			}), nil)
			columns = append(columns, builder.NewStringArray())
		case schemapb.DataType_Array:
			data := insertData.Data[fieldID].(*storage.ArrayFieldData).Data
			rows := len(data)
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			currOffset := int32(0)

			switch elementType {
			case schemapb.DataType_Bool:
				builder := array.NewListBuilder(mem, &arrow.BooleanType{})
				valueBuilder := builder.ValueBuilder().(*array.BooleanBuilder)
				for i := 0; i < rows; i++ {
					boolData := data[i].Data.(*schemapb.ScalarField_BoolData).BoolData.GetData()
					valueBuilder.AppendValues(boolData, nil)

					offsets = append(offsets, currOffset)
					valid = append(valid, true)
					currOffset = currOffset + int32(len(boolData))
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int8:
				builder := array.NewListBuilder(mem, &arrow.Int8Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int8Builder)
				for i := 0; i < rows; i++ {
					intData := data[i].Data.(*schemapb.ScalarField_IntData).IntData.GetData()
					int8Data := make([]int8, 0)
					for j := 0; j < len(intData); j++ {
						int8Data = append(int8Data, int8(intData[j]))
					}
					valueBuilder.AppendValues(int8Data, nil)

					offsets = append(offsets, currOffset)
					valid = append(valid, true)
					currOffset = currOffset + int32(len(int8Data))
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int16:
				builder := array.NewListBuilder(mem, &arrow.Int16Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int16Builder)
				for i := 0; i < rows; i++ {
					intData := data[i].Data.(*schemapb.ScalarField_IntData).IntData.GetData()
					int16Data := make([]int16, 0)
					for j := 0; j < len(intData); j++ {
						int16Data = append(int16Data, int16(intData[j]))
					}
					valueBuilder.AppendValues(int16Data, nil)

					offsets = append(offsets, currOffset)
					valid = append(valid, true)
					currOffset = currOffset + int32(len(int16Data))
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int32:
				builder := array.NewListBuilder(mem, &arrow.Int32Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int32Builder)
				for i := 0; i < rows; i++ {
					intData := data[i].Data.(*schemapb.ScalarField_IntData).IntData.GetData()
					valueBuilder.AppendValues(intData, nil)

					offsets = append(offsets, currOffset)
					valid = append(valid, true)
					currOffset = currOffset + int32(len(intData))
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int64:
				builder := array.NewListBuilder(mem, &arrow.Int64Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int64Builder)
				for i := 0; i < rows; i++ {
					longData := data[i].Data.(*schemapb.ScalarField_LongData).LongData.GetData()
					valueBuilder.AppendValues(longData, nil)

					offsets = append(offsets, currOffset)
					valid = append(valid, true)
					currOffset = currOffset + int32(len(longData))
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Float:
				builder := array.NewListBuilder(mem, &arrow.Float32Type{})
				valueBuilder := builder.ValueBuilder().(*array.Float32Builder)
				for i := 0; i < rows; i++ {
					floatData := data[i].Data.(*schemapb.ScalarField_FloatData).FloatData.GetData()
					valueBuilder.AppendValues(floatData, nil)

					offsets = append(offsets, currOffset)
					valid = append(valid, true)
					currOffset = currOffset + int32(len(floatData))
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Double:
				builder := array.NewListBuilder(mem, &arrow.Float64Type{})
				valueBuilder := builder.ValueBuilder().(*array.Float64Builder)
				for i := 0; i < rows; i++ {
					doubleData := data[i].Data.(*schemapb.ScalarField_DoubleData).DoubleData.GetData()
					valueBuilder.AppendValues(doubleData, nil)

					offsets = append(offsets, currOffset)
					valid = append(valid, true)
					currOffset = currOffset + int32(len(doubleData))
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_VarChar, schemapb.DataType_String:
				builder := array.NewListBuilder(mem, &arrow.StringType{})
				valueBuilder := builder.ValueBuilder().(*array.StringBuilder)
				for i := 0; i < rows; i++ {
					stringData := data[i].Data.(*schemapb.ScalarField_StringData).StringData.GetData()
					valueBuilder.AppendValues(stringData, nil)

					offsets = append(offsets, currOffset)
					valid = append(valid, true)
					currOffset = currOffset + int32(len(stringData))
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			}
		}
	}
	return columns, nil
}
