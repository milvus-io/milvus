package testutil

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
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

func CreateInsertData(schema *schemapb.CollectionSchema, rows int, nullPercent ...int) (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(schema)
	if err != nil {
		return nil, err
	}
	for _, f := range schema.GetFields() {
		if f.GetAutoID() || f.IsFunctionOutput {
			continue
		}
		switch f.GetDataType() {
		case schemapb.DataType_Bool:
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateBoolArray(rows))
		case schemapb.DataType_Int8:
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateInt8Array(rows))
		case schemapb.DataType_Int16:
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateInt16Array(rows))
		case schemapb.DataType_Int32:
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateInt32Array(rows))
		case schemapb.DataType_Int64:
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateInt64Array(rows))
		case schemapb.DataType_Float:
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateFloat32Array(rows))
		case schemapb.DataType_Double:
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateFloat64Array(rows))
		case schemapb.DataType_BinaryVector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			insertData.Data[f.FieldID] = &storage.BinaryVectorFieldData{
				Data: testutils.GenerateBinaryVectors(rows, int(dim)),
				Dim:  int(dim),
			}
		case schemapb.DataType_FloatVector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			insertData.Data[f.GetFieldID()] = &storage.FloatVectorFieldData{
				Data: testutils.GenerateFloatVectors(rows, int(dim)),
				Dim:  int(dim),
			}
		case schemapb.DataType_Float16Vector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			insertData.Data[f.FieldID] = &storage.Float16VectorFieldData{
				Data: testutils.GenerateFloat16Vectors(rows, int(dim)),
				Dim:  int(dim),
			}
		case schemapb.DataType_BFloat16Vector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			insertData.Data[f.FieldID] = &storage.BFloat16VectorFieldData{
				Data: testutils.GenerateBFloat16Vectors(rows, int(dim)),
				Dim:  int(dim),
			}
		case schemapb.DataType_SparseFloatVector:
			data, dim := testutils.GenerateSparseFloatVectorsData(rows)
			insertData.Data[f.FieldID] = &storage.SparseFloatVectorFieldData{
				SparseFloatArray: schemapb.SparseFloatArray{
					Contents: data,
					Dim:      dim,
				},
			}
		case schemapb.DataType_Int8Vector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			insertData.Data[f.FieldID] = &storage.Int8VectorFieldData{
				Data: testutils.GenerateInt8Vectors(rows, int(dim)),
				Dim:  int(dim),
			}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateStringArray(rows))
		case schemapb.DataType_JSON:
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateJSONArray(rows))
		case schemapb.DataType_Array:
			switch f.GetElementType() {
			case schemapb.DataType_Bool:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfBoolArray(rows))
			case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfIntArray(rows))
			case schemapb.DataType_Int64:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfLongArray(rows))
			case schemapb.DataType_Float:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfFloatArray(rows))
			case schemapb.DataType_Double:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfDoubleArray(rows))
			case schemapb.DataType_String, schemapb.DataType_VarChar:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfStringArray(rows))
			}
		default:
			panic(fmt.Sprintf("unsupported data type: %s", f.GetDataType().String()))
		}
		if f.GetNullable() {
			if len(nullPercent) > 1 {
				return nil, merr.WrapErrParameterInvalidMsg("the length of nullPercent is wrong")
			}
			if len(nullPercent) == 0 || nullPercent[0] == 50 {
				insertData.Data[f.FieldID].AppendValidDataRows(testutils.GenerateBoolArray(rows))
			} else if len(nullPercent) == 1 && nullPercent[0] == 100 {
				insertData.Data[f.FieldID].AppendValidDataRows(make([]bool, rows))
			} else if len(nullPercent) == 1 && nullPercent[0] == 0 {
				validData := make([]bool, rows)
				for i := range validData {
					validData[i] = true
				}
				insertData.Data[f.FieldID].AppendValidDataRows(validData)
			} else {
				return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("not support the number of nullPercent(%d)", nullPercent))
			}
		}
	}
	return insertData, nil
}

func CreateFieldWithDefaultValue(dataType schemapb.DataType, id int64, nullable bool) (*schemapb.FieldSchema, error) {
	field := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     dataType.String(),
		DataType: dataType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "128",
			},
			{
				Key:   common.MaxCapacityKey,
				Value: "128",
			},
		},
		Nullable: nullable,
	}

	switch field.GetDataType() {
	case schemapb.DataType_Bool:
		field.DefaultValue = &schemapb.ValueField{
			Data: &schemapb.ValueField_BoolData{
				BoolData: ([]bool{true, false})[rand.Intn(2)],
			},
		}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		field.DefaultValue = &schemapb.ValueField{
			Data: &schemapb.ValueField_IntData{
				IntData: ([]int32{1, 10, 100, 1000})[rand.Intn(4)],
			},
		}
	case schemapb.DataType_Int64:
		field.DefaultValue = &schemapb.ValueField{
			Data: &schemapb.ValueField_LongData{
				LongData: rand.Int63(),
			},
		}
	case schemapb.DataType_Float:
		field.DefaultValue = &schemapb.ValueField{
			Data: &schemapb.ValueField_FloatData{
				FloatData: rand.Float32(),
			},
		}
	case schemapb.DataType_Double:
		field.DefaultValue = &schemapb.ValueField{
			Data: &schemapb.ValueField_DoubleData{
				DoubleData: rand.Float64(),
			},
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		field.DefaultValue = &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{
				StringData: randomString(10),
			},
		}
	default:
		msg := fmt.Sprintf("type (%s) not support default_value", field.GetDataType().String())
		return nil, merr.WrapErrParameterInvalidMsg(msg)
	}
	return field, nil
}

func BuildArrayData(schema *schemapb.CollectionSchema, insertData *storage.InsertData, useNullType bool) ([]arrow.Array, error) {
	mem := memory.NewGoAllocator()
	columns := make([]arrow.Array, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		if field.GetIsPrimaryKey() && field.GetAutoID() || field.GetIsFunctionOutput() {
			continue
		}
		fieldID := field.GetFieldID()
		dataType := field.GetDataType()
		elementType := field.GetElementType()
		if field.GetNullable() && useNullType {
			columns = append(columns, array.NewNull(insertData.Data[fieldID].RowNum()))
			continue
		}
		switch dataType {
		case schemapb.DataType_Bool:
			builder := array.NewBooleanBuilder(mem)
			boolData := insertData.Data[fieldID].(*storage.BoolFieldData).Data
			validData := insertData.Data[fieldID].(*storage.BoolFieldData).ValidData
			builder.AppendValues(boolData, validData)

			columns = append(columns, builder.NewBooleanArray())
		case schemapb.DataType_Int8:
			builder := array.NewInt8Builder(mem)
			int8Data := insertData.Data[fieldID].(*storage.Int8FieldData).Data
			validData := insertData.Data[fieldID].(*storage.Int8FieldData).ValidData
			builder.AppendValues(int8Data, validData)
			columns = append(columns, builder.NewInt8Array())
		case schemapb.DataType_Int16:
			builder := array.NewInt16Builder(mem)
			int16Data := insertData.Data[fieldID].(*storage.Int16FieldData).Data
			validData := insertData.Data[fieldID].(*storage.Int16FieldData).ValidData
			builder.AppendValues(int16Data, validData)
			columns = append(columns, builder.NewInt16Array())
		case schemapb.DataType_Int32:
			builder := array.NewInt32Builder(mem)
			int32Data := insertData.Data[fieldID].(*storage.Int32FieldData).Data
			validData := insertData.Data[fieldID].(*storage.Int32FieldData).ValidData
			builder.AppendValues(int32Data, validData)
			columns = append(columns, builder.NewInt32Array())
		case schemapb.DataType_Int64:
			builder := array.NewInt64Builder(mem)
			int64Data := insertData.Data[fieldID].(*storage.Int64FieldData).Data
			validData := insertData.Data[fieldID].(*storage.Int64FieldData).ValidData
			builder.AppendValues(int64Data, validData)
			columns = append(columns, builder.NewInt64Array())
		case schemapb.DataType_Float:
			builder := array.NewFloat32Builder(mem)
			floatData := insertData.Data[fieldID].(*storage.FloatFieldData).Data
			validData := insertData.Data[fieldID].(*storage.FloatFieldData).ValidData
			builder.AppendValues(floatData, validData)
			columns = append(columns, builder.NewFloat32Array())
		case schemapb.DataType_Double:
			builder := array.NewFloat64Builder(mem)
			doubleData := insertData.Data[fieldID].(*storage.DoubleFieldData).Data
			validData := insertData.Data[fieldID].(*storage.DoubleFieldData).ValidData
			builder.AppendValues(doubleData, validData)
			columns = append(columns, builder.NewFloat64Array())
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			builder := array.NewStringBuilder(mem)
			stringData := insertData.Data[fieldID].(*storage.StringFieldData).Data
			validData := insertData.Data[fieldID].(*storage.StringFieldData).ValidData
			builder.AppendValues(stringData, validData)
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
			builder := array.NewStringBuilder(mem)
			contents := insertData.Data[fieldID].(*storage.SparseFloatVectorFieldData).GetContents()
			rows := len(contents)
			jsonBytesData := make([][]byte, 0)
			for i := 0; i < rows; i++ {
				rowVecData := contents[i]
				mapData := typeutil.SparseFloatBytesToMap(rowVecData)
				// convert to JSON format
				jsonBytes, err := json.Marshal(mapData)
				if err != nil {
					return nil, err
				}
				jsonBytesData = append(jsonBytesData, jsonBytes)
			}
			builder.AppendValues(lo.Map(jsonBytesData, func(bs []byte, _ int) string {
				return string(bs)
			}), nil)
			columns = append(columns, builder.NewStringArray())
		case schemapb.DataType_Int8Vector:
			builder := array.NewListBuilder(mem, &arrow.Int8Type{})
			dim := insertData.Data[fieldID].(*storage.Int8VectorFieldData).Dim
			int8VecData := insertData.Data[fieldID].(*storage.Int8VectorFieldData).Data
			rows := len(int8VecData) / dim
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			for i := 0; i < rows; i++ {
				offsets = append(offsets, int32(i*dim))
				valid = append(valid, true)
			}
			builder.ValueBuilder().(*array.Int8Builder).AppendValues(int8VecData, nil)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_JSON:
			builder := array.NewStringBuilder(mem)
			jsonData := insertData.Data[fieldID].(*storage.JSONFieldData).Data
			validData := insertData.Data[fieldID].(*storage.JSONFieldData).ValidData
			builder.AppendValues(lo.Map(jsonData, func(bs []byte, _ int) string {
				return string(bs)
			}), validData)
			columns = append(columns, builder.NewStringArray())
		case schemapb.DataType_Array:
			data := insertData.Data[fieldID].(*storage.ArrayFieldData).Data
			validData := insertData.Data[fieldID].(*storage.ArrayFieldData).ValidData
			rows := len(data)
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			currOffset := int32(0)

			switch elementType {
			case schemapb.DataType_Bool:
				builder := array.NewListBuilder(mem, &arrow.BooleanType{})
				valueBuilder := builder.ValueBuilder().(*array.BooleanBuilder)
				for i := 0; i < rows; i++ {
					if field.GetNullable() && !validData[i] {
						offsets = append(offsets, currOffset)
						valid = append(valid, false)
					} else {
						boolData := data[i].Data.(*schemapb.ScalarField_BoolData).BoolData.GetData()
						valueBuilder.AppendValues(boolData, nil)
						offsets = append(offsets, currOffset)
						currOffset = currOffset + int32(len(boolData))
						valid = append(valid, true)
					}
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int8:
				builder := array.NewListBuilder(mem, &arrow.Int8Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int8Builder)
				for i := 0; i < rows; i++ {
					if field.GetNullable() && !validData[i] {
						offsets = append(offsets, currOffset)
						valid = append(valid, false)
					} else {
						intData := data[i].Data.(*schemapb.ScalarField_IntData).IntData.GetData()
						int8Data := make([]int8, 0)
						for j := 0; j < len(intData); j++ {
							int8Data = append(int8Data, int8(intData[j]))
						}
						valueBuilder.AppendValues(int8Data, nil)
						offsets = append(offsets, currOffset)
						currOffset = currOffset + int32(len(int8Data))
						valid = append(valid, true)
					}
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int16:
				builder := array.NewListBuilder(mem, &arrow.Int16Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int16Builder)
				for i := 0; i < rows; i++ {
					if field.GetNullable() && !validData[i] {
						offsets = append(offsets, currOffset)
						valid = append(valid, false)
					} else {
						intData := data[i].Data.(*schemapb.ScalarField_IntData).IntData.GetData()
						int16Data := make([]int16, 0)
						for j := 0; j < len(intData); j++ {
							int16Data = append(int16Data, int16(intData[j]))
						}
						valueBuilder.AppendValues(int16Data, nil)
						offsets = append(offsets, currOffset)
						currOffset = currOffset + int32(len(int16Data))
						valid = append(valid, true)
					}
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int32:
				builder := array.NewListBuilder(mem, &arrow.Int32Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int32Builder)
				for i := 0; i < rows; i++ {
					if field.GetNullable() && !validData[i] {
						offsets = append(offsets, currOffset)
						valid = append(valid, false)
					} else {
						intData := data[i].Data.(*schemapb.ScalarField_IntData).IntData.GetData()
						valueBuilder.AppendValues(intData, nil)
						offsets = append(offsets, currOffset)
						currOffset = currOffset + int32(len(intData))
						valid = append(valid, true)
					}
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int64:
				builder := array.NewListBuilder(mem, &arrow.Int64Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int64Builder)
				for i := 0; i < rows; i++ {
					if field.GetNullable() && !validData[i] {
						offsets = append(offsets, currOffset)
						valid = append(valid, false)
					} else {
						longData := data[i].Data.(*schemapb.ScalarField_LongData).LongData.GetData()
						valueBuilder.AppendValues(longData, nil)
						offsets = append(offsets, currOffset)
						currOffset = currOffset + int32(len(longData))
						valid = append(valid, true)
					}
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Float:
				builder := array.NewListBuilder(mem, &arrow.Float32Type{})
				valueBuilder := builder.ValueBuilder().(*array.Float32Builder)
				for i := 0; i < rows; i++ {
					if field.GetNullable() && !validData[i] {
						offsets = append(offsets, currOffset)
						valid = append(valid, false)
					} else {
						floatData := data[i].Data.(*schemapb.ScalarField_FloatData).FloatData.GetData()
						valueBuilder.AppendValues(floatData, nil)
						offsets = append(offsets, currOffset)
						currOffset = currOffset + int32(len(floatData))
						valid = append(valid, true)
					}
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Double:
				builder := array.NewListBuilder(mem, &arrow.Float64Type{})
				valueBuilder := builder.ValueBuilder().(*array.Float64Builder)
				for i := 0; i < rows; i++ {
					if field.GetNullable() && !validData[i] {
						offsets = append(offsets, currOffset)
						valid = append(valid, false)
					} else {
						doubleData := data[i].Data.(*schemapb.ScalarField_DoubleData).DoubleData.GetData()
						valueBuilder.AppendValues(doubleData, nil)
						offsets = append(offsets, currOffset)
						currOffset = currOffset + int32(len(doubleData))
						valid = append(valid, true)
					}
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_VarChar, schemapb.DataType_String:
				builder := array.NewListBuilder(mem, &arrow.StringType{})
				valueBuilder := builder.ValueBuilder().(*array.StringBuilder)
				for i := 0; i < rows; i++ {
					if field.GetNullable() && !validData[i] {
						offsets = append(offsets, currOffset)
						valid = append(valid, false)
					} else {
						stringData := data[i].Data.(*schemapb.ScalarField_StringData).StringData.GetData()
						valueBuilder.AppendValues(stringData, nil)
						offsets = append(offsets, currOffset)
						currOffset = currOffset + int32(len(stringData))
						valid = append(valid, true)
					}
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			}
		}
	}
	return columns, nil
}

func CreateInsertDataRowsForJSON(schema *schemapb.CollectionSchema, insertData *storage.InsertData) ([]map[string]any, error) {
	fieldIDToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})

	rowNum := insertData.GetRowNum()
	rows := make([]map[string]any, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		data := make(map[int64]interface{})
		for fieldID, v := range insertData.Data {
			field := fieldIDToField[fieldID]
			dataType := field.GetDataType()
			elemType := field.GetElementType()
			if field.GetAutoID() || field.IsFunctionOutput {
				continue
			}
			if v.GetRow(i) == nil {
				data[fieldID] = nil
				continue
			}
			switch dataType {
			case schemapb.DataType_Array:
				switch elemType {
				case schemapb.DataType_Bool:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetBoolData().GetData()
				case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetIntData().GetData()
				case schemapb.DataType_Int64:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetLongData().GetData()
				case schemapb.DataType_Float:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetFloatData().GetData()
				case schemapb.DataType_Double:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetDoubleData().GetData()
				case schemapb.DataType_String:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetStringData().GetData()
				}
			case schemapb.DataType_JSON:
				data[fieldID] = string(v.GetRow(i).([]byte))
			case schemapb.DataType_BinaryVector:
				bytes := v.GetRow(i).([]byte)
				ints := make([]int, 0, len(bytes))
				for _, b := range bytes {
					ints = append(ints, int(b))
				}
				data[fieldID] = ints
			case schemapb.DataType_Float16Vector:
				bytes := v.GetRow(i).([]byte)
				data[fieldID] = typeutil.Float16BytesToFloat32Vector(bytes)
			case schemapb.DataType_BFloat16Vector:
				bytes := v.GetRow(i).([]byte)
				data[fieldID] = typeutil.BFloat16BytesToFloat32Vector(bytes)
			case schemapb.DataType_SparseFloatVector:
				bytes := v.GetRow(i).([]byte)
				data[fieldID] = typeutil.SparseFloatBytesToMap(bytes)
			default:
				data[fieldID] = v.GetRow(i)
			}
		}
		row := lo.MapKeys(data, func(_ any, fieldID int64) string {
			return fieldIDToField[fieldID].GetName()
		})
		rows = append(rows, row)
	}

	return rows, nil
}

func CreateInsertDataForCSV(schema *schemapb.CollectionSchema, insertData *storage.InsertData, nullkey string) ([][]string, error) {
	rowNum := insertData.GetRowNum()
	csvData := make([][]string, 0, rowNum+1)

	header := make([]string, 0)
	fields := lo.Filter(schema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
		return !field.GetAutoID() && !field.IsFunctionOutput
	})
	nameToFields := lo.KeyBy(fields, func(field *schemapb.FieldSchema) string {
		name := field.GetName()
		header = append(header, name)
		return name
	})
	csvData = append(csvData, header)

	for i := 0; i < rowNum; i++ {
		data := make([]string, 0)
		for _, name := range header {
			field := nameToFields[name]
			value := insertData.Data[field.FieldID]
			dataType := field.GetDataType()
			elemType := field.GetElementType()
			// deal with null value
			if field.GetNullable() && value.GetRow(i) == nil {
				data = append(data, nullkey)
				continue
			}
			switch dataType {
			case schemapb.DataType_Array:
				var arr any
				switch elemType {
				case schemapb.DataType_Bool:
					arr = value.GetRow(i).(*schemapb.ScalarField).GetBoolData().GetData()
				case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
					arr = value.GetRow(i).(*schemapb.ScalarField).GetIntData().GetData()
				case schemapb.DataType_Int64:
					arr = value.GetRow(i).(*schemapb.ScalarField).GetLongData().GetData()
				case schemapb.DataType_Float:
					arr = value.GetRow(i).(*schemapb.ScalarField).GetFloatData().GetData()
				case schemapb.DataType_Double:
					arr = value.GetRow(i).(*schemapb.ScalarField).GetDoubleData().GetData()
				case schemapb.DataType_String:
					arr = value.GetRow(i).(*schemapb.ScalarField).GetStringData().GetData()
				}
				j, err := json.Marshal(arr)
				if err != nil {
					return nil, err
				}
				data = append(data, string(j))
			case schemapb.DataType_JSON:
				data = append(data, string(value.GetRow(i).([]byte)))
			case schemapb.DataType_FloatVector:
				vec := value.GetRow(i).([]float32)
				j, err := json.Marshal(vec)
				if err != nil {
					return nil, err
				}
				data = append(data, string(j))
			case schemapb.DataType_BinaryVector:
				bytes := value.GetRow(i).([]byte)
				vec := make([]int, 0, len(bytes))
				for _, b := range bytes {
					vec = append(vec, int(b))
				}
				j, err := json.Marshal(vec)
				if err != nil {
					return nil, err
				}
				data = append(data, string(j))
			case schemapb.DataType_Float16Vector:
				bytes := value.GetRow(i).([]byte)
				vec := typeutil.Float16BytesToFloat32Vector(bytes)
				j, err := json.Marshal(vec)
				if err != nil {
					return nil, err
				}
				data = append(data, string(j))
			case schemapb.DataType_BFloat16Vector:
				bytes := value.GetRow(i).([]byte)
				vec := typeutil.BFloat16BytesToFloat32Vector(bytes)
				j, err := json.Marshal(vec)
				if err != nil {
					return nil, err
				}
				data = append(data, string(j))
			case schemapb.DataType_SparseFloatVector:
				bytes := value.GetRow(i).([]byte)
				m := typeutil.SparseFloatBytesToMap(bytes)
				j, err := json.Marshal(m)
				if err != nil {
					return nil, err
				}
				data = append(data, string(j))
			case schemapb.DataType_Int8Vector:
				vec := value.GetRow(i).([]int8)
				j, err := json.Marshal(vec)
				if err != nil {
					return nil, err
				}
				data = append(data, string(j))
			default:
				str := fmt.Sprintf("%v", value.GetRow(i))
				data = append(data, str)
			}
		}
		csvData = append(csvData, data)
	}

	return csvData, nil
}
