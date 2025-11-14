package testutil

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	allFields := typeutil.GetAllFieldSchemas(schema)
	for _, f := range allFields {
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
		case schemapb.DataType_Geometry:
			// wkt array
			insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateGeometryWktArray(rows))
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
		case schemapb.DataType_ArrayOfVector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			switch f.GetElementType() {
			case schemapb.DataType_FloatVector:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfFloatVectorArray(rows, int(dim)))
			default:
				panic(fmt.Sprintf("unimplemented data type: %s", f.GetElementType().String()))
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

func BuildSparseVectorData(mem *memory.GoAllocator, contents [][]byte, arrowType arrow.DataType) (arrow.Array, error) {
	if arrowType == nil || arrowType.ID() == arrow.STRING {
		// build sparse vector as JSON-format string
		builder := array.NewStringBuilder(mem)
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
		return builder.NewStringArray(), nil
	} else if arrowType.ID() == arrow.STRUCT {
		// build sparse vector as parquet struct
		stType, _ := arrowType.(*arrow.StructType)
		indicesField, ok1 := stType.FieldByName("indices")
		valuesField, ok2 := stType.FieldByName("values")
		if !ok1 || !ok2 {
			return nil, merr.WrapErrParameterInvalidMsg("Indices type or values type is missed for sparse vector")
		}

		indicesList, ok1 := indicesField.Type.(*arrow.ListType)
		valuesList, ok2 := valuesField.Type.(*arrow.ListType)
		if !ok1 || !ok2 {
			return nil, merr.WrapErrParameterInvalidMsg("Indices type and values type of sparse vector should be list")
		}
		indexType := indicesList.Elem().ID()
		valueType := valuesList.Elem().ID()

		fields := []arrow.Field{indicesField, valuesField}
		structType := arrow.StructOf(fields...)
		builder := array.NewStructBuilder(mem, structType)
		indicesBuilder := builder.FieldBuilder(0).(*array.ListBuilder)
		valuesBuilder := builder.FieldBuilder(1).(*array.ListBuilder)

		// The array.Uint32Builder/array.Int64Builder/array.Float32Builder/array.Float64Builder
		// are derived from array.Builder, but array.Builder doesn't have Append() interface
		// To call array.Uint32Builder.Value(uint32), we need to explicitly cast the indicesBuilder.ValueBuilder()
		// to array.Uint32Builder
		// So, we declare two methods here to avoid type casting in the "for" loop
		type AppendIndex func(index uint32)
		type AppendValue func(value float32)

		var appendIndexFunc AppendIndex
		switch indexType {
		case arrow.INT32:
			indicesArrayBuilder := indicesBuilder.ValueBuilder().(*array.Int32Builder)
			appendIndexFunc = func(index uint32) {
				indicesArrayBuilder.Append((int32)(index))
			}
		case arrow.UINT32:
			indicesArrayBuilder := indicesBuilder.ValueBuilder().(*array.Uint32Builder)
			appendIndexFunc = func(index uint32) {
				indicesArrayBuilder.Append(index)
			}
		case arrow.INT64:
			indicesArrayBuilder := indicesBuilder.ValueBuilder().(*array.Int64Builder)
			appendIndexFunc = func(index uint32) {
				indicesArrayBuilder.Append((int64)(index))
			}
		case arrow.UINT64:
			indicesArrayBuilder := indicesBuilder.ValueBuilder().(*array.Uint64Builder)
			appendIndexFunc = func(index uint32) {
				indicesArrayBuilder.Append((uint64)(index))
			}
		default:
			msg := fmt.Sprintf("Not able to write this type (%s) for sparse vector index", indexType.String())
			return nil, merr.WrapErrImportFailed(msg)
		}

		var appendValueFunc AppendValue
		switch valueType {
		case arrow.FLOAT32:
			valuesArrayBuilder := valuesBuilder.ValueBuilder().(*array.Float32Builder)
			appendValueFunc = func(value float32) {
				valuesArrayBuilder.Append(value)
			}
		case arrow.FLOAT64:
			valuesArrayBuilder := valuesBuilder.ValueBuilder().(*array.Float64Builder)
			appendValueFunc = func(value float32) {
				valuesArrayBuilder.Append((float64)(value))
			}
		default:
			msg := fmt.Sprintf("Not able to write this type (%s) for sparse vector index", indexType.String())
			return nil, merr.WrapErrImportFailed(msg)
		}

		for i := 0; i < len(contents); i++ {
			builder.Append(true)
			indicesBuilder.Append(true)
			valuesBuilder.Append(true)
			rowVecData := contents[i]
			elemCount := len(rowVecData) / 8
			for j := 0; j < elemCount; j++ {
				appendIndexFunc(common.Endian.Uint32(rowVecData[j*8:]))
				appendValueFunc(math.Float32frombits(common.Endian.Uint32(rowVecData[j*8+4:])))
			}
		}
		return builder.NewStructArray(), nil
	}

	return nil, merr.WrapErrParameterInvalidMsg("Invalid arrow data type for sparse vector")
}

func BuildArrayData(schema *schemapb.CollectionSchema, insertData *storage.InsertData, useNullType bool) ([]arrow.Array, error) {
	mem := memory.NewGoAllocator()
	columns := make([]arrow.Array, 0)

	// Filter out auto-generated, function output, and nested struct sub-fields
	fields := lo.Filter(schema.Fields, func(field *schemapb.FieldSchema, _ int) bool {
		// Skip auto PK, function output, and struct sub-fields (if using nested format)
		if (field.GetIsPrimaryKey() && field.GetAutoID()) || field.GetIsFunctionOutput() {
			return false
		}
		return true
	})

	// Build regular field columns
	for _, field := range fields {
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
			contents := insertData.Data[fieldID].(*storage.SparseFloatVectorFieldData).GetContents()
			arr, err := BuildSparseVectorData(mem, contents, nil)
			if err != nil {
				return nil, err
			}
			columns = append(columns, arr)
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
		case schemapb.DataType_Geometry:
			builder := array.NewStringBuilder(mem)
			wktData := insertData.Data[fieldID].(*storage.GeometryFieldData).Data
			validData := insertData.Data[fieldID].(*storage.GeometryFieldData).ValidData
			builder.AppendValues(lo.Map(wktData, func(bs []byte, _ int) string {
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
		case schemapb.DataType_ArrayOfVector:
			vectorArrayData := insertData.Data[fieldID].(*storage.VectorArrayFieldData)
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			elemType, err := storage.VectorArrayToArrowType(elementType, int(dim))
			if err != nil {
				return nil, err
			}

			// Create ListBuilder with "item" field name to match convertToArrowDataType
			// Always represented as a list of fixed-size binary values
			listBuilder := array.NewListBuilderWithField(mem, arrow.Field{
				Name:     "item",
				Type:     elemType,
				Nullable: true,
				Metadata: arrow.Metadata{},
			})
			fixedSizeBuilder, ok := listBuilder.ValueBuilder().(*array.FixedSizeBinaryBuilder)
			if !ok {
				return nil, fmt.Errorf("unexpected list value builder for VectorArray field %s: %T", field.GetName(), listBuilder.ValueBuilder())
			}

			vectorArrayData.Dim = dim

			bytesPerVector := fixedSizeBuilder.Type().(*arrow.FixedSizeBinaryType).ByteWidth

			appendBinarySlice := func(data []byte, stride int) error {
				if stride == 0 {
					return fmt.Errorf("zero stride for VectorArray field %s", field.GetName())
				}
				if len(data)%stride != 0 {
					return fmt.Errorf("vector array data length %d is not divisible by stride %d for field %s", len(data), stride, field.GetName())
				}
				for offset := 0; offset < len(data); offset += stride {
					fixedSizeBuilder.Append(data[offset : offset+stride])
				}
				return nil
			}

			for _, vectorField := range vectorArrayData.Data {
				if vectorField == nil {
					listBuilder.Append(false)
					continue
				}

				listBuilder.Append(true)

				switch elementType {
				case schemapb.DataType_FloatVector:
					floatArray := vectorField.GetFloatVector()
					if floatArray == nil {
						return nil, fmt.Errorf("expected FloatVector data for field %s", field.GetName())
					}
					data := floatArray.GetData()
					if len(data) == 0 {
						continue
					}
					if len(data)%int(dim) != 0 {
						return nil, fmt.Errorf("float vector data length %d is not divisible by dim %d for field %s", len(data), dim, field.GetName())
					}
					for offset := 0; offset < len(data); offset += int(dim) {
						vectorBytes := make([]byte, bytesPerVector)
						for j := 0; j < int(dim); j++ {
							binary.LittleEndian.PutUint32(vectorBytes[j*4:], math.Float32bits(data[offset+j]))
						}
						fixedSizeBuilder.Append(vectorBytes)
					}
				case schemapb.DataType_BinaryVector:
					binaryData := vectorField.GetBinaryVector()
					if len(binaryData) == 0 {
						continue
					}
					bytesPer := int((dim + 7) / 8)
					if err := appendBinarySlice(binaryData, bytesPer); err != nil {
						return nil, err
					}
				case schemapb.DataType_Float16Vector:
					float16Data := vectorField.GetFloat16Vector()
					if len(float16Data) == 0 {
						continue
					}
					if err := appendBinarySlice(float16Data, int(dim)*2); err != nil {
						return nil, err
					}
				case schemapb.DataType_BFloat16Vector:
					bfloat16Data := vectorField.GetBfloat16Vector()
					if len(bfloat16Data) == 0 {
						continue
					}
					if err := appendBinarySlice(bfloat16Data, int(dim)*2); err != nil {
						return nil, err
					}
				case schemapb.DataType_Int8Vector:
					int8Data := vectorField.GetInt8Vector()
					if len(int8Data) == 0 {
						continue
					}
					if err := appendBinarySlice(int8Data, int(dim)); err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("unsupported element type in VectorArray: %s", elementType.String())
				}
			}

			columns = append(columns, listBuilder.NewListArray())
		}
	}

	// Process StructArrayFields as nested list<struct> format
	for _, structField := range schema.StructArrayFields {
		// Build arrow fields for the struct
		structFields := make([]arrow.Field, 0, len(structField.Fields))
		for _, subField := range structField.Fields {
			// Extract actual field name (remove structName[] prefix)
			fieldName := subField.Name
			if len(structField.Name) > 0 && len(subField.Name) > len(structField.Name)+2 {
				fieldName = subField.Name[len(structField.Name)+1 : len(subField.Name)-1]
			}

			// Determine arrow type for the field
			var arrType arrow.DataType
			switch subField.DataType {
			case schemapb.DataType_Array:
				switch subField.ElementType {
				case schemapb.DataType_Bool:
					arrType = arrow.FixedWidthTypes.Boolean
				case schemapb.DataType_Int8:
					arrType = arrow.PrimitiveTypes.Int8
				case schemapb.DataType_Int16:
					arrType = arrow.PrimitiveTypes.Int16
				case schemapb.DataType_Int32:
					arrType = arrow.PrimitiveTypes.Int32
				case schemapb.DataType_Int64:
					arrType = arrow.PrimitiveTypes.Int64
				case schemapb.DataType_Float:
					arrType = arrow.PrimitiveTypes.Float32
				case schemapb.DataType_Double:
					arrType = arrow.PrimitiveTypes.Float64
				case schemapb.DataType_String, schemapb.DataType_VarChar:
					arrType = arrow.BinaryTypes.String
				default:
					// Default to string for unknown element types
					arrType = arrow.BinaryTypes.String
				}
			case schemapb.DataType_ArrayOfVector:
				// For user data, use list<float> format for vectors
				switch subField.ElementType {
				case schemapb.DataType_FloatVector:
					arrType = arrow.ListOf(arrow.PrimitiveTypes.Float32)
				case schemapb.DataType_BinaryVector:
					arrType = arrow.ListOf(arrow.PrimitiveTypes.Uint8)
				case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
					arrType = arrow.ListOf(arrow.PrimitiveTypes.Float32)
				case schemapb.DataType_Int8Vector:
					arrType = arrow.ListOf(arrow.PrimitiveTypes.Int8)
				default:
					panic("unimplemented element type for ArrayOfVector")
				}
			default:
				panic("unimplemented")
			}

			structFields = append(structFields, arrow.Field{
				Name:     fieldName,
				Type:     arrType,
				Nullable: subField.GetNullable(),
			})
		}

		// Build list<struct> column
		listBuilder := array.NewListBuilder(mem, arrow.StructOf(structFields...))
		structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)

		// Get row count from first sub-field
		var rowCount int
		for _, subField := range structField.Fields {
			if data, ok := insertData.Data[subField.FieldID]; ok {
				rowCount = data.RowNum()
				break
			}
		}

		// row to column
		for i := 0; i < rowCount; i++ {
			var arrayLen int
			subField := structField.Fields[0]
			data := insertData.Data[subField.FieldID]
			if data == nil {
				panic(fmt.Sprintf("data for struct sub-field %s (ID: %d) is nil", subField.Name, subField.FieldID))
			}
			rowData := data.GetRow(i)
			switch subField.DataType {
			case schemapb.DataType_Array:
				scalarField := rowData.(*schemapb.ScalarField)
				switch subField.ElementType {
				case schemapb.DataType_Bool:
					arrayLen = len(scalarField.GetBoolData().GetData())
				case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
					arrayLen = len(scalarField.GetIntData().GetData())
				case schemapb.DataType_Int64:
					arrayLen = len(scalarField.GetLongData().GetData())
				case schemapb.DataType_Float:
					arrayLen = len(scalarField.GetFloatData().GetData())
				case schemapb.DataType_Double:
					arrayLen = len(scalarField.GetDoubleData().GetData())
				case schemapb.DataType_String, schemapb.DataType_VarChar:
					arrayLen = len(scalarField.GetStringData().GetData())
				}
			case schemapb.DataType_ArrayOfVector:
				vectorField := rowData.(*schemapb.VectorField)
				if vectorField.GetFloatVector() != nil {
					dim, _ := typeutil.GetDim(subField)
					arrayLen = len(vectorField.GetFloatVector().Data) / int(dim)
				}
			}

			listBuilder.Append(true)
			// generate a struct for each array element
			for j := 0; j < arrayLen; j++ {
				// add data for each field at this position
				for fieldIdx, subField := range structField.Fields {
					data := insertData.Data[subField.FieldID]
					fieldBuilder := structBuilder.FieldBuilder(fieldIdx)

					rowData := data.GetRow(i)
					switch subField.DataType {
					case schemapb.DataType_Array:
						scalarField := rowData.(*schemapb.ScalarField)
						switch subField.ElementType {
						case schemapb.DataType_Bool:
							if boolData := scalarField.GetBoolData(); boolData != nil && j < len(boolData.GetData()) {
								fieldBuilder.(*array.BooleanBuilder).Append(boolData.GetData()[j])
							} else {
								fieldBuilder.(*array.BooleanBuilder).AppendNull()
							}
						case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
							if intData := scalarField.GetIntData(); intData != nil && j < len(intData.GetData()) {
								fieldBuilder.(*array.Int32Builder).Append(intData.GetData()[j])
							} else {
								fieldBuilder.(*array.Int32Builder).AppendNull()
							}
						case schemapb.DataType_Int64:
							if longData := scalarField.GetLongData(); longData != nil && j < len(longData.GetData()) {
								fieldBuilder.(*array.Int64Builder).Append(longData.GetData()[j])
							} else {
								fieldBuilder.(*array.Int64Builder).AppendNull()
							}
						case schemapb.DataType_Float:
							if floatData := scalarField.GetFloatData(); floatData != nil && j < len(floatData.GetData()) {
								fieldBuilder.(*array.Float32Builder).Append(floatData.GetData()[j])
							} else {
								fieldBuilder.(*array.Float32Builder).AppendNull()
							}
						case schemapb.DataType_Double:
							if doubleData := scalarField.GetDoubleData(); doubleData != nil && j < len(doubleData.GetData()) {
								fieldBuilder.(*array.Float64Builder).Append(doubleData.GetData()[j])
							} else {
								fieldBuilder.(*array.Float64Builder).AppendNull()
							}
						case schemapb.DataType_String, schemapb.DataType_VarChar:
							if stringData := scalarField.GetStringData(); stringData != nil && j < len(stringData.GetData()) {
								fieldBuilder.(*array.StringBuilder).Append(stringData.GetData()[j])
							} else {
								fieldBuilder.(*array.StringBuilder).AppendNull()
							}
						}

					case schemapb.DataType_ArrayOfVector:
						vectorField := rowData.(*schemapb.VectorField)
						listBuilder := fieldBuilder.(*array.ListBuilder)
						listBuilder.Append(true)

						if floatVectors := vectorField.GetFloatVector(); floatVectors != nil {
							dim, _ := typeutil.GetDim(subField)
							floatBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)
							start := j * int(dim)
							end := start + int(dim)
							if end <= len(floatVectors.Data) {
								for k := start; k < end; k++ {
									floatBuilder.Append(floatVectors.Data[k])
								}
							}
						}
					}
				}

				structBuilder.Append(true)
			}
		}

		columns = append(columns, listBuilder.NewArray())
	}

	return columns, nil
}

// reconstructStructArrayForJSON reconstructs struct array data for JSON format
// Returns an array of maps where each element represents a struct
func reconstructStructArrayForJSON(structField *schemapb.StructArrayFieldSchema, insertData *storage.InsertData, rowIndex int) ([]map[string]any, error) {
	subFields := structField.GetFields()
	if len(subFields) == 0 {
		return []map[string]any{}, nil
	}

	// Determine the array length from the first sub-field's data
	var arrayLen int
	for _, subField := range subFields {
		if fieldData, ok := insertData.Data[subField.GetFieldID()]; ok {
			rowData := fieldData.GetRow(rowIndex)
			if rowData == nil {
				continue
			}

			switch subField.GetDataType() {
			case schemapb.DataType_Array:
				if scalarField, ok := rowData.(*schemapb.ScalarField); ok {
					switch subField.GetElementType() {
					case schemapb.DataType_Bool:
						if data := scalarField.GetBoolData(); data != nil {
							arrayLen = len(data.GetData())
						}
					case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
						if data := scalarField.GetIntData(); data != nil {
							arrayLen = len(data.GetData())
						}
					case schemapb.DataType_Int64:
						if data := scalarField.GetLongData(); data != nil {
							arrayLen = len(data.GetData())
						}
					case schemapb.DataType_Float:
						if data := scalarField.GetFloatData(); data != nil {
							arrayLen = len(data.GetData())
						}
					case schemapb.DataType_Double:
						if data := scalarField.GetDoubleData(); data != nil {
							arrayLen = len(data.GetData())
						}
					case schemapb.DataType_String, schemapb.DataType_VarChar:
						if data := scalarField.GetStringData(); data != nil {
							arrayLen = len(data.GetData())
						}
					}
				}
			case schemapb.DataType_ArrayOfVector:
				if vectorField, ok := rowData.(*schemapb.VectorField); ok {
					switch subField.GetElementType() {
					case schemapb.DataType_FloatVector:
						if data := vectorField.GetFloatVector(); data != nil {
							dim, _ := typeutil.GetDim(subField)
							if dim > 0 {
								arrayLen = len(data.GetData()) / int(dim)
							}
						}
					case schemapb.DataType_BinaryVector:
						if data := vectorField.GetBinaryVector(); data != nil {
							dim, _ := typeutil.GetDim(subField)
							if dim > 0 {
								bytesPerVector := int(dim) / 8
								arrayLen = len(data) / bytesPerVector
							}
						}
					case schemapb.DataType_Float16Vector:
						if data := vectorField.GetFloat16Vector(); data != nil {
							dim, _ := typeutil.GetDim(subField)
							if dim > 0 {
								bytesPerVector := int(dim) * 2
								arrayLen = len(data) / bytesPerVector
							}
						}
					case schemapb.DataType_BFloat16Vector:
						if data := vectorField.GetBfloat16Vector(); data != nil {
							dim, _ := typeutil.GetDim(subField)
							if dim > 0 {
								bytesPerVector := int(dim) * 2
								arrayLen = len(data) / bytesPerVector
							}
						}
					}
				}
			}

			if arrayLen > 0 {
				break
			}
		}
	}

	// Build the struct array
	structArray := make([]map[string]any, arrayLen)
	for j := 0; j < arrayLen; j++ {
		structElem := make(map[string]any)

		for _, subField := range subFields {
			if fieldData, ok := insertData.Data[subField.GetFieldID()]; ok {
				rowData := fieldData.GetRow(rowIndex)
				if rowData == nil {
					continue
				}

				// Extract the j-th element
				switch subField.GetDataType() {
				case schemapb.DataType_Array:
					if scalarField, ok := rowData.(*schemapb.ScalarField); ok {
						switch subField.GetElementType() {
						case schemapb.DataType_Bool:
							if data := scalarField.GetBoolData(); data != nil && j < len(data.GetData()) {
								structElem[subField.GetName()] = data.GetData()[j]
							}
						case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
							if data := scalarField.GetIntData(); data != nil && j < len(data.GetData()) {
								structElem[subField.GetName()] = data.GetData()[j]
							}
						case schemapb.DataType_Int64:
							if data := scalarField.GetLongData(); data != nil && j < len(data.GetData()) {
								structElem[subField.GetName()] = data.GetData()[j]
							}
						case schemapb.DataType_Float:
							if data := scalarField.GetFloatData(); data != nil && j < len(data.GetData()) {
								structElem[subField.GetName()] = data.GetData()[j]
							}
						case schemapb.DataType_Double:
							if data := scalarField.GetDoubleData(); data != nil && j < len(data.GetData()) {
								structElem[subField.GetName()] = data.GetData()[j]
							}
						case schemapb.DataType_String, schemapb.DataType_VarChar:
							if data := scalarField.GetStringData(); data != nil && j < len(data.GetData()) {
								structElem[subField.GetName()] = data.GetData()[j]
							}
						}
					}
				case schemapb.DataType_ArrayOfVector:
					if vectorField, ok := rowData.(*schemapb.VectorField); ok {
						switch subField.GetElementType() {
						case schemapb.DataType_FloatVector:
							if data := vectorField.GetFloatVector(); data != nil {
								dim, _ := typeutil.GetDim(subField)
								if dim > 0 {
									startIdx := j * int(dim)
									endIdx := startIdx + int(dim)
									if endIdx <= len(data.GetData()) {
										structElem[subField.GetName()] = data.GetData()[startIdx:endIdx]
									}
								}
							}
						case schemapb.DataType_BinaryVector:
							if data := vectorField.GetBinaryVector(); data != nil {
								dim, _ := typeutil.GetDim(subField)
								if dim > 0 {
									bytesPerVector := int(dim) / 8
									startIdx := j * bytesPerVector
									endIdx := startIdx + bytesPerVector
									if endIdx <= len(data) {
										structElem[subField.GetName()] = data[startIdx:endIdx]
									}
								}
							}
						case schemapb.DataType_Float16Vector:
							if data := vectorField.GetFloat16Vector(); data != nil {
								dim, _ := typeutil.GetDim(subField)
								if dim > 0 {
									bytesPerVector := int(dim) * 2
									startIdx := j * bytesPerVector
									endIdx := startIdx + bytesPerVector
									if endIdx <= len(data) {
										// Convert Float16 bytes to float32 for JSON representation
										structElem[subField.GetName()] = typeutil.Float16BytesToFloat32Vector(data[startIdx:endIdx])
									}
								}
							}
						case schemapb.DataType_BFloat16Vector:
							if data := vectorField.GetBfloat16Vector(); data != nil {
								dim, _ := typeutil.GetDim(subField)
								if dim > 0 {
									bytesPerVector := int(dim) * 2
									startIdx := j * bytesPerVector
									endIdx := startIdx + bytesPerVector
									if endIdx <= len(data) {
										// Convert BFloat16 bytes to float32 for JSON representation
										structElem[subField.GetName()] = typeutil.BFloat16BytesToFloat32Vector(data[startIdx:endIdx])
									}
								}
							}
						}
					}
				}
			}
		}

		structArray[j] = structElem
	}

	return structArray, nil
}

func CreateInsertDataRowsForJSON(schema *schemapb.CollectionSchema, insertData *storage.InsertData) ([]map[string]any, error) {
	fieldIDToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})

	// Track which field IDs belong to struct array sub-fields
	structSubFieldIDs := make(map[int64]bool)
	for _, structField := range schema.GetStructArrayFields() {
		for _, subField := range structField.GetFields() {
			structSubFieldIDs[subField.GetFieldID()] = true
		}
	}

	rowNum := insertData.GetRowNum()
	rows := make([]map[string]any, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		data := make(map[int64]interface{})

		// First process regular fields
		for fieldID, v := range insertData.Data {
			// Skip if this is a sub-field of a struct array
			if structSubFieldIDs[fieldID] {
				continue
			}

			field, ok := fieldIDToField[fieldID]
			if !ok {
				continue
			}

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
				case schemapb.DataType_String, schemapb.DataType_VarChar:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetStringData().GetData()
				}
			case schemapb.DataType_ArrayOfVector:
				panic("unreachable")
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

		// Now process struct array fields - reconstruct the nested structure
		for _, structField := range schema.GetStructArrayFields() {
			structArray, err := reconstructStructArrayForJSON(structField, insertData, i)
			if err != nil {
				return nil, err
			}
			data[structField.GetFieldID()] = structArray
		}

		// Convert field IDs to field names
		row := make(map[string]any)
		for fieldID, value := range data {
			if field, ok := fieldIDToField[fieldID]; ok {
				row[field.GetName()] = value
			} else {
				// Check if it's a struct array field
				for _, structField := range schema.GetStructArrayFields() {
					if structField.GetFieldID() == fieldID {
						row[structField.GetName()] = value
						break
					}
				}
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// reconstructStructArrayForCSV reconstructs struct array data for CSV format
// Returns a JSON string where each sub-field value is also a JSON string
func reconstructStructArrayForCSV(structField *schemapb.StructArrayFieldSchema, insertData *storage.InsertData, rowIndex int) (string, error) {
	// Use the JSON reconstruction function to get the struct array
	structArray, err := reconstructStructArrayForJSON(structField, insertData, rowIndex)
	if err != nil {
		return "", err
	}

	// Convert the entire struct array to JSON string
	jsonBytes, err := json.Marshal(structArray)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func CreateInsertDataForCSV(schema *schemapb.CollectionSchema, insertData *storage.InsertData, nullkey string) ([][]string, error) {
	rowNum := insertData.GetRowNum()
	csvData := make([][]string, 0, rowNum+1)

	// Build header - regular fields and struct array fields (not sub-fields)
	header := make([]string, 0)

	// Track which field IDs belong to struct array sub-fields
	structSubFieldIDs := make(map[int64]bool)
	for _, structField := range schema.GetStructArrayFields() {
		for _, subField := range structField.GetFields() {
			structSubFieldIDs[subField.GetFieldID()] = true
		}
	}

	// Add regular fields to header (excluding struct array sub-fields)
	allFields := typeutil.GetAllFieldSchemas(schema)
	fields := lo.Filter(allFields, func(field *schemapb.FieldSchema, _ int) bool {
		return !field.GetAutoID() && !field.IsFunctionOutput && !structSubFieldIDs[field.GetFieldID()]
	})
	nameToFields := lo.KeyBy(fields, func(field *schemapb.FieldSchema) string {
		name := field.GetName()
		header = append(header, name)
		return name
	})

	// Build map for struct array fields for quick lookup
	structArrayFields := make(map[string]*schemapb.StructArrayFieldSchema)
	for _, structField := range schema.GetStructArrayFields() {
		structArrayFields[structField.GetName()] = structField
		header = append(header, structField.GetName())
	}

	csvData = append(csvData, header)

	for i := 0; i < rowNum; i++ {
		data := make([]string, 0)
		for _, name := range header {
			if structArrayField, ok := structArrayFields[name]; ok {
				structArrayData, err := reconstructStructArrayForCSV(structArrayField, insertData, i)
				if err != nil {
					return nil, err
				}
				data = append(data, structArrayData)
				continue
			}

			// Handle regular field
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
			case schemapb.DataType_ArrayOfVector:
				// ArrayOfVector should not appear as a top-level field
				// It can only be a sub-field in struct arrays
				panic("ArrayOfVector cannot be a top-level field")
			default:
				str := fmt.Sprintf("%v", value.GetRow(i))
				data = append(data, str)
			}
		}
		csvData = append(csvData, data)
	}

	return csvData, nil
}
