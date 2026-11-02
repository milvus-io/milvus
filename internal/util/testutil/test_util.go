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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	genValidData := func() ([]bool, error) {
		if len(nullPercent) > 1 {
			return nil, merr.WrapErrParameterInvalidMsg("the length of nullPercent is wrong")
		}
		var validData []bool
		if len(nullPercent) == 0 || nullPercent[0] == 50 {
			validData = testutils.GenerateBoolArray(rows)
		} else if len(nullPercent) == 1 && nullPercent[0] == 100 {
			validData = make([]bool, rows)
		} else if len(nullPercent) == 1 && nullPercent[0] == 0 {
			validData = make([]bool, rows)
			for i := range validData {
				validData[i] = true
			}
		} else {
			return nil, merr.WrapErrParameterInvalidMsg("not support the number of nullPercent(%d)", nullPercent)
		}
		return validData, nil
	}

	// Struct sub-fields are generated per struct below, so that a null struct
	// row is null in every sub-field.
	structSubFieldIDs := make(map[int64]bool)
	for _, structField := range schema.GetStructArrayFields() {
		for _, subField := range structField.GetFields() {
			structSubFieldIDs[subField.GetFieldID()] = true
		}
	}

	// Pre-generate validData for nullable fields to determine sparse storage size
	validDataMap := make(map[int64][]bool)
	allFields := lo.Filter(typeutil.GetAllFieldSchemas(schema), func(f *schemapb.FieldSchema, _ int) bool {
		return !structSubFieldIDs[f.GetFieldID()]
	})
	for _, f := range allFields {
		if f.GetAutoID() || f.IsFunctionOutput {
			continue
		}
		if f.GetNullable() {
			validData, err := genValidData()
			if err != nil {
				return nil, err
			}
			validDataMap[f.FieldID] = validData
		}
	}

	// Helper function to check if a type is a vector type (uses sparse storage)
	isVectorType := func(dataType schemapb.DataType) bool {
		switch dataType {
		case schemapb.DataType_BinaryVector,
			schemapb.DataType_FloatVector,
			schemapb.DataType_Float16Vector,
			schemapb.DataType_BFloat16Vector,
			schemapb.DataType_SparseFloatVector,
			schemapb.DataType_Int8Vector:
			return true
		default:
			return false
		}
	}

	// Helper function to count valid rows
	countValidRows := func(validData []bool) int {
		if len(validData) == 0 {
			return rows
		}
		count := 0
		for _, v := range validData {
			if v {
				count++
			}
		}
		return count
	}

	for _, f := range allFields {
		if f.GetAutoID() || f.IsFunctionOutput {
			continue
		}
		validData := validDataMap[f.FieldID]
		// Vector types use sparse storage (only valid rows), scalar types use dense storage (all rows)
		var dataRows int
		if isVectorType(f.GetDataType()) {
			dataRows = countValidRows(validData)
		} else {
			dataRows = rows
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
			// For nullable vectors, use sparse storage (only generate valid rows)
			insertData.Data[f.FieldID].(*storage.BinaryVectorFieldData).Data = testutils.GenerateBinaryVectors(dataRows, int(dim))
			insertData.Data[f.FieldID].(*storage.BinaryVectorFieldData).Dim = int(dim)
		case schemapb.DataType_FloatVector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			// For nullable vectors, use sparse storage (only generate valid rows)
			insertData.Data[f.GetFieldID()].(*storage.FloatVectorFieldData).Data = testutils.GenerateFloatVectors(dataRows, int(dim))
			insertData.Data[f.GetFieldID()].(*storage.FloatVectorFieldData).Dim = int(dim)
		case schemapb.DataType_Float16Vector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			// For nullable vectors, use sparse storage (only generate valid rows)
			insertData.Data[f.FieldID].(*storage.Float16VectorFieldData).Data = testutils.GenerateFloat16Vectors(dataRows, int(dim))
			insertData.Data[f.FieldID].(*storage.Float16VectorFieldData).Dim = int(dim)
		case schemapb.DataType_BFloat16Vector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			// For nullable vectors, use sparse storage (only generate valid rows)
			insertData.Data[f.FieldID].(*storage.BFloat16VectorFieldData).Data = testutils.GenerateBFloat16Vectors(dataRows, int(dim))
			insertData.Data[f.FieldID].(*storage.BFloat16VectorFieldData).Dim = int(dim)
		case schemapb.DataType_SparseFloatVector:
			// For nullable vectors, use sparse storage (only generate valid rows)
			data, dim := testutils.GenerateSparseFloatVectorsData(dataRows)
			sparseData := insertData.Data[f.FieldID].(*storage.SparseFloatVectorFieldData)
			sparseData.Contents = data
			sparseData.Dim = dim
		case schemapb.DataType_Int8Vector:
			dim, err := typeutil.GetDim(f)
			if err != nil {
				return nil, err
			}
			// For nullable vectors, use sparse storage (only generate valid rows)
			insertData.Data[f.FieldID].(*storage.Int8VectorFieldData).Data = testutils.GenerateInt8Vectors(dataRows, int(dim))
			insertData.Data[f.FieldID].(*storage.Int8VectorFieldData).Dim = int(dim)
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
			case schemapb.DataType_Float16Vector:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfFloat16VectorArray(rows, int(dim)))
			case schemapb.DataType_BFloat16Vector:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfBFloat16VectorArray(rows, int(dim)))
			case schemapb.DataType_Int8Vector:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfInt8VectorArray(rows, int(dim)))
			case schemapb.DataType_BinaryVector:
				insertData.Data[f.FieldID].AppendDataRows(testutils.GenerateArrayOfBinaryVectorArray(rows, int(dim)))
			default:
				panic(fmt.Sprintf("unimplemented data type: %s", f.GetElementType().String()))
			}

		default:
			panic(fmt.Sprintf("unsupported data type: %s", f.GetDataType().String()))
		}
		// Apply pre-generated validData for nullable fields
		if f.GetNullable() {
			validData := validDataMap[f.FieldID]
			insertData.Data[f.FieldID].AppendValidDataRows(validData)
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		if err := fillStructArrayInsertData(insertData, structField, rows, genValidData); err != nil {
			return nil, err
		}
	}
	return insertData, nil
}

// fillStructArrayInsertData generates the sub-field columns of one struct field.
// Row nulls are shared by all sub-fields and exist only when the struct and
// every sub-field are nullable. Every non-null row has testutils.ElemCountOfArray elements. Legacy
// sub-fields use the dense generators; native-list sub-fields (element-nullable
// scalar Array, nested Array, element-nullable ArrayOfVector) follow the null
// pattern of the nativeList* helpers, in the layout import readers produce.
func fillStructArrayInsertData(insertData *storage.InsertData, structField *schemapb.StructArrayFieldSchema, rows int, genValidData func() ([]bool, error)) error {
	subFields := structField.GetFields()
	if len(subFields) == 0 {
		return nil
	}
	var rowValid []bool
	if structField.GetNullable() && lo.EveryBy(subFields, func(f *schemapb.FieldSchema) bool { return f.GetNullable() }) {
		var err error
		rowValid, err = genValidData()
		if err != nil {
			return err
		}
	}
	for _, subField := range subFields {
		fieldData, ok := insertData.Data[subField.GetFieldID()]
		if !ok {
			return merr.WrapErrParameterInvalidMsg("no insert data for struct sub-field %s", subField.GetName())
		}
		values, err := generateStructSubFieldRows(subField, rows)
		if err != nil {
			return err
		}
		for i := 0; i < rows; i++ {
			row := values[i]
			if rowValid != nil && !rowValid[i] {
				row = nil
			}
			if err := fieldData.AppendRow(row); err != nil {
				return err
			}
		}
	}
	return nil
}

// generateStructSubFieldRows returns one non-null row value per row:
// *schemapb.ScalarField for Array, *schemapb.VectorField for ArrayOfVector.
func generateStructSubFieldRows(subField *schemapb.FieldSchema, rows int) ([]any, error) {
	values := make([]any, 0, rows)
	switch subField.GetDataType() {
	case schemapb.DataType_Array:
		if typeutil.IsNativeListArrayField(subField) {
			for i := 0; i < rows; i++ {
				row, err := nativeListArrayRow(subField, i)
				if err != nil {
					return nil, err
				}
				values = append(values, row)
			}
			return values, nil
		}
		var legacy []*schemapb.ScalarField
		switch subField.GetElementType() {
		case schemapb.DataType_Bool:
			legacy = testutils.GenerateArrayOfBoolArray(rows)
		case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
			legacy = testutils.GenerateArrayOfIntArray(rows)
		case schemapb.DataType_Int64:
			legacy = testutils.GenerateArrayOfLongArray(rows)
		case schemapb.DataType_Float:
			legacy = testutils.GenerateArrayOfFloatArray(rows)
		case schemapb.DataType_Double:
			legacy = testutils.GenerateArrayOfDoubleArray(rows)
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			legacy = testutils.GenerateArrayOfStringArray(rows)
		default:
			return nil, merr.WrapErrParameterInvalidMsg("unsupported element type %s of struct sub-field %s", subField.GetElementType(), subField.GetName())
		}
		for _, row := range legacy {
			values = append(values, row)
		}
		return values, nil
	case schemapb.DataType_ArrayOfVector:
		dim, err := typeutil.GetDim(subField)
		if err != nil {
			return nil, err
		}
		if subField.GetElementNullable() {
			for i := 0; i < rows; i++ {
				row, err := nativeListVectorArrayRow(subField, int(dim), i)
				if err != nil {
					return nil, err
				}
				values = append(values, row)
			}
			return values, nil
		}
		var legacy []*schemapb.VectorField
		switch subField.GetElementType() {
		case schemapb.DataType_FloatVector:
			legacy = testutils.GenerateArrayOfFloatVectorArray(rows, int(dim))
		case schemapb.DataType_Float16Vector:
			legacy = testutils.GenerateArrayOfFloat16VectorArray(rows, int(dim))
		case schemapb.DataType_BFloat16Vector:
			legacy = testutils.GenerateArrayOfBFloat16VectorArray(rows, int(dim))
		case schemapb.DataType_Int8Vector:
			legacy = testutils.GenerateArrayOfInt8VectorArray(rows, int(dim))
		case schemapb.DataType_BinaryVector:
			legacy = testutils.GenerateArrayOfBinaryVectorArray(rows, int(dim))
		default:
			return nil, merr.WrapErrParameterInvalidMsg("unsupported element type %s of struct sub-field %s", subField.GetElementType(), subField.GetName())
		}
		for _, row := range legacy {
			values = append(values, row)
		}
		return values, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported data type %s of struct sub-field %s", subField.GetDataType(), subField.GetName())
	}
}

// Deterministic null pattern of the native-list struct sub-fields generated by
// CreateInsertData, by row i, element j and leaf k. Every seventh row (i%7 == 3)
// has all elements null; otherwise every third scalar element and every third
// vector (with a different phase) is null. A nested Array has inner arrays of
// length 0..3, every fifth inner array null, and every third leaf null when the
// leaf is nullable. Null levels are only generated where the schema allows them.
func nativeListElementIsNull(i, j int) bool { return i%7 == 3 || (i+j)%3 == 2 }

func nativeListVectorIsNull(i, j int) bool { return i%7 == 3 || (i+j)%3 == 1 }

func nativeListInnerIsNull(i, j int) bool { return i%7 == 3 || (i+j)%5 == 4 }

func nativeListInnerLen(i, j int) int { return (i + j) % 4 }

func nativeListLeafIsNull(i, j, k int) bool { return (i+j+k)%3 == 2 }

func nativeListValue(i, j, k int) int { return i*31 + j*7 + k }

// nativeListLeafRow builds a dense leaf payload of n slots with the zero value
// at null slots. The element validity is set, full length, iff nullable.
func nativeListLeafRow(leaf schemapb.DataType, n int, nullable bool, isNull func(k int) bool, value func(k int) int) (*schemapb.ScalarField, error) {
	valid := make([]bool, n)
	for k := range valid {
		valid[k] = !nullable || !isNull(k)
	}
	row := &schemapb.ScalarField{}
	switch leaf {
	case schemapb.DataType_Bool:
		data := make([]bool, n)
		for k := range data {
			if valid[k] {
				data[k] = value(k)%2 == 0
			}
		}
		row.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		bound := math.MaxInt32
		switch leaf {
		case schemapb.DataType_Int8:
			bound = math.MaxInt8 + 1
		case schemapb.DataType_Int16:
			bound = math.MaxInt16 + 1
		}
		data := make([]int32, n)
		for k := range data {
			if valid[k] {
				data[k] = int32(value(k) % bound)
			}
		}
		row.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}}
	case schemapb.DataType_Int64:
		data := make([]int64, n)
		for k := range data {
			if valid[k] {
				data[k] = int64(value(k))
			}
		}
		row.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}
	case schemapb.DataType_Float:
		data := make([]float32, n)
		for k := range data {
			if valid[k] {
				data[k] = float32(value(k)) / 4
			}
		}
		row.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}}
	case schemapb.DataType_Double:
		data := make([]float64, n)
		for k := range data {
			if valid[k] {
				data[k] = float64(value(k)) / 8
			}
		}
		row.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		data := make([]string, n)
		for k := range data {
			if valid[k] {
				data[k] = fmt.Sprintf("v%d", value(k))
			}
		}
		row.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported native Array leaf type %s", leaf)
	}
	if nullable {
		typeutil.SetArrayElementValidData(row, valid)
	}
	return row, nil
}

// nativeListArrayRow builds row i of an element-nullable or nested scalar Array
// sub-field. A nested row is ArrayData{ElementType: leaf} with one child per
// inner array; a null inner array is a typed empty leaf without validity.
func nativeListArrayRow(field *schemapb.FieldSchema, i int) (*schemapb.ScalarField, error) {
	leaf, leafNullable, depth, err := typeutil.GetFieldArrayLeaf(field)
	if err != nil {
		return nil, err
	}
	n := testutils.ElemCountOfArray
	switch depth {
	case 1:
		return nativeListLeafRow(leaf, n, leafNullable,
			func(k int) bool { return nativeListElementIsNull(i, k) },
			func(k int) int { return nativeListValue(i, k, 0) })
	case 2:
		elementNullable := field.GetElementNullable()
		children := make([]*schemapb.ScalarField, 0, n)
		valid := make([]bool, 0, n)
		for j := 0; j < n; j++ {
			if elementNullable && nativeListInnerIsNull(i, j) {
				child, err := nativeListLeafRow(leaf, 0, false, nil, nil)
				if err != nil {
					return nil, err
				}
				children = append(children, child)
				valid = append(valid, false)
				continue
			}
			child, err := nativeListLeafRow(leaf, nativeListInnerLen(i, j), leafNullable,
				func(k int) bool { return nativeListLeafIsNull(i, j, k) },
				func(k int) int { return nativeListValue(i, j, k) })
			if err != nil {
				return nil, err
			}
			children = append(children, child)
			valid = append(valid, true)
		}
		row := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			Data:        children,
			ElementType: leaf,
		}}}
		if elementNullable {
			typeutil.SetArrayElementValidData(row, valid)
		}
		return row, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported Array depth %d of field %s", depth, field.GetName())
	}
}

// nativeListVectorArrayRow builds row i of an element-nullable ArrayOfVector
// sub-field: the payload holds only the valid vectors (typed empty when none)
// and the element validity is always full length.
func nativeListVectorArrayRow(field *schemapb.FieldSchema, dim int, i int) (*schemapb.VectorField, error) {
	n := testutils.ElemCountOfArray
	valid := make([]bool, n)
	count := 0
	for j := range valid {
		valid[j] = !nativeListVectorIsNull(i, j)
		if valid[j] {
			count++
		}
	}
	row := &schemapb.VectorField{Dim: int64(dim)}
	switch field.GetElementType() {
	case schemapb.DataType_FloatVector:
		row.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: testutils.GenerateFloatVectors(count, dim)}}
	case schemapb.DataType_BinaryVector:
		row.Data = &schemapb.VectorField_BinaryVector{BinaryVector: testutils.GenerateBinaryVectors(count, dim)}
	case schemapb.DataType_Float16Vector:
		row.Data = &schemapb.VectorField_Float16Vector{Float16Vector: testutils.GenerateFloat16Vectors(count, dim)}
	case schemapb.DataType_BFloat16Vector:
		row.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: testutils.GenerateBFloat16Vectors(count, dim)}
	case schemapb.DataType_Int8Vector:
		row.Data = &schemapb.VectorField_Int8Vector{Int8Vector: typeutil.Int8ArrayToBytes(testutils.GenerateInt8Vectors(count, dim))}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported element type %s of ArrayOfVector field %s", field.GetElementType(), field.GetName())
	}
	typeutil.SetVectorArrayElementValidData(row, valid)
	return row, nil
}

// structSubFieldRowLen returns the logical element count of one non-null
// struct sub-field row, counting null elements.
func structSubFieldRowLen(subField *schemapb.FieldSchema, row any) (int, error) {
	switch subField.GetDataType() {
	case schemapb.DataType_Array:
		scalarField, ok := row.(*schemapb.ScalarField)
		if !ok {
			return 0, merr.WrapErrParameterInvalidMsg("expected ScalarField row for struct sub-field %s, got %T", subField.GetName(), row)
		}
		if typeutil.IsNestedArrayTypeSchema(subField.GetTypeSchema()) {
			return len(scalarField.GetArrayData().GetData()), nil
		}
		if subField.GetElementNullable() {
			return len(typeutil.GetArrayElementValidData(scalarField)), nil
		}
		return scalarFieldLen(subField.GetElementType(), scalarField), nil
	case schemapb.DataType_ArrayOfVector:
		vectorField, ok := row.(*schemapb.VectorField)
		if !ok {
			return 0, merr.WrapErrParameterInvalidMsg("expected VectorField row for struct sub-field %s, got %T", subField.GetName(), row)
		}
		if subField.GetElementNullable() {
			return len(typeutil.GetVectorArrayElementValidData(vectorField)), nil
		}
		width, err := vectorElementWidth(subField)
		if err != nil {
			return 0, err
		}
		return vectorPayloadLen(subField.GetElementType(), vectorField) / width, nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unsupported data type %s of struct sub-field %s", subField.GetDataType(), subField.GetName())
	}
}

func scalarFieldLen(elementType schemapb.DataType, row *schemapb.ScalarField) int {
	switch elementType {
	case schemapb.DataType_Bool:
		return len(row.GetBoolData().GetData())
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return len(row.GetIntData().GetData())
	case schemapb.DataType_Int64:
		return len(row.GetLongData().GetData())
	case schemapb.DataType_Float:
		return len(row.GetFloatData().GetData())
	case schemapb.DataType_Double:
		return len(row.GetDoubleData().GetData())
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return len(row.GetStringData().GetData())
	}
	return 0
}

// scalarFieldValue returns slot k of a scalar payload as the Go value the JSON
// encoder writes: bool, int32 (Int8/Int16/Int32), int64, float32, float64 or string.
func scalarFieldValue(elementType schemapb.DataType, row *schemapb.ScalarField, k int) (any, error) {
	if k < 0 || k >= scalarFieldLen(elementType, row) {
		return nil, merr.WrapErrParameterInvalidMsg("slot %d out of range of %s payload", k, elementType)
	}
	switch elementType {
	case schemapb.DataType_Bool:
		return row.GetBoolData().GetData()[k], nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return row.GetIntData().GetData()[k], nil
	case schemapb.DataType_Int64:
		return row.GetLongData().GetData()[k], nil
	case schemapb.DataType_Float:
		return row.GetFloatData().GetData()[k], nil
	case schemapb.DataType_Double:
		return row.GetDoubleData().GetData()[k], nil
	default:
		return row.GetStringData().GetData()[k], nil
	}
}

// vectorElementWidth returns the payload units of one vector: float32 values
// for FloatVector, bytes otherwise.
func vectorElementWidth(field *schemapb.FieldSchema) (int, error) {
	dim, err := typeutil.GetDim(field)
	if err != nil {
		return 0, err
	}
	var width int
	switch field.GetElementType() {
	case schemapb.DataType_FloatVector, schemapb.DataType_Int8Vector:
		width = int(dim)
	case schemapb.DataType_BinaryVector:
		width = int(dim) / 8
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		width = int(dim) * 2
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unsupported element type %s of ArrayOfVector field %s", field.GetElementType(), field.GetName())
	}
	if width <= 0 {
		return 0, merr.WrapErrParameterInvalidMsg("invalid dim %d of ArrayOfVector field %s", dim, field.GetName())
	}
	return width, nil
}

func vectorPayloadLen(elementType schemapb.DataType, row *schemapb.VectorField) int {
	switch elementType {
	case schemapb.DataType_FloatVector:
		return len(row.GetFloatVector().GetData())
	case schemapb.DataType_BinaryVector:
		return len(row.GetBinaryVector())
	case schemapb.DataType_Float16Vector:
		return len(row.GetFloat16Vector())
	case schemapb.DataType_BFloat16Vector:
		return len(row.GetBfloat16Vector())
	case schemapb.DataType_Int8Vector:
		return len(row.GetInt8Vector())
	}
	return 0
}

// vectorPhysicalIndex maps logical element j of an ArrayOfVector row to its
// index in the payload; ok is false for a null element.
func vectorPhysicalIndex(field *schemapb.FieldSchema, row *schemapb.VectorField, j int) (int, bool) {
	if !field.GetElementNullable() {
		return j, true
	}
	valid := typeutil.GetVectorArrayElementValidData(row)
	if j >= len(valid) || !valid[j] {
		return 0, false
	}
	return lo.Count(valid[:j], true), true
}

// vectorElementValue returns vector p of an ArrayOfVector payload as the Go
// value the JSON encoder writes: []float32 for Float/Float16/BFloat16 vectors,
// []int for BinaryVector (a []byte would be encoded as base64) and []int8.
func vectorElementValue(field *schemapb.FieldSchema, row *schemapb.VectorField, p int) (any, error) {
	width, err := vectorElementWidth(field)
	if err != nil {
		return nil, err
	}
	start, end := p*width, (p+1)*width
	if end > vectorPayloadLen(field.GetElementType(), row) {
		return nil, merr.WrapErrParameterInvalidMsg("vector %d out of range of field %s payload", p, field.GetName())
	}
	switch field.GetElementType() {
	case schemapb.DataType_FloatVector:
		return row.GetFloatVector().GetData()[start:end], nil
	case schemapb.DataType_BinaryVector:
		values := make([]int, 0, width)
		for _, value := range row.GetBinaryVector()[start:end] {
			values = append(values, int(value))
		}
		return values, nil
	case schemapb.DataType_Float16Vector:
		return typeutil.Float16BytesToFloat32Vector(row.GetFloat16Vector()[start:end]), nil
	case schemapb.DataType_BFloat16Vector:
		return typeutil.BFloat16BytesToFloat32Vector(row.GetBfloat16Vector()[start:end]), nil
	default:
		values := make([]int8, 0, width)
		for _, value := range row.GetInt8Vector()[start:end] {
			values = append(values, int8(value))
		}
		return values, nil
	}
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

func BuildSparseVectorData(mem *memory.GoAllocator, contents [][]byte, arrowType arrow.DataType, validData []bool) (arrow.Array, error) {
	if arrowType == nil || arrowType.ID() == arrow.STRING {
		// build sparse vector as JSON-format string
		builder := array.NewStringBuilder(mem)
		// For sparse storage: iterate over logical rows, use physical index for contents
		var logicalRows int
		if len(validData) > 0 {
			logicalRows = len(validData)
		} else {
			logicalRows = len(contents)
		}
		physicalIdx := 0
		for i := 0; i < logicalRows; i++ {
			isValid := len(validData) == 0 || validData[i]
			if isValid {
				rowVecData := contents[physicalIdx]
				mapData := typeutil.SparseFloatBytesToMap(rowVecData)
				// convert to JSON format
				jsonBytes, err := json.Marshal(mapData)
				if err != nil {
					return nil, err
				}
				builder.Append(string(jsonBytes))
				physicalIdx++
			} else {
				builder.AppendNull()
			}
		}
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

		// For sparse storage: iterate over logical rows, use physical index for contents
		var logicalRows int
		if len(validData) > 0 {
			logicalRows = len(validData)
		} else {
			logicalRows = len(contents)
		}
		physicalIdx := 0
		for i := 0; i < logicalRows; i++ {
			isValid := len(validData) == 0 || validData[i]
			builder.Append(isValid)
			if !isValid {
				continue
			}
			indicesBuilder.Append(true)
			valuesBuilder.Append(true)
			rowVecData := contents[physicalIdx]
			elemCount := len(rowVecData) / 8
			for j := 0; j < elemCount; j++ {
				appendIndexFunc(common.Endian.Uint32(rowVecData[j*8:]))
				appendValueFunc(math.Float32frombits(common.Endian.Uint32(rowVecData[j*8+4:])))
			}
			physicalIdx++
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
			valueBuilder := builder.ValueBuilder().(*array.Uint8Builder)
			dim := insertData.Data[fieldID].(*storage.BinaryVectorFieldData).Dim
			binVecData := insertData.Data[fieldID].(*storage.BinaryVectorFieldData).Data
			validData := insertData.Data[fieldID].(*storage.BinaryVectorFieldData).ValidData
			rowBytes := dim / 8
			// For sparse storage: logicalRows from validData, physicalRows from data
			var logicalRows int
			if len(validData) > 0 {
				logicalRows = len(validData)
			} else {
				logicalRows = len(binVecData) / rowBytes
			}
			offsets := make([]int32, 0, logicalRows+1)
			valid := make([]bool, 0, logicalRows)
			currOffset := int32(0)
			physicalIdx := 0 // Track physical index in sparse data
			for i := 0; i < logicalRows; i++ {
				offsets = append(offsets, currOffset)
				if len(validData) > 0 && !validData[i] {
					valid = append(valid, false)
				} else {
					// Use physical index for sparse storage
					start := physicalIdx * rowBytes
					end := start + rowBytes
					valueBuilder.AppendValues(binVecData[start:end], nil)
					currOffset += int32(rowBytes)
					valid = append(valid, true)
					physicalIdx++ // Increment only for valid rows
				}
			}
			offsets = append(offsets, currOffset)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_FloatVector:
			builder := array.NewListBuilder(mem, &arrow.Float32Type{})
			valueBuilder := builder.ValueBuilder().(*array.Float32Builder)
			dim := insertData.Data[fieldID].(*storage.FloatVectorFieldData).Dim
			floatVecData := insertData.Data[fieldID].(*storage.FloatVectorFieldData).Data
			validData := insertData.Data[fieldID].(*storage.FloatVectorFieldData).ValidData
			// For sparse storage: logicalRows from validData, physicalRows from data
			var logicalRows int
			if len(validData) > 0 {
				logicalRows = len(validData)
			} else {
				logicalRows = len(floatVecData) / dim
			}
			offsets := make([]int32, 0, logicalRows+1)
			valid := make([]bool, 0, logicalRows)
			currOffset := int32(0)
			physicalIdx := 0 // Track physical index in sparse data
			for i := 0; i < logicalRows; i++ {
				offsets = append(offsets, currOffset)
				if len(validData) > 0 && !validData[i] {
					valid = append(valid, false)
				} else {
					// Use physical index for sparse storage
					start := physicalIdx * dim
					end := start + dim
					valueBuilder.AppendValues(floatVecData[start:end], nil)
					currOffset += int32(dim)
					valid = append(valid, true)
					physicalIdx++ // Increment only for valid rows
				}
			}
			offsets = append(offsets, currOffset)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_Float16Vector:
			builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
			valueBuilder := builder.ValueBuilder().(*array.Uint8Builder)
			dim := insertData.Data[fieldID].(*storage.Float16VectorFieldData).Dim
			float16VecData := insertData.Data[fieldID].(*storage.Float16VectorFieldData).Data
			validData := insertData.Data[fieldID].(*storage.Float16VectorFieldData).ValidData
			rowBytes := dim * 2
			// For sparse storage: logicalRows from validData, physicalRows from data
			var logicalRows int
			if len(validData) > 0 {
				logicalRows = len(validData)
			} else {
				logicalRows = len(float16VecData) / rowBytes
			}
			offsets := make([]int32, 0, logicalRows+1)
			valid := make([]bool, 0, logicalRows)
			currOffset := int32(0)
			physicalIdx := 0 // Track physical index in sparse data
			for i := 0; i < logicalRows; i++ {
				offsets = append(offsets, currOffset)
				if len(validData) > 0 && !validData[i] {
					valid = append(valid, false)
				} else {
					// Use physical index for sparse storage
					start := physicalIdx * rowBytes
					end := start + rowBytes
					valueBuilder.AppendValues(float16VecData[start:end], nil)
					currOffset += int32(rowBytes)
					valid = append(valid, true)
					physicalIdx++ // Increment only for valid rows
				}
			}
			offsets = append(offsets, currOffset)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_BFloat16Vector:
			builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
			valueBuilder := builder.ValueBuilder().(*array.Uint8Builder)
			dim := insertData.Data[fieldID].(*storage.BFloat16VectorFieldData).Dim
			bfloat16VecData := insertData.Data[fieldID].(*storage.BFloat16VectorFieldData).Data
			validData := insertData.Data[fieldID].(*storage.BFloat16VectorFieldData).ValidData
			rowBytes := dim * 2
			// For sparse storage: logicalRows from validData, physicalRows from data
			var logicalRows int
			if len(validData) > 0 {
				logicalRows = len(validData)
			} else {
				logicalRows = len(bfloat16VecData) / rowBytes
			}
			offsets := make([]int32, 0, logicalRows+1)
			valid := make([]bool, 0, logicalRows)
			currOffset := int32(0)
			physicalIdx := 0 // Track physical index in sparse data
			for i := 0; i < logicalRows; i++ {
				offsets = append(offsets, currOffset)
				if len(validData) > 0 && !validData[i] {
					valid = append(valid, false)
				} else {
					// Use physical index for sparse storage
					start := physicalIdx * rowBytes
					end := start + rowBytes
					valueBuilder.AppendValues(bfloat16VecData[start:end], nil)
					currOffset += int32(rowBytes)
					valid = append(valid, true)
					physicalIdx++ // Increment only for valid rows
				}
			}
			offsets = append(offsets, currOffset)
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_SparseFloatVector:
			contents := insertData.Data[fieldID].(*storage.SparseFloatVectorFieldData).GetContents()
			validData := insertData.Data[fieldID].(*storage.SparseFloatVectorFieldData).ValidData
			arr, err := BuildSparseVectorData(mem, contents, nil, validData)
			if err != nil {
				return nil, err
			}
			columns = append(columns, arr)
		case schemapb.DataType_Int8Vector:
			builder := array.NewListBuilder(mem, &arrow.Int8Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int8Builder)
			dim := insertData.Data[fieldID].(*storage.Int8VectorFieldData).Dim
			int8VecData := insertData.Data[fieldID].(*storage.Int8VectorFieldData).Data
			validData := insertData.Data[fieldID].(*storage.Int8VectorFieldData).ValidData
			// For sparse storage: logicalRows from validData, physicalRows from data
			var logicalRows int
			if len(validData) > 0 {
				logicalRows = len(validData)
			} else {
				logicalRows = len(int8VecData) / dim
			}
			offsets := make([]int32, 0, logicalRows+1)
			valid := make([]bool, 0, logicalRows)
			currOffset := int32(0)
			physicalIdx := 0 // Track physical index in sparse data
			for i := 0; i < logicalRows; i++ {
				offsets = append(offsets, currOffset)
				if len(validData) > 0 && !validData[i] {
					valid = append(valid, false)
				} else {
					// Use physical index for sparse storage
					start := physicalIdx * dim
					end := start + dim
					valueBuilder.AppendValues(int8VecData[start:end], nil)
					currOffset += int32(dim)
					valid = append(valid, true)
					physicalIdx++ // Increment only for valid rows
				}
			}
			offsets = append(offsets, currOffset)
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
				return nil, merr.WrapErrParameterInvalidMsg("unexpected list value builder for VectorArray field %s: %T", field.GetName(), listBuilder.ValueBuilder())
			}

			vectorArrayData.Dim = dim

			bytesPerVector := fixedSizeBuilder.Type().(*arrow.FixedSizeBinaryType).ByteWidth

			appendBinarySlice := func(data []byte, stride int) error {
				if stride == 0 {
					return merr.WrapErrParameterInvalidMsg("zero stride for VectorArray field %s", field.GetName())
				}
				if len(data)%stride != 0 {
					return merr.WrapErrParameterInvalidMsg("vector array data length %d is not divisible by stride %d for field %s", len(data), stride, field.GetName())
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
						return nil, merr.WrapErrParameterInvalidMsg("expected FloatVector data for field %s", field.GetName())
					}
					data := floatArray.GetData()
					if len(data) == 0 {
						continue
					}
					if len(data)%int(dim) != 0 {
						return nil, merr.WrapErrParameterInvalidMsg("float vector data length %d is not divisible by dim %d for field %s", len(data), dim, field.GetName())
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
					return nil, merr.WrapErrParameterInvalidMsg("unsupported element type in VectorArray: %s", elementType.String())
				}
			}

			columns = append(columns, listBuilder.NewListArray())
		}
	}

	// Process StructArrayFields as nested list<struct> format
	for _, structField := range schema.StructArrayFields {
		column, err := buildStructArrayColumn(mem, structField, insertData)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	return columns, nil
}

// structChildArrowType returns the Arrow type of a struct sub-field inside the
// list<struct<...>> column: the scalar element type for Array, list<leaf> for a
// nested Array (item nullable iff the leaf is nullable), and list<float32>,
// list<uint8> or list<int8> for ArrayOfVector.
func structChildArrowType(subField *schemapb.FieldSchema) (arrow.DataType, error) {
	switch subField.GetDataType() {
	case schemapb.DataType_Array:
		if typeutil.IsNestedArrayTypeSchema(subField.GetTypeSchema()) {
			leaf, leafNullable, _, err := typeutil.GetFieldArrayLeaf(subField)
			if err != nil {
				return nil, err
			}
			leafType, err := scalarArrowType(leaf)
			if err != nil {
				return nil, err
			}
			return arrow.ListOfField(arrow.Field{Name: "item", Type: leafType, Nullable: leafNullable}), nil
		}
		return scalarArrowType(subField.GetElementType())
	case schemapb.DataType_ArrayOfVector:
		switch subField.GetElementType() {
		case schemapb.DataType_FloatVector:
			return arrow.ListOf(arrow.PrimitiveTypes.Float32), nil
		case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
			return arrow.ListOf(arrow.PrimitiveTypes.Uint8), nil
		case schemapb.DataType_Int8Vector:
			return arrow.ListOf(arrow.PrimitiveTypes.Int8), nil
		}
	}
	return nil, merr.WrapErrParameterInvalidMsg("unsupported struct sub-field %s of type %s/%s", subField.GetName(), subField.GetDataType(), subField.GetElementType())
}

func scalarArrowType(dataType schemapb.DataType) (arrow.DataType, error) {
	switch dataType {
	case schemapb.DataType_Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case schemapb.DataType_Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case schemapb.DataType_Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case schemapb.DataType_Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case schemapb.DataType_Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case schemapb.DataType_Float:
		return arrow.PrimitiveTypes.Float32, nil
	case schemapb.DataType_Double:
		return arrow.PrimitiveTypes.Float64, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return arrow.BinaryTypes.String, nil
	}
	return nil, merr.WrapErrParameterInvalidMsg("unsupported scalar element type %s", dataType)
}

// buildStructArrayColumn builds one struct field as list<struct<...>>. A null
// struct row is a null list; a struct child is nullable iff its sub-field is
// element-nullable, and null elements, inner arrays, leaves and vectors are
// Arrow nulls at their level.
func buildStructArrayColumn(mem memory.Allocator, structField *schemapb.StructArrayFieldSchema, insertData *storage.InsertData) (arrow.Array, error) {
	// Sub-fields without data in insertData get no struct child, so callers can
	// delete a column to produce files missing that sub-field.
	subFields := lo.Filter(structField.GetFields(), func(subField *schemapb.FieldSchema, _ int) bool {
		data, ok := insertData.Data[subField.GetFieldID()]
		return ok && data != nil
	})
	if len(subFields) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("no data for any sub-field of struct field %s", structField.GetName())
	}
	structFields := make([]arrow.Field, 0, len(subFields))
	subFieldData := make([]storage.FieldData, 0, len(subFields))
	for _, subField := range subFields {
		fieldName, err := typeutil.ExtractStructFieldName(subField.GetName())
		if err != nil {
			return nil, err
		}
		arrType, err := structChildArrowType(subField)
		if err != nil {
			return nil, err
		}
		structFields = append(structFields, arrow.Field{
			Name:     fieldName,
			Type:     arrType,
			Nullable: subField.GetElementNullable(),
		})
		subFieldData = append(subFieldData, insertData.Data[subField.GetFieldID()])
	}

	listBuilder := array.NewListBuilder(mem, arrow.StructOf(structFields...))
	defer listBuilder.Release()
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)

	rowCount := subFieldData[0].RowNum()
	for i := 0; i < rowCount; i++ {
		rows := make([]any, len(subFields))
		for idx, data := range subFieldData {
			rows[idx] = data.GetRow(i)
		}
		if rows[0] == nil {
			listBuilder.AppendNull()
			continue
		}
		arrayLen, err := structSubFieldRowLen(subFields[0], rows[0])
		if err != nil {
			return nil, err
		}
		listBuilder.Append(true)
		for j := 0; j < arrayLen; j++ {
			structBuilder.Append(true)
			for idx, subField := range subFields {
				if err := appendStructChildValue(structBuilder.FieldBuilder(idx), subField, rows[idx], j); err != nil {
					return nil, err
				}
			}
		}
	}
	return listBuilder.NewArray(), nil
}

// appendStructChildValue appends element j of one struct sub-field row to the
// child builder of the struct.
func appendStructChildValue(b array.Builder, subField *schemapb.FieldSchema, row any, j int) error {
	switch subField.GetDataType() {
	case schemapb.DataType_Array:
		scalarField, ok := row.(*schemapb.ScalarField)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("expected ScalarField row for struct sub-field %s, got %T", subField.GetName(), row)
		}
		valid := typeutil.GetArrayElementValidData(scalarField)
		if subField.GetElementNullable() && j >= len(valid) {
			return merr.WrapErrParameterInvalidMsg("element %d out of range of struct sub-field %s validity", j, subField.GetName())
		}
		if typeutil.IsNestedArrayTypeSchema(subField.GetTypeSchema()) {
			leaf, leafNullable, _, err := typeutil.GetFieldArrayLeaf(subField)
			if err != nil {
				return err
			}
			innerBuilder := b.(*array.ListBuilder)
			if subField.GetElementNullable() && !valid[j] {
				innerBuilder.AppendNull()
				return nil
			}
			children := scalarField.GetArrayData().GetData()
			if j >= len(children) {
				return merr.WrapErrParameterInvalidMsg("element %d out of range of nested struct sub-field %s", j, subField.GetName())
			}
			child := children[j]
			childValid := typeutil.GetArrayElementValidData(child)
			childLen := scalarFieldLen(leaf, child)
			if leafNullable && len(childValid) != childLen {
				return merr.WrapErrParameterInvalidMsg("leaf validity length %d does not match payload length %d of struct sub-field %s", len(childValid), childLen, subField.GetName())
			}
			innerBuilder.Append(true)
			for k := 0; k < childLen; k++ {
				if leafNullable && !childValid[k] {
					innerBuilder.ValueBuilder().AppendNull()
					continue
				}
				if err := appendScalarArrowValue(innerBuilder.ValueBuilder(), leaf, child, k); err != nil {
					return err
				}
			}
			return nil
		}
		if subField.GetElementNullable() && !valid[j] {
			b.AppendNull()
			return nil
		}
		return appendScalarArrowValue(b, subField.GetElementType(), scalarField, j)
	case schemapb.DataType_ArrayOfVector:
		vectorField, ok := row.(*schemapb.VectorField)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("expected VectorField row for struct sub-field %s, got %T", subField.GetName(), row)
		}
		vectorBuilder := b.(*array.ListBuilder)
		p, ok := vectorPhysicalIndex(subField, vectorField, j)
		if !ok {
			vectorBuilder.AppendNull()
			return nil
		}
		width, err := vectorElementWidth(subField)
		if err != nil {
			return err
		}
		start, end := p*width, (p+1)*width
		if end > vectorPayloadLen(subField.GetElementType(), vectorField) {
			return merr.WrapErrParameterInvalidMsg("vector %d out of range of struct sub-field %s payload", p, subField.GetName())
		}
		vectorBuilder.Append(true)
		switch subField.GetElementType() {
		case schemapb.DataType_FloatVector:
			vectorBuilder.ValueBuilder().(*array.Float32Builder).AppendValues(vectorField.GetFloatVector().GetData()[start:end], nil)
		case schemapb.DataType_BinaryVector:
			vectorBuilder.ValueBuilder().(*array.Uint8Builder).AppendValues(vectorField.GetBinaryVector()[start:end], nil)
		case schemapb.DataType_Float16Vector:
			vectorBuilder.ValueBuilder().(*array.Uint8Builder).AppendValues(vectorField.GetFloat16Vector()[start:end], nil)
		case schemapb.DataType_BFloat16Vector:
			vectorBuilder.ValueBuilder().(*array.Uint8Builder).AppendValues(vectorField.GetBfloat16Vector()[start:end], nil)
		case schemapb.DataType_Int8Vector:
			int8Builder := vectorBuilder.ValueBuilder().(*array.Int8Builder)
			for _, value := range vectorField.GetInt8Vector()[start:end] {
				int8Builder.Append(int8(value))
			}
		}
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported data type %s of struct sub-field %s", subField.GetDataType(), subField.GetName())
	}
}

func appendScalarArrowValue(b array.Builder, elementType schemapb.DataType, row *schemapb.ScalarField, k int) error {
	value, err := scalarFieldValue(elementType, row, k)
	if err != nil {
		return err
	}
	switch elementType {
	case schemapb.DataType_Bool:
		b.(*array.BooleanBuilder).Append(value.(bool))
	case schemapb.DataType_Int8:
		b.(*array.Int8Builder).Append(int8(value.(int32)))
	case schemapb.DataType_Int16:
		b.(*array.Int16Builder).Append(int16(value.(int32)))
	case schemapb.DataType_Int32:
		b.(*array.Int32Builder).Append(value.(int32))
	case schemapb.DataType_Int64:
		b.(*array.Int64Builder).Append(value.(int64))
	case schemapb.DataType_Float:
		b.(*array.Float32Builder).Append(value.(float32))
	case schemapb.DataType_Double:
		b.(*array.Float64Builder).Append(value.(float64))
	default:
		b.(*array.StringBuilder).Append(value.(string))
	}
	return nil
}

// reconstructStructArrayForJSON reconstructs struct array data for JSON format.
// Returns an array of maps where each element represents a struct, keyed by the
// raw sub-field names, or nil for a null struct row. A null element of an
// element-nullable sub-field is written as JSON null, and its key is omitted in
// rows with rowIndex%4 >= 2 so that both import paths for a null element
// (explicit null and missing key) are covered. A nested Array value is a list of
// lists with null inner arrays and null leaves.
func reconstructStructArrayForJSON(structField *schemapb.StructArrayFieldSchema, insertData *storage.InsertData, rowIndex int) ([]map[string]any, error) {
	subFields := structField.GetFields()
	if len(subFields) == 0 {
		return []map[string]any{}, nil
	}

	// Sub-fields without data in insertData are left out of every element, so
	// callers can delete a column to produce files missing that sub-field.
	presentFields := make([]*schemapb.FieldSchema, 0, len(subFields))
	rows := make([]any, 0, len(subFields))
	names := make([]string, 0, len(subFields))
	for _, subField := range subFields {
		fieldData, ok := insertData.Data[subField.GetFieldID()]
		if !ok || fieldData == nil {
			continue
		}
		name, err := typeutil.ExtractStructFieldName(subField.GetName())
		if err != nil {
			return nil, err
		}
		presentFields = append(presentFields, subField)
		rows = append(rows, fieldData.GetRow(rowIndex))
		names = append(names, name)
	}
	if len(presentFields) == 0 {
		return []map[string]any{}, nil
	}
	if rows[0] == nil {
		return nil, nil
	}
	arrayLen, err := structSubFieldRowLen(presentFields[0], rows[0])
	if err != nil {
		return nil, err
	}

	structArray := make([]map[string]any, arrayLen)
	for j := 0; j < arrayLen; j++ {
		structElem := make(map[string]any, len(presentFields))
		for idx, subField := range presentFields {
			value, err := structElementValueForJSON(subField, rows[idx], j)
			if err != nil {
				return nil, err
			}
			if value == nil && rowIndex%4 >= 2 {
				continue
			}
			structElem[names[idx]] = value
		}
		structArray[j] = structElem
	}
	return structArray, nil
}

// structElementValueForJSON returns element j of one struct sub-field row as a
// JSON value; nil is a null element.
func structElementValueForJSON(subField *schemapb.FieldSchema, row any, j int) (any, error) {
	switch subField.GetDataType() {
	case schemapb.DataType_Array:
		scalarField, ok := row.(*schemapb.ScalarField)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("expected ScalarField row for struct sub-field %s, got %T", subField.GetName(), row)
		}
		valid := typeutil.GetArrayElementValidData(scalarField)
		if subField.GetElementNullable() {
			if j >= len(valid) {
				return nil, merr.WrapErrParameterInvalidMsg("element %d out of range of struct sub-field %s validity", j, subField.GetName())
			}
			if !valid[j] {
				return nil, nil
			}
		}
		if !typeutil.IsNestedArrayTypeSchema(subField.GetTypeSchema()) {
			return scalarFieldValue(subField.GetElementType(), scalarField, j)
		}
		leaf, leafNullable, _, err := typeutil.GetFieldArrayLeaf(subField)
		if err != nil {
			return nil, err
		}
		children := scalarField.GetArrayData().GetData()
		if j >= len(children) {
			return nil, merr.WrapErrParameterInvalidMsg("element %d out of range of nested struct sub-field %s", j, subField.GetName())
		}
		child := children[j]
		childValid := typeutil.GetArrayElementValidData(child)
		childLen := scalarFieldLen(leaf, child)
		if leafNullable && len(childValid) != childLen {
			return nil, merr.WrapErrParameterInvalidMsg("leaf validity length %d does not match payload length %d of struct sub-field %s", len(childValid), childLen, subField.GetName())
		}
		inner := make([]any, 0, childLen)
		for k := 0; k < childLen; k++ {
			if leafNullable && !childValid[k] {
				inner = append(inner, nil)
				continue
			}
			value, err := scalarFieldValue(leaf, child, k)
			if err != nil {
				return nil, err
			}
			inner = append(inner, value)
		}
		return inner, nil
	case schemapb.DataType_ArrayOfVector:
		vectorField, ok := row.(*schemapb.VectorField)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("expected VectorField row for struct sub-field %s, got %T", subField.GetName(), row)
		}
		p, ok := vectorPhysicalIndex(subField, vectorField, j)
		if !ok {
			return nil, nil
		}
		return vectorElementValue(subField, vectorField, p)
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported data type %s of struct sub-field %s", subField.GetDataType(), subField.GetName())
	}
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
// Returns a JSON string containing an array of structs with numeric vector values,
// in the same shape as the JSON format, or nullkey for a null struct row.
func reconstructStructArrayForCSV(structField *schemapb.StructArrayFieldSchema, insertData *storage.InsertData, rowIndex int, nullkey string) (string, error) {
	// Use the JSON reconstruction function to get the struct array
	structArray, err := reconstructStructArrayForJSON(structField, insertData, rowIndex)
	if err != nil {
		return "", err
	}
	if structArray == nil {
		return nullkey, nil
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
				structArrayData, err := reconstructStructArrayForCSV(structArrayField, insertData, i, nullkey)
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
