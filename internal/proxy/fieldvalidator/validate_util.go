package fieldvalidator

import (
	"context"
	"fmt"
	"math"
	"reflect"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/nullutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type ValidateUtil struct {
	checkNAN      bool
	checkMaxLen   bool
	checkOverflow bool
	checkMaxCap   bool
}

type ValidateOption func(*ValidateUtil)

func WithNANCheck() ValidateOption {
	return func(v *ValidateUtil) {
		v.checkNAN = true
	}
}

func WithMaxLenCheck() ValidateOption {
	return func(v *ValidateUtil) {
		v.checkMaxLen = true
	}
}

func WithOverflowCheck() ValidateOption {
	return func(v *ValidateUtil) {
		v.checkOverflow = true
	}
}

func WithMaxCapCheck() ValidateOption {
	return func(v *ValidateUtil) {
		v.checkMaxCap = true
	}
}

func ValidateGeometryFieldSearchResult(fieldData **schemapb.FieldData) error {
	if *fieldData == nil || (*fieldData).GetScalars() == nil || (*fieldData).GetScalars().Data == nil {
		return nil
	}
	// Check if the field data already contains GeometryWktData
	_, ok := (*fieldData).GetScalars().Data.(*schemapb.ScalarField_GeometryWktData)
	if ok {
		// Already in WKT format, no conversion needed
		mlog.Debug(context.TODO(), "Geometry field data already contains WKT data, skipping conversion",
			mlog.String("fieldName", (*fieldData).GetFieldName()))
		return nil
	}
	wkbArray := (*fieldData).GetScalars().GetGeometryData().GetData()
	wktArray := make([]string, len(wkbArray))
	validData := typeutil.GetFieldDataValidData(*fieldData)
	for i, data := range wkbArray {
		if validData != nil && !validData[i] {
			continue
		}
		wktStr, err := common.ConvertWKBToWKT(data)
		if err != nil {
			mlog.Error(context.TODO(), "translate the geomery  into its wkt failed")
			return err
		}
		wktArray[i] = wktStr
	}
	// modify the field data in place
	*fieldData = &schemapb.FieldData{
		Type:      (*fieldData).GetType(),
		FieldName: (*fieldData).GetFieldName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryWktData{
					GeometryWktData: &schemapb.GeometryWktArray{
						Data: wktArray,
					},
				},
			},
		},
		FieldId:   (*fieldData).GetFieldId(),
		IsDynamic: (*fieldData).GetIsDynamic(),
	}
	typeutil.SetFieldDataValidData(*fieldData, validData)
	return nil
}

func (v *ValidateUtil) apply(opts ...ValidateOption) {
	for _, opt := range opts {
		opt(v)
	}
}

func (v *ValidateUtil) Validate(data []*schemapb.FieldData, helper *typeutil.SchemaHelper, numRows uint64) error {
	if helper == nil {
		return merr.WrapErrServiceInternal("nil schema helper provided for Validation")
	}
	for _, field := range data {
		if !typeutil.ValidateAndNormalizeFieldDataValidData(field) {
			return merr.WrapErrParameterInvalidMsg("field %s has different legacy and field-specific valid_data", field.GetFieldName())
		}
		fieldSchema, err := helper.GetFieldFromName(field.GetFieldName())
		if err != nil {
			return err
		}

		switch fieldSchema.GetDataType() {
		case schemapb.DataType_FloatVector:
			if err := v.checkFloatVectorFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Float16Vector:
			if err := v.checkFloat16VectorFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_BFloat16Vector:
			if err := v.checkBFloat16VectorFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_BinaryVector:
			if err := v.checkBinaryVectorFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_SparseFloatVector:
			if err := v.checkSparseFloatVectorFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Int8Vector:
			if err := v.checkInt8VectorFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_VarChar:
			if err := v.checkVarCharFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Text:
			if err := v.checkTextFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Geometry:
			if err := v.checkGeometryFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_JSON:
			if err := v.checkJSONFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
			if err := v.checkIntegerFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Int64:
			if err := v.checkLongFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Float:
			if err := v.checkFloatFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Double:
			if err := v.checkDoubleFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Array:
			if err := v.checkArrayFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_ArrayOfVector:
			if err := v.checkArrayOfVectorFieldData(field, fieldSchema); err != nil {
				return err
			}

		case schemapb.DataType_ArrayOfStruct:
			panic("unreachable, array of struct should have been flattened")
		case schemapb.DataType_Timestamptz:
			if err := v.checkTimestamptzFieldData(field, helper.GetTimezone()); err != nil {
				return err
			}
		default:
		}
	}
	err := v.fillWithValue(data, helper, int(numRows))
	if err != nil {
		return err
	}

	if err := v.CheckAligned(data, helper, numRows); err != nil {
		return err
	}

	return nil
}

func (v *ValidateUtil) CheckAligned(data []*schemapb.FieldData, schema *typeutil.SchemaHelper, numRows uint64) error {
	errNumRowsMismatch := func(fieldName string, fieldNumRows uint64) error {
		msg := fmt.Sprintf("the num_rows (%d) of field (%s) is not equal to passed num_rows (%d)", fieldNumRows, fieldName, numRows)
		return merr.WrapErrParameterInvalid(numRows, fieldNumRows, msg)
	}
	errDimMismatch := func(fieldName string, dataDim int64, schemaDim int64) error {
		msg := fmt.Sprintf("the dim (%d) of field data(%s) is not equal to schema dim (%d)", dataDim, fieldName, schemaDim)
		return merr.WrapErrParameterInvalid(schemaDim, dataDim, msg)
	}
	getExpectedVectorRows := func(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) uint64 {
		validData := typeutil.GetFieldDataValidData(field)
		if fieldSchema.GetNullable() && len(validData) > 0 {
			return uint64(GetValidNumber(validData))
		}
		return numRows
	}
	for _, field := range data {
		switch field.GetType() {
		case schemapb.DataType_FloatVector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			expectedRows := getExpectedVectorRows(field, f)
			if field.GetVectors() == nil {
				if expectedRows != 0 {
					return errNumRowsMismatch(field.GetFieldName(), 0)
				}
				continue
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}

			n, err := funcutil.GetNumRowsOfFloatVectorField(field.GetVectors().GetFloatVector().GetData(), dim)
			if err != nil {
				return err
			}
			dataDim := field.GetVectors().Dim
			if dataDim != dim {
				return errDimMismatch(field.GetFieldName(), dataDim, dim)
			}

			if n != expectedRows {
				return errNumRowsMismatch(field.GetFieldName(), n)
			}

		case schemapb.DataType_BinaryVector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			expectedRows := getExpectedVectorRows(field, f)
			if field.GetVectors() == nil {
				if expectedRows != 0 {
					return errNumRowsMismatch(field.GetFieldName(), 0)
				}
				continue
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}
			dataDim := field.GetVectors().Dim
			if dataDim != dim {
				return errDimMismatch(field.GetFieldName(), dataDim, dim)
			}

			n, err := funcutil.GetNumRowsOfBinaryVectorField(field.GetVectors().GetBinaryVector(), dim)
			if err != nil {
				return err
			}

			if n != expectedRows {
				return errNumRowsMismatch(field.GetFieldName(), n)
			}

		case schemapb.DataType_Float16Vector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			expectedRows := getExpectedVectorRows(field, f)
			if field.GetVectors() == nil {
				if expectedRows != 0 {
					return errNumRowsMismatch(field.GetFieldName(), 0)
				}
				continue
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}
			dataDim := field.GetVectors().Dim
			if dataDim != dim {
				return errDimMismatch(field.GetFieldName(), dataDim, dim)
			}

			n, err := funcutil.GetNumRowsOfFloat16VectorField(field.GetVectors().GetFloat16Vector(), dim)
			if err != nil {
				return err
			}

			if n != expectedRows {
				return errNumRowsMismatch(field.GetFieldName(), n)
			}

		case schemapb.DataType_BFloat16Vector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			expectedRows := getExpectedVectorRows(field, f)
			if field.GetVectors() == nil {
				if expectedRows != 0 {
					return errNumRowsMismatch(field.GetFieldName(), 0)
				}
				continue
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}
			dataDim := field.GetVectors().Dim
			if dataDim != dim {
				return errDimMismatch(field.GetFieldName(), dataDim, dim)
			}

			n, err := funcutil.GetNumRowsOfBFloat16VectorField(field.GetVectors().GetBfloat16Vector(), dim)
			if err != nil {
				return err
			}

			if n != expectedRows {
				return errNumRowsMismatch(field.GetFieldName(), n)
			}

		case schemapb.DataType_SparseFloatVector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			expectedRows := getExpectedVectorRows(field, f)
			if field.GetVectors() == nil || field.GetVectors().GetSparseFloatVector() == nil {
				if expectedRows != 0 {
					return errNumRowsMismatch(field.GetFieldName(), 0)
				}
				continue
			}

			n := uint64(len(field.GetVectors().GetSparseFloatVector().Contents))
			if n != expectedRows {
				return errNumRowsMismatch(field.GetFieldName(), n)
			}

		case schemapb.DataType_Int8Vector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			expectedRows := getExpectedVectorRows(field, f)
			if field.GetVectors() == nil {
				if expectedRows != 0 {
					return errNumRowsMismatch(field.GetFieldName(), 0)
				}
				continue
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}

			n, err := funcutil.GetNumRowsOfInt8VectorField(field.GetVectors().GetInt8Vector(), dim)
			if err != nil {
				return err
			}
			dataDim := field.GetVectors().Dim
			if dataDim != dim {
				return errDimMismatch(field.GetFieldName(), dataDim, dim)
			}

			if n != expectedRows {
				return errNumRowsMismatch(field.GetFieldName(), n)
			}

		case schemapb.DataType_ArrayOfVector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			// ArrayOfVector is dense after fillWithValue: null rows are filled with empty
			// per-row VectorField placeholders, so Data length must equal numRows.
			if field.GetVectors() == nil || field.GetVectors().GetVectorArray() == nil {
				if numRows != 0 {
					return errNumRowsMismatch(field.GetFieldName(), 0)
				}
				continue
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}

			vectorArray := field.GetVectors().GetVectorArray()
			dataDim := vectorArray.GetDim()
			if dataDim != dim {
				return errDimMismatch(field.GetFieldName(), dataDim, dim)
			}

			n := uint64(len(vectorArray.GetData()))
			if n != numRows {
				return errNumRowsMismatch(field.GetFieldName(), n)
			}

		default:
			// error won't happen here.
			n, err := funcutil.GetNumRowOfFieldDataWithSchema(field, schema)
			if err != nil {
				return err
			}

			if n != numRows {
				mlog.Warn(context.TODO(), "the num_rows of field is not equal to passed num_rows", mlog.String("fieldName", field.GetFieldName()),
					mlog.Int64("fieldNumRows", int64(n)), mlog.Int64("passedNumRows", int64(numRows)),
					mlog.Bools("ValidData", typeutil.GetFieldDataValidData(field)))
				return errNumRowsMismatch(field.GetFieldName(), n)
			}
		}
	}

	return nil
}

// fill data in two situation
//  1. has no default_value, if nullable,
//     will fill nullValue when passed num_rows not equal to expected num_rows
//  2. has default_value,
//     will fill default_value when passed num_rows not equal to expected num_rows,
//
// after fillWithValue, only nullable field will has valid_data, the length of all data will be passed num_rows.
// Element-nullable scalar Array rows are also expanded from compact input to dense payload.
func (v *ValidateUtil) fillWithValue(data []*schemapb.FieldData, schema *typeutil.SchemaHelper, numRows int) error {
	for _, field := range data {
		fieldSchema, err := schema.GetFieldFromName(field.GetFieldName())
		if err != nil {
			return err
		}

		// adapt all valid data for nullable or default value column
		if (fieldSchema.GetNullable() || fieldSchema.GetDefaultValue() != nil) && len(typeutil.GetFieldDataValidData(field)) == 0 {
			typeutil.SetFieldDataValidData(field, lo.RepeatBy(numRows, func(i int) bool { return true }))
		}

		if fieldSchema.GetDefaultValue() == nil {
			err = FillWithNullValue(field, fieldSchema, numRows)
			if err != nil {
				return err
			}
		} else {
			err = FillWithDefaultValue(field, fieldSchema, numRows)
			if err != nil {
				return err
			}
		}

		if fieldSchema.GetDataType() == schemapb.DataType_Array &&
			fieldSchema.GetElementNullable() &&
			!typeutil.IsNestedArrayTypeSchema(fieldSchema.GetTypeSchema()) {
			arrayData := field.GetScalars().GetArrayData()
			if arrayData == nil {
				return merr.WrapErrParameterInvalidMsg("array data is nil, field: %s", field.GetFieldName())
			}
			rowValidData := typeutil.GetFieldDataValidData(field)
			for rowIdx, row := range arrayData.GetData() {
				if len(rowValidData) > 0 && !rowValidData[rowIdx] {
					continue
				}
				elementValidData := typeutil.GetArrayElementValidData(row)
				switch rowData := row.GetData().(type) {
				case *schemapb.ScalarField_BoolData:
					rowData.BoolData.Data, err = fillWithNullValueImpl(rowData.BoolData.Data, elementValidData)
				case *schemapb.ScalarField_IntData:
					rowData.IntData.Data, err = fillWithNullValueImpl(rowData.IntData.Data, elementValidData)
				case *schemapb.ScalarField_LongData:
					rowData.LongData.Data, err = fillWithNullValueImpl(rowData.LongData.Data, elementValidData)
				case *schemapb.ScalarField_FloatData:
					rowData.FloatData.Data, err = fillWithNullValueImpl(rowData.FloatData.Data, elementValidData)
				case *schemapb.ScalarField_DoubleData:
					rowData.DoubleData.Data, err = fillWithNullValueImpl(rowData.DoubleData.Data, elementValidData)
				case *schemapb.ScalarField_StringData:
					rowData.StringData.Data, err = fillWithNullValueImpl(rowData.StringData.Data, elementValidData)
				default:
					return merr.WrapErrParameterInvalidMsg("undefined array element type:%s", fieldSchema.GetElementType().String())
				}
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func FillWithNullValue(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema, numRows int) error {
	if !typeutil.ValidateAndNormalizeFieldDataValidData(field) {
		return merr.WrapErrParameterInvalidMsg("field %s has different legacy and field-specific valid_data", field.GetFieldName())
	}
	validData := typeutil.GetFieldDataValidData(field)
	err := nullutil.CheckValidData(validData, fieldSchema, numRows)
	if err != nil {
		return err
	}

	if !fieldSchema.GetNullable() {
		return nil
	}

	switch field.Field.(type) {
	case *schemapb.FieldData_Scalars:
		switch sd := field.GetScalars().GetData().(type) {
		case *schemapb.ScalarField_BoolData:
			sd.BoolData.Data, err = fillWithNullValueImpl(sd.BoolData.Data, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_IntData:
			sd.IntData.Data, err = fillWithNullValueImpl(sd.IntData.Data, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_LongData:
			sd.LongData.Data, err = fillWithNullValueImpl(sd.LongData.Data, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_FloatData:
			sd.FloatData.Data, err = fillWithNullValueImpl(sd.FloatData.Data, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_DoubleData:
			sd.DoubleData.Data, err = fillWithNullValueImpl(sd.DoubleData.Data, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_TimestamptzData:
			sd.TimestamptzData.Data, err = fillWithNullValueImpl(sd.TimestamptzData.Data, validData)
			if err != nil {
				return err
			}
		case *schemapb.ScalarField_StringData:
			sd.StringData.Data, err = fillWithNullValueImpl(sd.StringData.Data, validData)
			if err != nil {
				return err
			}
		case *schemapb.ScalarField_ArrayData:
			sd.ArrayData.Data, err = fillWithNullValueImpl(sd.ArrayData.Data, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_JsonData:
			sd.JsonData.Data, err = fillWithNullValueImpl(sd.JsonData.Data, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_GeometryData:
			sd.GeometryData.Data, err = fillWithNullValueImpl(sd.GeometryData.Data, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_GeometryWktData:
			sd.GeometryWktData.Data, err = fillWithNullValueImpl(sd.GeometryWktData.Data, validData)
			if err != nil {
				return err
			}
		default:
			return merr.WrapErrParameterInvalidMsg("undefined data type:%s", field.Type.String())
		}

	case *schemapb.FieldData_Vectors:
		// Only ArrayOfVector needs null-expansion. Regular vectors stay compact and
		// rely on ValidData + isNullRow to carry null semantics downstream.
		// ArrayOfVector is treated as a per-row array whose null rows are represented
		// by an empty VectorField placeholder, so downstream consumers can read it
		// uniformly as "row has zero vectors" without special null handling.
		if field.Type == schemapb.DataType_ArrayOfVector {
			vectorArray := field.GetVectors().GetVectorArray()
			if vectorArray == nil {
				return merr.WrapErrParameterInvalidMsg("array of vector data is nil, field: %s", field.GetFieldName())
			}
			expanded, err := fillVectorArrayNullValueImpl(vectorArray.GetData(), validData, vectorArray.GetDim(), vectorArray.GetElementType())
			if err != nil {
				return err
			}
			vectorArray.Data = expanded
		}
	default:
		return merr.WrapErrParameterInvalidMsg("undefined data type:%s", field.Type.String())
	}

	return nil
}

func fillVectorArrayNullValueImpl(array []*schemapb.VectorField, validData []bool, dim int64, elementType schemapb.DataType) ([]*schemapb.VectorField, error) {
	n := GetValidNumber(validData)
	if len(array) != n {
		return nil, merr.WrapErrParameterInvalid(n, len(array), "the length of field is wrong")
	}
	if n == len(validData) {
		return array, nil
	}
	res := make([]*schemapb.VectorField, len(validData))
	srcIdx := 0
	for i, v := range validData {
		if v {
			res[i] = array[srcIdx]
			srcIdx++
		} else {
			emptyRow, err := typeutil.NewEmptyArrayOfVectorRow(dim, elementType)
			if err != nil {
				return nil, err
			}
			res[i] = emptyRow
		}
	}
	return res, nil
}

func FillWithDefaultValue(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema, numRows int) error {
	if !typeutil.ValidateAndNormalizeFieldDataValidData(field) {
		return merr.WrapErrParameterInvalidMsg("field %s has different legacy and field-specific valid_data", field.GetFieldName())
	}
	var err error
	validData := typeutil.GetFieldDataValidData(field)
	switch field.Field.(type) {
	case *schemapb.FieldData_Scalars:
		switch sd := field.GetScalars().GetData().(type) {
		case *schemapb.ScalarField_BoolData:
			if len(validData) != numRows {
				msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", field.GetFieldName())
				return merr.WrapErrParameterInvalid(numRows, len(validData), msg)
			}
			defaultValue := fieldSchema.GetDefaultValue().GetBoolData()
			sd.BoolData.Data, err = fillWithDefaultValueImpl(sd.BoolData.Data, defaultValue, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_IntData:
			if len(validData) != numRows {
				msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", field.GetFieldName())
				return merr.WrapErrParameterInvalid(numRows, len(validData), msg)
			}
			defaultValue := fieldSchema.GetDefaultValue().GetIntData()
			sd.IntData.Data, err = fillWithDefaultValueImpl(sd.IntData.Data, defaultValue, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_LongData:
			if len(validData) != numRows {
				msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", field.GetFieldName())
				return merr.WrapErrParameterInvalid(numRows, len(validData), msg)
			}
			defaultValue := fieldSchema.GetDefaultValue().GetLongData()
			sd.LongData.Data, err = fillWithDefaultValueImpl(sd.LongData.Data, defaultValue, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_FloatData:
			if len(validData) != numRows {
				msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", field.GetFieldName())
				return merr.WrapErrParameterInvalid(numRows, len(validData), msg)
			}
			defaultValue := fieldSchema.GetDefaultValue().GetFloatData()
			sd.FloatData.Data, err = fillWithDefaultValueImpl(sd.FloatData.Data, defaultValue, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_DoubleData:
			if len(validData) != numRows {
				msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", field.GetFieldName())
				return merr.WrapErrParameterInvalid(numRows, len(validData), msg)
			}
			defaultValue := fieldSchema.GetDefaultValue().GetDoubleData()
			sd.DoubleData.Data, err = fillWithDefaultValueImpl(sd.DoubleData.Data, defaultValue, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_TimestamptzData:
			// Basic validation: Check if the length of the validity mask matches the number of rows.
			if len(validData) != numRows {
				msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", field.GetFieldName())
				return merr.WrapErrParameterInvalid(numRows, len(validData), msg)
			}

			// Retrieve the default value, which is usually stored as int64 (UTC microseconds).
			defaultValue := fieldSchema.GetDefaultValue().GetTimestamptzData()

			// If the int64 default value is 0 (which might happen if it was not fully persisted
			// or if the underlying storage is being checked), attempt to fall back to the string value.
			if defaultValue == 0 {
				strDefaultValue := fieldSchema.GetDefaultValue().GetStringData()

				// If a non-empty string default value exists, perform conversion.
				if len(strDefaultValue) != 0 {
					// NOTE: The strDefaultValue is guaranteed to be a valid ISO 8601 timestamp string,
					// as it was validated during collection schema creation (by checkAndRewriteTimestampTzDefaultValue).
					//
					// Since the string either contains a UTC offset (e.g., '+08:00') or should be treated
					// as UTC/the collection's primary timezone, the 'common.DefaultTimezone' passed here
					// as the fallback timezone is generally inconsequential (negligible)
					// for the final conversion result in this specific context.
					defaultValue, _ = timestamptz.ValidateAndReturnUnixMicroTz(strDefaultValue, common.DefaultTimezone)
				}
			}
			sd.TimestamptzData.Data, err = fillWithDefaultValueImpl(sd.TimestamptzData.Data, defaultValue, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_StringData:
			if len(validData) != numRows {
				msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", field.GetFieldName())
				return merr.WrapErrParameterInvalid(numRows, len(validData), msg)
			}
			defaultValue := fieldSchema.GetDefaultValue().GetStringData()
			sd.StringData.Data, err = fillWithDefaultValueImpl(sd.StringData.Data, defaultValue, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_ArrayData:
			// Todo: support it
			mlog.Error(context.TODO(), "array type not support default value", mlog.String("fieldSchemaName", field.GetFieldName()))
			return merr.WrapErrParameterInvalid("not set default value", "", "array type not support default value")

		case *schemapb.ScalarField_JsonData:
			if len(validData) != numRows {
				msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", field.GetFieldName())
				return merr.WrapErrParameterInvalid(numRows, len(validData), msg)
			}
			defaultValue := fieldSchema.GetDefaultValue().GetBytesData()
			sd.JsonData.Data, err = fillWithDefaultValueImpl(sd.JsonData.Data, defaultValue, validData)
			if err != nil {
				return err
			}

		case *schemapb.ScalarField_GeometryData:
			if len(validData) != numRows {
				msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", field.GetFieldName())
				return merr.WrapErrParameterInvalid(numRows, len(validData), msg)
			}
			defaultValue := fieldSchema.GetDefaultValue().GetStringData()
			defaultValueWkbBytes, err := common.ConvertWKTToWKB(defaultValue)
			if err != nil {
				mlog.Warn(context.TODO(), "invalid default value for geometry field", mlog.Err(err))
				return merr.WrapErrParameterInvalidMsg("invalid default value for geometry field")
			}
			sd.GeometryData.Data, err = fillWithDefaultValueImpl(sd.GeometryData.Data, defaultValueWkbBytes, validData)
			if err != nil {
				return err
			}

		default:
			return merr.WrapErrParameterInvalidMsg("undefined data type:%s", field.Type.String())
		}

	case *schemapb.FieldData_Vectors:
		mlog.Error(context.TODO(), "vector not support default value", mlog.String("fieldSchemaName", field.GetFieldName()))
		return merr.WrapErrParameterInvalidMsg("vector type not support default value")

	default:
		return merr.WrapErrParameterInvalidMsg("undefined data type:%s", field.Type.String())
	}

	if !typeutil.IsVectorType(field.Type) {
		if fieldSchema.GetNullable() {
			validData := make([]bool, numRows)
			for i := range validData {
				validData[i] = true
			}
			typeutil.SetFieldDataValidData(field, validData)
		} else {
			typeutil.SetFieldDataValidData(field, nil)
		}
	}

	err = nullutil.CheckValidData(typeutil.GetFieldDataValidData(field), fieldSchema, numRows)
	if err != nil {
		return err
	}

	return nil
}

func fillWithNullValueImpl[T any](array []T, validData []bool) ([]T, error) {
	n := GetValidNumber(validData)
	if len(array) != n {
		return nil, merr.WrapErrParameterInvalid(n, len(array), "the length of field is wrong")
	}
	if n == len(validData) {
		return array, nil
	}
	res := make([]T, len(validData))
	srcIdx := 0
	for i, v := range validData {
		if v {
			res[i] = array[srcIdx]
			srcIdx++
		}
	}
	return res, nil
}

func fillWithDefaultValueImpl[T any](array []T, value T, validData []bool) ([]T, error) {
	n := GetValidNumber(validData)
	if len(array) != n {
		return nil, merr.WrapErrParameterInvalid(n, len(array), "the length of field is wrong")
	}
	if n == len(validData) {
		return array, nil
	}
	res := make([]T, len(validData))
	srcIdx := 0
	for i, v := range validData {
		if v {
			res[i] = array[srcIdx]
			srcIdx++
		} else {
			res[i] = value
		}
	}
	return res, nil
}

func GetValidNumber(validData []bool) int {
	res := 0
	for _, v := range validData {
		if v {
			res++
		}
	}
	return res
}

func (v *ValidateUtil) checkFloatVectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	floatArray := field.GetVectors().GetFloatVector().GetData()
	if floatArray == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("float vector field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need float vector", "got nil", msg)
	}

	if v.checkNAN {
		return typeutil.VerifyFloats32(floatArray)
	}

	return nil
}

func (v *ValidateUtil) checkFloat16VectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	float16VecArray := field.GetVectors().GetFloat16Vector()
	if float16VecArray == nil {
		if !fieldSchema.GetNullable() {
			msg := fmt.Sprintf("float16 vector field '%v' is illegal, array type mismatch", field.GetFieldName())
			return merr.WrapErrParameterInvalid("need float16 vector", "got nil", msg)
		}
		return nil
	}
	if v.checkNAN {
		return typeutil.VerifyFloats16(float16VecArray)
	}
	return nil
}

func (v *ValidateUtil) checkBFloat16VectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	bfloat16VecArray := field.GetVectors().GetBfloat16Vector()
	if bfloat16VecArray == nil {
		if !fieldSchema.GetNullable() {
			msg := fmt.Sprintf("bfloat16 vector field '%v' is illegal, array type mismatch", field.GetFieldName())
			return merr.WrapErrParameterInvalid("need bfloat16 vector", "got nil", msg)
		}
		return nil
	}
	if v.checkNAN {
		return typeutil.VerifyBFloats16(bfloat16VecArray)
	}
	return nil
}

func (v *ValidateUtil) checkBinaryVectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	bVecArray := field.GetVectors().GetBinaryVector()
	if bVecArray == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("binary vector field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need binary vector", "got nil", msg)
	}
	return nil
}

func (v *ValidateUtil) checkSparseFloatVectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	if field.GetVectors() == nil || field.GetVectors().GetSparseFloatVector() == nil {
		if !fieldSchema.GetNullable() {
			msg := fmt.Sprintf("sparse float vector field '%v' is illegal, array type mismatch", field.GetFieldName())
			return merr.WrapErrParameterInvalid("need sparse float vector", "got nil", msg)
		}
		return nil
	}
	sparseRows := field.GetVectors().GetSparseFloatVector().GetContents()
	return typeutil.ValidateSparseFloatRows(sparseRows...)
}

func (v *ValidateUtil) checkInt8VectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	int8VecArray := field.GetVectors().GetInt8Vector()
	if int8VecArray == nil {
		if !fieldSchema.GetNullable() {
			msg := fmt.Sprintf("int8 vector field '%v' is illegal, array type mismatch", field.GetFieldName())
			return merr.WrapErrParameterInvalid("need int8 vector", "got nil", msg)
		}
		return nil
	}
	return nil
}

func (v *ValidateUtil) checkVarCharFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	strArr := field.GetScalars().GetStringData().GetData()
	if strArr == nil && fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("varchar field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need string array", "got nil", msg)
	}

	// fieldSchema autoID is true means that field is pk and primaryData is auto generated
	// no need to do max length check
	// ignore the parameter of MaxLength
	// related https://github.com/milvus-io/milvus/issues/25580
	if v.checkMaxLen && !fieldSchema.AutoID {
		maxLength, err := parameterutil.GetMaxLength(fieldSchema)
		if err != nil {
			return err
		}

		if i, ok := verifyLengthPerRow(strArr, maxLength); !ok {
			return merr.WrapErrParameterInvalidMsg("length of varchar field %s exceeds max length, row number: %d, length: %d, max length: %d",
				fieldSchema.GetName(), i, len(strArr[i]), maxLength)
		}
		return nil
	}

	return nil
}

func (v *ValidateUtil) checkTextFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	strArr := field.GetScalars().GetStringData().GetData()
	if strArr == nil && fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("text field '%v' is illegal", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need text array", msg)
	}

	// Text type does not require max_length validation
	return nil
}

func (v *ValidateUtil) checkGeometryFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	geometryArray := field.GetScalars().GetGeometryWktData().GetData()
	validData := typeutil.GetFieldDataValidData(field)
	wkbArray := make([][]byte, len(geometryArray))
	if geometryArray == nil && fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("geometry field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need geometry array", "got nil", msg)
	}
	var err error
	for index, wktdata := range geometryArray {
		// ignore parsed geom, the check is during insert task pre execute,so geo data became wkb
		// fmt.Println(strings.Trim(string(wktdata), "\""))
		wkbArray[index], err = common.ConvertWKTToWKB(wktdata)
		if err != nil {
			mlog.Warn(context.TODO(), "insert invalid Geometry data!! Transform to wkb failed, has errors", mlog.Err(err))
			return merr.WrapErrParameterInvalidMsg("invalid Geometry data, transform to wkb failed: %s", err.Error())
		}
	}
	// replace the field data with wkb data array
	*field = schemapb.FieldData{
		Type:      field.GetType(),
		FieldName: field.GetFieldName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: wkbArray}},
			},
		},
		FieldId:   field.GetFieldId(),
		IsDynamic: field.GetIsDynamic(),
	}
	typeutil.SetFieldDataValidData(field, validData)
	return nil
}

func (v *ValidateUtil) checkJSONFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	jsonArray := field.GetScalars().GetJsonData().GetData()
	if jsonArray == nil && fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("json field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need json array", "got nil", msg)
	}

	if v.checkMaxLen {
		// Resolved once per field rather than once per row: the GetAsX scalar
		// getters go through config.Manager.GetCachedValue, which read-locks the config
		// manager's cache before the map lookup, and this loop runs for every
		// row of every insert that carries a JSON or dynamic field.
		// checkVarCharFieldData likewise resolves its limit before its row loop
		// (from the schema's type params rather than from config).
		maxLength := paramtable.Get().CommonCfg.JSONMaxLength.GetAsInt64()
		for _, s := range jsonArray {
			if int64(len(s)) > maxLength {
				if field.GetIsDynamic() {
					msg := fmt.Sprintf("the length (%d) of dynamic field exceeds max length (%d)", len(s), maxLength)
					return merr.WrapErrParameterInvalid("valid length dynamic field", "length exceeds max length", msg)
				}
				msg := fmt.Sprintf("the length (%d) of json field (%s) exceeds max length (%d)", len(s),
					field.GetFieldName(), maxLength)
				return merr.WrapErrParameterInvalid("valid length json string", "length exceeds max length", msg)
			}
		}
	}
	return nil
}

func (v *ValidateUtil) checkIntegerFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	data := field.GetScalars().GetIntData().GetData()
	if data == nil && fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need int array", "got nil", msg)
	}

	if v.checkOverflow {
		switch fieldSchema.GetDataType() {
		case schemapb.DataType_Int8:
			return verifyOverflowByRange(data, math.MinInt8, math.MaxInt8)
		case schemapb.DataType_Int16:
			return verifyOverflowByRange(data, math.MinInt16, math.MaxInt16)
		}
	}

	return nil
}

func (v *ValidateUtil) checkLongFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	data := field.GetScalars().GetLongData().GetData()
	if data == nil && fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need long int array", "got nil", msg)
	}

	return nil
}

func (v *ValidateUtil) checkFloatFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	data := field.GetScalars().GetFloatData().GetData()
	if data == nil && fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need float32 array", "got nil", msg)
	}

	if v.checkNAN {
		return typeutil.VerifyFloats32(data)
	}

	return nil
}

func (v *ValidateUtil) checkDoubleFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	data := field.GetScalars().GetDoubleData().GetData()
	if data == nil && fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
		msg := fmt.Sprintf("field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need float64(double) array", "got nil", msg)
	}

	if v.checkNAN {
		return typeutil.VerifyFloats64(data)
	}

	return nil
}

func (v *ValidateUtil) checkArrayElement(array *schemapb.ArrayArray, field *schemapb.FieldSchema) error {
	data := array.GetData()
	validateValidity := func(validData []bool, payloadLen, rowIdx int) error {
		if field.GetElementNullable() {
			validElements := GetValidNumber(validData)
			if validElements != payloadLen {
				return merr.WrapErrParameterInvalid(validElements, payloadLen,
					fmt.Sprintf("field %s row %d has %d valid elements, but compact payload has %d elements", field.GetName(), rowIdx, validElements, payloadLen))
			}
			return nil
		}
		if len(validData) > 0 {
			return merr.WrapErrParameterInvalidMsg("field %s is not element nullable but row %d has element valid_data", field.GetName(), rowIdx)
		}
		return nil
	}
	switch field.GetElementType() {
	case schemapb.DataType_Bool:
		for rowIdx, row := range data {
			validData := typeutil.GetArrayElementValidData(row)
			if row.GetData() == nil {
				return merr.WrapErrParameterInvalid("bool array", "nil array", "insert data does not match")
			}
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_BoolData)(nil)) {
				return merr.WrapErrParameterInvalid("bool array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
			if err := validateValidity(validData, len(row.GetBoolData().GetData()), rowIdx); err != nil {
				return err
			}
		}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		for rowIdx, row := range data {
			validData := typeutil.GetArrayElementValidData(row)
			if row.GetData() == nil {
				return merr.WrapErrParameterInvalid("int array", "nil array", "insert data does not match")
			}
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_IntData)(nil)) {
				return merr.WrapErrParameterInvalid("int array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
			values := row.GetIntData().GetData()
			if err := validateValidity(validData, len(values), rowIdx); err != nil {
				return err
			}
			if v.checkOverflow {
				if field.GetElementType() == schemapb.DataType_Int8 {
					if err := verifyOverflowByRange(values, math.MinInt8, math.MaxInt8); err != nil {
						return err
					}
				}
				if field.GetElementType() == schemapb.DataType_Int16 {
					if err := verifyOverflowByRange(values, math.MinInt16, math.MaxInt16); err != nil {
						return err
					}
				}
			}
		}
	case schemapb.DataType_Int64:
		for rowIdx, row := range data {
			validData := typeutil.GetArrayElementValidData(row)
			if row.GetData() == nil {
				return merr.WrapErrParameterInvalid("int64 array", "nil array", "insert data does not match")
			}
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_LongData)(nil)) {
				return merr.WrapErrParameterInvalid("int64 array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
			if err := validateValidity(validData, len(row.GetLongData().GetData()), rowIdx); err != nil {
				return err
			}
		}
	case schemapb.DataType_Float:
		for rowIdx, row := range data {
			validData := typeutil.GetArrayElementValidData(row)
			if row.GetData() == nil {
				return merr.WrapErrParameterInvalid("float array", "nil array", "insert data does not match")
			}
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_FloatData)(nil)) {
				return merr.WrapErrParameterInvalid("float array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
			if err := validateValidity(validData, len(row.GetFloatData().GetData()), rowIdx); err != nil {
				return err
			}
		}
	case schemapb.DataType_Double:
		for rowIdx, row := range data {
			validData := typeutil.GetArrayElementValidData(row)
			if row.GetData() == nil {
				return merr.WrapErrParameterInvalid("double array", "nil array", "insert data does not match")
			}
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_DoubleData)(nil)) {
				return merr.WrapErrParameterInvalid("double array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
			if err := validateValidity(validData, len(row.GetDoubleData().GetData()), rowIdx); err != nil {
				return err
			}
		}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		for rowIdx, row := range data {
			validData := typeutil.GetArrayElementValidData(row)
			if row.GetData() == nil {
				return merr.WrapErrParameterInvalid("string array", "nil array", "insert data does not match")
			}
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_StringData)(nil)) {
				return merr.WrapErrParameterInvalid("string array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
			values := row.GetStringData().GetData()
			if err := validateValidity(validData, len(values), rowIdx); err != nil {
				return err
			}
			if v.checkMaxLen {
				maxLength, err := parameterutil.GetMaxLength(field)
				if err != nil {
					return err
				}
				i, ok := verifyLengthPerRow(values, maxLength)
				if !ok {
					return merr.WrapErrParameterInvalidMsg("length of %s array field \"%s\" exceeds max length, row number: %d, array index: %d, length: %d, max length: %d",
						field.GetDataType().String(), field.GetName(), rowIdx, i, len(values[i]), maxLength,
					)
				}
			}
		}
	default:
		msg := fmt.Sprintf("array element type: %s is not supported", field.GetElementType().String())
		return merr.WrapErrParameterInvalid("valid array element type", "array element type is not supported", msg)
	}
	return nil
}

func arraySchemaElementType(arrayType *schemapb.TypeSchema) schemapb.DataType {
	element := arrayType.GetArrayElement()
	if element.GetArrayElement() != nil {
		return schemapb.DataType_Array
	}
	return element.GetLeafType()
}

func normalizeNestedArrayElementType(
	array *schemapb.ArrayArray,
	expectedType schemapb.DataType,
	fieldName string,
) error {
	elementType := array.GetElementType()
	if elementType != schemapb.DataType_None && elementType != expectedType {
		return merr.WrapErrParameterInvalidMsg(
			"nested array field %s expects %s element type, got %s",
			fieldName,
			expectedType.String(),
			elementType.String(),
		)
	}
	array.ElementType = expectedType
	return nil
}

func (v *ValidateUtil) checkNestedArrayValue(
	row *schemapb.ScalarField,
	arrayType *schemapb.TypeSchema,
	fieldName string,
	level int,
) error {
	if row == nil || row.GetData() == nil {
		return merr.WrapErrParameterInvalidMsg(
			"nested array field %s has an undeclared null value at level %d",
			fieldName,
			level,
		)
	}

	elementSchema := arrayType.GetArrayElement()
	if elementSchema.GetArrayElement() != nil {
		// TODO: handle this when we support element valid_data for nested array
		if len(typeutil.GetArrayElementValidData(row)) > 0 {
			return merr.WrapErrParameterInvalidMsg(
				"nested array field %s does not support element valid_data at level %d",
				fieldName,
				level,
			)
		}
		arrayData := row.GetArrayData()
		if arrayData == nil {
			return merr.WrapErrParameterInvalidMsg(
				"nested array field %s level %d expects Array data",
				fieldName,
				level,
			)
		}
		expectedType := arraySchemaElementType(elementSchema)
		if err := normalizeNestedArrayElementType(arrayData, expectedType, fieldName); err != nil {
			return err
		}
		if v.checkMaxCap {
			maxCapacity, err := parameterutil.GetMaxCapacityFromTypeSchema(arrayType)
			if err != nil {
				return err
			}
			if int64(len(arrayData.GetData())) > maxCapacity {
				return merr.WrapErrParameterInvalidMsg(
					"the length (%d) of nested array field %s at level %d exceeds max capacity (%d)",
					len(arrayData.GetData()),
					fieldName,
					level,
					maxCapacity,
				)
			}
		}
		for index, child := range arrayData.GetData() {
			if err := v.checkNestedArrayValue(
				child,
				elementSchema,
				fieldName,
				level+1,
			); err != nil {
				return merr.Wrapf(err, "nested array element %d", index)
			}
		}
		return nil
	}

	elementType := elementSchema.GetLeafType()
	leafField := &schemapb.FieldSchema{
		Name:        fieldName,
		DataType:    schemapb.DataType_Array,
		ElementType: elementType,
		TypeParams:  elementSchema.GetTypeParams(),
	}
	leafArray := &schemapb.ArrayArray{
		Data:        []*schemapb.ScalarField{row},
		ElementType: elementType,
	}
	if v.checkMaxCap {
		maxCapacity, err := parameterutil.GetMaxCapacityFromTypeSchema(arrayType)
		if err != nil {
			return err
		}
		if err := verifyCapacityPerRow(leafArray, maxCapacity, elementType, false); err != nil {
			return err
		}
	}
	return v.checkArrayElement(leafArray, leafField)
}

func (v *ValidateUtil) checkNestedArrayFieldData(
	data *schemapb.ArrayArray,
	fieldSchema *schemapb.FieldSchema,
) error {
	rootType := fieldSchema.GetTypeSchema()
	if err := normalizeNestedArrayElementType(
		data, arraySchemaElementType(rootType), fieldSchema.GetName(),
	); err != nil {
		return err
	}
	for rowIndex, row := range data.GetData() {
		if err := v.checkNestedArrayValue(row, rootType, fieldSchema.GetName(), 0); err != nil {
			return merr.Wrapf(err, "nested array row %d", rowIndex)
		}
	}
	return nil
}

func (v *ValidateUtil) checkArrayFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	data := field.GetScalars().GetArrayData()
	if data == nil {
		elementTypeStr := fieldSchema.GetElementType().String()
		msg := fmt.Sprintf("array field '%v' is illegal, array type mismatch", field.GetFieldName())
		expectStr := fmt.Sprintf("need %s array", elementTypeStr)
		return merr.WrapErrParameterInvalid(expectStr, "got nil", msg)
	}
	if typeutil.IsNestedArrayTypeSchema(fieldSchema.GetTypeSchema()) {
		return v.checkNestedArrayFieldData(data, fieldSchema)
	}
	if v.checkMaxCap {
		maxCapacity, err := parameterutil.GetMaxCapacity(fieldSchema)
		if err != nil {
			return err
		}
		if err := verifyCapacityPerRow(data, maxCapacity, fieldSchema.GetElementType(), fieldSchema.GetElementNullable()); err != nil {
			return err
		}
	}
	return v.checkArrayElement(data, fieldSchema)
}

func (v *ValidateUtil) checkArrayOfVectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	data := field.GetVectors().GetVectorArray()
	if data == nil {
		elementTypeStr := fieldSchema.GetElementType().String()
		msg := fmt.Sprintf("array of vector field '%v' is illegal, array type mismatch", field.GetFieldName())
		expectStr := fmt.Sprintf("need %s array", elementTypeStr)
		return merr.WrapErrParameterInvalid(expectStr, "got nil", msg)
	}

	dim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return err
	}

	var maxCapacity int64
	if v.checkMaxCap {
		maxCapacity, err = parameterutil.GetMaxCapacity(fieldSchema)
		if err != nil {
			return err
		}
	}

	checkCapacity := func(vectorCount, rowIdx int) error {
		if !v.checkMaxCap || int64(vectorCount) <= maxCapacity {
			return nil
		}
		msg := fmt.Sprintf("the length (%d) of array of vector field %s row %d exceeds max capacity (%d)", vectorCount, field.GetFieldName(), rowIdx, maxCapacity)
		return merr.WrapErrParameterInvalid("valid length array", "array length exceeds max capacity", msg)
	}

	validateVectorCount := func(payloadLength int, elementsPerVector int) (int, error) {
		if elementsPerVector <= 0 {
			return 0, merr.WrapErrParameterInvalidMsg("invalid dim %d for array of vector field %s", dim, field.GetFieldName())
		}
		if payloadLength%elementsPerVector != 0 {
			msg := fmt.Sprintf("array of vector field %s has invalid payload length %d, should be divisible by vector width %d",
				field.GetFieldName(), payloadLength, elementsPerVector)
			return 0, merr.WrapErrParameterInvalid("valid array of vector payload length", "invalid payload length", msg)
		}
		return payloadLength / elementsPerVector, nil
	}

	checkVector := func(vector *schemapb.VectorField) (int, error) {
		switch fieldSchema.GetElementType() {
		case schemapb.DataType_FloatVector:
			floatVector := vector.GetFloatVector()
			if floatVector == nil {
				msg := fmt.Sprintf("array of vector field '%v' is illegal, array type mismatch", field.GetFieldName())
				return 0, merr.WrapErrParameterInvalid("need float vector array", "got nil", msg)
			}
			vectorCount, err := validateVectorCount(len(floatVector.GetData()), int(dim))
			if err != nil {
				return 0, err
			}
			if v.checkNAN {
				if err := typeutil.VerifyFloats32(floatVector.GetData()); err != nil {
					return 0, err
				}
			}
			return vectorCount, nil
		case schemapb.DataType_BinaryVector:
			binaryVector, ok := vector.GetData().(*schemapb.VectorField_BinaryVector)
			if !ok || binaryVector == nil {
				msg := fmt.Sprintf("array of vector field '%v' is illegal, array type mismatch", field.GetFieldName())
				return 0, merr.WrapErrParameterInvalid("need binary vector array", "got nil", msg)
			}
			vectorCount, err := validateVectorCount(len(binaryVector.BinaryVector), int((dim+7)/8))
			if err != nil {
				return 0, err
			}
			return vectorCount, nil
		case schemapb.DataType_Float16Vector:
			float16Vector, ok := vector.GetData().(*schemapb.VectorField_Float16Vector)
			if !ok || float16Vector == nil {
				msg := fmt.Sprintf("array of vector field '%v' is illegal, array type mismatch", field.GetFieldName())
				return 0, merr.WrapErrParameterInvalid("need float16 vector array", "got nil", msg)
			}
			vectorCount, err := validateVectorCount(len(float16Vector.Float16Vector), int(dim)*2)
			if err != nil {
				return 0, err
			}
			if v.checkNAN {
				if err := typeutil.VerifyFloats16(float16Vector.Float16Vector); err != nil {
					return 0, err
				}
			}
			return vectorCount, nil
		case schemapb.DataType_BFloat16Vector:
			bfloat16Vector, ok := vector.GetData().(*schemapb.VectorField_Bfloat16Vector)
			if !ok || bfloat16Vector == nil {
				msg := fmt.Sprintf("array of vector field '%v' is illegal, array type mismatch", field.GetFieldName())
				return 0, merr.WrapErrParameterInvalid("need bfloat16 vector array", "got nil", msg)
			}
			vectorCount, err := validateVectorCount(len(bfloat16Vector.Bfloat16Vector), int(dim)*2)
			if err != nil {
				return 0, err
			}
			if v.checkNAN {
				if err := typeutil.VerifyBFloats16(bfloat16Vector.Bfloat16Vector); err != nil {
					return 0, err
				}
			}
			return vectorCount, nil
		case schemapb.DataType_Int8Vector:
			int8Vector, ok := vector.GetData().(*schemapb.VectorField_Int8Vector)
			if !ok || int8Vector == nil {
				msg := fmt.Sprintf("array of vector field '%v' is illegal, array type mismatch", field.GetFieldName())
				return 0, merr.WrapErrParameterInvalid("need int8 vector array", "got nil", msg)
			}
			vectorCount, err := validateVectorCount(len(int8Vector.Int8Vector), int(dim))
			if err != nil {
				return 0, err
			}
			return vectorCount, nil
		default:
			msg := fmt.Sprintf("unsupported element type for ArrayOfVector: %v", fieldSchema.GetElementType())
			return 0, merr.WrapErrParameterInvalid("supported vector type", fieldSchema.GetElementType().String(), msg)
		}
	}

	for rowIdx, vector := range data.GetData() {
		vectorCount, err := checkVector(vector)
		if err != nil {
			return err
		}
		validData := typeutil.GetVectorArrayElementValidData(vector)
		if fieldSchema.GetElementNullable() {
			requireValidData := vectorCount > 0 || len(validData) > 0
			if err := funcutil.ValidateNullableVectorCompactRow(
				field.GetFieldName(),
				rowIdx,
				validData,
				uint64(vectorCount),
				uint64(len(validData)),
				requireValidData,
			); err != nil {
				return err
			}
			if err := checkCapacity(len(validData), rowIdx); err != nil {
				return err
			}
		} else {
			if len(validData) > 0 {
				return merr.WrapErrParameterInvalidMsg("array of vector field %s is not element nullable but row %d has element valid_data", field.GetFieldName(), rowIdx)
			}
			if err := checkCapacity(vectorCount, rowIdx); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkTimestamptzFieldData validates the input string data for a Timestamptz field,
// converts it into UTC Unix Microseconds (int64), and replaces the data in place.
func (v *ValidateUtil) checkTimestamptzFieldData(field *schemapb.FieldData, timezone string) error {
	// 1. Structural Check: Data must be present and must be a string array
	scalarField := field.GetScalars()
	if scalarField == nil || scalarField.GetStringData() == nil {
		mlog.Warn(context.TODO(), "timestamptz field data is not string array", mlog.String("fieldName", field.GetFieldName()))
		return merr.WrapErrParameterInvalidMsg("timestamptz field data must be a string array")
	}

	stringData := scalarField.GetStringData().GetData()
	utcTimestamps := make([]int64, len(stringData))

	// 2. Validation and Conversion Loop
	for i, isoStr := range stringData {
		// Use the centralized parser (timestamptz.ParseTimeTz) for validation and parsing.
		t, err := timestamptz.ParseTimeTz(isoStr, timezone)
		if err != nil {
			mlog.Info(context.TODO(), "cannot parse timestamptz string", mlog.String("timestamp_string", isoStr), mlog.String("timezone", timezone), mlog.Err(err))
			// Use the recommended refined error message structure
			const invalidMsg = "invalid timezone name; must be a valid IANA Time Zone ID (e.g., 'Asia/Shanghai' or 'UTC')"
			return merr.WrapErrParameterInvalidMsg("got invalid timestamptz string '%s': %s", isoStr, invalidMsg)
		}

		// Convert the time object to Unix Microseconds (int64)
		utcTimestamps[i] = t.UnixMicro()
	}

	// 3. In-Place Data Replacement: Replace StringData with converted TimestamptzData (int64)
	field.GetScalars().Data = &schemapb.ScalarField_TimestamptzData{
		TimestamptzData: &schemapb.TimestamptzArray{
			Data: utcTimestamps,
		},
	}
	return nil
}

func verifyLengthPerRow[E interface{ ~string | ~[]byte }](strArr []E, maxLength int64) (int, bool) {
	for i, s := range strArr {
		if int64(len(s)) > maxLength {
			return i, false
		}
	}

	return 0, true
}

func verifyCapacityPerRow(arrayArray *schemapb.ArrayArray, maxCapacity int64, elementType schemapb.DataType, elementNullable bool) error {
	for i, array := range arrayArray.GetData() {
		arrayLen := len(typeutil.GetArrayElementValidData(array))
		if !elementNullable {
			switch elementType {
			case schemapb.DataType_Bool:
				arrayLen = len(array.GetBoolData().GetData())
			case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
				arrayLen = len(array.GetIntData().GetData())
			case schemapb.DataType_Int64:
				arrayLen = len(array.GetLongData().GetData())
			case schemapb.DataType_String, schemapb.DataType_VarChar:
				arrayLen = len(array.GetStringData().GetData())
			case schemapb.DataType_Float:
				arrayLen = len(array.GetFloatData().GetData())
			case schemapb.DataType_Double:
				arrayLen = len(array.GetDoubleData().GetData())
			default:
				msg := fmt.Sprintf("array element type: %s is not supported", elementType.String())
				return merr.WrapErrParameterInvalid("valid array element type", "array element type is not supported", msg)
			}
		}

		if int64(arrayLen) <= maxCapacity {
			continue
		}
		msg := fmt.Sprintf("the length (%d) of %dth array exceeds max capacity (%d)", arrayLen, i, maxCapacity)
		return merr.WrapErrParameterInvalid("valid length array", "array length exceeds max capacity", msg)
	}

	return nil
}

func verifyOverflowByRange(arr []int32, lb int64, ub int64) error {
	for idx, e := range arr {
		if lb > int64(e) || ub < int64(e) {
			msg := fmt.Sprintf("the %dth element (%d) out of range: [%d, %d]", idx, e, lb, ub)
			return merr.WrapErrParameterInvalid("integer doesn't overflow", "out of range", msg)
		}
	}
	return nil
}

func NewValidateUtil(opts ...ValidateOption) *ValidateUtil {
	v := &ValidateUtil{
		checkNAN:      true,
		checkMaxLen:   false,
		checkOverflow: false,
	}

	v.apply(opts...)

	return v
}

func ValidateAutoIndexMmapConfig(isVectorField bool, indexParams map[string]string) error {
	return common.ValidateAutoIndexMmapConfig(paramtable.Get().AutoIndexConfig.Enable.GetAsBool(), isVectorField, indexParams)
}
