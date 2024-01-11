package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type validateUtil struct {
	checkNAN      bool
	checkMaxLen   bool
	checkOverflow bool
	checkMaxCap   bool
}

type validateOption func(*validateUtil)

func withNANCheck() validateOption {
	return func(v *validateUtil) {
		v.checkNAN = true
	}
}

func withMaxLenCheck() validateOption {
	return func(v *validateUtil) {
		v.checkMaxLen = true
	}
}

func withOverflowCheck() validateOption {
	return func(v *validateUtil) {
		v.checkOverflow = true
	}
}

func withMaxCapCheck() validateOption {
	return func(v *validateUtil) {
		v.checkMaxCap = true
	}
}

func (v *validateUtil) apply(opts ...validateOption) {
	for _, opt := range opts {
		opt(v)
	}
}

func (v *validateUtil) Validate(data []*schemapb.FieldData, schema *schemapb.CollectionSchema, numRows uint64) error {
	helper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return err
	}

	for _, field := range data {
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
		case schemapb.DataType_VarChar:
			if err := v.checkVarCharFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_JSON:
			if err := v.checkJSONFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Int8, schemapb.DataType_Int16:
			if err := v.checkIntegerFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_Array:
			if err := v.checkArrayFieldData(field, fieldSchema); err != nil {
				return err
			}

		default:
		}
	}

	err = v.fillWithDefaultValue(data, helper, numRows)
	if err != nil {
		return err
	}

	if err := v.checkAligned(data, helper, numRows); err != nil {
		return err
	}

	return nil
}

func (v *validateUtil) checkAligned(data []*schemapb.FieldData, schema *typeutil.SchemaHelper, numRows uint64) error {
	errNumRowsMismatch := func(fieldName string, fieldNumRows, passedNumRows uint64) error {
		msg := fmt.Sprintf("the num_rows (%d) of field (%s) is not equal to passed num_rows (%d)", fieldNumRows, fieldName, passedNumRows)
		return merr.WrapErrParameterInvalid(passedNumRows, numRows, msg)
	}

	for _, field := range data {
		switch field.GetType() {
		case schemapb.DataType_FloatVector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}

			n, err := funcutil.GetNumRowsOfFloatVectorField(field.GetVectors().GetFloatVector().GetData(), dim)
			if err != nil {
				return err
			}

			if n != numRows {
				return errNumRowsMismatch(field.GetFieldName(), n, numRows)
			}

		case schemapb.DataType_BinaryVector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}

			n, err := funcutil.GetNumRowsOfBinaryVectorField(field.GetVectors().GetBinaryVector(), dim)
			if err != nil {
				return err
			}

			if n != numRows {
				return errNumRowsMismatch(field.GetFieldName(), n, numRows)
			}

		case schemapb.DataType_Float16Vector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}

			n, err := funcutil.GetNumRowsOfFloat16VectorField(field.GetVectors().GetFloat16Vector(), dim)
			if err != nil {
				return err
			}

			if n != numRows {
				return errNumRowsMismatch(field.GetFieldName(), n, numRows)
			}

		case schemapb.DataType_BFloat16Vector:
			f, err := schema.GetFieldFromName(field.GetFieldName())
			if err != nil {
				return err
			}

			dim, err := typeutil.GetDim(f)
			if err != nil {
				return err
			}

			n, err := funcutil.GetNumRowsOfBFloat16VectorField(field.GetVectors().GetBfloat16Vector(), dim)
			if err != nil {
				return err
			}

			if n != numRows {
				return errNumRowsMismatch(field.GetFieldName(), n, numRows)
			}
		default:
			// error won't happen here.
			n, err := funcutil.GetNumRowOfFieldData(field)
			if err != nil {
				return err
			}

			if n != numRows {
				return errNumRowsMismatch(field.GetFieldName(), n, numRows)
			}
		}
	}

	return nil
}

func (v *validateUtil) fillWithDefaultValue(data []*schemapb.FieldData, schema *typeutil.SchemaHelper, numRows uint64) error {
	for _, field := range data {
		fieldSchema, err := schema.GetFieldFromName(field.GetFieldName())
		if err != nil {
			return err
		}

		// if default value is not set, continue
		// compatible with 2.2.x
		if fieldSchema.GetDefaultValue() == nil {
			continue
		}

		switch field.Field.(type) {
		case *schemapb.FieldData_Scalars:
			switch sd := field.GetScalars().GetData().(type) {
			case *schemapb.ScalarField_BoolData:
				if len(sd.BoolData.Data) == 0 {
					defaultValue := fieldSchema.GetDefaultValue().GetBoolData()
					sd.BoolData.Data = memsetLoop(defaultValue, int(numRows))
				}

			case *schemapb.ScalarField_IntData:
				if len(sd.IntData.Data) == 0 {
					defaultValue := fieldSchema.GetDefaultValue().GetIntData()
					sd.IntData.Data = memsetLoop(defaultValue, int(numRows))
				}

			case *schemapb.ScalarField_LongData:
				if len(sd.LongData.Data) == 0 {
					defaultValue := fieldSchema.GetDefaultValue().GetLongData()
					sd.LongData.Data = memsetLoop(defaultValue, int(numRows))
				}

			case *schemapb.ScalarField_FloatData:
				if len(sd.FloatData.Data) == 0 {
					defaultValue := fieldSchema.GetDefaultValue().GetFloatData()
					sd.FloatData.Data = memsetLoop(defaultValue, int(numRows))
				}

			case *schemapb.ScalarField_DoubleData:
				if len(sd.DoubleData.Data) == 0 {
					defaultValue := fieldSchema.GetDefaultValue().GetDoubleData()
					sd.DoubleData.Data = memsetLoop(defaultValue, int(numRows))
				}

			case *schemapb.ScalarField_StringData:
				if len(sd.StringData.Data) == 0 {
					defaultValue := fieldSchema.GetDefaultValue().GetStringData()
					sd.StringData.Data = memsetLoop(defaultValue, int(numRows))
				}

			case *schemapb.ScalarField_ArrayData:
				log.Error("array type not support default value", zap.String("fieldSchemaName", field.GetFieldName()))
				return merr.WrapErrParameterInvalid("not set default value", "", "array type not support default value")

			case *schemapb.ScalarField_JsonData:
				log.Error("json type not support default value", zap.String("fieldSchemaName", field.GetFieldName()))
				return merr.WrapErrParameterInvalid("not set default value", "", "json type not support default value")

			default:
				panic("undefined data type " + field.Type.String())
			}

		case *schemapb.FieldData_Vectors:
			log.Error("vector not support default value", zap.String("fieldSchemaName", field.GetFieldName()))
			return merr.WrapErrParameterInvalid("not set default value", "", "vector type not support default value")

		default:
			panic("undefined data type " + field.Type.String())
		}
	}

	return nil
}

func (v *validateUtil) checkFloatVectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	floatArray := field.GetVectors().GetFloatVector().GetData()
	if floatArray == nil {
		msg := fmt.Sprintf("float vector field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need float vector", "got nil", msg)
	}

	if v.checkNAN {
		return typeutil.VerifyFloats32(floatArray)
	}

	return nil
}

func (v *validateUtil) checkFloat16VectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	// TODO
	return nil
}

func (v *validateUtil) checkBFloat16VectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	// TODO
	return nil
}

func (v *validateUtil) checkBinaryVectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	// TODO
	return nil
}

func (v *validateUtil) checkVarCharFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	strArr := field.GetScalars().GetStringData().GetData()
	if strArr == nil && fieldSchema.GetDefaultValue() == nil {
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
		return verifyLengthPerRow(strArr, maxLength)
	}

	return nil
}

func (v *validateUtil) checkJSONFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	jsonArray := field.GetScalars().GetJsonData().GetData()
	if jsonArray == nil {
		msg := fmt.Sprintf("json field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need json array", "got nil", msg)
	}

	if v.checkMaxLen {
		for _, s := range jsonArray {
			if int64(len(s)) > paramtable.Get().CommonCfg.JSONMaxLength.GetAsInt64() {
				if field.GetIsDynamic() {
					msg := fmt.Sprintf("the length (%d) of dynamic field exceeds max length (%d)", len(s),
						paramtable.Get().CommonCfg.JSONMaxLength.GetAsInt64())
					return merr.WrapErrParameterInvalid("valid length dynamic field", "length exceeds max length", msg)
				}
				msg := fmt.Sprintf("the length (%d) of json field (%s) exceeds max length (%d)", len(s),
					field.GetFieldName(), paramtable.Get().CommonCfg.JSONMaxLength.GetAsInt64())
				return merr.WrapErrParameterInvalid("valid length json string", "length exceeds max length", msg)
			}
		}
	}

	var jsonMap map[string]interface{}
	for _, data := range jsonArray {
		err := json.Unmarshal(data, &jsonMap)
		if err != nil {
			log.Warn("insert invalid JSON data",
				zap.ByteString("data", data),
				zap.Error(err),
			)
			return merr.WrapErrIoFailedReason(err.Error())
		}
	}

	return nil
}

func (v *validateUtil) checkIntegerFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	if !v.checkOverflow {
		return nil
	}

	data := field.GetScalars().GetIntData().GetData()
	if data == nil && fieldSchema.GetDefaultValue() == nil {
		msg := fmt.Sprintf("field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need int array", "got nil", msg)
	}

	switch fieldSchema.GetDataType() {
	case schemapb.DataType_Int8:
		return verifyOverflowByRange(data, math.MinInt8, math.MaxInt8)
	case schemapb.DataType_Int16:
		return verifyOverflowByRange(data, math.MinInt16, math.MaxInt16)
	}

	return nil
}

func (v *validateUtil) checkArrayElement(array *schemapb.ArrayArray, field *schemapb.FieldSchema) error {
	switch field.GetElementType() {
	case schemapb.DataType_Bool:
		for _, row := range array.GetData() {
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_BoolData)(nil)) {
				return merr.WrapErrParameterInvalid("bool array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
		}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		for _, row := range array.GetData() {
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_IntData)(nil)) {
				return merr.WrapErrParameterInvalid("int array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
			if v.checkOverflow {
				if field.GetElementType() == schemapb.DataType_Int8 {
					if err := verifyOverflowByRange(row.GetIntData().GetData(), math.MinInt8, math.MaxInt8); err != nil {
						return err
					}
				}
				if field.GetElementType() == schemapb.DataType_Int16 {
					if err := verifyOverflowByRange(row.GetIntData().GetData(), math.MinInt16, math.MaxInt16); err != nil {
						return err
					}
				}
			}
		}
	case schemapb.DataType_Int64:
		for _, row := range array.GetData() {
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_LongData)(nil)) {
				return merr.WrapErrParameterInvalid("int64 array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
		}
	case schemapb.DataType_Float:
		for _, row := range array.GetData() {
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_FloatData)(nil)) {
				return merr.WrapErrParameterInvalid("float array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
		}
	case schemapb.DataType_Double:
		for _, row := range array.GetData() {
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_DoubleData)(nil)) {
				return merr.WrapErrParameterInvalid("double array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
		}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		for _, row := range array.GetData() {
			actualType := reflect.TypeOf(row.GetData())
			if actualType != reflect.TypeOf((*schemapb.ScalarField_StringData)(nil)) {
				return merr.WrapErrParameterInvalid("string array",
					fmt.Sprintf("%s array", actualType.String()), "insert data does not match")
			}
		}
	}
	return nil
}

func (v *validateUtil) checkArrayFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	data := field.GetScalars().GetArrayData()
	if data == nil {
		elementTypeStr := fieldSchema.GetElementType().String()
		msg := fmt.Sprintf("array field '%v' is illegal, array type mismatch", field.GetFieldName())
		expectStr := fmt.Sprintf("need %s array", elementTypeStr)
		return merr.WrapErrParameterInvalid(expectStr, "got nil", msg)
	}
	if v.checkMaxCap {
		maxCapacity, err := parameterutil.GetMaxCapacity(fieldSchema)
		if err != nil {
			return err
		}
		if err := verifyCapacityPerRow(data.GetData(), maxCapacity, fieldSchema.GetElementType()); err != nil {
			return err
		}
	}
	if typeutil.IsStringType(data.GetElementType()) && v.checkMaxLen {
		maxLength, err := parameterutil.GetMaxLength(fieldSchema)
		if err != nil {
			return err
		}
		for _, row := range data.GetData() {
			if err := verifyLengthPerRow(row.GetStringData().GetData(), maxLength); err != nil {
				return err
			}
		}
	}
	return v.checkArrayElement(data, fieldSchema)
}

func verifyLengthPerRow[E interface{ ~string | ~[]byte }](strArr []E, maxLength int64) error {
	for i, s := range strArr {
		if int64(len(s)) > maxLength {
			msg := fmt.Sprintf("the length (%d) of %dth string exceeds max length (%d)", len(s), i, maxLength)
			return merr.WrapErrParameterInvalid("valid length string", "string length exceeds max length", msg)
		}
	}

	return nil
}

func verifyCapacityPerRow(arrayArray []*schemapb.ScalarField, maxCapacity int64, elementType schemapb.DataType) error {
	for i, array := range arrayArray {
		arrayLen := 0
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

func newValidateUtil(opts ...validateOption) *validateUtil {
	v := &validateUtil{
		checkNAN:      true,
		checkMaxLen:   false,
		checkOverflow: false,
	}

	v.apply(opts...)

	return v
}
