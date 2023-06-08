package proxy

import (
	"fmt"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/parameterutil.go"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

type validateUtil struct {
	checkNAN      bool
	checkMaxLen   bool
	checkOverflow bool
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
			log.Error("vectors not support default value", zap.String("fieldSchemaName", field.GetFieldName()))
			return merr.WrapErrParameterInvalid("not set default value", "", "json type not support default value")

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

	if v.checkMaxLen {
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
		msg := fmt.Sprintf("varchar field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need string array", "got nil", msg)
	}

	if v.checkMaxLen {
		return verifyLengthPerRow(jsonArray, paramtable.Get().CommonCfg.JSONMaxLength.GetAsInt64())
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

func verifyLengthPerRow[E interface{ ~string | ~[]byte }](strArr []E, maxLength int64) error {
	for i, s := range strArr {
		if int64(len(s)) > maxLength {
			msg := fmt.Sprintf("the length (%d) of %dth string exceeds max length (%d)", len(s), i, maxLength)
			return merr.WrapErrParameterInvalid("valid length string", "string length exceeds max length", msg)
		}
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
