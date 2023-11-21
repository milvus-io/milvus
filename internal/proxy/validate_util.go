package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/util/parameterutil.go"
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
		case schemapb.DataType_Int8, schemapb.DataType_Int16:
			if err := v.checkIntegerFieldData(field, fieldSchema); err != nil {
				return err
			}
		case schemapb.DataType_JSON:
			if err := v.checkJSONFieldData(field, fieldSchema); err != nil {
				return err
			}
		default:
		}
	}

	if err := v.checkAligned(data, helper, numRows); err != nil {
		return err
	}

	return nil
}

func (v *validateUtil) checkAligned(data []*schemapb.FieldData, schema *typeutil.SchemaHelper, numRows uint64) error {
	errNumRowsMismatch := func(fieldName string, fieldNumRows, passedNumRows uint64) error {
		msg := fmt.Sprintf("the num_rows (%d) of field (%s) is not equal to passed num_rows (%d)", fieldNumRows, fieldName, passedNumRows)
		return errors.New(msg)
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

func (v *validateUtil) checkFloatVectorFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	floatArray := field.GetVectors().GetFloatVector().GetData()
	if floatArray == nil {
		msg := fmt.Sprintf("float vector field '%v' is illegal, array type mismatch", field.GetFieldName())
		return errors.New(msg)
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
	if strArr == nil {
		msg := fmt.Sprintf("varchar field '%v' is illegal, array type mismatch", field.GetFieldName())
		return errors.New(msg)
	}

	if v.checkMaxLen {
		maxLength, err := parameterutil.GetMaxLength(fieldSchema)
		if err != nil {
			return err
		}
		return verifyStringsLength(strArr, maxLength)
	}

	return nil
}

func (v *validateUtil) checkIntegerFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	if !v.checkOverflow {
		return nil
	}

	data := field.GetScalars().GetIntData().GetData()
	if data == nil {
		msg := fmt.Sprintf("field '%v' is illegal, array type mismatch", field.GetFieldName())
		return errors.New(msg)
	}

	switch fieldSchema.GetDataType() {
	case schemapb.DataType_Int8:
		return verifyOverflowByRange(data, math.MinInt8, math.MaxInt8)
	case schemapb.DataType_Int16:
		return verifyOverflowByRange(data, math.MinInt16, math.MaxInt16)
	}

	return nil
}

func (v *validateUtil) checkJSONFieldData(field *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	jsonArray := field.GetScalars().GetJsonData().GetData()
	if jsonArray == nil {
		msg := fmt.Sprintf("varchar field '%v' is illegal, array type mismatch", field.GetFieldName())
		return errors.New(msg)
	}

	if v.checkMaxLen {
		return verifyBytesArrayLength(jsonArray, int64(Params.CommonCfg.JSONMaxLength))
	}

	var jsonMap map[string]interface{}
	for _, data := range jsonArray {
		err := json.Unmarshal(data, &jsonMap)
		if err != nil {
			log.Warn("insert invalid JSON data",
				zap.ByteString("data", data),
				zap.Error(err),
			)
			return err
		}
	}

	return nil
}

func verifyStringsLength(strArray []string, maxLength int64) error {
	for i, s := range strArray {
		if int64(len(s)) > maxLength {
			msg := fmt.Sprintf("the length (%d) of %dth string exceeds max length (%d)", len(s), i, maxLength)
			return errors.New(msg)
		}
	}

	return nil
}

func verifyBytesArrayLength(bytesArray [][]byte, maxLength int64) error {
	for i, s := range bytesArray {
		if int64(len(s)) > maxLength {
			msg := fmt.Sprintf("the length (%d) of %dth bytes exceeds max length (%d)", len(s), i, maxLength)
			return errors.New(msg)
		}
	}

	return nil
}

func verifyOverflowByRange(arr []int32, lb int64, ub int64) error {
	for idx, e := range arr {
		if lb > int64(e) || ub < int64(e) {
			msg := fmt.Sprintf("the %dth element (%d) out of range: [%d, %d]", idx, e, lb, ub)
			return errors.New(msg)
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
