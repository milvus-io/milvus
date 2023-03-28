package proxy

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/util/merr"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type validateUtil struct {
	checkNAN    bool
	checkMaxLen bool
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
		default:

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
	if strArr == nil {
		msg := fmt.Sprintf("varchar field '%v' is illegal, array type mismatch", field.GetFieldName())
		return merr.WrapErrParameterInvalid("need string array", "got nil", msg)
	}

	if v.checkMaxLen {
		maxLength, err := GetMaxLength(fieldSchema)
		if err != nil {
			return err
		}
		return verifyLengthPerRow(strArr, maxLength)
	}

	return nil
}

func verifyLengthPerRow(strArr []string, maxLength int64) error {
	for i, s := range strArr {
		if int64(len(s)) > maxLength {
			msg := fmt.Sprintf("the length (%d) of %dth string exceeds max length (%d)", len(s), i, maxLength)
			return merr.WrapErrParameterInvalid("valid length string", "string length exceeds max length", msg)
		}
	}

	return nil
}

func newValidateUtil(opts ...validateOption) *validateUtil {
	v := &validateUtil{
		checkNAN:    true,
		checkMaxLen: false,
	}

	v.apply(opts...)

	return v
}
