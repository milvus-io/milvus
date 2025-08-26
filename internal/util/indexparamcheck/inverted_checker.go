package indexparamcheck

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// INVERTEDChecker checks if a INVERTED index can be built.
type INVERTEDChecker struct {
	scalarIndexChecker
}

var validJSONCastTypes = []string{"BOOL", "DOUBLE", "VARCHAR", "ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR", "JSON"}

var validJSONCastFunctions = []string{"STRING_TO_DOUBLE"}

func (c *INVERTEDChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	// check json index params
	isJSONIndex := typeutil.IsJSONType(dataType)
	if isJSONIndex {
		castType, exist := params[common.JSONCastTypeKey]
		if !exist {
			return merr.WrapErrParameterMissing(common.JSONCastTypeKey, "json index must specify cast type")
		}

		if !lo.Contains(validJSONCastTypes, castType) {
			return merr.WrapErrParameterInvalidMsg("json_cast_type %v is not supported", castType)
		}
		castFunction, exist := params[common.JSONCastFunctionKey]
		if exist {
			switch castFunction {
			case "STRING_TO_DOUBLE":
				if castType != "DOUBLE" {
					return merr.WrapErrParameterInvalidMsg("json_cast_function %v is not supported for json_cast_type %v", castFunction, castType)
				}
			default:
				return merr.WrapErrParameterInvalidMsg("json_cast_function %v is not supported", castFunction)
			}
		}
	}
	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func (c *INVERTEDChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	dType := field.GetDataType()
	if !typeutil.IsBoolType(dType) && !typeutil.IsArithmetic(dType) && !typeutil.IsStringType(dType) &&
		!typeutil.IsArrayType(dType) && !typeutil.IsJSONType(dType) {
		return fmt.Errorf("INVERTED are not supported on %s field", dType.String())
	}
	return nil
}

func newINVERTEDChecker() *INVERTEDChecker {
	return &INVERTEDChecker{}
}
