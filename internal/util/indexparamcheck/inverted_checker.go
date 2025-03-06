package indexparamcheck

import (
	"fmt"
	"strconv"

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

var validJSONCastTypes = []int{int(schemapb.DataType_Bool), int(schemapb.DataType_Int8), int(schemapb.DataType_Int16), int(schemapb.DataType_Int32), int(schemapb.DataType_Int64), int(schemapb.DataType_Float), int(schemapb.DataType_Double), int(schemapb.DataType_String), int(schemapb.DataType_VarChar)}

func (c *INVERTEDChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	// check json index params
	isJSONIndex := typeutil.IsJSONType(dataType)
	if isJSONIndex {
		castType, exist := params[common.JSONCastTypeKey]
		if !exist {
			return merr.WrapErrParameterMissing(common.JSONCastTypeKey, "json index must specify cast type")
		}
		castTypeInt, err := strconv.Atoi(castType)
		if err != nil {
			return merr.WrapErrParameterInvalid(common.JSONCastTypeKey, "json_cast_type must be DataType")
		}
		if !lo.Contains(validJSONCastTypes, castTypeInt) {
			return merr.WrapErrParameterInvalidMsg("json_cast_type %v is not supported", castType)
		}
	}
	return c.scalarIndexChecker.CheckTrain(dataType, params)
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
