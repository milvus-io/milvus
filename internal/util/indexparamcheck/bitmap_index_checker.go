package indexparamcheck

import (
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type BITMAPChecker struct {
	scalarIndexChecker
}

var validBITMAPJSONCastTypes = []string{"BOOL", "VARCHAR"}

func (c *BITMAPChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	if typeutil.IsJSONType(dataType) {
		castType, exist := params[common.JSONCastTypeKey]
		if !exist {
			return merr.WrapErrParameterMissing(common.JSONCastTypeKey, "json index must specify cast type")
		}
		if !lo.Contains(validBITMAPJSONCastTypes, castType) {
			return merr.WrapErrParameterInvalidMsg("json_cast_type %v is not supported for BITMAP index", castType)
		}
		if _, exist := params[common.JSONPathKey]; !exist {
			return merr.WrapErrParameterMissing(common.JSONPathKey, "json index must specify json path")
		}
	}
	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func (c *BITMAPChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if field.IsPrimaryKey {
		return errors.New("create bitmap index on primary key not supported")
	}
	mainType := field.GetDataType()
	elemType := field.GetElementType()
	if typeutil.IsJSONType(mainType) {
		return nil
	}
	if !typeutil.IsBoolType(mainType) && !typeutil.IsIntegerType(mainType) &&
		!typeutil.IsStringType(mainType) && !typeutil.IsArrayType(mainType) {
		return errors.New("bitmap index are only supported on bool, int, string and array field")
	}
	if typeutil.IsArrayType(mainType) {
		if !typeutil.IsBoolType(elemType) && !typeutil.IsIntegerType(elemType) &&
			!typeutil.IsStringType(elemType) {
			return errors.New("bitmap index are only supported on bool, int, string for array field")
		}
	}
	return nil
}

func newBITMAPChecker() *BITMAPChecker {
	return &BITMAPChecker{}
}
