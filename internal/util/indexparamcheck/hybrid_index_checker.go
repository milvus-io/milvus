package indexparamcheck

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type HYBRIDChecker struct {
	scalarIndexChecker
}

var validHYBRIDJSONCastTypes = []string{"BOOL", "DOUBLE", "VARCHAR"}

func (c *HYBRIDChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	if typeutil.IsJSONType(dataType) {
		castType, exist := params[common.JSONCastTypeKey]
		if !exist {
			return merr.WrapErrParameterMissing(common.JSONCastTypeKey, "json index must specify cast type")
		}
		if !lo.Contains(validHYBRIDJSONCastTypes, castType) {
			return merr.WrapErrParameterInvalidMsg("json_cast_type %v is not supported for HYBRID index", castType)
		}
		if _, exist := params[common.JSONPathKey]; !exist {
			return merr.WrapErrParameterMissing(common.JSONPathKey, "json index must specify json path")
		}
		// For JSON, bitmap_cardinality_limit is optional — validate only if present
		if _, exist := params[common.BitmapCardinalityLimitKey]; exist {
			if !CheckIntByRange(params, common.BitmapCardinalityLimitKey, 1, MaxBitmapCardinalityLimit) {
				return fmt.Errorf("failed to check bitmap cardinality limit, should be larger than 0 and smaller than %d",
					MaxBitmapCardinalityLimit)
			}
		}
		return nil
	}
	if !CheckIntByRange(params, common.BitmapCardinalityLimitKey, 1, MaxBitmapCardinalityLimit) {
		return fmt.Errorf("failed to check bitmap cardinality limit, should be larger than 0 and smaller than %d",
			MaxBitmapCardinalityLimit)
	}
	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func (c *HYBRIDChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	mainType := field.GetDataType()
	elemType := field.GetElementType()
	if typeutil.IsJSONType(mainType) {
		return nil
	}
	if !typeutil.IsBoolType(mainType) && !typeutil.IsIntegerType(mainType) &&
		!typeutil.IsStringType(mainType) && !typeutil.IsArrayType(mainType) &&
		!typeutil.IsFloatingType(mainType) {
		return errors.New("hybrid index are only supported on bool, int, float, string and array field")
	}
	if typeutil.IsArrayType(mainType) {
		if !typeutil.IsBoolType(elemType) && !typeutil.IsIntegerType(elemType) &&
			!typeutil.IsStringType(elemType) && !typeutil.IsFloatingType(elemType) {
			return errors.New("hybrid index are only supported on bool, int, float, string for array field")
		}
	}
	return nil
}

func newHYBRIDChecker() *HYBRIDChecker {
	return &HYBRIDChecker{}
}

func IsHYBRIDChecker(checker interface{}) bool {
	_, ok := checker.(*HYBRIDChecker)
	return ok
}
