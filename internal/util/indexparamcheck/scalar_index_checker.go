package indexparamcheck

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type scalarIndexChecker struct {
	baseChecker
}

func (c scalarIndexChecker) CheckTrain(dataType schemapb.DataType, _ schemapb.DataType, params map[string]string) error {
	return nil
}

func checkJSONCastFunction(castType string, params map[string]string) error {
	castFunction, exists := params[common.JSONCastFunctionKey]
	if !exists {
		return nil
	}
	if castFunction != "STRING_TO_DOUBLE" {
		return merr.WrapErrParameterInvalidMsg("json_cast_function %v is not supported", castFunction)
	}
	if castType != "DOUBLE" {
		return merr.WrapErrParameterInvalidMsg("json_cast_function %v is not supported for json_cast_type %v", castFunction, castType)
	}
	return nil
}
