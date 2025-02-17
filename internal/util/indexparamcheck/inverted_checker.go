package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// INVERTEDChecker checks if a INVERTED index can be built.
type INVERTEDChecker struct {
	scalarIndexChecker
}

func (c *INVERTEDChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	// check json index params
	isJSONIndex := typeutil.IsJSONType(dataType)
	if isJSONIndex {
		if _, exist := params[common.JSONCastTypeKey]; !exist {
			return merr.WrapErrParameterMissing(common.JSONCastTypeKey, "json index must specify cast type")
		}
		if _, exist := params[common.JSONPathKey]; !exist {
			return merr.WrapErrParameterMissing(common.JSONPathKey, "json index must specify json path")
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
