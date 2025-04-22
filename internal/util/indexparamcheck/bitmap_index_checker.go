package indexparamcheck

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type BITMAPChecker struct {
	scalarIndexChecker
}

func (c *BITMAPChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(dataType, params)
}

func (c *BITMAPChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if field.IsPrimaryKey {
		return errors.New("create bitmap index on primary key not supported")
	}
	mainType := field.GetDataType()
	elemType := field.GetElementType()
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
