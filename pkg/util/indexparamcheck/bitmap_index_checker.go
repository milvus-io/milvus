package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type BITMAPChecker struct {
	scalarIndexChecker
}

func (c *BITMAPChecker) CheckTrain(params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(params)
}

func (c *BITMAPChecker) CheckValidDataType(field *schemapb.FieldSchema) error {
	if field.IsPrimaryKey {
		return fmt.Errorf("create bitmap index on primary key not supported")
	}
	mainType := field.GetDataType()
	elemType := field.GetElementType()
	if !typeutil.IsBoolType(mainType) && !typeutil.IsIntegerType(mainType) &&
		!typeutil.IsStringType(mainType) && !typeutil.IsArrayType(mainType) {
		return fmt.Errorf("bitmap index are only supported on bool, int, string and array field")
	}
	if typeutil.IsArrayType(mainType) {
		if !typeutil.IsBoolType(elemType) && !typeutil.IsIntegerType(elemType) &&
			!typeutil.IsStringType(elemType) {
			return fmt.Errorf("bitmap index are only supported on bool, int, string for array field")
		}
	}
	return nil
}

func newBITMAPChecker() *BITMAPChecker {
	return &BITMAPChecker{}
}
