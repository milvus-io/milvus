package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_STLSORTIndexChecker(t *testing.T) {
	c := newSTLSORTChecker()

	assert.NoError(t, c.CheckTrain(schemapb.DataType_Int64, schemapb.DataType_None, map[string]string{}))

	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_Float}))
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}))

	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}))
	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_JSON}))

	// struct sub-fields: name follows "structName[fieldName]" convention, DataType is Array
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[age]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[score]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float}))
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[name]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar}))
	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[flag]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Bool}))
	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[meta]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_JSON}))

	// regular Array field (not a struct sub-field) should still be rejected
	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}))
}
