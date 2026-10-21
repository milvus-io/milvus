package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func Test_STLSORTIndexChecker(t *testing.T) {
	c := newSTLSORTChecker()

	assert.NoError(t, c.CheckTrain(schemapb.DataType_Int64, schemapb.DataType_None, map[string]string{}))

	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_Float}))
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}))

	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}))
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: schemapb.DataType_JSON}))

	// struct sub-fields: name follows "structName[fieldName]" convention, DataType is Array
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[age]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[score]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float}))
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[name]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar}))
	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[flag]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Bool}))
	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[meta]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_JSON}))

	// Ordinary ARRAY includes finite floating values.
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}))

	// JSON path index: CheckTrain validation
	assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "DOUBLE", "json_path": "/price",
	}))
	assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "VARCHAR", "json_path": "/name",
	}))
	// missing json_cast_type
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_path": "/price",
	}))
	// missing json_path
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "DOUBLE",
	}))
	// unsupported cast type for STL_SORT
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "BOOL", "json_path": "/flag",
	}))
}

func TestSTLSORTOrdinaryArrayElementTypes(t *testing.T) {
	c := newSTLSORTChecker()
	for _, elem := range []schemapb.DataType{schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double, schemapb.DataType_VarChar} {
		assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "tags", DataType: schemapb.DataType_Array, ElementType: elem}), elem.String())
	}
	for _, elem := range []schemapb.DataType{schemapb.DataType_None, schemapb.DataType_JSON, schemapb.DataType_Array, schemapb.DataType_FloatVector} {
		assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "tags", DataType: schemapb.DataType_Array, ElementType: elem}), elem.String())
	}
}
