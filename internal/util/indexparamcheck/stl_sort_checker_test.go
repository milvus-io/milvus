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
	assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[time]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Timestamptz}))
	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[flag]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Bool}))
	assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{Name: "myStruct[meta]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_JSON}))

	for _, elementType := range []schemapb.DataType{
		schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16,
		schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_Float,
		schemapb.DataType_Double, schemapb.DataType_VarChar,
	} {
		assert.NoError(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{
			Name: "values", DataType: schemapb.DataType_Array, ElementType: elementType,
		}), elementType.String())
	}
	for _, unsupported := range []schemapb.DataType{
		schemapb.DataType_JSON, schemapb.DataType_Array, schemapb.DataType_ArrayOfStruct,
		schemapb.DataType_FloatVector, schemapb.DataType_Geometry, schemapb.DataType_Timestamptz,
	} {
		assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{
			Name: "values", DataType: schemapb.DataType_Array, ElementType: unsupported,
		}), unsupported.String())
	}
	for _, unsupported := range []schemapb.DataType{schemapb.DataType_ArrayOfStruct, schemapb.DataType_FloatVector, schemapb.DataType_Geometry} {
		assert.Error(t, c.CheckValidDataType(IndexSTLSORT, &schemapb.FieldSchema{DataType: unsupported}))
	}

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
	for _, cast := range []string{"ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR"} {
		params := map[string]string{"json_cast_type": cast, "json_path": "/values"}
		assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, params))
		delete(params, "json_path")
		assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, params))
		params["json_path"] = "/values"
		params["json_cast_function"] = "STRING_TO_DOUBLE"
		assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, params))
	}
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "JSON", "json_path": "/object",
	}))
}
