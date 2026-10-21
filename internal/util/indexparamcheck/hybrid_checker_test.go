package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func Test_HybridIndexChecker(t *testing.T) {
	c := newHYBRIDChecker()

	assert.NoError(t, c.CheckTrain(schemapb.DataType_Bool, schemapb.DataType_None, map[string]string{"bitmap_cardinality_limit": "100"}))

	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Int8}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Int16}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Int32}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_String}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Bool}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int8}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int16}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_String}))

	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_JSON}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Float}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Double}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float}))
	assert.NoError(t, c.CheckValidDataType(IndexHybrid, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double}))
	// JSON path index: CheckTrain validation
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{}))
	assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "DOUBLE", "json_path": "/price",
	}))
	assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "BOOL", "json_path": "/flag",
	}))
	assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "VARCHAR", "json_path": "/name",
	}))
	for _, cast := range []string{"ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR"} {
		t.Run(cast, func(t *testing.T) {
			assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
				"json_cast_type": cast, "json_path": "/values",
			}))
			assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
				"json_cast_type": cast,
			}))
			assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
				"json_cast_type": cast, "json_path": "/values", "json_cast_function": "STRING_TO_DOUBLE",
			}))
		})
	}
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "JSON", "json_path": "/values",
	}))

	assert.Error(t, c.CheckTrain(schemapb.DataType_Float, schemapb.DataType_None, map[string]string{"bitmap_cardinality_limit": "0"}))
	assert.Error(t, c.CheckTrain(schemapb.DataType_Double, schemapb.DataType_None, map[string]string{"bitmap_cardinality_limit": "2000"}))
}

func Test_JSONArrayRejectsCastFunction(t *testing.T) {
	for _, indexType := range []string{IndexHybrid, IndexSTLSORT} {
		for _, cast := range []string{"ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR"} {
			for _, function := range []string{"STRING_TO_DOUBLE", "", "UNKNOWN"} {
				t.Run(indexType+"/"+cast+"/"+function, func(t *testing.T) {
					// Validate through the field-aware entry point used by CreateIndex.
					assert.ErrorContains(t, ValidateFieldIndexParams(&schemapb.FieldSchema{DataType: schemapb.DataType_JSON}, map[string]string{
						"index_type": indexType, "json_cast_type": cast, "json_path": "/a", "json_cast_function": function,
					}), "json_cast_function")
				})
			}
		}
	}
}
