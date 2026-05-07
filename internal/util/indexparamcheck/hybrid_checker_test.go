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
	// unsupported cast type for HYBRID
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "ARRAY_BOOL", "json_path": "/flags",
	}))

	assert.Error(t, c.CheckTrain(schemapb.DataType_Float, schemapb.DataType_None, map[string]string{"bitmap_cardinality_limit": "0"}))
	assert.Error(t, c.CheckTrain(schemapb.DataType_Double, schemapb.DataType_None, map[string]string{"bitmap_cardinality_limit": "2000"}))
}
