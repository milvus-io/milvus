package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_HybridIndexChecker(t *testing.T) {
	c := newHYBRIDChecker()

	assert.NoError(t, c.CheckTrain(map[string]string{"bitmap_cardinality_limit": "100"}))

	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Bool}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Int8}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Int16}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Int32}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_String}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Bool}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int8}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int16}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_String}))

	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_JSON}))
	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Float}))
	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Double}))
	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float}))
	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double}))
	assert.Error(t, c.CheckTrain(map[string]string{}))
	assert.Error(t, c.CheckTrain(map[string]string{"bitmap_cardinality_limit": "0"}))
	assert.Error(t, c.CheckTrain(map[string]string{"bitmap_cardinality_limit": "2000"}))
}
