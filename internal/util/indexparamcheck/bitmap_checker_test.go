package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_BitmapIndexChecker(t *testing.T) {
	c := newBITMAPChecker()

	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Int8}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Int16}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Int32}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_String}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Bool}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int8}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int16}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_String}))

	assert.NoError(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_JSON}))
	assert.Error(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Float}))
	assert.Error(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Double}))
	assert.Error(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float}))
	assert.Error(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double}))
	assert.Error(t, c.CheckValidDataType(IndexBitmap, &schemapb.FieldSchema{DataType: schemapb.DataType_Double, IsPrimaryKey: true}))

	// JSON path index: CheckTrain validation
	assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "BOOL", "json_path": "/flag",
	}))
	assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "VARCHAR", "json_path": "/status",
	}))
	// unsupported cast type for BITMAP
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{
		"json_cast_type": "DOUBLE", "json_path": "/price",
	}))
	// missing params
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{}))
}
