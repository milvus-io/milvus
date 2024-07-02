package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_BitmapIndexChecker(t *testing.T) {
	c := newBITMAPChecker()

	assert.NoError(t, c.CheckTrain(map[string]string{"bitmap_cardinality_limit": "100"}))

	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Bool, schemapb.DataType_None))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Int8, schemapb.DataType_None))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Int16, schemapb.DataType_None))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Int32, schemapb.DataType_None))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Int64, schemapb.DataType_None))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Float, schemapb.DataType_None))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_String, schemapb.DataType_None))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Array, schemapb.DataType_Bool))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Array, schemapb.DataType_Int8))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Array, schemapb.DataType_Int16))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Array, schemapb.DataType_Int32))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Array, schemapb.DataType_Int64))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Array, schemapb.DataType_String))

	assert.Error(t, c.CheckValidDataType(schemapb.DataType_JSON, schemapb.DataType_None))
	assert.Error(t, c.CheckValidDataType(schemapb.DataType_Float, schemapb.DataType_None))
	assert.Error(t, c.CheckValidDataType(schemapb.DataType_Double, schemapb.DataType_None))
	assert.Error(t, c.CheckValidDataType(schemapb.DataType_Array, schemapb.DataType_Float))
	assert.Error(t, c.CheckValidDataType(schemapb.DataType_Array, schemapb.DataType_Double))
	assert.Error(t, c.CheckTrain(map[string]string{}))
	assert.Error(t, c.CheckTrain(map[string]string{"bitmap_cardinality_limit": "0"}))
}
