package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_BitmapIndexChecker(t *testing.T) {
	c := newBITMAPChecker()

	assert.NoError(t, c.CheckTrain(map[string]string{}))

	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Int64))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Float))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_String))

	assert.Error(t, c.CheckValidDataType(schemapb.DataType_JSON))
	assert.Error(t, c.CheckValidDataType(schemapb.DataType_Array))
}
