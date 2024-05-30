package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_INVERTEDIndexChecker(t *testing.T) {
	c := newINVERTEDChecker()

	assert.NoError(t, c.CheckTrain(map[string]string{}))

	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_VarChar))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_String))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Bool))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Int64))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Float))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_Array))

	assert.Error(t, c.CheckValidDataType(schemapb.DataType_JSON))
	assert.Error(t, c.CheckValidDataType(schemapb.DataType_FloatVector))
}
