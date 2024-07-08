package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_STLSORTIndexChecker(t *testing.T) {
	c := newSTLSORTChecker()

	assert.NoError(t, c.CheckTrain(map[string]string{}))

	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Float}))

	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}))
	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Bool}))
	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_JSON}))
}
