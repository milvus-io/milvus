package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_TrieIndexChecker(t *testing.T) {
	c := newTRIEChecker()

	assert.NoError(t, c.CheckTrain(map[string]string{}))

	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}))
	assert.NoError(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_String}))

	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Bool}))
	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Int64}))
	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_Float}))
	assert.Error(t, c.CheckValidDataType(&schemapb.FieldSchema{DataType: schemapb.DataType_JSON}))
}
