package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_TrieIndexChecker(t *testing.T) {
	c := newTRIEChecker()

	assert.NoError(t, c.CheckTrain(map[string]string{}))

	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_VarChar, schemapb.DataType_None))
	assert.NoError(t, c.CheckValidDataType(schemapb.DataType_String, schemapb.DataType_None))

	assert.Error(t, c.CheckValidDataType(schemapb.DataType_Bool, schemapb.DataType_None))
	assert.Error(t, c.CheckValidDataType(schemapb.DataType_Int64, schemapb.DataType_None))
	assert.Error(t, c.CheckValidDataType(schemapb.DataType_Float, schemapb.DataType_None))
	assert.Error(t, c.CheckValidDataType(schemapb.DataType_JSON, schemapb.DataType_None))
}
