package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestCheckIndexValid(t *testing.T) {
	scalarIndexChecker := &scalarIndexChecker{}
	assert.NoError(t, scalarIndexChecker.CheckTrain(schemapb.DataType_Bool, schemapb.DataType_None, map[string]string{}))
}
