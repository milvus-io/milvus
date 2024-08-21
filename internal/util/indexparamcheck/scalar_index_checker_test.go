package indexparamcheck

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckIndexValid(t *testing.T) {
	scalarIndexChecker := &scalarIndexChecker{}
	assert.NoError(t, scalarIndexChecker.CheckTrain(schemapb.DataType_Bool, map[string]string{}))
}
