package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestCheckIndexValid(t *testing.T) {
	assert.NoError(t, CheckIndexValid(schemapb.DataType_Int64, "inverted_index", nil))
}
