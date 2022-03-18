package indexparamcheck

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestCheckIndexValid(t *testing.T) {
	assert.NoError(t, CheckIndexValid(schemapb.DataType_Int64, "inverted_index", nil))
}
