package querynode

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestCGoHelper_Naive(t *testing.T) {
	pb := schemapb.BoolArray{
		Data: []bool{true, false, true, true, true},
	}
	_, err := MarshalForCGo(&pb)
	assert.Nil(t, err)
}
