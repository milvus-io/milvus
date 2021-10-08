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
	cpb, err := MarshalForCGo(&pb)
	assert.Nil(t, err)

	// this function will accept a BoolArray input,
	// and return a BoolArray output
	// which negates all elements of the input
	ba, err := TestBoolArray(cpb)

	assert.Nil(t, err)
	for index, b := range ba.Data {
		assert.Equal(t, b, !pb.Data[index])
	}
}
