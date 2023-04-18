package planner

import (
	"math"

	"github.com/milvus-io/milvus/pkg/common"
)

type NodeFloatVector struct {
	Array []float32
}

func (n NodeFloatVector) Serialize() []byte {
	data := make([]byte, 0, 4*len(n.Array)) // float32 occupies 4 bytes
	buf := make([]byte, 4)
	for _, f := range n.Array {
		common.Endian.PutUint32(buf, math.Float32bits(f))
		data = append(data, buf...)
	}
	return data
}

func NewNodeFloatVector(arr []float32) *NodeFloatVector {
	return &NodeFloatVector{
		Array: arr,
	}
}
