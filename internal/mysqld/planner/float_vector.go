package planner

type NodeFloatVector struct {
	Array []float32
}

func NewNodeFloatVector(arr []float32) *NodeFloatVector {
	return &NodeFloatVector{
		Array: arr,
	}
}
