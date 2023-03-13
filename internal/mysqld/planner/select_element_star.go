package planner

type NodeSelectElementStar struct{} // select *;

func (n *NodeSelectElementStar) String() string {
	return "NodeSelectElementStar"
}

func (n *NodeSelectElementStar) GetText() string {
	return "*"
}

func (n *NodeSelectElementStar) GetChildren() []Node {
	return nil
}

func (n *NodeSelectElementStar) Accept(v Visitor) interface{} {
	return v.VisitSelectElementStar(n)
}
