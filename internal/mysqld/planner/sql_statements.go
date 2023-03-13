package planner

type NodeSqlStatements struct {
	baseNode
	Statements []*NodeSqlStatement
}

func (n *NodeSqlStatements) String() string {
	return "NodeSqlStatements"
}

func (n *NodeSqlStatements) GetChildren() []Node {
	children := make([]Node, 0, len(n.Statements))
	for _, child := range n.Statements {
		children = append(children, child)
	}
	return children
}

func (n *NodeSqlStatements) Accept(v Visitor) interface{} {
	return v.VisitSqlStatements(n)
}

func NewNodeSqlStatements(statements []*NodeSqlStatement, text string) *NodeSqlStatements {
	return &NodeSqlStatements{
		baseNode:   newBaseNode(text),
		Statements: statements,
	}
}
