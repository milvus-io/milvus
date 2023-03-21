package planner

func (v jsonVisitor) VisitSqlStatements(n *NodeSqlStatements) interface{} {
	var stmts []interface{}
	for _, stmt := range n.Statements {
		stmts = append(stmts, stmt.Accept(v))
	}
	j := map[string]interface{}{
		"sql_statements": stmts,
	}
	return j
}

func (v jsonVisitor) VisitSqlStatement(n *NodeSqlStatement) interface{} {
	var r interface{}
	if n.DmlStatement.IsSome() {
		r = n.DmlStatement.Unwrap().Accept(v)
	}
	j := map[string]interface{}{
		"sql_statement": r,
	}
	return j
}
