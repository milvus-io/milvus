package antlrparser

import "github.com/milvus-io/milvus/internal/mysqld/planner"

func GetSqlStatements(obj interface{}) *planner.NodeSqlStatements {
	n, ok := obj.(*planner.NodeSqlStatements)
	if !ok {
		return nil
	}
	return n
}

func GetSqlStatement(obj interface{}) *planner.NodeSqlStatement {
	n, ok := obj.(*planner.NodeSqlStatement)
	if !ok {
		return nil
	}
	return n
}
