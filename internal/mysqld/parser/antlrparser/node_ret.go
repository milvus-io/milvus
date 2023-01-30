package antlrparser

import (
	"github.com/milvus-io/milvus/internal/mysqld/planner"
)

func GetError(obj interface{}) error {
	err, ok := obj.(error)
	if !ok {
		// obj is not an error.
		return nil
	}
	return err
}

func GetNode(obj interface{}) planner.Node {
	n, ok := obj.(planner.Node)
	if !ok {
		return nil
	}
	return n
}

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

func GetDmlStatement(obj interface{}) *planner.NodeDmlStatement {
	n, ok := obj.(*planner.NodeDmlStatement)
	if !ok {
		return nil
	}
	return n
}

func GetSelectStatement(obj interface{}) *planner.NodeSelectStatement {
	n, ok := obj.(*planner.NodeSelectStatement)
	if !ok {
		return nil
	}
	return n
}

func GetQuerySpecification(obj interface{}) *planner.NodeQuerySpecification {
	n, ok := obj.(*planner.NodeQuerySpecification)
	if !ok {
		return nil
	}
	return n
}

func GetLockClause(obj interface{}) *planner.NodeLockClause {
	n, ok := obj.(*planner.NodeLockClause)
	if !ok {
		return nil
	}
	return n
}

func GetSelectSpec(obj interface{}) *planner.NodeSelectSpec {
	n, ok := obj.(*planner.NodeSelectSpec)
	if !ok {
		return nil
	}
	return n
}

func GetSelectElement(obj interface{}) *planner.NodeSelectElement {
	n, ok := obj.(*planner.NodeSelectElement)
	if !ok {
		return nil
	}
	return n
}

func GetFromClause(obj interface{}) *planner.NodeFromClause {
	n, ok := obj.(*planner.NodeFromClause)
	if !ok {
		return nil
	}
	return n
}

func GetLimitClause(obj interface{}) *planner.NodeLimitClause {
	n, ok := obj.(*planner.NodeLimitClause)
	if !ok {
		return nil
	}
	return n
}

func GetAggregateWindowedFunction(obj interface{}) *planner.NodeAggregateWindowedFunction {
	n, ok := obj.(*planner.NodeAggregateWindowedFunction)
	if !ok {
		return nil
	}
	return n
}

func GetCount(obj interface{}) *planner.NodeCount {
	n, ok := obj.(*planner.NodeCount)
	if !ok {
		return nil
	}
	return n
}

func GetTableSource(obj interface{}) *planner.NodeTableSource {
	n, ok := obj.(*planner.NodeTableSource)
	if !ok {
		return nil
	}
	return n
}

func GetExpression(obj interface{}) *planner.NodeExpression {
	n, ok := obj.(*planner.NodeExpression)
	if !ok {
		return nil
	}
	return n
}

func GetIsExpression(obj interface{}) *planner.NodeIsExpression {
	n, ok := obj.(*planner.NodeIsExpression)
	if !ok {
		return nil
	}
	return n
}

func GetNotExpression(obj interface{}) *planner.NodeNotExpression {
	n, ok := obj.(*planner.NodeNotExpression)
	if !ok {
		return nil
	}
	return n
}

func GetPredicate(obj interface{}) *planner.NodePredicate {
	n, ok := obj.(*planner.NodePredicate)
	if !ok {
		return nil
	}
	return n
}

func GetExpressionAtom(obj interface{}) *planner.NodeExpressionAtom {
	n, ok := obj.(*planner.NodeExpressionAtom)
	if !ok {
		return nil
	}
	return n
}

func GetExpressions(obj interface{}) *planner.NodeExpressions {
	n, ok := obj.(*planner.NodeExpressions)
	if !ok {
		return nil
	}
	return n
}

func GetFullColumnName(obj interface{}) *planner.NodeFullColumnName {
	n, ok := obj.(*planner.NodeFullColumnName)
	if !ok {
		return nil
	}
	return n
}
