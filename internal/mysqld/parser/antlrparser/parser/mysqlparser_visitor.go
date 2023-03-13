// Code generated from java-escape by ANTLR 4.11.1. DO NOT EDIT.

package parser // MySqlParser
import "github.com/antlr/antlr4/runtime/Go/antlr/v4"

// A complete Visitor for a parse tree produced by MySqlParser.
type MySqlParserVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by MySqlParser#root.
	VisitRoot(ctx *RootContext) interface{}

	// Visit a parse tree produced by MySqlParser#sqlStatements.
	VisitSqlStatements(ctx *SqlStatementsContext) interface{}

	// Visit a parse tree produced by MySqlParser#sqlStatement.
	VisitSqlStatement(ctx *SqlStatementContext) interface{}

	// Visit a parse tree produced by MySqlParser#emptyStatement_.
	VisitEmptyStatement_(ctx *EmptyStatement_Context) interface{}

	// Visit a parse tree produced by MySqlParser#dmlStatement.
	VisitDmlStatement(ctx *DmlStatementContext) interface{}

	// Visit a parse tree produced by MySqlParser#simpleSelect.
	VisitSimpleSelect(ctx *SimpleSelectContext) interface{}

	// Visit a parse tree produced by MySqlParser#lockClause.
	VisitLockClause(ctx *LockClauseContext) interface{}

	// Visit a parse tree produced by MySqlParser#tableSources.
	VisitTableSources(ctx *TableSourcesContext) interface{}

	// Visit a parse tree produced by MySqlParser#tableSourceBase.
	VisitTableSourceBase(ctx *TableSourceBaseContext) interface{}

	// Visit a parse tree produced by MySqlParser#querySpecification.
	VisitQuerySpecification(ctx *QuerySpecificationContext) interface{}

	// Visit a parse tree produced by MySqlParser#selectSpec.
	VisitSelectSpec(ctx *SelectSpecContext) interface{}

	// Visit a parse tree produced by MySqlParser#selectElements.
	VisitSelectElements(ctx *SelectElementsContext) interface{}

	// Visit a parse tree produced by MySqlParser#selectColumnElement.
	VisitSelectColumnElement(ctx *SelectColumnElementContext) interface{}

	// Visit a parse tree produced by MySqlParser#selectFunctionElement.
	VisitSelectFunctionElement(ctx *SelectFunctionElementContext) interface{}

	// Visit a parse tree produced by MySqlParser#fromClause.
	VisitFromClause(ctx *FromClauseContext) interface{}

	// Visit a parse tree produced by MySqlParser#limitClause.
	VisitLimitClause(ctx *LimitClauseContext) interface{}

	// Visit a parse tree produced by MySqlParser#limitClauseAtom.
	VisitLimitClauseAtom(ctx *LimitClauseAtomContext) interface{}

	// Visit a parse tree produced by MySqlParser#fullId.
	VisitFullId(ctx *FullIdContext) interface{}

	// Visit a parse tree produced by MySqlParser#tableName.
	VisitTableName(ctx *TableNameContext) interface{}

	// Visit a parse tree produced by MySqlParser#fullColumnName.
	VisitFullColumnName(ctx *FullColumnNameContext) interface{}

	// Visit a parse tree produced by MySqlParser#collationName.
	VisitCollationName(ctx *CollationNameContext) interface{}

	// Visit a parse tree produced by MySqlParser#uid.
	VisitUid(ctx *UidContext) interface{}

	// Visit a parse tree produced by MySqlParser#simpleId.
	VisitSimpleId(ctx *SimpleIdContext) interface{}

	// Visit a parse tree produced by MySqlParser#dottedId.
	VisitDottedId(ctx *DottedIdContext) interface{}

	// Visit a parse tree produced by MySqlParser#decimalLiteral.
	VisitDecimalLiteral(ctx *DecimalLiteralContext) interface{}

	// Visit a parse tree produced by MySqlParser#stringLiteral.
	VisitStringLiteral(ctx *StringLiteralContext) interface{}

	// Visit a parse tree produced by MySqlParser#booleanLiteral.
	VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{}

	// Visit a parse tree produced by MySqlParser#hexadecimalLiteral.
	VisitHexadecimalLiteral(ctx *HexadecimalLiteralContext) interface{}

	// Visit a parse tree produced by MySqlParser#constant.
	VisitConstant(ctx *ConstantContext) interface{}

	// Visit a parse tree produced by MySqlParser#expressions.
	VisitExpressions(ctx *ExpressionsContext) interface{}

	// Visit a parse tree produced by MySqlParser#aggregateFunctionCall.
	VisitAggregateFunctionCall(ctx *AggregateFunctionCallContext) interface{}

	// Visit a parse tree produced by MySqlParser#aggregateWindowedFunction.
	VisitAggregateWindowedFunction(ctx *AggregateWindowedFunctionContext) interface{}

	// Visit a parse tree produced by MySqlParser#isExpression.
	VisitIsExpression(ctx *IsExpressionContext) interface{}

	// Visit a parse tree produced by MySqlParser#notExpression.
	VisitNotExpression(ctx *NotExpressionContext) interface{}

	// Visit a parse tree produced by MySqlParser#logicalExpression.
	VisitLogicalExpression(ctx *LogicalExpressionContext) interface{}

	// Visit a parse tree produced by MySqlParser#predicateExpression.
	VisitPredicateExpression(ctx *PredicateExpressionContext) interface{}

	// Visit a parse tree produced by MySqlParser#expressionAtomPredicate.
	VisitExpressionAtomPredicate(ctx *ExpressionAtomPredicateContext) interface{}

	// Visit a parse tree produced by MySqlParser#binaryComparisonPredicate.
	VisitBinaryComparisonPredicate(ctx *BinaryComparisonPredicateContext) interface{}

	// Visit a parse tree produced by MySqlParser#inPredicate.
	VisitInPredicate(ctx *InPredicateContext) interface{}

	// Visit a parse tree produced by MySqlParser#constantExpressionAtom.
	VisitConstantExpressionAtom(ctx *ConstantExpressionAtomContext) interface{}

	// Visit a parse tree produced by MySqlParser#fullColumnNameExpressionAtom.
	VisitFullColumnNameExpressionAtom(ctx *FullColumnNameExpressionAtomContext) interface{}

	// Visit a parse tree produced by MySqlParser#unaryExpressionAtom.
	VisitUnaryExpressionAtom(ctx *UnaryExpressionAtomContext) interface{}

	// Visit a parse tree produced by MySqlParser#nestedExpressionAtom.
	VisitNestedExpressionAtom(ctx *NestedExpressionAtomContext) interface{}

	// Visit a parse tree produced by MySqlParser#unaryOperator.
	VisitUnaryOperator(ctx *UnaryOperatorContext) interface{}

	// Visit a parse tree produced by MySqlParser#comparisonOperator.
	VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{}

	// Visit a parse tree produced by MySqlParser#logicalOperator.
	VisitLogicalOperator(ctx *LogicalOperatorContext) interface{}
}
