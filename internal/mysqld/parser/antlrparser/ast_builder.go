package antlrparser

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/internal/mysqld/planner"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	parsergen "github.com/milvus-io/milvus/internal/mysqld/parser/antlrparser/parser"
)

type AstBuilder struct {
	parsergen.BaseMySqlParserVisitor
}

type AstBuilderOption func(*AstBuilder)

func (v *AstBuilder) apply(opts ...AstBuilderOption) {
	for _, opt := range opts {
		opt(v)
	}
}

func (v *AstBuilder) VisitRoot(ctx *parsergen.RootContext) interface{} {
	return ctx.SqlStatements().Accept(v)
}

func (v *AstBuilder) VisitSqlStatements(ctx *parsergen.SqlStatementsContext) interface{} {
	allSqlStatementsCtx := ctx.AllSqlStatement()
	sqlStatements := make([]*planner.NodeSqlStatement, 0, len(allSqlStatementsCtx))
	for _, sqlStatementCtx := range allSqlStatementsCtx {
		r := sqlStatementCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}
		n := GetSqlStatement(r)
		if n == nil {
			return fmt.Errorf("failed to parse sql statement: %s", sqlStatementCtx.GetText())
		}
		sqlStatements = append(sqlStatements, n)
	}
	return planner.NewNodeSqlStatements(GetOriginalText(ctx), sqlStatements)
}

func (v *AstBuilder) VisitSqlStatement(ctx *parsergen.SqlStatementContext) interface{} {
	dmlStatement := ctx.DmlStatement()
	if dmlStatement == nil {
		return fmt.Errorf("sql statement only support dml statement now: %s", GetOriginalText(ctx))
	}
	r := dmlStatement.Accept(v)
	if err := GetError(r); err != nil {
		return err
	}
	n := GetDmlStatement(r)
	if n == nil {
		return fmt.Errorf("failed to parse dml statement: %s", dmlStatement.GetText())
	}
	return planner.NewNodeSqlStatement(GetOriginalText(ctx), planner.WithDmlStatement(n))
}

func (v *AstBuilder) VisitEmptyStatement_(ctx *parsergen.EmptyStatement_Context) interface{} {
	// Should not be visited.
	return nil
}

func (v *AstBuilder) VisitDmlStatement(ctx *parsergen.DmlStatementContext) interface{} {
	selectStatement := ctx.SelectStatement()
	if selectStatement == nil {
		return fmt.Errorf("dml statement only support select statement now: %s", GetOriginalText(ctx))
	}
	r := selectStatement.Accept(v)
	if err := GetError(r); err != nil {
		return err
	}
	n := GetSelectStatement(r)
	if n == nil {
		return fmt.Errorf("failed to parse select statement: %s", selectStatement.GetText())
	}
	return planner.NewNodeDmlStatement(GetOriginalText(ctx), planner.WithSelectStatement(n))
}

func (v *AstBuilder) VisitSimpleSelect(ctx *parsergen.SimpleSelectContext) interface{} {
	var opts []planner.NodeSimpleSelectOption

	querySpecificationCtx := ctx.QuerySpecification()
	if querySpecificationCtx == nil {
		return fmt.Errorf("simple select only support query specification now: %s", GetOriginalText(ctx))
	}

	r := querySpecificationCtx.Accept(v)
	if err := GetError(r); err != nil {
		return err
	}

	n := GetQuerySpecification(r)
	if n == nil {
		return fmt.Errorf("failed to parse query specification: %s", GetOriginalText(querySpecificationCtx))
	}

	opts = append(opts, planner.WithQuery(n))

	lockClauseCtx := ctx.LockClause()
	if lockClauseCtx != nil {
		r := lockClauseCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}

		if n := GetLockClause(r); n != nil {
			opts = append(opts, planner.WithLockClause(n))
		}
	}

	text := GetOriginalText(ctx)
	nodeSimpleSelect := planner.NewNodeSimpleSelect(text, opts...)
	return planner.NewNodeSelectStatement(text, planner.WithSimpleSelect(nodeSimpleSelect))
}

func (v *AstBuilder) VisitLockClause(ctx *parsergen.LockClauseContext) interface{} {
	//o := planner.LockClauseOptionUnknown
	//text := GetOriginalText(ctx)
	//if CaseInsensitiveEqual(text, planner.LockClauseOption_ForUpdate_Str) {
	//	o = planner.LockClauseOptionForUpdate
	//} else if CaseInsensitiveEqual(text, planner.LockClauseOption_LockInShareMode_Str) {
	//	o = planner.LockClauseOptionLockInShareMode
	//}
	//return planner.NewNodeLockClause(text, o)
	return fmt.Errorf("lock clause is not supported: %s", GetOriginalText(ctx))
}

func (v *AstBuilder) VisitQuerySpecification(ctx *parsergen.QuerySpecificationContext) interface{} {
	allSelectSpecCtx := ctx.AllSelectSpec()
	selectSpecs := make([]*planner.NodeSelectSpec, 0, len(allSelectSpecCtx))
	for _, selectSpecCtx := range allSelectSpecCtx {
		r := selectSpecCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}
		if n := GetSelectSpec(r); n != nil {
			selectSpecs = append(selectSpecs, n)
		}
	}

	selectElementsCtx := ctx.SelectElements()
	allSelectElementsCtx := selectElementsCtx.(*parsergen.SelectElementsContext).AllSelectElement()
	selectElements := make([]*planner.NodeSelectElement, 0, len(allSelectElementsCtx))
	if star := selectElementsCtx.GetStar(); star != nil {
		selectElements = append(selectElements, planner.NewNodeSelectElement(star.GetText(), planner.WithStar()))
	}

	for _, selectElementCtx := range allSelectElementsCtx {
		r := selectElementCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}
		if n := GetSelectElement(r); n != nil {
			selectElements = append(selectElements, n)
		}
	}

	var opts []planner.NodeQuerySpecificationOption

	fromCtx := ctx.FromClause()
	if fromCtx != nil {
		r := fromCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}
		if n := GetFromClause(r); n != nil {
			opts = append(opts, planner.WithFrom(n))
		}
	}

	limitCtx := ctx.LimitClause()
	if limitCtx != nil {
		r := limitCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}
		if n := GetLimitClause(r); n != nil {
			opts = append(opts, planner.WithLimit(n))
		}
	}

	return planner.NewNodeQuerySpecification(GetOriginalText(ctx), selectSpecs, selectElements, opts...)
}

func (v *AstBuilder) VisitSelectSpec(ctx *parsergen.SelectSpecContext) interface{} {
	return fmt.Errorf("select spec is not supported: %s", GetOriginalText(ctx))
}

func (v *AstBuilder) VisitSelectElements(ctx *parsergen.SelectElementsContext) interface{} {
	// Should not be visited.
	return nil
}

func (v *AstBuilder) VisitSelectColumnElement(ctx *parsergen.SelectColumnElementContext) interface{} {
	var opts []planner.NodeFullColumnNameOption

	asCtx := ctx.AS()
	uidCtx := ctx.Uid()

	if asCtx != nil && uidCtx != nil {
		r := uidCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}
		if alias, ok := r.(string); ok {
			opts = append(opts, planner.FullColumnNameWithAlias(alias))
		}
	}

	if asCtx == nil && uidCtx != nil {
		return fmt.Errorf("invalid alias of column: %s", GetOriginalText(ctx))
	}

	r := ctx.FullColumnName().Accept(v)
	if err := GetError(r); err != nil {
		return err
	}
	s := r.(string)
	text := GetOriginalText(ctx)
	n := planner.NewNodeFullColumnName(text, s, opts...)
	return planner.NewNodeSelectElement(text, planner.WithFullColumnName(n))
}

func (v *AstBuilder) VisitSelectFunctionElement(ctx *parsergen.SelectFunctionElementContext) interface{} {
	var opts []planner.NodeFunctionCallOption

	asCtx := ctx.AS()
	uidCtx := ctx.Uid()

	if asCtx != nil && uidCtx != nil {
		r := uidCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}
		if alias, ok := r.(string); ok {
			opts = append(opts, planner.FunctionCallWithAlias(alias))
		}
	}

	if asCtx == nil && uidCtx != nil {
		return fmt.Errorf("invalid alias of function call: %s", ctx.GetText())
	}

	r := ctx.FunctionCall().Accept(v)
	if err := GetError(r); err != nil {
		return err
	}
	if n := GetAggregateWindowedFunction(r); n != nil {
		opts = append(opts, planner.WithAgg(n))
	}
	text := GetOriginalText(ctx)

	n := planner.NewNodeFunctionCall(text, opts...)
	return planner.NewNodeSelectElement(text, planner.WithFunctionCall(n))
}

func (v *AstBuilder) VisitFromClause(ctx *parsergen.FromClauseContext) interface{} {
	text := GetOriginalText(ctx)

	tableSourcesCtx := ctx.TableSources().(*parsergen.TableSourcesContext)
	allTableSourcesCtx := tableSourcesCtx.AllTableSource()
	tableSources := make([]*planner.NodeTableSource, 0, len(allTableSourcesCtx))
	for _, tableSourceCtx := range allTableSourcesCtx {
		r := tableSourceCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}
		if n := GetTableSource(r); n != nil {
			tableSources = append(tableSources, n)
		}
	}

	var opts []planner.NodeFromClauseOption

	whereCtx := ctx.Expression()
	if whereCtx != nil {
		r := whereCtx.Accept(v)
		if err := GetError(r); err != nil {
			return err
		}
		if n := GetExpression(r); n != nil {
			opts = append(opts, planner.WithWhere(n))
		}
	}

	return planner.NewNodeFromClause(text, tableSources, opts...)
}

func (v *AstBuilder) VisitTableSources(ctx *parsergen.TableSourcesContext) interface{} {
	// Should not be visited.
	return nil
}

func (v *AstBuilder) VisitTableSourceBase(ctx *parsergen.TableSourceBaseContext) interface{} {
	text := GetOriginalText(ctx)

	tableNameCtx := ctx.TableName()
	r := tableNameCtx.Accept(v)

	if err := GetError(r); err != nil {
		return err
	}

	s, ok := r.(string)
	if !ok {
		return fmt.Errorf("not supported table source: %s", text)
	}

	return planner.NewNodeTableSource(text, planner.WithTableName(s))
}

func (v *AstBuilder) VisitLimitClause(ctx *parsergen.LimitClauseContext) interface{} {
	text := GetOriginalText(ctx)
	offset, limit := int64(0), int64(0)

	getInt := func(tree antlr.ParseTree) (int64, error) {
		r := tree.Accept(v)
		if err := GetError(r); err != nil {
			return 0, err
		}
		return r.(int64), nil
	}

	offsetCtx := ctx.GetOffset()
	if offsetCtx != nil {
		i, err := getInt(offsetCtx)
		if err != nil {
			return err
		}
		offset = i
	}

	limitCtx := ctx.GetLimit()
	i, err := getInt(limitCtx)
	if err != nil {
		return err
	}
	limit = i

	return planner.NewNodeLimitClause(text, limit, offset)
}

func (v *AstBuilder) VisitLimitClauseAtom(ctx *parsergen.LimitClauseAtomContext) interface{} {
	text := GetOriginalText(ctx)

	decimalLiteralCtx := ctx.DecimalLiteral()
	if decimalLiteralCtx == nil {
		return fmt.Errorf("limit only support literal now: %s", text)
	}

	return decimalLiteralCtx.Accept(v)
}

func (v *AstBuilder) VisitFullId(ctx *parsergen.FullIdContext) interface{} {
	return GetOriginalText(ctx)
}

func (v *AstBuilder) VisitTableName(ctx *parsergen.TableNameContext) interface{} {
	return GetOriginalText(ctx)
}

func (v *AstBuilder) VisitFullColumnName(ctx *parsergen.FullColumnNameContext) interface{} {
	return GetOriginalText(ctx)
}

func (v *AstBuilder) VisitCollationName(ctx *parsergen.CollationNameContext) interface{} {
	return GetOriginalText(ctx)
}

func (v *AstBuilder) VisitUid(ctx *parsergen.UidContext) interface{} {
	return GetOriginalText(ctx)
}

func (v *AstBuilder) VisitSimpleId(ctx *parsergen.SimpleIdContext) interface{} {
	return GetOriginalText(ctx)
}

func (v *AstBuilder) VisitDottedId(ctx *parsergen.DottedIdContext) interface{} {
	return GetOriginalText(ctx)
}

func (v *AstBuilder) VisitDecimalLiteral(ctx *parsergen.DecimalLiteralContext) interface{} {
	if decimalCtx := ctx.DECIMAL_LITERAL(); decimalCtx != nil {
		i, err := strconv.Atoi(decimalCtx.GetText())
		if err != nil {
			return err
		}
		return int64(i)
	}

	if zeroCtx := ctx.ZERO_DECIMAL(); zeroCtx != nil {
		return int64(0)
	}

	if oneCtx := ctx.ONE_DECIMAL(); oneCtx != nil {
		return int64(1)
	}

	if twoCtx := ctx.TWO_DECIMAL(); twoCtx != nil {
		return int64(2)
	}

	if realCtx := ctx.REAL_LITERAL(); realCtx != nil {
		f, err := processRealCtx(realCtx)
		if err != nil {
			return err
		}
		return f
	}

	return nil
}

func processRealCtx(n antlr.TerminalNode) (float64, error) {
	f, err := strconv.ParseFloat(n.GetText(), 64)
	if err != nil {
		return 0.0, err
	}
	return f, nil
}

func (v *AstBuilder) VisitStringLiteral(ctx *parsergen.StringLiteralContext) interface{} {
	return GetOriginalText(ctx)
}

func (v *AstBuilder) VisitBooleanLiteral(ctx *parsergen.BooleanLiteralContext) interface{} {
	return ctx.TRUE() != nil
}

func (v *AstBuilder) VisitHexadecimalLiteral(ctx *parsergen.HexadecimalLiteralContext) interface{} {
	text := GetOriginalText(ctx)
	i, err := strconv.ParseInt(text, 16, 64)
	if err != nil {
		return err
	}
	return i
}

func (v *AstBuilder) VisitConstant(ctx *parsergen.ConstantContext) interface{} {
	if stringLiteralCtx := ctx.StringLiteral(); stringLiteralCtx != nil {
		return stringLiteralCtx.Accept(v)
	}

	if decimalLiteralCtx := ctx.DecimalLiteral(); decimalLiteralCtx != nil {
		decimal := decimalLiteralCtx.Accept(v)
		if err := GetError(decimal); err != nil {
			return err
		}
		if ctx.MINUS() != nil {
			switch realType := decimal.(type) {
			case int:
				return -realType
			case int32:
				return -realType
			case int64:
				return -realType
			case float32:
				return -realType
			case float64:
				return -realType
			default:
				return fmt.Errorf("invalid data type: %s", decimalLiteralCtx.GetText())
			}
		}
		return decimal
	}

	if hexadecimalLiteralCtx := ctx.HexadecimalLiteral(); hexadecimalLiteralCtx != nil {
		return hexadecimalLiteralCtx.Accept(v)
	}

	if realCtx := ctx.REAL_LITERAL(); realCtx != nil {
		f, err := processRealCtx(realCtx)
		if err != nil {
			return err
		}
		return f
	}

	return nil
}

func (v *AstBuilder) VisitExpressions(ctx *parsergen.ExpressionsContext) interface{} {
	text := GetOriginalText(ctx)
	expressionsCtx := ctx.AllExpression()
	expressions := make([]*planner.NodeExpression, 0, len(expressionsCtx))
	for _, expressionCtx := range expressionsCtx {
		n, err := v.getExpression(expressionCtx)
		if err != nil {
			return err
		}
		if n == nil {
			return fmt.Errorf("failed to parse expression: %s", GetOriginalText(expressionCtx))
		}
		expressions = append(expressions, n)
	}
	return planner.NewNodeExpressions(text, expressions)
}

func (v *AstBuilder) VisitAggregateFunctionCall(ctx *parsergen.AggregateFunctionCallContext) interface{} {
	r := ctx.AggregateWindowedFunction().Accept(v)
	if err := GetError(r); err != nil {
		return err
	}
	if n := GetCount(r); n != nil {
		return planner.NewNodeAggregateWindowedFunction(GetOriginalText(ctx), planner.WithAggCount(n))
	}
	return nil
}

func (v *AstBuilder) VisitAggregateWindowedFunction(ctx *parsergen.AggregateWindowedFunctionContext) interface{} {
	text := GetOriginalText(ctx)
	return planner.NewNodeCount(text)
}

func (v *AstBuilder) getPredicate(tree antlr.ParseTree) (*planner.NodePredicate, error) {
	r := tree.Accept(v)
	if err := GetError(r); err != nil {
		return nil, err
	}
	return GetPredicate(r), nil
}

func (v *AstBuilder) VisitIsExpression(ctx *parsergen.IsExpressionContext) interface{} {
	text := GetOriginalText(ctx)

	testValue := planner.TestValueUnknown
	if ctx.TRUE() != nil {
		testValue = planner.TestValueTrue
	}
	if ctx.FALSE() != nil {
		testValue = planner.TestValueFalse
	}

	op := planner.IsOperatorIs
	if ctx.NOT() != nil {
		op = planner.IsOperatorIsNot
	}

	n, err := v.getPredicate(ctx.Predicate())

	if err != nil {
		return err
	}

	if n == nil {
		return fmt.Errorf("failed to parse [is expression]: %s", text)
	}

	isExpr := planner.NewNodeIsExpression(text, n, testValue, op)
	return planner.NewNodeExpression(text, planner.WithIsExpr(isExpr))
}

func (v *AstBuilder) getExpression(tree antlr.ParseTree) (*planner.NodeExpression, error) {
	r := tree.Accept(v)
	if err := GetError(r); err != nil {
		return nil, err
	}
	return GetExpression(r), nil
}

func (v *AstBuilder) VisitNotExpression(ctx *parsergen.NotExpressionContext) interface{} {
	text := GetOriginalText(ctx)
	expressionCtx := ctx.Expression()
	n, err := v.getExpression(expressionCtx)
	if err != nil {
		return err
	}
	if n == nil {
		return fmt.Errorf("failed to parse [not expression]: %s", text)
	}
	notExpr := planner.NewNodeNotExpression(text, n)
	return planner.NewNodeExpression(text, planner.WithNotExpr(notExpr))
}

func (v *AstBuilder) VisitLogicalExpression(ctx *parsergen.LogicalExpressionContext) interface{} {
	text := GetOriginalText(ctx)
	op := ctx.LogicalOperator().Accept(v).(planner.LogicalOperator)
	left, right := ctx.Expression(0), ctx.Expression(1)

	leftExpr, err := v.getExpression(left)
	if err != nil {
		return err
	}

	if leftExpr == nil {
		return fmt.Errorf("failed to parse left expr: %s", left.GetText())
	}

	rightExpr, err := v.getExpression(right)
	if err != nil {
		return err
	}

	if rightExpr == nil {
		return fmt.Errorf("failed to parse right expr: %s", right.GetText())
	}

	logicalExpr := planner.NewNodeLogicalExpression(text, leftExpr, rightExpr, op)
	return planner.NewNodeExpression(text, planner.WithLogicalExpr(logicalExpr))
}

func (v *AstBuilder) VisitPredicateExpression(ctx *parsergen.PredicateExpressionContext) interface{} {
	text := GetOriginalText(ctx)
	predicateCtx := ctx.Predicate()
	predicate, err := v.getPredicate(predicateCtx)
	if err != nil {
		return err
	}
	if predicate == nil {
		return fmt.Errorf("failed to parse predicate expression: %s", GetOriginalText(predicateCtx))
	}
	return planner.NewNodeExpression(text, planner.WithPredicate(predicate))
}

func (v *AstBuilder) getExpressionAtom(tree antlr.ParseTree) (*planner.NodeExpressionAtom, error) {
	r := tree.Accept(v)
	if err := GetError(r); err != nil {
		return nil, err
	}
	return GetExpressionAtom(r), nil
}

func (v *AstBuilder) VisitExpressionAtomPredicate(ctx *parsergen.ExpressionAtomPredicateContext) interface{} {
	text := GetOriginalText(ctx)
	if ctx.LOCAL_ID() != nil || ctx.VAR_ASSIGN() != nil {
		return fmt.Errorf("assignment not supported: %s", text)
	}
	n, err := v.getExpressionAtom(ctx.ExpressionAtom())
	if err != nil {
		return err
	}
	if n == nil {
		return fmt.Errorf("failed to parse expression atom predicate: %s", text)
	}
	expr := planner.NewNodeExpressionAtomPredicate(text, n)
	return planner.NewNodePredicate(text, planner.WithNodeExpressionAtomPredicate(expr))
}

func (v *AstBuilder) VisitBinaryComparisonPredicate(ctx *parsergen.BinaryComparisonPredicateContext) interface{} {
	text := GetOriginalText(ctx)

	op := ctx.ComparisonOperator().Accept(v).(planner.ComparisonOperator)

	leftCtx, rightCtx := ctx.GetLeft(), ctx.GetRight()

	left, err := v.getPredicate(leftCtx)
	if err != nil {
		return err
	}
	if left == nil {
		return fmt.Errorf("failed to parse left predicate: %s", GetOriginalText(leftCtx))
	}

	right, err := v.getPredicate(rightCtx)
	if err != nil {
		return err
	}
	if right == nil {
		return fmt.Errorf("failed to parse right predicate: %s", GetOriginalText(rightCtx))
	}

	expr := planner.NewNodeBinaryComparisonPredicate(text, left, right, op)
	return planner.NewNodePredicate(text, planner.WithNodeBinaryComparisonPredicate(expr))
}

func (v *AstBuilder) VisitInPredicate(ctx *parsergen.InPredicateContext) interface{} {
	text := GetOriginalText(ctx)

	op := planner.InOperatorIn
	if ctx.NOT() != nil {
		op = planner.InOperatorNotIn
	}

	predicateCtx := ctx.Predicate()
	n, err := v.getPredicate(predicateCtx)
	if err != nil {
		return err
	}
	if n == nil {
		return fmt.Errorf("failed to parse left in predicate: %s", GetOriginalText(predicateCtx))
	}

	expressionsCtx := ctx.Expressions()
	r := expressionsCtx.Accept(v)
	if err := GetError(r); err != nil {
		return err
	}
	exprs := GetExpressions(r)
	if exprs == nil {
		return fmt.Errorf("failed to parse in expressions: %s", GetOriginalText(expressionsCtx))
	}

	expr := planner.NewNodeInPredicate(text, n, exprs, op)
	return planner.NewNodePredicate(text, planner.WithInPredicate(expr))
}

func (v *AstBuilder) VisitConstantExpressionAtom(ctx *parsergen.ConstantExpressionAtomContext) interface{} {
	text := GetOriginalText(ctx)
	constantCtx := ctx.Constant().(*parsergen.ConstantContext)
	r := constantCtx.Accept(v)
	if err := GetError(r); err != nil {
		return err
	}
	var opts []planner.NodeConstantOption
	switch t := r.(type) {
	case int64:
		opts = append(opts, planner.WithDecimalLiteral(t))
	case string:
		opts = append(opts, planner.WithStringLiteral(t))
	case float64:
		opts = append(opts, planner.WithRealLiteral(t))
	case bool:
		opts = append(opts, planner.WithBooleanLiteral(t))
	}
	c := planner.NewNodeConstant(GetOriginalText(constantCtx), opts...)
	return planner.NewNodeExpressionAtom(text, planner.ExpressionAtomWithConstant(c))
}

func (v *AstBuilder) VisitFullColumnNameExpressionAtom(ctx *parsergen.FullColumnNameExpressionAtomContext) interface{} {
	text := GetOriginalText(ctx)
	fullColumnNameCtx := ctx.FullColumnName()
	r := fullColumnNameCtx.Accept(v)
	if err := GetError(r); err != nil {
		return err
	}
	n := planner.NewNodeFullColumnName(GetOriginalText(fullColumnNameCtx), r.(string))
	return planner.NewNodeExpressionAtom(text, planner.ExpressionAtomWithFullColumnName(n))
}

func (v *AstBuilder) VisitUnaryExpressionAtom(ctx *parsergen.UnaryExpressionAtomContext) interface{} {
	text := GetOriginalText(ctx)
	op := ctx.UnaryOperator().Accept(v).(planner.UnaryOperator)
	expressionAtomCtx := ctx.ExpressionAtom()
	r := expressionAtomCtx.Accept(v)
	if err := GetError(r); err != nil {
		return err
	}
	n := GetExpressionAtom(r)
	expr := planner.NewNodeUnaryExpressionAtom(GetOriginalText(expressionAtomCtx), n, op)
	return planner.NewNodeExpressionAtom(text, planner.ExpressionAtomWithUnaryExpr(expr))
}

func (v *AstBuilder) VisitNestedExpressionAtom(ctx *parsergen.NestedExpressionAtomContext) interface{} {
	text := GetOriginalText(ctx)
	expressionsCtx := ctx.AllExpression()
	expressions := make([]*planner.NodeExpression, 0, len(expressionsCtx))
	for _, expressionCtx := range expressionsCtx {
		n, err := v.getExpression(expressionCtx)
		if err != nil {
			return err
		}
		expressions = append(expressions, n)
	}
	expr := planner.NewNodeNestedExpressionAtom(text, expressions)
	return planner.NewNodeExpressionAtom(text, planner.ExpressionAtomWithNestedExpr(expr))
}

func (v *AstBuilder) VisitUnaryOperator(ctx *parsergen.UnaryOperatorContext) interface{} {
	op := planner.UnaryOperatorUnknown
	if ctx.EXCLAMATION_SYMBOL() != nil {
		op = planner.UnaryOperatorExclamationSymbol
	}
	if ctx.BIT_NOT_OP() != nil {
		op = planner.UnaryOperatorTilde
	}
	if ctx.PLUS() != nil {
		op = planner.UnaryOperatorPositive
	}
	if ctx.MINUS() != nil {
		op = planner.UnaryOperatorNegative
	}
	if ctx.NOT() != nil {
		op = planner.UnaryOperatorNot
	}
	return op
}

func (v *AstBuilder) VisitComparisonOperator(ctx *parsergen.ComparisonOperatorContext) interface{} {
	op := planner.ComparisonOperatorUnknown
	if ctx.EXCLAMATION_SYMBOL() != nil {
		op = planner.ComparisonOperatorNotEqual
	}
	if ctx.EQUAL_SYMBOL() != nil {
		op |= planner.ComparisonOperatorEqual
	}
	if ctx.LESS_SYMBOL() != nil {
		op |= planner.ComparisonOperatorLessThan
	}
	if ctx.GREATER_SYMBOL() != nil {
		op |= planner.ComparisonOperatorGreaterThan
	}
	return op
}

func (v *AstBuilder) VisitLogicalOperator(ctx *parsergen.LogicalOperatorContext) interface{} {
	op := planner.LogicalOperatorUnknown
	if ctx.AND() != nil {
		op = planner.LogicalOperatorAnd
	}
	if len(ctx.AllBIT_AND_OP()) != 0 {
		op = planner.LogicalOperatorAnd
	}
	if ctx.OR() != nil {
		op = planner.LogicalOperatorOr
	}
	if len(ctx.AllBIT_OR_OP()) != 0 {
		op = planner.LogicalOperatorOr
	}
	return op
}

func (v *AstBuilder) Build(ctx antlr.ParserRuleContext) (planner.Node, error) {
	r := ctx.Accept(v)
	if err := GetError(r); err != nil {
		return nil, err
	}
	if n := GetNode(r); n != nil {
		return n, nil
	}
	return nil, fmt.Errorf("failed to parse ast tree: %s", GetOriginalText(ctx))
}

func NewAstBuilder(opts ...AstBuilderOption) *AstBuilder {
	v := &AstBuilder{}
	v.apply(opts...)
	return v
}
