// Code generated from Plan.g4 by ANTLR 4.13.2. DO NOT EDIT.

package planparserv2 // Plan
import "github.com/antlr4-go/antlr/v4"

type BasePlanVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BasePlanVisitor) VisitExpr(ctx *ExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitLogicalOrExpr(ctx *LogicalOrExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitLogicalAndExpr(ctx *LogicalAndExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitEqualityExpr(ctx *EqualityExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitComparisonExpr(ctx *ComparisonExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitCompOp(ctx *CompOpContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitAdditiveExpr(ctx *AdditiveExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitMultiplicativeExpr(ctx *MultiplicativeExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitPowerExpr(ctx *PowerExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitUnaryExpr(ctx *UnaryExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitPostfixExpr(ctx *PostfixExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitFunctionCall(ctx *FunctionCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitIsNull(ctx *IsNullContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitIsNotNull(ctx *IsNotNullContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitArgumentList(ctx *ArgumentListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitInteger(ctx *IntegerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitFloating(ctx *FloatingContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitBoolean(ctx *BooleanContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitString(ctx *StringContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitIdentifier(ctx *IdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitMeta(ctx *MetaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitJSONIdentifier(ctx *JSONIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitTemplateVariable(ctx *TemplateVariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitParens(ctx *ParensContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitArray(ctx *ArrayContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitEmptyArray(ctx *EmptyArrayContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitTextMatch(ctx *TextMatchContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitPhraseMatch(ctx *PhraseMatchContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitJSONContains(ctx *JSONContainsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitJSONContainsAll(ctx *JSONContainsAllContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitJSONContainsAny(ctx *JSONContainsAnyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitArrayLength(ctx *ArrayLengthContext) interface{} {
	return v.VisitChildren(ctx)
}
