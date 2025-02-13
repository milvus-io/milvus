// Code generated from Plan.g4 by ANTLR 4.13.2. DO NOT EDIT.

package planparserv2 // Plan
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by PlanParser.
type PlanVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by PlanParser#expr.
	VisitExpr(ctx *ExprContext) interface{}

	// Visit a parse tree produced by PlanParser#logicalOrExpr.
	VisitLogicalOrExpr(ctx *LogicalOrExprContext) interface{}

	// Visit a parse tree produced by PlanParser#logicalAndExpr.
	VisitLogicalAndExpr(ctx *LogicalAndExprContext) interface{}

	// Visit a parse tree produced by PlanParser#equalityExpr.
	VisitEqualityExpr(ctx *EqualityExprContext) interface{}

	// Visit a parse tree produced by PlanParser#comparisonExpr.
	VisitComparisonExpr(ctx *ComparisonExprContext) interface{}

	// Visit a parse tree produced by PlanParser#compOp.
	VisitCompOp(ctx *CompOpContext) interface{}

	// Visit a parse tree produced by PlanParser#additiveExpr.
	VisitAdditiveExpr(ctx *AdditiveExprContext) interface{}

	// Visit a parse tree produced by PlanParser#multiplicativeExpr.
	VisitMultiplicativeExpr(ctx *MultiplicativeExprContext) interface{}

	// Visit a parse tree produced by PlanParser#powerExpr.
	VisitPowerExpr(ctx *PowerExprContext) interface{}

	// Visit a parse tree produced by PlanParser#unaryExpr.
	VisitUnaryExpr(ctx *UnaryExprContext) interface{}

	// Visit a parse tree produced by PlanParser#postfixExpr.
	VisitPostfixExpr(ctx *PostfixExprContext) interface{}

	// Visit a parse tree produced by PlanParser#FunctionCall.
	VisitFunctionCall(ctx *FunctionCallContext) interface{}

	// Visit a parse tree produced by PlanParser#IsNull.
	VisitIsNull(ctx *IsNullContext) interface{}

	// Visit a parse tree produced by PlanParser#IsNotNull.
	VisitIsNotNull(ctx *IsNotNullContext) interface{}

	// Visit a parse tree produced by PlanParser#argumentList.
	VisitArgumentList(ctx *ArgumentListContext) interface{}

	// Visit a parse tree produced by PlanParser#Integer.
	VisitInteger(ctx *IntegerContext) interface{}

	// Visit a parse tree produced by PlanParser#Floating.
	VisitFloating(ctx *FloatingContext) interface{}

	// Visit a parse tree produced by PlanParser#Boolean.
	VisitBoolean(ctx *BooleanContext) interface{}

	// Visit a parse tree produced by PlanParser#String.
	VisitString(ctx *StringContext) interface{}

	// Visit a parse tree produced by PlanParser#Identifier.
	VisitIdentifier(ctx *IdentifierContext) interface{}

	// Visit a parse tree produced by PlanParser#Meta.
	VisitMeta(ctx *MetaContext) interface{}

	// Visit a parse tree produced by PlanParser#JSONIdentifier.
	VisitJSONIdentifier(ctx *JSONIdentifierContext) interface{}

	// Visit a parse tree produced by PlanParser#TemplateVariable.
	VisitTemplateVariable(ctx *TemplateVariableContext) interface{}

	// Visit a parse tree produced by PlanParser#Parens.
	VisitParens(ctx *ParensContext) interface{}

	// Visit a parse tree produced by PlanParser#Array.
	VisitArray(ctx *ArrayContext) interface{}

	// Visit a parse tree produced by PlanParser#EmptyArray.
	VisitEmptyArray(ctx *EmptyArrayContext) interface{}

	// Visit a parse tree produced by PlanParser#TextMatch.
	VisitTextMatch(ctx *TextMatchContext) interface{}

	// Visit a parse tree produced by PlanParser#PhraseMatch.
	VisitPhraseMatch(ctx *PhraseMatchContext) interface{}

	// Visit a parse tree produced by PlanParser#JSONContains.
	VisitJSONContains(ctx *JSONContainsContext) interface{}

	// Visit a parse tree produced by PlanParser#JSONContainsAll.
	VisitJSONContainsAll(ctx *JSONContainsAllContext) interface{}

	// Visit a parse tree produced by PlanParser#JSONContainsAny.
	VisitJSONContainsAny(ctx *JSONContainsAnyContext) interface{}

	// Visit a parse tree produced by PlanParser#ArrayLength.
	VisitArrayLength(ctx *ArrayLengthContext) interface{}
}
