// Code generated from Plan.g4 by ANTLR 4.13.2. DO NOT EDIT.

package planparserv2 // Plan
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by PlanParser.
type PlanVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by PlanParser#JSONIdentifier.
	VisitJSONIdentifier(ctx *JSONIdentifierContext) interface{}

	// Visit a parse tree produced by PlanParser#RandomSample.
	VisitRandomSample(ctx *RandomSampleContext) interface{}

	// Visit a parse tree produced by PlanParser#Parens.
	VisitParens(ctx *ParensContext) interface{}

	// Visit a parse tree produced by PlanParser#String.
	VisitString(ctx *StringContext) interface{}

	// Visit a parse tree produced by PlanParser#Floating.
	VisitFloating(ctx *FloatingContext) interface{}

	// Visit a parse tree produced by PlanParser#JSONContainsAll.
	VisitJSONContainsAll(ctx *JSONContainsAllContext) interface{}

	// Visit a parse tree produced by PlanParser#LogicalOr.
	VisitLogicalOr(ctx *LogicalOrContext) interface{}

	// Visit a parse tree produced by PlanParser#IsNotNull.
	VisitIsNotNull(ctx *IsNotNullContext) interface{}

	// Visit a parse tree produced by PlanParser#MulDivMod.
	VisitMulDivMod(ctx *MulDivModContext) interface{}

	// Visit a parse tree produced by PlanParser#Identifier.
	VisitIdentifier(ctx *IdentifierContext) interface{}

	// Visit a parse tree produced by PlanParser#Like.
	VisitLike(ctx *LikeContext) interface{}

	// Visit a parse tree produced by PlanParser#LogicalAnd.
	VisitLogicalAnd(ctx *LogicalAndContext) interface{}

	// Visit a parse tree produced by PlanParser#TemplateVariable.
	VisitTemplateVariable(ctx *TemplateVariableContext) interface{}

	// Visit a parse tree produced by PlanParser#Equality.
	VisitEquality(ctx *EqualityContext) interface{}

	// Visit a parse tree produced by PlanParser#Boolean.
	VisitBoolean(ctx *BooleanContext) interface{}

	// Visit a parse tree produced by PlanParser#Shift.
	VisitShift(ctx *ShiftContext) interface{}

	// Visit a parse tree produced by PlanParser#Call.
	VisitCall(ctx *CallContext) interface{}

	// Visit a parse tree produced by PlanParser#ReverseRange.
	VisitReverseRange(ctx *ReverseRangeContext) interface{}

	// Visit a parse tree produced by PlanParser#BitOr.
	VisitBitOr(ctx *BitOrContext) interface{}

	// Visit a parse tree produced by PlanParser#EmptyArray.
	VisitEmptyArray(ctx *EmptyArrayContext) interface{}

	// Visit a parse tree produced by PlanParser#AddSub.
	VisitAddSub(ctx *AddSubContext) interface{}

	// Visit a parse tree produced by PlanParser#PhraseMatch.
	VisitPhraseMatch(ctx *PhraseMatchContext) interface{}

	// Visit a parse tree produced by PlanParser#Relational.
	VisitRelational(ctx *RelationalContext) interface{}

	// Visit a parse tree produced by PlanParser#ArrayLength.
	VisitArrayLength(ctx *ArrayLengthContext) interface{}

	// Visit a parse tree produced by PlanParser#TextMatch.
	VisitTextMatch(ctx *TextMatchContext) interface{}

	// Visit a parse tree produced by PlanParser#Term.
	VisitTerm(ctx *TermContext) interface{}

	// Visit a parse tree produced by PlanParser#JSONContains.
	VisitJSONContains(ctx *JSONContainsContext) interface{}

	// Visit a parse tree produced by PlanParser#Range.
	VisitRange(ctx *RangeContext) interface{}

	// Visit a parse tree produced by PlanParser#Unary.
	VisitUnary(ctx *UnaryContext) interface{}

	// Visit a parse tree produced by PlanParser#Integer.
	VisitInteger(ctx *IntegerContext) interface{}

	// Visit a parse tree produced by PlanParser#Array.
	VisitArray(ctx *ArrayContext) interface{}

	// Visit a parse tree produced by PlanParser#JSONContainsAny.
	VisitJSONContainsAny(ctx *JSONContainsAnyContext) interface{}

	// Visit a parse tree produced by PlanParser#BitXor.
	VisitBitXor(ctx *BitXorContext) interface{}

	// Visit a parse tree produced by PlanParser#Exists.
	VisitExists(ctx *ExistsContext) interface{}

	// Visit a parse tree produced by PlanParser#BitAnd.
	VisitBitAnd(ctx *BitAndContext) interface{}

	// Visit a parse tree produced by PlanParser#IsNull.
	VisitIsNull(ctx *IsNullContext) interface{}

	// Visit a parse tree produced by PlanParser#Power.
	VisitPower(ctx *PowerContext) interface{}
}
