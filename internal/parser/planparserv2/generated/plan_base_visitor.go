// Code generated from Plan.g4 by ANTLR 4.13.2. DO NOT EDIT.

package planparserv2 // Plan
import "github.com/antlr4-go/antlr/v4"

type BasePlanVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BasePlanVisitor) VisitJSONIdentifier(ctx *JSONIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitParens(ctx *ParensContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitString(ctx *StringContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitFloating(ctx *FloatingContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitJSONContainsAll(ctx *JSONContainsAllContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitLogicalOr(ctx *LogicalOrContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitMulDivMod(ctx *MulDivModContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitIdentifier(ctx *IdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitLike(ctx *LikeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitLogicalAnd(ctx *LogicalAndContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitTemplateVariable(ctx *TemplateVariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitEquality(ctx *EqualityContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitBoolean(ctx *BooleanContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitShift(ctx *ShiftContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitCall(ctx *CallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitReverseRange(ctx *ReverseRangeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitBitOr(ctx *BitOrContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitEmptyArray(ctx *EmptyArrayContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitAddSub(ctx *AddSubContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitPhraseMatch(ctx *PhraseMatchContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitRelational(ctx *RelationalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitArrayLength(ctx *ArrayLengthContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitTextMatch(ctx *TextMatchContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitTerm(ctx *TermContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitJSONContains(ctx *JSONContainsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitRange(ctx *RangeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitUnary(ctx *UnaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitInteger(ctx *IntegerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitArray(ctx *ArrayContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitJSONContainsAny(ctx *JSONContainsAnyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitBitXor(ctx *BitXorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitExists(ctx *ExistsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitBitAnd(ctx *BitAndContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePlanVisitor) VisitPower(ctx *PowerContext) interface{} {
	return v.VisitChildren(ctx)
}
