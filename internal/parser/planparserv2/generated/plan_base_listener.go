// Code generated from Plan.g4 by ANTLR 4.13.2. DO NOT EDIT.

package planparserv2 // Plan
import "github.com/antlr4-go/antlr/v4"

// BasePlanListener is a complete listener for a parse tree produced by PlanParser.
type BasePlanListener struct{}

var _ PlanListener = &BasePlanListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BasePlanListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BasePlanListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BasePlanListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BasePlanListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterJSONIdentifier is called when production JSONIdentifier is entered.
func (s *BasePlanListener) EnterJSONIdentifier(ctx *JSONIdentifierContext) {}

// ExitJSONIdentifier is called when production JSONIdentifier is exited.
func (s *BasePlanListener) ExitJSONIdentifier(ctx *JSONIdentifierContext) {}

// EnterRandomSample is called when production RandomSample is entered.
func (s *BasePlanListener) EnterRandomSample(ctx *RandomSampleContext) {}

// ExitRandomSample is called when production RandomSample is exited.
func (s *BasePlanListener) ExitRandomSample(ctx *RandomSampleContext) {}

// EnterParens is called when production Parens is entered.
func (s *BasePlanListener) EnterParens(ctx *ParensContext) {}

// ExitParens is called when production Parens is exited.
func (s *BasePlanListener) ExitParens(ctx *ParensContext) {}

// EnterString is called when production String is entered.
func (s *BasePlanListener) EnterString(ctx *StringContext) {}

// ExitString is called when production String is exited.
func (s *BasePlanListener) ExitString(ctx *StringContext) {}

// EnterFloating is called when production Floating is entered.
func (s *BasePlanListener) EnterFloating(ctx *FloatingContext) {}

// ExitFloating is called when production Floating is exited.
func (s *BasePlanListener) ExitFloating(ctx *FloatingContext) {}

// EnterJSONContainsAll is called when production JSONContainsAll is entered.
func (s *BasePlanListener) EnterJSONContainsAll(ctx *JSONContainsAllContext) {}

// ExitJSONContainsAll is called when production JSONContainsAll is exited.
func (s *BasePlanListener) ExitJSONContainsAll(ctx *JSONContainsAllContext) {}

// EnterLogicalOr is called when production LogicalOr is entered.
func (s *BasePlanListener) EnterLogicalOr(ctx *LogicalOrContext) {}

// ExitLogicalOr is called when production LogicalOr is exited.
func (s *BasePlanListener) ExitLogicalOr(ctx *LogicalOrContext) {}

// EnterIsNotNull is called when production IsNotNull is entered.
func (s *BasePlanListener) EnterIsNotNull(ctx *IsNotNullContext) {}

// ExitIsNotNull is called when production IsNotNull is exited.
func (s *BasePlanListener) ExitIsNotNull(ctx *IsNotNullContext) {}

// EnterMulDivMod is called when production MulDivMod is entered.
func (s *BasePlanListener) EnterMulDivMod(ctx *MulDivModContext) {}

// ExitMulDivMod is called when production MulDivMod is exited.
func (s *BasePlanListener) ExitMulDivMod(ctx *MulDivModContext) {}

// EnterIdentifier is called when production Identifier is entered.
func (s *BasePlanListener) EnterIdentifier(ctx *IdentifierContext) {}

// ExitIdentifier is called when production Identifier is exited.
func (s *BasePlanListener) ExitIdentifier(ctx *IdentifierContext) {}

// EnterSTIntersects is called when production STIntersects is entered.
func (s *BasePlanListener) EnterSTIntersects(ctx *STIntersectsContext) {}

// ExitSTIntersects is called when production STIntersects is exited.
func (s *BasePlanListener) ExitSTIntersects(ctx *STIntersectsContext) {}

// EnterLike is called when production Like is entered.
func (s *BasePlanListener) EnterLike(ctx *LikeContext) {}

// ExitLike is called when production Like is exited.
func (s *BasePlanListener) ExitLike(ctx *LikeContext) {}

// EnterLogicalAnd is called when production LogicalAnd is entered.
func (s *BasePlanListener) EnterLogicalAnd(ctx *LogicalAndContext) {}

// ExitLogicalAnd is called when production LogicalAnd is exited.
func (s *BasePlanListener) ExitLogicalAnd(ctx *LogicalAndContext) {}

// EnterTemplateVariable is called when production TemplateVariable is entered.
func (s *BasePlanListener) EnterTemplateVariable(ctx *TemplateVariableContext) {}

// ExitTemplateVariable is called when production TemplateVariable is exited.
func (s *BasePlanListener) ExitTemplateVariable(ctx *TemplateVariableContext) {}

// EnterEquality is called when production Equality is entered.
func (s *BasePlanListener) EnterEquality(ctx *EqualityContext) {}

// ExitEquality is called when production Equality is exited.
func (s *BasePlanListener) ExitEquality(ctx *EqualityContext) {}

// EnterBoolean is called when production Boolean is entered.
func (s *BasePlanListener) EnterBoolean(ctx *BooleanContext) {}

// ExitBoolean is called when production Boolean is exited.
func (s *BasePlanListener) ExitBoolean(ctx *BooleanContext) {}

// EnterSTDWithin is called when production STDWithin is entered.
func (s *BasePlanListener) EnterSTDWithin(ctx *STDWithinContext) {}

// ExitSTDWithin is called when production STDWithin is exited.
func (s *BasePlanListener) ExitSTDWithin(ctx *STDWithinContext) {}

// EnterShift is called when production Shift is entered.
func (s *BasePlanListener) EnterShift(ctx *ShiftContext) {}

// ExitShift is called when production Shift is exited.
func (s *BasePlanListener) ExitShift(ctx *ShiftContext) {}

// EnterCall is called when production Call is entered.
func (s *BasePlanListener) EnterCall(ctx *CallContext) {}

// ExitCall is called when production Call is exited.
func (s *BasePlanListener) ExitCall(ctx *CallContext) {}

// EnterSTCrosses is called when production STCrosses is entered.
func (s *BasePlanListener) EnterSTCrosses(ctx *STCrossesContext) {}

// ExitSTCrosses is called when production STCrosses is exited.
func (s *BasePlanListener) ExitSTCrosses(ctx *STCrossesContext) {}

// EnterReverseRange is called when production ReverseRange is entered.
func (s *BasePlanListener) EnterReverseRange(ctx *ReverseRangeContext) {}

// ExitReverseRange is called when production ReverseRange is exited.
func (s *BasePlanListener) ExitReverseRange(ctx *ReverseRangeContext) {}

// EnterBitOr is called when production BitOr is entered.
func (s *BasePlanListener) EnterBitOr(ctx *BitOrContext) {}

// ExitBitOr is called when production BitOr is exited.
func (s *BasePlanListener) ExitBitOr(ctx *BitOrContext) {}

// EnterEmptyArray is called when production EmptyArray is entered.
func (s *BasePlanListener) EnterEmptyArray(ctx *EmptyArrayContext) {}

// ExitEmptyArray is called when production EmptyArray is exited.
func (s *BasePlanListener) ExitEmptyArray(ctx *EmptyArrayContext) {}

// EnterAddSub is called when production AddSub is entered.
func (s *BasePlanListener) EnterAddSub(ctx *AddSubContext) {}

// ExitAddSub is called when production AddSub is exited.
func (s *BasePlanListener) ExitAddSub(ctx *AddSubContext) {}

// EnterPhraseMatch is called when production PhraseMatch is entered.
func (s *BasePlanListener) EnterPhraseMatch(ctx *PhraseMatchContext) {}

// ExitPhraseMatch is called when production PhraseMatch is exited.
func (s *BasePlanListener) ExitPhraseMatch(ctx *PhraseMatchContext) {}

// EnterRelational is called when production Relational is entered.
func (s *BasePlanListener) EnterRelational(ctx *RelationalContext) {}

// ExitRelational is called when production Relational is exited.
func (s *BasePlanListener) ExitRelational(ctx *RelationalContext) {}

// EnterArrayLength is called when production ArrayLength is entered.
func (s *BasePlanListener) EnterArrayLength(ctx *ArrayLengthContext) {}

// ExitArrayLength is called when production ArrayLength is exited.
func (s *BasePlanListener) ExitArrayLength(ctx *ArrayLengthContext) {}

// EnterTextMatch is called when production TextMatch is entered.
func (s *BasePlanListener) EnterTextMatch(ctx *TextMatchContext) {}

// ExitTextMatch is called when production TextMatch is exited.
func (s *BasePlanListener) ExitTextMatch(ctx *TextMatchContext) {}

// EnterSTTouches is called when production STTouches is entered.
func (s *BasePlanListener) EnterSTTouches(ctx *STTouchesContext) {}

// ExitSTTouches is called when production STTouches is exited.
func (s *BasePlanListener) ExitSTTouches(ctx *STTouchesContext) {}

// EnterSTContains is called when production STContains is entered.
func (s *BasePlanListener) EnterSTContains(ctx *STContainsContext) {}

// ExitSTContains is called when production STContains is exited.
func (s *BasePlanListener) ExitSTContains(ctx *STContainsContext) {}

// EnterTerm is called when production Term is entered.
func (s *BasePlanListener) EnterTerm(ctx *TermContext) {}

// ExitTerm is called when production Term is exited.
func (s *BasePlanListener) ExitTerm(ctx *TermContext) {}

// EnterJSONContains is called when production JSONContains is entered.
func (s *BasePlanListener) EnterJSONContains(ctx *JSONContainsContext) {}

// ExitJSONContains is called when production JSONContains is exited.
func (s *BasePlanListener) ExitJSONContains(ctx *JSONContainsContext) {}

// EnterSTWithin is called when production STWithin is entered.
func (s *BasePlanListener) EnterSTWithin(ctx *STWithinContext) {}

// ExitSTWithin is called when production STWithin is exited.
func (s *BasePlanListener) ExitSTWithin(ctx *STWithinContext) {}

// EnterRange is called when production Range is entered.
func (s *BasePlanListener) EnterRange(ctx *RangeContext) {}

// ExitRange is called when production Range is exited.
func (s *BasePlanListener) ExitRange(ctx *RangeContext) {}

// EnterUnary is called when production Unary is entered.
func (s *BasePlanListener) EnterUnary(ctx *UnaryContext) {}

// ExitUnary is called when production Unary is exited.
func (s *BasePlanListener) ExitUnary(ctx *UnaryContext) {}

// EnterTimestamptzCompare is called when production TimestamptzCompare is entered.
func (s *BasePlanListener) EnterTimestamptzCompare(ctx *TimestamptzCompareContext) {}

// ExitTimestamptzCompare is called when production TimestamptzCompare is exited.
func (s *BasePlanListener) ExitTimestamptzCompare(ctx *TimestamptzCompareContext) {}

// EnterInteger is called when production Integer is entered.
func (s *BasePlanListener) EnterInteger(ctx *IntegerContext) {}

// ExitInteger is called when production Integer is exited.
func (s *BasePlanListener) ExitInteger(ctx *IntegerContext) {}

// EnterArray is called when production Array is entered.
func (s *BasePlanListener) EnterArray(ctx *ArrayContext) {}

// ExitArray is called when production Array is exited.
func (s *BasePlanListener) ExitArray(ctx *ArrayContext) {}

// EnterJSONContainsAny is called when production JSONContainsAny is entered.
func (s *BasePlanListener) EnterJSONContainsAny(ctx *JSONContainsAnyContext) {}

// ExitJSONContainsAny is called when production JSONContainsAny is exited.
func (s *BasePlanListener) ExitJSONContainsAny(ctx *JSONContainsAnyContext) {}

// EnterBitXor is called when production BitXor is entered.
func (s *BasePlanListener) EnterBitXor(ctx *BitXorContext) {}

// ExitBitXor is called when production BitXor is exited.
func (s *BasePlanListener) ExitBitXor(ctx *BitXorContext) {}

// EnterExists is called when production Exists is entered.
func (s *BasePlanListener) EnterExists(ctx *ExistsContext) {}

// ExitExists is called when production Exists is exited.
func (s *BasePlanListener) ExitExists(ctx *ExistsContext) {}

// EnterBitAnd is called when production BitAnd is entered.
func (s *BasePlanListener) EnterBitAnd(ctx *BitAndContext) {}

// ExitBitAnd is called when production BitAnd is exited.
func (s *BasePlanListener) ExitBitAnd(ctx *BitAndContext) {}

// EnterSTEuqals is called when production STEuqals is entered.
func (s *BasePlanListener) EnterSTEuqals(ctx *STEuqalsContext) {}

// ExitSTEuqals is called when production STEuqals is exited.
func (s *BasePlanListener) ExitSTEuqals(ctx *STEuqalsContext) {}

// EnterIsNull is called when production IsNull is entered.
func (s *BasePlanListener) EnterIsNull(ctx *IsNullContext) {}

// ExitIsNull is called when production IsNull is exited.
func (s *BasePlanListener) ExitIsNull(ctx *IsNullContext) {}

// EnterPower is called when production Power is entered.
func (s *BasePlanListener) EnterPower(ctx *PowerContext) {}

// ExitPower is called when production Power is exited.
func (s *BasePlanListener) ExitPower(ctx *PowerContext) {}

// EnterSTOverlaps is called when production STOverlaps is entered.
func (s *BasePlanListener) EnterSTOverlaps(ctx *STOverlapsContext) {}

// ExitSTOverlaps is called when production STOverlaps is exited.
func (s *BasePlanListener) ExitSTOverlaps(ctx *STOverlapsContext) {}

// EnterTextMatchOption is called when production textMatchOption is entered.
func (s *BasePlanListener) EnterTextMatchOption(ctx *TextMatchOptionContext) {}

// ExitTextMatchOption is called when production textMatchOption is exited.
func (s *BasePlanListener) ExitTextMatchOption(ctx *TextMatchOptionContext) {}
