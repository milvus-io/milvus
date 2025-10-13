// Code generated from Plan.g4 by ANTLR 4.13.2. DO NOT EDIT.

package planparserv2 // Plan
import "github.com/antlr4-go/antlr/v4"

// PlanListener is a complete listener for a parse tree produced by PlanParser.
type PlanListener interface {
	antlr.ParseTreeListener

	// EnterJSONIdentifier is called when entering the JSONIdentifier production.
	EnterJSONIdentifier(c *JSONIdentifierContext)

	// EnterRandomSample is called when entering the RandomSample production.
	EnterRandomSample(c *RandomSampleContext)

	// EnterParens is called when entering the Parens production.
	EnterParens(c *ParensContext)

	// EnterString is called when entering the String production.
	EnterString(c *StringContext)

	// EnterFloating is called when entering the Floating production.
	EnterFloating(c *FloatingContext)

	// EnterJSONContainsAll is called when entering the JSONContainsAll production.
	EnterJSONContainsAll(c *JSONContainsAllContext)

	// EnterLogicalOr is called when entering the LogicalOr production.
	EnterLogicalOr(c *LogicalOrContext)

	// EnterIsNotNull is called when entering the IsNotNull production.
	EnterIsNotNull(c *IsNotNullContext)

	// EnterMulDivMod is called when entering the MulDivMod production.
	EnterMulDivMod(c *MulDivModContext)

	// EnterIdentifier is called when entering the Identifier production.
	EnterIdentifier(c *IdentifierContext)

	// EnterSTIntersects is called when entering the STIntersects production.
	EnterSTIntersects(c *STIntersectsContext)

	// EnterLike is called when entering the Like production.
	EnterLike(c *LikeContext)

	// EnterLogicalAnd is called when entering the LogicalAnd production.
	EnterLogicalAnd(c *LogicalAndContext)

	// EnterTemplateVariable is called when entering the TemplateVariable production.
	EnterTemplateVariable(c *TemplateVariableContext)

	// EnterEquality is called when entering the Equality production.
	EnterEquality(c *EqualityContext)

	// EnterBoolean is called when entering the Boolean production.
	EnterBoolean(c *BooleanContext)

	// EnterSTDWithin is called when entering the STDWithin production.
	EnterSTDWithin(c *STDWithinContext)

	// EnterShift is called when entering the Shift production.
	EnterShift(c *ShiftContext)

	// EnterCall is called when entering the Call production.
	EnterCall(c *CallContext)

	// EnterSTCrosses is called when entering the STCrosses production.
	EnterSTCrosses(c *STCrossesContext)

	// EnterReverseRange is called when entering the ReverseRange production.
	EnterReverseRange(c *ReverseRangeContext)

	// EnterBitOr is called when entering the BitOr production.
	EnterBitOr(c *BitOrContext)

	// EnterEmptyArray is called when entering the EmptyArray production.
	EnterEmptyArray(c *EmptyArrayContext)

	// EnterAddSub is called when entering the AddSub production.
	EnterAddSub(c *AddSubContext)

	// EnterPhraseMatch is called when entering the PhraseMatch production.
	EnterPhraseMatch(c *PhraseMatchContext)

	// EnterRelational is called when entering the Relational production.
	EnterRelational(c *RelationalContext)

	// EnterArrayLength is called when entering the ArrayLength production.
	EnterArrayLength(c *ArrayLengthContext)

	// EnterTextMatch is called when entering the TextMatch production.
	EnterTextMatch(c *TextMatchContext)

	// EnterSTTouches is called when entering the STTouches production.
	EnterSTTouches(c *STTouchesContext)

	// EnterSTContains is called when entering the STContains production.
	EnterSTContains(c *STContainsContext)

	// EnterTerm is called when entering the Term production.
	EnterTerm(c *TermContext)

	// EnterJSONContains is called when entering the JSONContains production.
	EnterJSONContains(c *JSONContainsContext)

	// EnterSTWithin is called when entering the STWithin production.
	EnterSTWithin(c *STWithinContext)

	// EnterRange is called when entering the Range production.
	EnterRange(c *RangeContext)

	// EnterUnary is called when entering the Unary production.
	EnterUnary(c *UnaryContext)

	// EnterTimestamptzCompare is called when entering the TimestamptzCompare production.
	EnterTimestamptzCompare(c *TimestamptzCompareContext)

	// EnterInteger is called when entering the Integer production.
	EnterInteger(c *IntegerContext)

	// EnterArray is called when entering the Array production.
	EnterArray(c *ArrayContext)

	// EnterJSONContainsAny is called when entering the JSONContainsAny production.
	EnterJSONContainsAny(c *JSONContainsAnyContext)

	// EnterBitXor is called when entering the BitXor production.
	EnterBitXor(c *BitXorContext)

	// EnterExists is called when entering the Exists production.
	EnterExists(c *ExistsContext)

	// EnterBitAnd is called when entering the BitAnd production.
	EnterBitAnd(c *BitAndContext)

	// EnterSTEuqals is called when entering the STEuqals production.
	EnterSTEuqals(c *STEuqalsContext)

	// EnterIsNull is called when entering the IsNull production.
	EnterIsNull(c *IsNullContext)

	// EnterPower is called when entering the Power production.
	EnterPower(c *PowerContext)

	// EnterSTOverlaps is called when entering the STOverlaps production.
	EnterSTOverlaps(c *STOverlapsContext)

	// EnterTextMatchOption is called when entering the textMatchOption production.
	EnterTextMatchOption(c *TextMatchOptionContext)

	// ExitJSONIdentifier is called when exiting the JSONIdentifier production.
	ExitJSONIdentifier(c *JSONIdentifierContext)

	// ExitRandomSample is called when exiting the RandomSample production.
	ExitRandomSample(c *RandomSampleContext)

	// ExitParens is called when exiting the Parens production.
	ExitParens(c *ParensContext)

	// ExitString is called when exiting the String production.
	ExitString(c *StringContext)

	// ExitFloating is called when exiting the Floating production.
	ExitFloating(c *FloatingContext)

	// ExitJSONContainsAll is called when exiting the JSONContainsAll production.
	ExitJSONContainsAll(c *JSONContainsAllContext)

	// ExitLogicalOr is called when exiting the LogicalOr production.
	ExitLogicalOr(c *LogicalOrContext)

	// ExitIsNotNull is called when exiting the IsNotNull production.
	ExitIsNotNull(c *IsNotNullContext)

	// ExitMulDivMod is called when exiting the MulDivMod production.
	ExitMulDivMod(c *MulDivModContext)

	// ExitIdentifier is called when exiting the Identifier production.
	ExitIdentifier(c *IdentifierContext)

	// ExitSTIntersects is called when exiting the STIntersects production.
	ExitSTIntersects(c *STIntersectsContext)

	// ExitLike is called when exiting the Like production.
	ExitLike(c *LikeContext)

	// ExitLogicalAnd is called when exiting the LogicalAnd production.
	ExitLogicalAnd(c *LogicalAndContext)

	// ExitTemplateVariable is called when exiting the TemplateVariable production.
	ExitTemplateVariable(c *TemplateVariableContext)

	// ExitEquality is called when exiting the Equality production.
	ExitEquality(c *EqualityContext)

	// ExitBoolean is called when exiting the Boolean production.
	ExitBoolean(c *BooleanContext)

	// ExitSTDWithin is called when exiting the STDWithin production.
	ExitSTDWithin(c *STDWithinContext)

	// ExitShift is called when exiting the Shift production.
	ExitShift(c *ShiftContext)

	// ExitCall is called when exiting the Call production.
	ExitCall(c *CallContext)

	// ExitSTCrosses is called when exiting the STCrosses production.
	ExitSTCrosses(c *STCrossesContext)

	// ExitReverseRange is called when exiting the ReverseRange production.
	ExitReverseRange(c *ReverseRangeContext)

	// ExitBitOr is called when exiting the BitOr production.
	ExitBitOr(c *BitOrContext)

	// ExitEmptyArray is called when exiting the EmptyArray production.
	ExitEmptyArray(c *EmptyArrayContext)

	// ExitAddSub is called when exiting the AddSub production.
	ExitAddSub(c *AddSubContext)

	// ExitPhraseMatch is called when exiting the PhraseMatch production.
	ExitPhraseMatch(c *PhraseMatchContext)

	// ExitRelational is called when exiting the Relational production.
	ExitRelational(c *RelationalContext)

	// ExitArrayLength is called when exiting the ArrayLength production.
	ExitArrayLength(c *ArrayLengthContext)

	// ExitTextMatch is called when exiting the TextMatch production.
	ExitTextMatch(c *TextMatchContext)

	// ExitSTTouches is called when exiting the STTouches production.
	ExitSTTouches(c *STTouchesContext)

	// ExitSTContains is called when exiting the STContains production.
	ExitSTContains(c *STContainsContext)

	// ExitTerm is called when exiting the Term production.
	ExitTerm(c *TermContext)

	// ExitJSONContains is called when exiting the JSONContains production.
	ExitJSONContains(c *JSONContainsContext)

	// ExitSTWithin is called when exiting the STWithin production.
	ExitSTWithin(c *STWithinContext)

	// ExitRange is called when exiting the Range production.
	ExitRange(c *RangeContext)

	// ExitUnary is called when exiting the Unary production.
	ExitUnary(c *UnaryContext)

	// ExitTimestamptzCompare is called when exiting the TimestamptzCompare production.
	ExitTimestamptzCompare(c *TimestamptzCompareContext)

	// ExitInteger is called when exiting the Integer production.
	ExitInteger(c *IntegerContext)

	// ExitArray is called when exiting the Array production.
	ExitArray(c *ArrayContext)

	// ExitJSONContainsAny is called when exiting the JSONContainsAny production.
	ExitJSONContainsAny(c *JSONContainsAnyContext)

	// ExitBitXor is called when exiting the BitXor production.
	ExitBitXor(c *BitXorContext)

	// ExitExists is called when exiting the Exists production.
	ExitExists(c *ExistsContext)

	// ExitBitAnd is called when exiting the BitAnd production.
	ExitBitAnd(c *BitAndContext)

	// ExitSTEuqals is called when exiting the STEuqals production.
	ExitSTEuqals(c *STEuqalsContext)

	// ExitIsNull is called when exiting the IsNull production.
	ExitIsNull(c *IsNullContext)

	// ExitPower is called when exiting the Power production.
	ExitPower(c *PowerContext)

	// ExitSTOverlaps is called when exiting the STOverlaps production.
	ExitSTOverlaps(c *STOverlapsContext)

	// ExitTextMatchOption is called when exiting the textMatchOption production.
	ExitTextMatchOption(c *TextMatchOptionContext)
}
