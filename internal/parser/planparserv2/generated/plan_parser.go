// Code generated from Plan.g4 by ANTLR 4.13.2. DO NOT EDIT.

package planparserv2 // Plan
import (
	"fmt"
	"strconv"
	"sync"

	"github.com/antlr4-go/antlr/v4"
)

// Suppress unused import errors
var _ = fmt.Printf
var _ = strconv.Itoa
var _ = sync.Once{}

type PlanParser struct {
	*antlr.BaseParser
}

var PlanParserStaticData struct {
	once                   sync.Once
	serializedATN          []int32
	LiteralNames           []string
	SymbolicNames          []string
	RuleNames              []string
	PredictionContextCache *antlr.PredictionContextCache
	atn                    *antlr.ATN
	decisionToDFA          []*antlr.DFA
}

func planParserInit() {
	staticData := &PlanParserStaticData
	staticData.LiteralNames = []string{
		"", "'('", "')'", "','", "'['", "']'", "'{'", "'}'", "'<'", "'<='",
		"'>'", "'>='", "'=='", "'!='", "", "", "", "", "'+'", "'-'", "'*'",
		"'/'", "'%'", "'**'", "'<<'", "'>>'", "'&'", "'|'", "'^'", "", "", "",
		"", "'~'", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "'$meta'",
	}
	staticData.SymbolicNames = []string{
		"", "", "", "", "", "", "LBRACE", "RBRACE", "LT", "LE", "GT", "GE",
		"EQ", "NE", "LIKE", "EXISTS", "TEXTMATCH", "PHRASEMATCH", "ADD", "SUB",
		"MUL", "DIV", "MOD", "POW", "SHL", "SHR", "BAND", "BOR", "BXOR", "AND",
		"OR", "ISNULL", "ISNOTNULL", "BNOT", "NOT", "IN", "EmptyArray", "JSONContains",
		"JSONContainsAll", "JSONContainsAny", "ArrayContains", "ArrayContainsAll",
		"ArrayContainsAny", "ArrayLength", "BooleanConstant", "IntegerConstant",
		"FloatingConstant", "Identifier", "Meta", "StringLiteral", "JSONIdentifier",
		"Whitespace", "Newline",
	}
	staticData.RuleNames = []string{
		"expr", "logicalOrExpr", "logicalAndExpr", "equalityExpr", "comparisonExpr",
		"compOp", "additiveExpr", "multiplicativeExpr", "powerExpr", "unaryExpr",
		"postfixExpr", "postfixOp", "argumentList", "primaryExpr",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 1, 52, 204, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7,
		4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 7,
		10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 1, 0, 1, 0, 1, 1, 1, 1, 1,
		1, 5, 1, 34, 8, 1, 10, 1, 12, 1, 37, 9, 1, 1, 2, 1, 2, 1, 2, 5, 2, 42,
		8, 2, 10, 2, 12, 2, 45, 9, 2, 1, 3, 1, 3, 1, 3, 5, 3, 50, 8, 3, 10, 3,
		12, 3, 53, 9, 3, 1, 4, 1, 4, 1, 4, 1, 4, 5, 4, 59, 8, 4, 10, 4, 12, 4,
		62, 9, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 3, 4, 70, 8, 4, 1, 4, 1,
		4, 1, 4, 1, 4, 3, 4, 76, 8, 4, 1, 5, 1, 5, 1, 6, 1, 6, 1, 6, 5, 6, 83,
		8, 6, 10, 6, 12, 6, 86, 9, 6, 1, 7, 1, 7, 1, 7, 5, 7, 91, 8, 7, 10, 7,
		12, 7, 94, 9, 7, 1, 8, 1, 8, 1, 8, 3, 8, 99, 8, 8, 1, 9, 1, 9, 1, 9, 1,
		9, 1, 9, 3, 9, 106, 8, 9, 1, 10, 1, 10, 3, 10, 110, 8, 10, 1, 11, 1, 11,
		3, 11, 114, 8, 11, 1, 11, 1, 11, 1, 11, 3, 11, 119, 8, 11, 1, 12, 1, 12,
		1, 12, 5, 12, 124, 8, 12, 10, 12, 12, 12, 127, 9, 12, 1, 12, 3, 12, 130,
		8, 12, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1,
		13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 5, 13, 150,
		8, 13, 10, 13, 12, 13, 153, 9, 13, 1, 13, 3, 13, 156, 8, 13, 1, 13, 1,
		13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13,
		1, 13, 1, 13, 1, 13, 1, 13, 3, 13, 174, 8, 13, 1, 13, 1, 13, 1, 13, 1,
		13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13,
		1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1,
		13, 1, 13, 3, 13, 202, 8, 13, 1, 13, 0, 0, 14, 0, 2, 4, 6, 8, 10, 12, 14,
		16, 18, 20, 22, 24, 26, 0, 9, 1, 0, 12, 13, 1, 0, 8, 11, 1, 0, 18, 19,
		1, 0, 20, 22, 2, 0, 18, 19, 33, 34, 2, 0, 37, 37, 40, 40, 2, 0, 38, 38,
		41, 41, 2, 0, 39, 39, 42, 42, 2, 0, 47, 47, 50, 50, 226, 0, 28, 1, 0, 0,
		0, 2, 30, 1, 0, 0, 0, 4, 38, 1, 0, 0, 0, 6, 46, 1, 0, 0, 0, 8, 75, 1, 0,
		0, 0, 10, 77, 1, 0, 0, 0, 12, 79, 1, 0, 0, 0, 14, 87, 1, 0, 0, 0, 16, 95,
		1, 0, 0, 0, 18, 105, 1, 0, 0, 0, 20, 107, 1, 0, 0, 0, 22, 118, 1, 0, 0,
		0, 24, 120, 1, 0, 0, 0, 26, 201, 1, 0, 0, 0, 28, 29, 3, 2, 1, 0, 29, 1,
		1, 0, 0, 0, 30, 35, 3, 4, 2, 0, 31, 32, 5, 30, 0, 0, 32, 34, 3, 4, 2, 0,
		33, 31, 1, 0, 0, 0, 34, 37, 1, 0, 0, 0, 35, 33, 1, 0, 0, 0, 35, 36, 1,
		0, 0, 0, 36, 3, 1, 0, 0, 0, 37, 35, 1, 0, 0, 0, 38, 43, 3, 6, 3, 0, 39,
		40, 5, 29, 0, 0, 40, 42, 3, 6, 3, 0, 41, 39, 1, 0, 0, 0, 42, 45, 1, 0,
		0, 0, 43, 41, 1, 0, 0, 0, 43, 44, 1, 0, 0, 0, 44, 5, 1, 0, 0, 0, 45, 43,
		1, 0, 0, 0, 46, 51, 3, 8, 4, 0, 47, 48, 7, 0, 0, 0, 48, 50, 3, 8, 4, 0,
		49, 47, 1, 0, 0, 0, 50, 53, 1, 0, 0, 0, 51, 49, 1, 0, 0, 0, 51, 52, 1,
		0, 0, 0, 52, 7, 1, 0, 0, 0, 53, 51, 1, 0, 0, 0, 54, 60, 3, 12, 6, 0, 55,
		56, 3, 10, 5, 0, 56, 57, 3, 12, 6, 0, 57, 59, 1, 0, 0, 0, 58, 55, 1, 0,
		0, 0, 59, 62, 1, 0, 0, 0, 60, 58, 1, 0, 0, 0, 60, 61, 1, 0, 0, 0, 61, 76,
		1, 0, 0, 0, 62, 60, 1, 0, 0, 0, 63, 64, 3, 12, 6, 0, 64, 65, 5, 14, 0,
		0, 65, 66, 5, 49, 0, 0, 66, 76, 1, 0, 0, 0, 67, 69, 3, 12, 6, 0, 68, 70,
		5, 34, 0, 0, 69, 68, 1, 0, 0, 0, 69, 70, 1, 0, 0, 0, 70, 71, 1, 0, 0, 0,
		71, 72, 5, 35, 0, 0, 72, 73, 1, 0, 0, 0, 73, 74, 3, 12, 6, 0, 74, 76, 1,
		0, 0, 0, 75, 54, 1, 0, 0, 0, 75, 63, 1, 0, 0, 0, 75, 67, 1, 0, 0, 0, 76,
		9, 1, 0, 0, 0, 77, 78, 7, 1, 0, 0, 78, 11, 1, 0, 0, 0, 79, 84, 3, 14, 7,
		0, 80, 81, 7, 2, 0, 0, 81, 83, 3, 14, 7, 0, 82, 80, 1, 0, 0, 0, 83, 86,
		1, 0, 0, 0, 84, 82, 1, 0, 0, 0, 84, 85, 1, 0, 0, 0, 85, 13, 1, 0, 0, 0,
		86, 84, 1, 0, 0, 0, 87, 92, 3, 16, 8, 0, 88, 89, 7, 3, 0, 0, 89, 91, 3,
		16, 8, 0, 90, 88, 1, 0, 0, 0, 91, 94, 1, 0, 0, 0, 92, 90, 1, 0, 0, 0, 92,
		93, 1, 0, 0, 0, 93, 15, 1, 0, 0, 0, 94, 92, 1, 0, 0, 0, 95, 98, 3, 18,
		9, 0, 96, 97, 5, 23, 0, 0, 97, 99, 3, 16, 8, 0, 98, 96, 1, 0, 0, 0, 98,
		99, 1, 0, 0, 0, 99, 17, 1, 0, 0, 0, 100, 101, 7, 4, 0, 0, 101, 106, 3,
		18, 9, 0, 102, 103, 5, 15, 0, 0, 103, 106, 3, 18, 9, 0, 104, 106, 3, 20,
		10, 0, 105, 100, 1, 0, 0, 0, 105, 102, 1, 0, 0, 0, 105, 104, 1, 0, 0, 0,
		106, 19, 1, 0, 0, 0, 107, 109, 3, 26, 13, 0, 108, 110, 3, 22, 11, 0, 109,
		108, 1, 0, 0, 0, 109, 110, 1, 0, 0, 0, 110, 21, 1, 0, 0, 0, 111, 113, 5,
		1, 0, 0, 112, 114, 3, 24, 12, 0, 113, 112, 1, 0, 0, 0, 113, 114, 1, 0,
		0, 0, 114, 115, 1, 0, 0, 0, 115, 119, 5, 2, 0, 0, 116, 119, 5, 31, 0, 0,
		117, 119, 5, 32, 0, 0, 118, 111, 1, 0, 0, 0, 118, 116, 1, 0, 0, 0, 118,
		117, 1, 0, 0, 0, 119, 23, 1, 0, 0, 0, 120, 125, 3, 0, 0, 0, 121, 122, 5,
		3, 0, 0, 122, 124, 3, 0, 0, 0, 123, 121, 1, 0, 0, 0, 124, 127, 1, 0, 0,
		0, 125, 123, 1, 0, 0, 0, 125, 126, 1, 0, 0, 0, 126, 129, 1, 0, 0, 0, 127,
		125, 1, 0, 0, 0, 128, 130, 5, 3, 0, 0, 129, 128, 1, 0, 0, 0, 129, 130,
		1, 0, 0, 0, 130, 25, 1, 0, 0, 0, 131, 202, 5, 45, 0, 0, 132, 202, 5, 46,
		0, 0, 133, 202, 5, 44, 0, 0, 134, 202, 5, 49, 0, 0, 135, 202, 5, 47, 0,
		0, 136, 202, 5, 48, 0, 0, 137, 202, 5, 50, 0, 0, 138, 139, 5, 6, 0, 0,
		139, 140, 5, 47, 0, 0, 140, 202, 5, 7, 0, 0, 141, 142, 5, 1, 0, 0, 142,
		143, 3, 0, 0, 0, 143, 144, 5, 2, 0, 0, 144, 202, 1, 0, 0, 0, 145, 146,
		5, 4, 0, 0, 146, 151, 3, 0, 0, 0, 147, 148, 5, 3, 0, 0, 148, 150, 3, 0,
		0, 0, 149, 147, 1, 0, 0, 0, 150, 153, 1, 0, 0, 0, 151, 149, 1, 0, 0, 0,
		151, 152, 1, 0, 0, 0, 152, 155, 1, 0, 0, 0, 153, 151, 1, 0, 0, 0, 154,
		156, 5, 3, 0, 0, 155, 154, 1, 0, 0, 0, 155, 156, 1, 0, 0, 0, 156, 157,
		1, 0, 0, 0, 157, 158, 5, 5, 0, 0, 158, 202, 1, 0, 0, 0, 159, 202, 5, 36,
		0, 0, 160, 161, 5, 16, 0, 0, 161, 162, 5, 1, 0, 0, 162, 163, 5, 47, 0,
		0, 163, 164, 5, 3, 0, 0, 164, 165, 5, 49, 0, 0, 165, 202, 5, 2, 0, 0, 166,
		167, 5, 17, 0, 0, 167, 168, 5, 1, 0, 0, 168, 169, 5, 47, 0, 0, 169, 170,
		5, 3, 0, 0, 170, 173, 5, 49, 0, 0, 171, 172, 5, 3, 0, 0, 172, 174, 3, 0,
		0, 0, 173, 171, 1, 0, 0, 0, 173, 174, 1, 0, 0, 0, 174, 175, 1, 0, 0, 0,
		175, 202, 5, 2, 0, 0, 176, 177, 7, 5, 0, 0, 177, 178, 5, 1, 0, 0, 178,
		179, 3, 0, 0, 0, 179, 180, 5, 3, 0, 0, 180, 181, 3, 0, 0, 0, 181, 182,
		5, 2, 0, 0, 182, 202, 1, 0, 0, 0, 183, 184, 7, 6, 0, 0, 184, 185, 5, 1,
		0, 0, 185, 186, 3, 0, 0, 0, 186, 187, 5, 3, 0, 0, 187, 188, 3, 0, 0, 0,
		188, 189, 5, 2, 0, 0, 189, 202, 1, 0, 0, 0, 190, 191, 7, 7, 0, 0, 191,
		192, 5, 1, 0, 0, 192, 193, 3, 0, 0, 0, 193, 194, 5, 3, 0, 0, 194, 195,
		3, 0, 0, 0, 195, 196, 5, 2, 0, 0, 196, 202, 1, 0, 0, 0, 197, 198, 5, 43,
		0, 0, 198, 199, 5, 1, 0, 0, 199, 200, 7, 8, 0, 0, 200, 202, 5, 2, 0, 0,
		201, 131, 1, 0, 0, 0, 201, 132, 1, 0, 0, 0, 201, 133, 1, 0, 0, 0, 201,
		134, 1, 0, 0, 0, 201, 135, 1, 0, 0, 0, 201, 136, 1, 0, 0, 0, 201, 137,
		1, 0, 0, 0, 201, 138, 1, 0, 0, 0, 201, 141, 1, 0, 0, 0, 201, 145, 1, 0,
		0, 0, 201, 159, 1, 0, 0, 0, 201, 160, 1, 0, 0, 0, 201, 166, 1, 0, 0, 0,
		201, 176, 1, 0, 0, 0, 201, 183, 1, 0, 0, 0, 201, 190, 1, 0, 0, 0, 201,
		197, 1, 0, 0, 0, 202, 27, 1, 0, 0, 0, 19, 35, 43, 51, 60, 69, 75, 84, 92,
		98, 105, 109, 113, 118, 125, 129, 151, 155, 173, 201,
	}
	deserializer := antlr.NewATNDeserializer(nil)
	staticData.atn = deserializer.Deserialize(staticData.serializedATN)
	atn := staticData.atn
	staticData.decisionToDFA = make([]*antlr.DFA, len(atn.DecisionToState))
	decisionToDFA := staticData.decisionToDFA
	for index, state := range atn.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(state, index)
	}
}

// PlanParserInit initializes any static state used to implement PlanParser. By default the
// static state used to implement the parser is lazily initialized during the first call to
// NewPlanParser(). You can call this function if you wish to initialize the static state ahead
// of time.
func PlanParserInit() {
	staticData := &PlanParserStaticData
	staticData.once.Do(planParserInit)
}

// NewPlanParser produces a new parser instance for the optional input antlr.TokenStream.
func NewPlanParser(input antlr.TokenStream) *PlanParser {
	PlanParserInit()
	this := new(PlanParser)
	this.BaseParser = antlr.NewBaseParser(input)
	staticData := &PlanParserStaticData
	this.Interpreter = antlr.NewParserATNSimulator(this, staticData.atn, staticData.decisionToDFA, staticData.PredictionContextCache)
	this.RuleNames = staticData.RuleNames
	this.LiteralNames = staticData.LiteralNames
	this.SymbolicNames = staticData.SymbolicNames
	this.GrammarFileName = "Plan.g4"

	return this
}

// PlanParser tokens.
const (
	PlanParserEOF              = antlr.TokenEOF
	PlanParserT__0             = 1
	PlanParserT__1             = 2
	PlanParserT__2             = 3
	PlanParserT__3             = 4
	PlanParserT__4             = 5
	PlanParserLBRACE           = 6
	PlanParserRBRACE           = 7
	PlanParserLT               = 8
	PlanParserLE               = 9
	PlanParserGT               = 10
	PlanParserGE               = 11
	PlanParserEQ               = 12
	PlanParserNE               = 13
	PlanParserLIKE             = 14
	PlanParserEXISTS           = 15
	PlanParserTEXTMATCH        = 16
	PlanParserPHRASEMATCH      = 17
	PlanParserADD              = 18
	PlanParserSUB              = 19
	PlanParserMUL              = 20
	PlanParserDIV              = 21
	PlanParserMOD              = 22
	PlanParserPOW              = 23
	PlanParserSHL              = 24
	PlanParserSHR              = 25
	PlanParserBAND             = 26
	PlanParserBOR              = 27
	PlanParserBXOR             = 28
	PlanParserAND              = 29
	PlanParserOR               = 30
	PlanParserISNULL           = 31
	PlanParserISNOTNULL        = 32
	PlanParserBNOT             = 33
	PlanParserNOT              = 34
	PlanParserIN               = 35
	PlanParserEmptyArray       = 36
	PlanParserJSONContains     = 37
	PlanParserJSONContainsAll  = 38
	PlanParserJSONContainsAny  = 39
	PlanParserArrayContains    = 40
	PlanParserArrayContainsAll = 41
	PlanParserArrayContainsAny = 42
	PlanParserArrayLength      = 43
	PlanParserBooleanConstant  = 44
	PlanParserIntegerConstant  = 45
	PlanParserFloatingConstant = 46
	PlanParserIdentifier       = 47
	PlanParserMeta             = 48
	PlanParserStringLiteral    = 49
	PlanParserJSONIdentifier   = 50
	PlanParserWhitespace       = 51
	PlanParserNewline          = 52
)

// PlanParser rules.
const (
	PlanParserRULE_expr               = 0
	PlanParserRULE_logicalOrExpr      = 1
	PlanParserRULE_logicalAndExpr     = 2
	PlanParserRULE_equalityExpr       = 3
	PlanParserRULE_comparisonExpr     = 4
	PlanParserRULE_compOp             = 5
	PlanParserRULE_additiveExpr       = 6
	PlanParserRULE_multiplicativeExpr = 7
	PlanParserRULE_powerExpr          = 8
	PlanParserRULE_unaryExpr          = 9
	PlanParserRULE_postfixExpr        = 10
	PlanParserRULE_postfixOp          = 11
	PlanParserRULE_argumentList       = 12
	PlanParserRULE_primaryExpr        = 13
)

// IExprContext is an interface to support dynamic dispatch.
type IExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	LogicalOrExpr() ILogicalOrExprContext

	// IsExprContext differentiates from other interfaces.
	IsExprContext()
}

type ExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyExprContext() *ExprContext {
	var p = new(ExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_expr
	return p
}

func InitEmptyExprContext(p *ExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_expr
}

func (*ExprContext) IsExprContext() {}

func NewExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ExprContext {
	var p = new(ExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_expr

	return p
}

func (s *ExprContext) GetParser() antlr.Parser { return s.parser }

func (s *ExprContext) LogicalOrExpr() ILogicalOrExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILogicalOrExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ILogicalOrExprContext)
}

func (s *ExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) Expr() (localctx IExprContext) {
	localctx = NewExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, PlanParserRULE_expr)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(28)
		p.LogicalOrExpr()
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ILogicalOrExprContext is an interface to support dynamic dispatch.
type ILogicalOrExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllLogicalAndExpr() []ILogicalAndExprContext
	LogicalAndExpr(i int) ILogicalAndExprContext
	AllOR() []antlr.TerminalNode
	OR(i int) antlr.TerminalNode

	// IsLogicalOrExprContext differentiates from other interfaces.
	IsLogicalOrExprContext()
}

type LogicalOrExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLogicalOrExprContext() *LogicalOrExprContext {
	var p = new(LogicalOrExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_logicalOrExpr
	return p
}

func InitEmptyLogicalOrExprContext(p *LogicalOrExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_logicalOrExpr
}

func (*LogicalOrExprContext) IsLogicalOrExprContext() {}

func NewLogicalOrExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *LogicalOrExprContext {
	var p = new(LogicalOrExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_logicalOrExpr

	return p
}

func (s *LogicalOrExprContext) GetParser() antlr.Parser { return s.parser }

func (s *LogicalOrExprContext) AllLogicalAndExpr() []ILogicalAndExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ILogicalAndExprContext); ok {
			len++
		}
	}

	tst := make([]ILogicalAndExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ILogicalAndExprContext); ok {
			tst[i] = t.(ILogicalAndExprContext)
			i++
		}
	}

	return tst
}

func (s *LogicalOrExprContext) LogicalAndExpr(i int) ILogicalAndExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILogicalAndExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ILogicalAndExprContext)
}

func (s *LogicalOrExprContext) AllOR() []antlr.TerminalNode {
	return s.GetTokens(PlanParserOR)
}

func (s *LogicalOrExprContext) OR(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserOR, i)
}

func (s *LogicalOrExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LogicalOrExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *LogicalOrExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitLogicalOrExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) LogicalOrExpr() (localctx ILogicalOrExprContext) {
	localctx = NewLogicalOrExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 2, PlanParserRULE_logicalOrExpr)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(30)
		p.LogicalAndExpr()
	}
	p.SetState(35)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == PlanParserOR {
		{
			p.SetState(31)
			p.Match(PlanParserOR)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(32)
			p.LogicalAndExpr()
		}

		p.SetState(37)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ILogicalAndExprContext is an interface to support dynamic dispatch.
type ILogicalAndExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllEqualityExpr() []IEqualityExprContext
	EqualityExpr(i int) IEqualityExprContext
	AllAND() []antlr.TerminalNode
	AND(i int) antlr.TerminalNode

	// IsLogicalAndExprContext differentiates from other interfaces.
	IsLogicalAndExprContext()
}

type LogicalAndExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLogicalAndExprContext() *LogicalAndExprContext {
	var p = new(LogicalAndExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_logicalAndExpr
	return p
}

func InitEmptyLogicalAndExprContext(p *LogicalAndExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_logicalAndExpr
}

func (*LogicalAndExprContext) IsLogicalAndExprContext() {}

func NewLogicalAndExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *LogicalAndExprContext {
	var p = new(LogicalAndExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_logicalAndExpr

	return p
}

func (s *LogicalAndExprContext) GetParser() antlr.Parser { return s.parser }

func (s *LogicalAndExprContext) AllEqualityExpr() []IEqualityExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IEqualityExprContext); ok {
			len++
		}
	}

	tst := make([]IEqualityExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IEqualityExprContext); ok {
			tst[i] = t.(IEqualityExprContext)
			i++
		}
	}

	return tst
}

func (s *LogicalAndExprContext) EqualityExpr(i int) IEqualityExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IEqualityExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IEqualityExprContext)
}

func (s *LogicalAndExprContext) AllAND() []antlr.TerminalNode {
	return s.GetTokens(PlanParserAND)
}

func (s *LogicalAndExprContext) AND(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserAND, i)
}

func (s *LogicalAndExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LogicalAndExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *LogicalAndExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitLogicalAndExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) LogicalAndExpr() (localctx ILogicalAndExprContext) {
	localctx = NewLogicalAndExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 4, PlanParserRULE_logicalAndExpr)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(38)
		p.EqualityExpr()
	}
	p.SetState(43)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == PlanParserAND {
		{
			p.SetState(39)
			p.Match(PlanParserAND)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(40)
			p.EqualityExpr()
		}

		p.SetState(45)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IEqualityExprContext is an interface to support dynamic dispatch.
type IEqualityExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllComparisonExpr() []IComparisonExprContext
	ComparisonExpr(i int) IComparisonExprContext
	AllEQ() []antlr.TerminalNode
	EQ(i int) antlr.TerminalNode
	AllNE() []antlr.TerminalNode
	NE(i int) antlr.TerminalNode

	// IsEqualityExprContext differentiates from other interfaces.
	IsEqualityExprContext()
}

type EqualityExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyEqualityExprContext() *EqualityExprContext {
	var p = new(EqualityExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_equalityExpr
	return p
}

func InitEmptyEqualityExprContext(p *EqualityExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_equalityExpr
}

func (*EqualityExprContext) IsEqualityExprContext() {}

func NewEqualityExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *EqualityExprContext {
	var p = new(EqualityExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_equalityExpr

	return p
}

func (s *EqualityExprContext) GetParser() antlr.Parser { return s.parser }

func (s *EqualityExprContext) AllComparisonExpr() []IComparisonExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IComparisonExprContext); ok {
			len++
		}
	}

	tst := make([]IComparisonExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IComparisonExprContext); ok {
			tst[i] = t.(IComparisonExprContext)
			i++
		}
	}

	return tst
}

func (s *EqualityExprContext) ComparisonExpr(i int) IComparisonExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IComparisonExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IComparisonExprContext)
}

func (s *EqualityExprContext) AllEQ() []antlr.TerminalNode {
	return s.GetTokens(PlanParserEQ)
}

func (s *EqualityExprContext) EQ(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserEQ, i)
}

func (s *EqualityExprContext) AllNE() []antlr.TerminalNode {
	return s.GetTokens(PlanParserNE)
}

func (s *EqualityExprContext) NE(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserNE, i)
}

func (s *EqualityExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *EqualityExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *EqualityExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitEqualityExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) EqualityExpr() (localctx IEqualityExprContext) {
	localctx = NewEqualityExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, PlanParserRULE_equalityExpr)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(46)
		p.ComparisonExpr()
	}
	p.SetState(51)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == PlanParserEQ || _la == PlanParserNE {
		{
			p.SetState(47)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserEQ || _la == PlanParserNE) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(48)
			p.ComparisonExpr()
		}

		p.SetState(53)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IComparisonExprContext is an interface to support dynamic dispatch.
type IComparisonExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllAdditiveExpr() []IAdditiveExprContext
	AdditiveExpr(i int) IAdditiveExprContext
	AllCompOp() []ICompOpContext
	CompOp(i int) ICompOpContext
	LIKE() antlr.TerminalNode
	StringLiteral() antlr.TerminalNode
	IN() antlr.TerminalNode
	NOT() antlr.TerminalNode

	// IsComparisonExprContext differentiates from other interfaces.
	IsComparisonExprContext()
}

type ComparisonExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyComparisonExprContext() *ComparisonExprContext {
	var p = new(ComparisonExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_comparisonExpr
	return p
}

func InitEmptyComparisonExprContext(p *ComparisonExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_comparisonExpr
}

func (*ComparisonExprContext) IsComparisonExprContext() {}

func NewComparisonExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ComparisonExprContext {
	var p = new(ComparisonExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_comparisonExpr

	return p
}

func (s *ComparisonExprContext) GetParser() antlr.Parser { return s.parser }

func (s *ComparisonExprContext) AllAdditiveExpr() []IAdditiveExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IAdditiveExprContext); ok {
			len++
		}
	}

	tst := make([]IAdditiveExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IAdditiveExprContext); ok {
			tst[i] = t.(IAdditiveExprContext)
			i++
		}
	}

	return tst
}

func (s *ComparisonExprContext) AdditiveExpr(i int) IAdditiveExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IAdditiveExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IAdditiveExprContext)
}

func (s *ComparisonExprContext) AllCompOp() []ICompOpContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ICompOpContext); ok {
			len++
		}
	}

	tst := make([]ICompOpContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ICompOpContext); ok {
			tst[i] = t.(ICompOpContext)
			i++
		}
	}

	return tst
}

func (s *ComparisonExprContext) CompOp(i int) ICompOpContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ICompOpContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ICompOpContext)
}

func (s *ComparisonExprContext) LIKE() antlr.TerminalNode {
	return s.GetToken(PlanParserLIKE, 0)
}

func (s *ComparisonExprContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *ComparisonExprContext) IN() antlr.TerminalNode {
	return s.GetToken(PlanParserIN, 0)
}

func (s *ComparisonExprContext) NOT() antlr.TerminalNode {
	return s.GetToken(PlanParserNOT, 0)
}

func (s *ComparisonExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ComparisonExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ComparisonExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitComparisonExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) ComparisonExpr() (localctx IComparisonExprContext) {
	localctx = NewComparisonExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 8, PlanParserRULE_comparisonExpr)
	var _la int

	p.SetState(75)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 5, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(54)
			p.AdditiveExpr()
		}
		p.SetState(60)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		for (int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&3840) != 0 {
			{
				p.SetState(55)
				p.CompOp()
			}
			{
				p.SetState(56)
				p.AdditiveExpr()
			}

			p.SetState(62)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(63)
			p.AdditiveExpr()
		}
		{
			p.SetState(64)
			p.Match(PlanParserLIKE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(65)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 3:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(67)
			p.AdditiveExpr()
		}

		p.SetState(69)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserNOT {
			{
				p.SetState(68)
				p.Match(PlanParserNOT)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		{
			p.SetState(71)
			p.Match(PlanParserIN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

		{
			p.SetState(73)
			p.AdditiveExpr()
		}

	case antlr.ATNInvalidAltNumber:
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ICompOpContext is an interface to support dynamic dispatch.
type ICompOpContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	LT() antlr.TerminalNode
	LE() antlr.TerminalNode
	GT() antlr.TerminalNode
	GE() antlr.TerminalNode

	// IsCompOpContext differentiates from other interfaces.
	IsCompOpContext()
}

type CompOpContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCompOpContext() *CompOpContext {
	var p = new(CompOpContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_compOp
	return p
}

func InitEmptyCompOpContext(p *CompOpContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_compOp
}

func (*CompOpContext) IsCompOpContext() {}

func NewCompOpContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CompOpContext {
	var p = new(CompOpContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_compOp

	return p
}

func (s *CompOpContext) GetParser() antlr.Parser { return s.parser }

func (s *CompOpContext) LT() antlr.TerminalNode {
	return s.GetToken(PlanParserLT, 0)
}

func (s *CompOpContext) LE() antlr.TerminalNode {
	return s.GetToken(PlanParserLE, 0)
}

func (s *CompOpContext) GT() antlr.TerminalNode {
	return s.GetToken(PlanParserGT, 0)
}

func (s *CompOpContext) GE() antlr.TerminalNode {
	return s.GetToken(PlanParserGE, 0)
}

func (s *CompOpContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CompOpContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CompOpContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitCompOp(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) CompOp() (localctx ICompOpContext) {
	localctx = NewCompOpContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 10, PlanParserRULE_compOp)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(77)
		_la = p.GetTokenStream().LA(1)

		if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&3840) != 0) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IAdditiveExprContext is an interface to support dynamic dispatch.
type IAdditiveExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllMultiplicativeExpr() []IMultiplicativeExprContext
	MultiplicativeExpr(i int) IMultiplicativeExprContext
	AllADD() []antlr.TerminalNode
	ADD(i int) antlr.TerminalNode
	AllSUB() []antlr.TerminalNode
	SUB(i int) antlr.TerminalNode

	// IsAdditiveExprContext differentiates from other interfaces.
	IsAdditiveExprContext()
}

type AdditiveExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyAdditiveExprContext() *AdditiveExprContext {
	var p = new(AdditiveExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_additiveExpr
	return p
}

func InitEmptyAdditiveExprContext(p *AdditiveExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_additiveExpr
}

func (*AdditiveExprContext) IsAdditiveExprContext() {}

func NewAdditiveExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *AdditiveExprContext {
	var p = new(AdditiveExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_additiveExpr

	return p
}

func (s *AdditiveExprContext) GetParser() antlr.Parser { return s.parser }

func (s *AdditiveExprContext) AllMultiplicativeExpr() []IMultiplicativeExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IMultiplicativeExprContext); ok {
			len++
		}
	}

	tst := make([]IMultiplicativeExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IMultiplicativeExprContext); ok {
			tst[i] = t.(IMultiplicativeExprContext)
			i++
		}
	}

	return tst
}

func (s *AdditiveExprContext) MultiplicativeExpr(i int) IMultiplicativeExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMultiplicativeExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMultiplicativeExprContext)
}

func (s *AdditiveExprContext) AllADD() []antlr.TerminalNode {
	return s.GetTokens(PlanParserADD)
}

func (s *AdditiveExprContext) ADD(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserADD, i)
}

func (s *AdditiveExprContext) AllSUB() []antlr.TerminalNode {
	return s.GetTokens(PlanParserSUB)
}

func (s *AdditiveExprContext) SUB(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserSUB, i)
}

func (s *AdditiveExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AdditiveExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *AdditiveExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitAdditiveExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) AdditiveExpr() (localctx IAdditiveExprContext) {
	localctx = NewAdditiveExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 12, PlanParserRULE_additiveExpr)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(79)
		p.MultiplicativeExpr()
	}
	p.SetState(84)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == PlanParserADD || _la == PlanParserSUB {
		{
			p.SetState(80)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserADD || _la == PlanParserSUB) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(81)
			p.MultiplicativeExpr()
		}

		p.SetState(86)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IMultiplicativeExprContext is an interface to support dynamic dispatch.
type IMultiplicativeExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllPowerExpr() []IPowerExprContext
	PowerExpr(i int) IPowerExprContext
	AllMUL() []antlr.TerminalNode
	MUL(i int) antlr.TerminalNode
	AllDIV() []antlr.TerminalNode
	DIV(i int) antlr.TerminalNode
	AllMOD() []antlr.TerminalNode
	MOD(i int) antlr.TerminalNode

	// IsMultiplicativeExprContext differentiates from other interfaces.
	IsMultiplicativeExprContext()
}

type MultiplicativeExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyMultiplicativeExprContext() *MultiplicativeExprContext {
	var p = new(MultiplicativeExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_multiplicativeExpr
	return p
}

func InitEmptyMultiplicativeExprContext(p *MultiplicativeExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_multiplicativeExpr
}

func (*MultiplicativeExprContext) IsMultiplicativeExprContext() {}

func NewMultiplicativeExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *MultiplicativeExprContext {
	var p = new(MultiplicativeExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_multiplicativeExpr

	return p
}

func (s *MultiplicativeExprContext) GetParser() antlr.Parser { return s.parser }

func (s *MultiplicativeExprContext) AllPowerExpr() []IPowerExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IPowerExprContext); ok {
			len++
		}
	}

	tst := make([]IPowerExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IPowerExprContext); ok {
			tst[i] = t.(IPowerExprContext)
			i++
		}
	}

	return tst
}

func (s *MultiplicativeExprContext) PowerExpr(i int) IPowerExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPowerExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPowerExprContext)
}

func (s *MultiplicativeExprContext) AllMUL() []antlr.TerminalNode {
	return s.GetTokens(PlanParserMUL)
}

func (s *MultiplicativeExprContext) MUL(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserMUL, i)
}

func (s *MultiplicativeExprContext) AllDIV() []antlr.TerminalNode {
	return s.GetTokens(PlanParserDIV)
}

func (s *MultiplicativeExprContext) DIV(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserDIV, i)
}

func (s *MultiplicativeExprContext) AllMOD() []antlr.TerminalNode {
	return s.GetTokens(PlanParserMOD)
}

func (s *MultiplicativeExprContext) MOD(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserMOD, i)
}

func (s *MultiplicativeExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MultiplicativeExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *MultiplicativeExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMultiplicativeExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) MultiplicativeExpr() (localctx IMultiplicativeExprContext) {
	localctx = NewMultiplicativeExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 14, PlanParserRULE_multiplicativeExpr)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(87)
		p.PowerExpr()
	}
	p.SetState(92)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for (int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&7340032) != 0 {
		{
			p.SetState(88)
			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&7340032) != 0) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(89)
			p.PowerExpr()
		}

		p.SetState(94)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IPowerExprContext is an interface to support dynamic dispatch.
type IPowerExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	UnaryExpr() IUnaryExprContext
	POW() antlr.TerminalNode
	PowerExpr() IPowerExprContext

	// IsPowerExprContext differentiates from other interfaces.
	IsPowerExprContext()
}

type PowerExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPowerExprContext() *PowerExprContext {
	var p = new(PowerExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_powerExpr
	return p
}

func InitEmptyPowerExprContext(p *PowerExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_powerExpr
}

func (*PowerExprContext) IsPowerExprContext() {}

func NewPowerExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PowerExprContext {
	var p = new(PowerExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_powerExpr

	return p
}

func (s *PowerExprContext) GetParser() antlr.Parser { return s.parser }

func (s *PowerExprContext) UnaryExpr() IUnaryExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IUnaryExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IUnaryExprContext)
}

func (s *PowerExprContext) POW() antlr.TerminalNode {
	return s.GetToken(PlanParserPOW, 0)
}

func (s *PowerExprContext) PowerExpr() IPowerExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPowerExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPowerExprContext)
}

func (s *PowerExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PowerExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *PowerExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitPowerExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) PowerExpr() (localctx IPowerExprContext) {
	localctx = NewPowerExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 16, PlanParserRULE_powerExpr)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(95)
		p.UnaryExpr()
	}
	p.SetState(98)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == PlanParserPOW {
		{
			p.SetState(96)
			p.Match(PlanParserPOW)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(97)
			p.PowerExpr()
		}

	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IUnaryExprContext is an interface to support dynamic dispatch.
type IUnaryExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	UnaryExpr() IUnaryExprContext
	ADD() antlr.TerminalNode
	SUB() antlr.TerminalNode
	BNOT() antlr.TerminalNode
	NOT() antlr.TerminalNode
	EXISTS() antlr.TerminalNode
	PostfixExpr() IPostfixExprContext

	// IsUnaryExprContext differentiates from other interfaces.
	IsUnaryExprContext()
}

type UnaryExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyUnaryExprContext() *UnaryExprContext {
	var p = new(UnaryExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_unaryExpr
	return p
}

func InitEmptyUnaryExprContext(p *UnaryExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_unaryExpr
}

func (*UnaryExprContext) IsUnaryExprContext() {}

func NewUnaryExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *UnaryExprContext {
	var p = new(UnaryExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_unaryExpr

	return p
}

func (s *UnaryExprContext) GetParser() antlr.Parser { return s.parser }

func (s *UnaryExprContext) UnaryExpr() IUnaryExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IUnaryExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IUnaryExprContext)
}

func (s *UnaryExprContext) ADD() antlr.TerminalNode {
	return s.GetToken(PlanParserADD, 0)
}

func (s *UnaryExprContext) SUB() antlr.TerminalNode {
	return s.GetToken(PlanParserSUB, 0)
}

func (s *UnaryExprContext) BNOT() antlr.TerminalNode {
	return s.GetToken(PlanParserBNOT, 0)
}

func (s *UnaryExprContext) NOT() antlr.TerminalNode {
	return s.GetToken(PlanParserNOT, 0)
}

func (s *UnaryExprContext) EXISTS() antlr.TerminalNode {
	return s.GetToken(PlanParserEXISTS, 0)
}

func (s *UnaryExprContext) PostfixExpr() IPostfixExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPostfixExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPostfixExprContext)
}

func (s *UnaryExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UnaryExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *UnaryExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitUnaryExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) UnaryExpr() (localctx IUnaryExprContext) {
	localctx = NewUnaryExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 18, PlanParserRULE_unaryExpr)
	var _la int

	p.SetState(105)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PlanParserADD, PlanParserSUB, PlanParserBNOT, PlanParserNOT:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(100)
			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&25770590208) != 0) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(101)
			p.UnaryExpr()
		}

	case PlanParserEXISTS:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(102)
			p.Match(PlanParserEXISTS)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(103)
			p.UnaryExpr()
		}

	case PlanParserT__0, PlanParserT__3, PlanParserLBRACE, PlanParserTEXTMATCH, PlanParserPHRASEMATCH, PlanParserEmptyArray, PlanParserJSONContains, PlanParserJSONContainsAll, PlanParserJSONContainsAny, PlanParserArrayContains, PlanParserArrayContainsAll, PlanParserArrayContainsAny, PlanParserArrayLength, PlanParserBooleanConstant, PlanParserIntegerConstant, PlanParserFloatingConstant, PlanParserIdentifier, PlanParserMeta, PlanParserStringLiteral, PlanParserJSONIdentifier:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(104)
			p.PostfixExpr()
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IPostfixExprContext is an interface to support dynamic dispatch.
type IPostfixExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	PrimaryExpr() IPrimaryExprContext
	PostfixOp() IPostfixOpContext

	// IsPostfixExprContext differentiates from other interfaces.
	IsPostfixExprContext()
}

type PostfixExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPostfixExprContext() *PostfixExprContext {
	var p = new(PostfixExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_postfixExpr
	return p
}

func InitEmptyPostfixExprContext(p *PostfixExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_postfixExpr
}

func (*PostfixExprContext) IsPostfixExprContext() {}

func NewPostfixExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PostfixExprContext {
	var p = new(PostfixExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_postfixExpr

	return p
}

func (s *PostfixExprContext) GetParser() antlr.Parser { return s.parser }

func (s *PostfixExprContext) PrimaryExpr() IPrimaryExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPrimaryExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPrimaryExprContext)
}

func (s *PostfixExprContext) PostfixOp() IPostfixOpContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPostfixOpContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPostfixOpContext)
}

func (s *PostfixExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PostfixExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *PostfixExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitPostfixExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) PostfixExpr() (localctx IPostfixExprContext) {
	localctx = NewPostfixExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 20, PlanParserRULE_postfixExpr)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(107)
		p.PrimaryExpr()
	}
	p.SetState(109)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if (int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&6442450946) != 0 {
		{
			p.SetState(108)
			p.PostfixOp()
		}

	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IPostfixOpContext is an interface to support dynamic dispatch.
type IPostfixOpContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsPostfixOpContext differentiates from other interfaces.
	IsPostfixOpContext()
}

type PostfixOpContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPostfixOpContext() *PostfixOpContext {
	var p = new(PostfixOpContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_postfixOp
	return p
}

func InitEmptyPostfixOpContext(p *PostfixOpContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_postfixOp
}

func (*PostfixOpContext) IsPostfixOpContext() {}

func NewPostfixOpContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PostfixOpContext {
	var p = new(PostfixOpContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_postfixOp

	return p
}

func (s *PostfixOpContext) GetParser() antlr.Parser { return s.parser }

func (s *PostfixOpContext) CopyAll(ctx *PostfixOpContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *PostfixOpContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PostfixOpContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type IsNotNullContext struct {
	PostfixOpContext
}

func NewIsNotNullContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IsNotNullContext {
	var p = new(IsNotNullContext)

	InitEmptyPostfixOpContext(&p.PostfixOpContext)
	p.parser = parser
	p.CopyAll(ctx.(*PostfixOpContext))

	return p
}

func (s *IsNotNullContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IsNotNullContext) ISNOTNULL() antlr.TerminalNode {
	return s.GetToken(PlanParserISNOTNULL, 0)
}

func (s *IsNotNullContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitIsNotNull(s)

	default:
		return t.VisitChildren(s)
	}
}

type IsNullContext struct {
	PostfixOpContext
}

func NewIsNullContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IsNullContext {
	var p = new(IsNullContext)

	InitEmptyPostfixOpContext(&p.PostfixOpContext)
	p.parser = parser
	p.CopyAll(ctx.(*PostfixOpContext))

	return p
}

func (s *IsNullContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IsNullContext) ISNULL() antlr.TerminalNode {
	return s.GetToken(PlanParserISNULL, 0)
}

func (s *IsNullContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitIsNull(s)

	default:
		return t.VisitChildren(s)
	}
}

type FunctionCallContext struct {
	PostfixOpContext
}

func NewFunctionCallContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *FunctionCallContext {
	var p = new(FunctionCallContext)

	InitEmptyPostfixOpContext(&p.PostfixOpContext)
	p.parser = parser
	p.CopyAll(ctx.(*PostfixOpContext))

	return p
}

func (s *FunctionCallContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FunctionCallContext) ArgumentList() IArgumentListContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IArgumentListContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IArgumentListContext)
}

func (s *FunctionCallContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitFunctionCall(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) PostfixOp() (localctx IPostfixOpContext) {
	localctx = NewPostfixOpContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 22, PlanParserRULE_postfixOp)
	var _la int

	p.SetState(118)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PlanParserT__0:
		localctx = NewFunctionCallContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(111)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(113)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if (int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&2251756865028178) != 0 {
			{
				p.SetState(112)
				p.ArgumentList()
			}

		}
		{
			p.SetState(115)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserISNULL:
		localctx = NewIsNullContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(116)
			p.Match(PlanParserISNULL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserISNOTNULL:
		localctx = NewIsNotNullContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(117)
			p.Match(PlanParserISNOTNULL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IArgumentListContext is an interface to support dynamic dispatch.
type IArgumentListContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllExpr() []IExprContext
	Expr(i int) IExprContext

	// IsArgumentListContext differentiates from other interfaces.
	IsArgumentListContext()
}

type ArgumentListContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyArgumentListContext() *ArgumentListContext {
	var p = new(ArgumentListContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_argumentList
	return p
}

func InitEmptyArgumentListContext(p *ArgumentListContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_argumentList
}

func (*ArgumentListContext) IsArgumentListContext() {}

func NewArgumentListContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ArgumentListContext {
	var p = new(ArgumentListContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_argumentList

	return p
}

func (s *ArgumentListContext) GetParser() antlr.Parser { return s.parser }

func (s *ArgumentListContext) AllExpr() []IExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExprContext); ok {
			len++
		}
	}

	tst := make([]IExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExprContext); ok {
			tst[i] = t.(IExprContext)
			i++
		}
	}

	return tst
}

func (s *ArgumentListContext) Expr(i int) IExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *ArgumentListContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ArgumentListContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ArgumentListContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitArgumentList(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) ArgumentList() (localctx IArgumentListContext) {
	localctx = NewArgumentListContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 24, PlanParserRULE_argumentList)
	var _la int

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(120)
		p.Expr()
	}
	p.SetState(125)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 13, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(121)
				p.Match(PlanParserT__2)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(122)
				p.Expr()
			}

		}
		p.SetState(127)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 13, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
	}
	p.SetState(129)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == PlanParserT__2 {
		{
			p.SetState(128)
			p.Match(PlanParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IPrimaryExprContext is an interface to support dynamic dispatch.
type IPrimaryExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsPrimaryExprContext differentiates from other interfaces.
	IsPrimaryExprContext()
}

type PrimaryExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPrimaryExprContext() *PrimaryExprContext {
	var p = new(PrimaryExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_primaryExpr
	return p
}

func InitEmptyPrimaryExprContext(p *PrimaryExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_primaryExpr
}

func (*PrimaryExprContext) IsPrimaryExprContext() {}

func NewPrimaryExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PrimaryExprContext {
	var p = new(PrimaryExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_primaryExpr

	return p
}

func (s *PrimaryExprContext) GetParser() antlr.Parser { return s.parser }

func (s *PrimaryExprContext) CopyAll(ctx *PrimaryExprContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *PrimaryExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PrimaryExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type MetaContext struct {
	PrimaryExprContext
}

func NewMetaContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MetaContext {
	var p = new(MetaContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *MetaContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MetaContext) Meta() antlr.TerminalNode {
	return s.GetToken(PlanParserMeta, 0)
}

func (s *MetaContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMeta(s)

	default:
		return t.VisitChildren(s)
	}
}

type JSONIdentifierContext struct {
	PrimaryExprContext
}

func NewJSONIdentifierContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *JSONIdentifierContext {
	var p = new(JSONIdentifierContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *JSONIdentifierContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *JSONIdentifierContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *JSONIdentifierContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitJSONIdentifier(s)

	default:
		return t.VisitChildren(s)
	}
}

type EmptyArrayContext struct {
	PrimaryExprContext
}

func NewEmptyArrayContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *EmptyArrayContext {
	var p = new(EmptyArrayContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *EmptyArrayContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *EmptyArrayContext) EmptyArray() antlr.TerminalNode {
	return s.GetToken(PlanParserEmptyArray, 0)
}

func (s *EmptyArrayContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitEmptyArray(s)

	default:
		return t.VisitChildren(s)
	}
}

type ParensContext struct {
	PrimaryExprContext
}

func NewParensContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ParensContext {
	var p = new(ParensContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *ParensContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ParensContext) Expr() IExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *ParensContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitParens(s)

	default:
		return t.VisitChildren(s)
	}
}

type PhraseMatchContext struct {
	PrimaryExprContext
}

func NewPhraseMatchContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PhraseMatchContext {
	var p = new(PhraseMatchContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *PhraseMatchContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PhraseMatchContext) PHRASEMATCH() antlr.TerminalNode {
	return s.GetToken(PlanParserPHRASEMATCH, 0)
}

func (s *PhraseMatchContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *PhraseMatchContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *PhraseMatchContext) Expr() IExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *PhraseMatchContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitPhraseMatch(s)

	default:
		return t.VisitChildren(s)
	}
}

type ArrayLengthContext struct {
	PrimaryExprContext
}

func NewArrayLengthContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ArrayLengthContext {
	var p = new(ArrayLengthContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *ArrayLengthContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ArrayLengthContext) ArrayLength() antlr.TerminalNode {
	return s.GetToken(PlanParserArrayLength, 0)
}

func (s *ArrayLengthContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *ArrayLengthContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *ArrayLengthContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitArrayLength(s)

	default:
		return t.VisitChildren(s)
	}
}

type StringContext struct {
	PrimaryExprContext
}

func NewStringContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *StringContext {
	var p = new(StringContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *StringContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StringContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *StringContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitString(s)

	default:
		return t.VisitChildren(s)
	}
}

type TextMatchContext struct {
	PrimaryExprContext
}

func NewTextMatchContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TextMatchContext {
	var p = new(TextMatchContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *TextMatchContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TextMatchContext) TEXTMATCH() antlr.TerminalNode {
	return s.GetToken(PlanParserTEXTMATCH, 0)
}

func (s *TextMatchContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *TextMatchContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *TextMatchContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTextMatch(s)

	default:
		return t.VisitChildren(s)
	}
}

type FloatingContext struct {
	PrimaryExprContext
}

func NewFloatingContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *FloatingContext {
	var p = new(FloatingContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *FloatingContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FloatingContext) FloatingConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserFloatingConstant, 0)
}

func (s *FloatingContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitFloating(s)

	default:
		return t.VisitChildren(s)
	}
}

type JSONContainsContext struct {
	PrimaryExprContext
}

func NewJSONContainsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *JSONContainsContext {
	var p = new(JSONContainsContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *JSONContainsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *JSONContainsContext) AllExpr() []IExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExprContext); ok {
			len++
		}
	}

	tst := make([]IExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExprContext); ok {
			tst[i] = t.(IExprContext)
			i++
		}
	}

	return tst
}

func (s *JSONContainsContext) Expr(i int) IExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *JSONContainsContext) JSONContains() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONContains, 0)
}

func (s *JSONContainsContext) ArrayContains() antlr.TerminalNode {
	return s.GetToken(PlanParserArrayContains, 0)
}

func (s *JSONContainsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitJSONContains(s)

	default:
		return t.VisitChildren(s)
	}
}

type JSONContainsAllContext struct {
	PrimaryExprContext
}

func NewJSONContainsAllContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *JSONContainsAllContext {
	var p = new(JSONContainsAllContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *JSONContainsAllContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *JSONContainsAllContext) AllExpr() []IExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExprContext); ok {
			len++
		}
	}

	tst := make([]IExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExprContext); ok {
			tst[i] = t.(IExprContext)
			i++
		}
	}

	return tst
}

func (s *JSONContainsAllContext) Expr(i int) IExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *JSONContainsAllContext) JSONContainsAll() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONContainsAll, 0)
}

func (s *JSONContainsAllContext) ArrayContainsAll() antlr.TerminalNode {
	return s.GetToken(PlanParserArrayContainsAll, 0)
}

func (s *JSONContainsAllContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitJSONContainsAll(s)

	default:
		return t.VisitChildren(s)
	}
}

type IntegerContext struct {
	PrimaryExprContext
}

func NewIntegerContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IntegerContext {
	var p = new(IntegerContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *IntegerContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IntegerContext) IntegerConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserIntegerConstant, 0)
}

func (s *IntegerContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitInteger(s)

	default:
		return t.VisitChildren(s)
	}
}

type ArrayContext struct {
	PrimaryExprContext
}

func NewArrayContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ArrayContext {
	var p = new(ArrayContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *ArrayContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ArrayContext) AllExpr() []IExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExprContext); ok {
			len++
		}
	}

	tst := make([]IExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExprContext); ok {
			tst[i] = t.(IExprContext)
			i++
		}
	}

	return tst
}

func (s *ArrayContext) Expr(i int) IExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *ArrayContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitArray(s)

	default:
		return t.VisitChildren(s)
	}
}

type JSONContainsAnyContext struct {
	PrimaryExprContext
}

func NewJSONContainsAnyContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *JSONContainsAnyContext {
	var p = new(JSONContainsAnyContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *JSONContainsAnyContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *JSONContainsAnyContext) AllExpr() []IExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExprContext); ok {
			len++
		}
	}

	tst := make([]IExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExprContext); ok {
			tst[i] = t.(IExprContext)
			i++
		}
	}

	return tst
}

func (s *JSONContainsAnyContext) Expr(i int) IExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *JSONContainsAnyContext) JSONContainsAny() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONContainsAny, 0)
}

func (s *JSONContainsAnyContext) ArrayContainsAny() antlr.TerminalNode {
	return s.GetToken(PlanParserArrayContainsAny, 0)
}

func (s *JSONContainsAnyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitJSONContainsAny(s)

	default:
		return t.VisitChildren(s)
	}
}

type IdentifierContext struct {
	PrimaryExprContext
}

func NewIdentifierContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IdentifierContext {
	var p = new(IdentifierContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *IdentifierContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IdentifierContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *IdentifierContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitIdentifier(s)

	default:
		return t.VisitChildren(s)
	}
}

type TemplateVariableContext struct {
	PrimaryExprContext
}

func NewTemplateVariableContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TemplateVariableContext {
	var p = new(TemplateVariableContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *TemplateVariableContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TemplateVariableContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(PlanParserLBRACE, 0)
}

func (s *TemplateVariableContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *TemplateVariableContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(PlanParserRBRACE, 0)
}

func (s *TemplateVariableContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTemplateVariable(s)

	default:
		return t.VisitChildren(s)
	}
}

type BooleanContext struct {
	PrimaryExprContext
}

func NewBooleanContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BooleanContext {
	var p = new(BooleanContext)

	InitEmptyPrimaryExprContext(&p.PrimaryExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*PrimaryExprContext))

	return p
}

func (s *BooleanContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BooleanContext) BooleanConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserBooleanConstant, 0)
}

func (s *BooleanContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitBoolean(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) PrimaryExpr() (localctx IPrimaryExprContext) {
	localctx = NewPrimaryExprContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 26, PlanParserRULE_primaryExpr)
	var _la int

	var _alt int

	p.SetState(201)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PlanParserIntegerConstant:
		localctx = NewIntegerContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(131)
			p.Match(PlanParserIntegerConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserFloatingConstant:
		localctx = NewFloatingContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(132)
			p.Match(PlanParserFloatingConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserBooleanConstant:
		localctx = NewBooleanContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(133)
			p.Match(PlanParserBooleanConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserStringLiteral:
		localctx = NewStringContext(p, localctx)
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(134)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserIdentifier:
		localctx = NewIdentifierContext(p, localctx)
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(135)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserMeta:
		localctx = NewMetaContext(p, localctx)
		p.EnterOuterAlt(localctx, 6)
		{
			p.SetState(136)
			p.Match(PlanParserMeta)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserJSONIdentifier:
		localctx = NewJSONIdentifierContext(p, localctx)
		p.EnterOuterAlt(localctx, 7)
		{
			p.SetState(137)
			p.Match(PlanParserJSONIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserLBRACE:
		localctx = NewTemplateVariableContext(p, localctx)
		p.EnterOuterAlt(localctx, 8)
		{
			p.SetState(138)
			p.Match(PlanParserLBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(139)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(140)
			p.Match(PlanParserRBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserT__0:
		localctx = NewParensContext(p, localctx)
		p.EnterOuterAlt(localctx, 9)
		{
			p.SetState(141)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(142)
			p.Expr()
		}
		{
			p.SetState(143)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserT__3:
		localctx = NewArrayContext(p, localctx)
		p.EnterOuterAlt(localctx, 10)
		{
			p.SetState(145)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(146)
			p.Expr()
		}
		p.SetState(151)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 15, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
		for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
			if _alt == 1 {
				{
					p.SetState(147)
					p.Match(PlanParserT__2)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(148)
					p.Expr()
				}

			}
			p.SetState(153)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 15, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
		}
		p.SetState(155)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__2 {
			{
				p.SetState(154)
				p.Match(PlanParserT__2)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		{
			p.SetState(157)
			p.Match(PlanParserT__4)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserEmptyArray:
		localctx = NewEmptyArrayContext(p, localctx)
		p.EnterOuterAlt(localctx, 11)
		{
			p.SetState(159)
			p.Match(PlanParserEmptyArray)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserTEXTMATCH:
		localctx = NewTextMatchContext(p, localctx)
		p.EnterOuterAlt(localctx, 12)
		{
			p.SetState(160)
			p.Match(PlanParserTEXTMATCH)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(161)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(162)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(163)
			p.Match(PlanParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(164)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(165)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserPHRASEMATCH:
		localctx = NewPhraseMatchContext(p, localctx)
		p.EnterOuterAlt(localctx, 13)
		{
			p.SetState(166)
			p.Match(PlanParserPHRASEMATCH)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(167)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(168)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(169)
			p.Match(PlanParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(170)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(173)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__2 {
			{
				p.SetState(171)
				p.Match(PlanParserT__2)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(172)
				p.Expr()
			}

		}
		{
			p.SetState(175)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserJSONContains, PlanParserArrayContains:
		localctx = NewJSONContainsContext(p, localctx)
		p.EnterOuterAlt(localctx, 14)
		{
			p.SetState(176)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContains || _la == PlanParserArrayContains) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(177)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(178)
			p.Expr()
		}
		{
			p.SetState(179)
			p.Match(PlanParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(180)
			p.Expr()
		}
		{
			p.SetState(181)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserJSONContainsAll, PlanParserArrayContainsAll:
		localctx = NewJSONContainsAllContext(p, localctx)
		p.EnterOuterAlt(localctx, 15)
		{
			p.SetState(183)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContainsAll || _la == PlanParserArrayContainsAll) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(184)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(185)
			p.Expr()
		}
		{
			p.SetState(186)
			p.Match(PlanParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(187)
			p.Expr()
		}
		{
			p.SetState(188)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserJSONContainsAny, PlanParserArrayContainsAny:
		localctx = NewJSONContainsAnyContext(p, localctx)
		p.EnterOuterAlt(localctx, 16)
		{
			p.SetState(190)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContainsAny || _la == PlanParserArrayContainsAny) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(191)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(192)
			p.Expr()
		}
		{
			p.SetState(193)
			p.Match(PlanParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(194)
			p.Expr()
		}
		{
			p.SetState(195)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserArrayLength:
		localctx = NewArrayLengthContext(p, localctx)
		p.EnterOuterAlt(localctx, 17)
		{
			p.SetState(197)
			p.Match(PlanParserArrayLength)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(198)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(199)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserIdentifier || _la == PlanParserJSONIdentifier) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(200)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}
