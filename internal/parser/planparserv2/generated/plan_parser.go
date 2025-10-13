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
		"", "'('", "')'", "'['", "','", "']'", "'{'", "'}'", "'<'", "'<='",
		"'>'", "'>='", "'=='", "'!='", "", "", "", "", "", "", "", "", "'='",
		"'+'", "'-'", "'*'", "'/'", "'%'", "'**'", "'<<'", "'>>'", "'&'", "'|'",
		"'^'", "", "", "", "", "'~'", "", "", "", "", "", "", "", "", "", "",
		"", "", "", "", "", "", "", "", "", "", "", "", "'$meta'",
	}
	staticData.SymbolicNames = []string{
		"", "", "", "", "", "", "LBRACE", "RBRACE", "LT", "LE", "GT", "GE",
		"EQ", "NE", "LIKE", "EXISTS", "TEXTMATCH", "PHRASEMATCH", "RANDOMSAMPLE",
		"INTERVAL", "ISO", "MINIMUM_SHOULD_MATCH", "ASSIGN", "ADD", "SUB", "MUL",
		"DIV", "MOD", "POW", "SHL", "SHR", "BAND", "BOR", "BXOR", "AND", "OR",
		"ISNULL", "ISNOTNULL", "BNOT", "NOT", "IN", "EmptyArray", "JSONContains",
		"JSONContainsAll", "JSONContainsAny", "ArrayContains", "ArrayContainsAll",
		"ArrayContainsAny", "ArrayLength", "STEuqals", "STTouches", "STOverlaps",
		"STCrosses", "STContains", "STIntersects", "STWithin", "STDWithin",
		"BooleanConstant", "IntegerConstant", "FloatingConstant", "Identifier",
		"Meta", "StringLiteral", "JSONIdentifier", "Whitespace", "Newline",
	}
	staticData.RuleNames = []string{
		"expr", "textMatchOption",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 1, 65, 231, 2, 0, 7, 0, 2, 1, 7, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3,
		0, 10, 8, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 5, 0, 32, 8,
		0, 10, 0, 12, 0, 35, 9, 0, 1, 0, 3, 0, 38, 8, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3, 0, 52, 8, 0, 1, 0, 1,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3, 0, 62, 8, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 5, 0, 153, 8, 0, 10, 0, 12, 0, 156, 9, 0, 1, 0, 3, 0, 159,
		8, 0, 3, 0, 161, 8, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3, 0, 168, 8, 0, 1,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
		0, 1, 0, 3, 0, 184, 8, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 5, 0, 222, 8, 0, 10, 0, 12, 0, 225, 9, 0, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 2, 0, 2, 0, 14, 1, 0, 23, 24, 1, 0,
		8, 13, 1, 0, 60, 61, 2, 0, 23, 24, 38, 39, 2, 0, 42, 42, 45, 45, 2, 0,
		43, 43, 46, 46, 2, 0, 44, 44, 47, 47, 2, 0, 60, 60, 63, 63, 1, 0, 25, 27,
		1, 0, 29, 30, 1, 0, 8, 9, 1, 0, 10, 11, 1, 0, 8, 11, 1, 0, 12, 13, 282,
		0, 167, 1, 0, 0, 0, 2, 226, 1, 0, 0, 0, 4, 5, 6, 0, -1, 0, 5, 9, 5, 60,
		0, 0, 6, 7, 7, 0, 0, 0, 7, 8, 5, 19, 0, 0, 8, 10, 5, 62, 0, 0, 9, 6, 1,
		0, 0, 0, 9, 10, 1, 0, 0, 0, 10, 11, 1, 0, 0, 0, 11, 12, 7, 1, 0, 0, 12,
		13, 5, 20, 0, 0, 13, 168, 5, 62, 0, 0, 14, 168, 5, 58, 0, 0, 15, 168, 5,
		59, 0, 0, 16, 168, 5, 57, 0, 0, 17, 168, 5, 62, 0, 0, 18, 168, 7, 2, 0,
		0, 19, 168, 5, 63, 0, 0, 20, 21, 5, 6, 0, 0, 21, 22, 5, 60, 0, 0, 22, 168,
		5, 7, 0, 0, 23, 24, 5, 1, 0, 0, 24, 25, 3, 0, 0, 0, 25, 26, 5, 2, 0, 0,
		26, 168, 1, 0, 0, 0, 27, 28, 5, 3, 0, 0, 28, 33, 3, 0, 0, 0, 29, 30, 5,
		4, 0, 0, 30, 32, 3, 0, 0, 0, 31, 29, 1, 0, 0, 0, 32, 35, 1, 0, 0, 0, 33,
		31, 1, 0, 0, 0, 33, 34, 1, 0, 0, 0, 34, 37, 1, 0, 0, 0, 35, 33, 1, 0, 0,
		0, 36, 38, 5, 4, 0, 0, 37, 36, 1, 0, 0, 0, 37, 38, 1, 0, 0, 0, 38, 39,
		1, 0, 0, 0, 39, 40, 5, 5, 0, 0, 40, 168, 1, 0, 0, 0, 41, 168, 5, 41, 0,
		0, 42, 43, 5, 15, 0, 0, 43, 168, 3, 0, 0, 35, 44, 45, 5, 16, 0, 0, 45,
		46, 5, 1, 0, 0, 46, 47, 5, 60, 0, 0, 47, 48, 5, 4, 0, 0, 48, 51, 5, 62,
		0, 0, 49, 50, 5, 4, 0, 0, 50, 52, 3, 2, 1, 0, 51, 49, 1, 0, 0, 0, 51, 52,
		1, 0, 0, 0, 52, 53, 1, 0, 0, 0, 53, 168, 5, 2, 0, 0, 54, 55, 5, 17, 0,
		0, 55, 56, 5, 1, 0, 0, 56, 57, 5, 60, 0, 0, 57, 58, 5, 4, 0, 0, 58, 61,
		5, 62, 0, 0, 59, 60, 5, 4, 0, 0, 60, 62, 3, 0, 0, 0, 61, 59, 1, 0, 0, 0,
		61, 62, 1, 0, 0, 0, 62, 63, 1, 0, 0, 0, 63, 168, 5, 2, 0, 0, 64, 65, 5,
		18, 0, 0, 65, 66, 5, 1, 0, 0, 66, 67, 3, 0, 0, 0, 67, 68, 5, 2, 0, 0, 68,
		168, 1, 0, 0, 0, 69, 70, 7, 3, 0, 0, 70, 168, 3, 0, 0, 29, 71, 72, 7, 4,
		0, 0, 72, 73, 5, 1, 0, 0, 73, 74, 3, 0, 0, 0, 74, 75, 5, 4, 0, 0, 75, 76,
		3, 0, 0, 0, 76, 77, 5, 2, 0, 0, 77, 168, 1, 0, 0, 0, 78, 79, 7, 5, 0, 0,
		79, 80, 5, 1, 0, 0, 80, 81, 3, 0, 0, 0, 81, 82, 5, 4, 0, 0, 82, 83, 3,
		0, 0, 0, 83, 84, 5, 2, 0, 0, 84, 168, 1, 0, 0, 0, 85, 86, 7, 6, 0, 0, 86,
		87, 5, 1, 0, 0, 87, 88, 3, 0, 0, 0, 88, 89, 5, 4, 0, 0, 89, 90, 3, 0, 0,
		0, 90, 91, 5, 2, 0, 0, 91, 168, 1, 0, 0, 0, 92, 93, 5, 49, 0, 0, 93, 94,
		5, 1, 0, 0, 94, 95, 5, 60, 0, 0, 95, 96, 5, 4, 0, 0, 96, 97, 5, 62, 0,
		0, 97, 168, 5, 2, 0, 0, 98, 99, 5, 50, 0, 0, 99, 100, 5, 1, 0, 0, 100,
		101, 5, 60, 0, 0, 101, 102, 5, 4, 0, 0, 102, 103, 5, 62, 0, 0, 103, 168,
		5, 2, 0, 0, 104, 105, 5, 51, 0, 0, 105, 106, 5, 1, 0, 0, 106, 107, 5, 60,
		0, 0, 107, 108, 5, 4, 0, 0, 108, 109, 5, 62, 0, 0, 109, 168, 5, 2, 0, 0,
		110, 111, 5, 52, 0, 0, 111, 112, 5, 1, 0, 0, 112, 113, 5, 60, 0, 0, 113,
		114, 5, 4, 0, 0, 114, 115, 5, 62, 0, 0, 115, 168, 5, 2, 0, 0, 116, 117,
		5, 53, 0, 0, 117, 118, 5, 1, 0, 0, 118, 119, 5, 60, 0, 0, 119, 120, 5,
		4, 0, 0, 120, 121, 5, 62, 0, 0, 121, 168, 5, 2, 0, 0, 122, 123, 5, 54,
		0, 0, 123, 124, 5, 1, 0, 0, 124, 125, 5, 60, 0, 0, 125, 126, 5, 4, 0, 0,
		126, 127, 5, 62, 0, 0, 127, 168, 5, 2, 0, 0, 128, 129, 5, 55, 0, 0, 129,
		130, 5, 1, 0, 0, 130, 131, 5, 60, 0, 0, 131, 132, 5, 4, 0, 0, 132, 133,
		5, 62, 0, 0, 133, 168, 5, 2, 0, 0, 134, 135, 5, 56, 0, 0, 135, 136, 5,
		1, 0, 0, 136, 137, 5, 60, 0, 0, 137, 138, 5, 4, 0, 0, 138, 139, 5, 62,
		0, 0, 139, 140, 5, 4, 0, 0, 140, 141, 3, 0, 0, 0, 141, 142, 5, 2, 0, 0,
		142, 168, 1, 0, 0, 0, 143, 144, 5, 48, 0, 0, 144, 145, 5, 1, 0, 0, 145,
		146, 7, 7, 0, 0, 146, 168, 5, 2, 0, 0, 147, 148, 5, 60, 0, 0, 148, 160,
		5, 1, 0, 0, 149, 154, 3, 0, 0, 0, 150, 151, 5, 4, 0, 0, 151, 153, 3, 0,
		0, 0, 152, 150, 1, 0, 0, 0, 153, 156, 1, 0, 0, 0, 154, 152, 1, 0, 0, 0,
		154, 155, 1, 0, 0, 0, 155, 158, 1, 0, 0, 0, 156, 154, 1, 0, 0, 0, 157,
		159, 5, 4, 0, 0, 158, 157, 1, 0, 0, 0, 158, 159, 1, 0, 0, 0, 159, 161,
		1, 0, 0, 0, 160, 149, 1, 0, 0, 0, 160, 161, 1, 0, 0, 0, 161, 162, 1, 0,
		0, 0, 162, 168, 5, 2, 0, 0, 163, 164, 7, 7, 0, 0, 164, 168, 5, 36, 0, 0,
		165, 166, 7, 7, 0, 0, 166, 168, 5, 37, 0, 0, 167, 4, 1, 0, 0, 0, 167, 14,
		1, 0, 0, 0, 167, 15, 1, 0, 0, 0, 167, 16, 1, 0, 0, 0, 167, 17, 1, 0, 0,
		0, 167, 18, 1, 0, 0, 0, 167, 19, 1, 0, 0, 0, 167, 20, 1, 0, 0, 0, 167,
		23, 1, 0, 0, 0, 167, 27, 1, 0, 0, 0, 167, 41, 1, 0, 0, 0, 167, 42, 1, 0,
		0, 0, 167, 44, 1, 0, 0, 0, 167, 54, 1, 0, 0, 0, 167, 64, 1, 0, 0, 0, 167,
		69, 1, 0, 0, 0, 167, 71, 1, 0, 0, 0, 167, 78, 1, 0, 0, 0, 167, 85, 1, 0,
		0, 0, 167, 92, 1, 0, 0, 0, 167, 98, 1, 0, 0, 0, 167, 104, 1, 0, 0, 0, 167,
		110, 1, 0, 0, 0, 167, 116, 1, 0, 0, 0, 167, 122, 1, 0, 0, 0, 167, 128,
		1, 0, 0, 0, 167, 134, 1, 0, 0, 0, 167, 143, 1, 0, 0, 0, 167, 147, 1, 0,
		0, 0, 167, 163, 1, 0, 0, 0, 167, 165, 1, 0, 0, 0, 168, 223, 1, 0, 0, 0,
		169, 170, 10, 30, 0, 0, 170, 171, 5, 28, 0, 0, 171, 222, 3, 0, 0, 31, 172,
		173, 10, 28, 0, 0, 173, 174, 7, 8, 0, 0, 174, 222, 3, 0, 0, 29, 175, 176,
		10, 27, 0, 0, 176, 177, 7, 0, 0, 0, 177, 222, 3, 0, 0, 28, 178, 179, 10,
		26, 0, 0, 179, 180, 7, 9, 0, 0, 180, 222, 3, 0, 0, 27, 181, 183, 10, 25,
		0, 0, 182, 184, 5, 39, 0, 0, 183, 182, 1, 0, 0, 0, 183, 184, 1, 0, 0, 0,
		184, 185, 1, 0, 0, 0, 185, 186, 5, 40, 0, 0, 186, 222, 3, 0, 0, 26, 187,
		188, 10, 11, 0, 0, 188, 189, 7, 10, 0, 0, 189, 190, 7, 7, 0, 0, 190, 191,
		7, 10, 0, 0, 191, 222, 3, 0, 0, 12, 192, 193, 10, 10, 0, 0, 193, 194, 7,
		11, 0, 0, 194, 195, 7, 7, 0, 0, 195, 196, 7, 11, 0, 0, 196, 222, 3, 0,
		0, 11, 197, 198, 10, 9, 0, 0, 198, 199, 7, 12, 0, 0, 199, 222, 3, 0, 0,
		10, 200, 201, 10, 8, 0, 0, 201, 202, 7, 13, 0, 0, 202, 222, 3, 0, 0, 9,
		203, 204, 10, 7, 0, 0, 204, 205, 5, 31, 0, 0, 205, 222, 3, 0, 0, 8, 206,
		207, 10, 6, 0, 0, 207, 208, 5, 33, 0, 0, 208, 222, 3, 0, 0, 7, 209, 210,
		10, 5, 0, 0, 210, 211, 5, 32, 0, 0, 211, 222, 3, 0, 0, 6, 212, 213, 10,
		4, 0, 0, 213, 214, 5, 34, 0, 0, 214, 222, 3, 0, 0, 5, 215, 216, 10, 3,
		0, 0, 216, 217, 5, 35, 0, 0, 217, 222, 3, 0, 0, 4, 218, 219, 10, 34, 0,
		0, 219, 220, 5, 14, 0, 0, 220, 222, 5, 62, 0, 0, 221, 169, 1, 0, 0, 0,
		221, 172, 1, 0, 0, 0, 221, 175, 1, 0, 0, 0, 221, 178, 1, 0, 0, 0, 221,
		181, 1, 0, 0, 0, 221, 187, 1, 0, 0, 0, 221, 192, 1, 0, 0, 0, 221, 197,
		1, 0, 0, 0, 221, 200, 1, 0, 0, 0, 221, 203, 1, 0, 0, 0, 221, 206, 1, 0,
		0, 0, 221, 209, 1, 0, 0, 0, 221, 212, 1, 0, 0, 0, 221, 215, 1, 0, 0, 0,
		221, 218, 1, 0, 0, 0, 222, 225, 1, 0, 0, 0, 223, 221, 1, 0, 0, 0, 223,
		224, 1, 0, 0, 0, 224, 1, 1, 0, 0, 0, 225, 223, 1, 0, 0, 0, 226, 227, 5,
		21, 0, 0, 227, 228, 5, 22, 0, 0, 228, 229, 5, 58, 0, 0, 229, 3, 1, 0, 0,
		0, 12, 9, 33, 37, 51, 61, 154, 158, 160, 167, 183, 221, 223,
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
	PlanParserEOF                  = antlr.TokenEOF
	PlanParserT__0                 = 1
	PlanParserT__1                 = 2
	PlanParserT__2                 = 3
	PlanParserT__3                 = 4
	PlanParserT__4                 = 5
	PlanParserLBRACE               = 6
	PlanParserRBRACE               = 7
	PlanParserLT                   = 8
	PlanParserLE                   = 9
	PlanParserGT                   = 10
	PlanParserGE                   = 11
	PlanParserEQ                   = 12
	PlanParserNE                   = 13
	PlanParserLIKE                 = 14
	PlanParserEXISTS               = 15
	PlanParserTEXTMATCH            = 16
	PlanParserPHRASEMATCH          = 17
	PlanParserRANDOMSAMPLE         = 18
	PlanParserINTERVAL             = 19
	PlanParserISO                  = 20
	PlanParserMINIMUM_SHOULD_MATCH = 21
	PlanParserASSIGN               = 22
	PlanParserADD                  = 23
	PlanParserSUB                  = 24
	PlanParserMUL                  = 25
	PlanParserDIV                  = 26
	PlanParserMOD                  = 27
	PlanParserPOW                  = 28
	PlanParserSHL                  = 29
	PlanParserSHR                  = 30
	PlanParserBAND                 = 31
	PlanParserBOR                  = 32
	PlanParserBXOR                 = 33
	PlanParserAND                  = 34
	PlanParserOR                   = 35
	PlanParserISNULL               = 36
	PlanParserISNOTNULL            = 37
	PlanParserBNOT                 = 38
	PlanParserNOT                  = 39
	PlanParserIN                   = 40
	PlanParserEmptyArray           = 41
	PlanParserJSONContains         = 42
	PlanParserJSONContainsAll      = 43
	PlanParserJSONContainsAny      = 44
	PlanParserArrayContains        = 45
	PlanParserArrayContainsAll     = 46
	PlanParserArrayContainsAny     = 47
	PlanParserArrayLength          = 48
	PlanParserSTEuqals             = 49
	PlanParserSTTouches            = 50
	PlanParserSTOverlaps           = 51
	PlanParserSTCrosses            = 52
	PlanParserSTContains           = 53
	PlanParserSTIntersects         = 54
	PlanParserSTWithin             = 55
	PlanParserSTDWithin            = 56
	PlanParserBooleanConstant      = 57
	PlanParserIntegerConstant      = 58
	PlanParserFloatingConstant     = 59
	PlanParserIdentifier           = 60
	PlanParserMeta                 = 61
	PlanParserStringLiteral        = 62
	PlanParserJSONIdentifier       = 63
	PlanParserWhitespace           = 64
	PlanParserNewline              = 65
)

// PlanParser rules.
const (
	PlanParserRULE_expr            = 0
	PlanParserRULE_textMatchOption = 1
)

// IExprContext is an interface to support dynamic dispatch.
type IExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
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

func (s *ExprContext) CopyAll(ctx *ExprContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *ExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type JSONIdentifierContext struct {
	ExprContext
}

func NewJSONIdentifierContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *JSONIdentifierContext {
	var p = new(JSONIdentifierContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *JSONIdentifierContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *JSONIdentifierContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *JSONIdentifierContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterJSONIdentifier(s)
	}
}

func (s *JSONIdentifierContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitJSONIdentifier(s)
	}
}

func (s *JSONIdentifierContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitJSONIdentifier(s)

	default:
		return t.VisitChildren(s)
	}
}

type RandomSampleContext struct {
	ExprContext
}

func NewRandomSampleContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *RandomSampleContext {
	var p = new(RandomSampleContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *RandomSampleContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RandomSampleContext) RANDOMSAMPLE() antlr.TerminalNode {
	return s.GetToken(PlanParserRANDOMSAMPLE, 0)
}

func (s *RandomSampleContext) Expr() IExprContext {
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

func (s *RandomSampleContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterRandomSample(s)
	}
}

func (s *RandomSampleContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitRandomSample(s)
	}
}

func (s *RandomSampleContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitRandomSample(s)

	default:
		return t.VisitChildren(s)
	}
}

type ParensContext struct {
	ExprContext
}

func NewParensContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ParensContext {
	var p = new(ParensContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

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

func (s *ParensContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterParens(s)
	}
}

func (s *ParensContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitParens(s)
	}
}

func (s *ParensContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitParens(s)

	default:
		return t.VisitChildren(s)
	}
}

type StringContext struct {
	ExprContext
}

func NewStringContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *StringContext {
	var p = new(StringContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *StringContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StringContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *StringContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterString(s)
	}
}

func (s *StringContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitString(s)
	}
}

func (s *StringContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitString(s)

	default:
		return t.VisitChildren(s)
	}
}

type FloatingContext struct {
	ExprContext
}

func NewFloatingContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *FloatingContext {
	var p = new(FloatingContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *FloatingContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FloatingContext) FloatingConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserFloatingConstant, 0)
}

func (s *FloatingContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterFloating(s)
	}
}

func (s *FloatingContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitFloating(s)
	}
}

func (s *FloatingContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitFloating(s)

	default:
		return t.VisitChildren(s)
	}
}

type JSONContainsAllContext struct {
	ExprContext
}

func NewJSONContainsAllContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *JSONContainsAllContext {
	var p = new(JSONContainsAllContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

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

func (s *JSONContainsAllContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterJSONContainsAll(s)
	}
}

func (s *JSONContainsAllContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitJSONContainsAll(s)
	}
}

func (s *JSONContainsAllContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitJSONContainsAll(s)

	default:
		return t.VisitChildren(s)
	}
}

type LogicalOrContext struct {
	ExprContext
}

func NewLogicalOrContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LogicalOrContext {
	var p = new(LogicalOrContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *LogicalOrContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LogicalOrContext) AllExpr() []IExprContext {
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

func (s *LogicalOrContext) Expr(i int) IExprContext {
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

func (s *LogicalOrContext) OR() antlr.TerminalNode {
	return s.GetToken(PlanParserOR, 0)
}

func (s *LogicalOrContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterLogicalOr(s)
	}
}

func (s *LogicalOrContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitLogicalOr(s)
	}
}

func (s *LogicalOrContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitLogicalOr(s)

	default:
		return t.VisitChildren(s)
	}
}

type IsNotNullContext struct {
	ExprContext
}

func NewIsNotNullContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IsNotNullContext {
	var p = new(IsNotNullContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *IsNotNullContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IsNotNullContext) ISNOTNULL() antlr.TerminalNode {
	return s.GetToken(PlanParserISNOTNULL, 0)
}

func (s *IsNotNullContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *IsNotNullContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *IsNotNullContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterIsNotNull(s)
	}
}

func (s *IsNotNullContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitIsNotNull(s)
	}
}

func (s *IsNotNullContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitIsNotNull(s)

	default:
		return t.VisitChildren(s)
	}
}

type MulDivModContext struct {
	ExprContext
	op antlr.Token
}

func NewMulDivModContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MulDivModContext {
	var p = new(MulDivModContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *MulDivModContext) GetOp() antlr.Token { return s.op }

func (s *MulDivModContext) SetOp(v antlr.Token) { s.op = v }

func (s *MulDivModContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MulDivModContext) AllExpr() []IExprContext {
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

func (s *MulDivModContext) Expr(i int) IExprContext {
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

func (s *MulDivModContext) MUL() antlr.TerminalNode {
	return s.GetToken(PlanParserMUL, 0)
}

func (s *MulDivModContext) DIV() antlr.TerminalNode {
	return s.GetToken(PlanParserDIV, 0)
}

func (s *MulDivModContext) MOD() antlr.TerminalNode {
	return s.GetToken(PlanParserMOD, 0)
}

func (s *MulDivModContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterMulDivMod(s)
	}
}

func (s *MulDivModContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitMulDivMod(s)
	}
}

func (s *MulDivModContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMulDivMod(s)

	default:
		return t.VisitChildren(s)
	}
}

type IdentifierContext struct {
	ExprContext
}

func NewIdentifierContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IdentifierContext {
	var p = new(IdentifierContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *IdentifierContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IdentifierContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *IdentifierContext) Meta() antlr.TerminalNode {
	return s.GetToken(PlanParserMeta, 0)
}

func (s *IdentifierContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterIdentifier(s)
	}
}

func (s *IdentifierContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitIdentifier(s)
	}
}

func (s *IdentifierContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitIdentifier(s)

	default:
		return t.VisitChildren(s)
	}
}

type STIntersectsContext struct {
	ExprContext
}

func NewSTIntersectsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *STIntersectsContext {
	var p = new(STIntersectsContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *STIntersectsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *STIntersectsContext) STIntersects() antlr.TerminalNode {
	return s.GetToken(PlanParserSTIntersects, 0)
}

func (s *STIntersectsContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *STIntersectsContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *STIntersectsContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterSTIntersects(s)
	}
}

func (s *STIntersectsContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitSTIntersects(s)
	}
}

func (s *STIntersectsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTIntersects(s)

	default:
		return t.VisitChildren(s)
	}
}

type LikeContext struct {
	ExprContext
}

func NewLikeContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LikeContext {
	var p = new(LikeContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *LikeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LikeContext) Expr() IExprContext {
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

func (s *LikeContext) LIKE() antlr.TerminalNode {
	return s.GetToken(PlanParserLIKE, 0)
}

func (s *LikeContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *LikeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterLike(s)
	}
}

func (s *LikeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitLike(s)
	}
}

func (s *LikeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitLike(s)

	default:
		return t.VisitChildren(s)
	}
}

type LogicalAndContext struct {
	ExprContext
}

func NewLogicalAndContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LogicalAndContext {
	var p = new(LogicalAndContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *LogicalAndContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LogicalAndContext) AllExpr() []IExprContext {
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

func (s *LogicalAndContext) Expr(i int) IExprContext {
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

func (s *LogicalAndContext) AND() antlr.TerminalNode {
	return s.GetToken(PlanParserAND, 0)
}

func (s *LogicalAndContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterLogicalAnd(s)
	}
}

func (s *LogicalAndContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitLogicalAnd(s)
	}
}

func (s *LogicalAndContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitLogicalAnd(s)

	default:
		return t.VisitChildren(s)
	}
}

type TemplateVariableContext struct {
	ExprContext
}

func NewTemplateVariableContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TemplateVariableContext {
	var p = new(TemplateVariableContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

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

func (s *TemplateVariableContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterTemplateVariable(s)
	}
}

func (s *TemplateVariableContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitTemplateVariable(s)
	}
}

func (s *TemplateVariableContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTemplateVariable(s)

	default:
		return t.VisitChildren(s)
	}
}

type EqualityContext struct {
	ExprContext
	op antlr.Token
}

func NewEqualityContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *EqualityContext {
	var p = new(EqualityContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *EqualityContext) GetOp() antlr.Token { return s.op }

func (s *EqualityContext) SetOp(v antlr.Token) { s.op = v }

func (s *EqualityContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *EqualityContext) AllExpr() []IExprContext {
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

func (s *EqualityContext) Expr(i int) IExprContext {
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

func (s *EqualityContext) EQ() antlr.TerminalNode {
	return s.GetToken(PlanParserEQ, 0)
}

func (s *EqualityContext) NE() antlr.TerminalNode {
	return s.GetToken(PlanParserNE, 0)
}

func (s *EqualityContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterEquality(s)
	}
}

func (s *EqualityContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitEquality(s)
	}
}

func (s *EqualityContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitEquality(s)

	default:
		return t.VisitChildren(s)
	}
}

type BooleanContext struct {
	ExprContext
}

func NewBooleanContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BooleanContext {
	var p = new(BooleanContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *BooleanContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BooleanContext) BooleanConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserBooleanConstant, 0)
}

func (s *BooleanContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterBoolean(s)
	}
}

func (s *BooleanContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitBoolean(s)
	}
}

func (s *BooleanContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitBoolean(s)

	default:
		return t.VisitChildren(s)
	}
}

type STDWithinContext struct {
	ExprContext
}

func NewSTDWithinContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *STDWithinContext {
	var p = new(STDWithinContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *STDWithinContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *STDWithinContext) STDWithin() antlr.TerminalNode {
	return s.GetToken(PlanParserSTDWithin, 0)
}

func (s *STDWithinContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *STDWithinContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *STDWithinContext) Expr() IExprContext {
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

func (s *STDWithinContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterSTDWithin(s)
	}
}

func (s *STDWithinContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitSTDWithin(s)
	}
}

func (s *STDWithinContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTDWithin(s)

	default:
		return t.VisitChildren(s)
	}
}

type ShiftContext struct {
	ExprContext
	op antlr.Token
}

func NewShiftContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ShiftContext {
	var p = new(ShiftContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *ShiftContext) GetOp() antlr.Token { return s.op }

func (s *ShiftContext) SetOp(v antlr.Token) { s.op = v }

func (s *ShiftContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ShiftContext) AllExpr() []IExprContext {
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

func (s *ShiftContext) Expr(i int) IExprContext {
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

func (s *ShiftContext) SHL() antlr.TerminalNode {
	return s.GetToken(PlanParserSHL, 0)
}

func (s *ShiftContext) SHR() antlr.TerminalNode {
	return s.GetToken(PlanParserSHR, 0)
}

func (s *ShiftContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterShift(s)
	}
}

func (s *ShiftContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitShift(s)
	}
}

func (s *ShiftContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitShift(s)

	default:
		return t.VisitChildren(s)
	}
}

type CallContext struct {
	ExprContext
}

func NewCallContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *CallContext {
	var p = new(CallContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *CallContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CallContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *CallContext) AllExpr() []IExprContext {
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

func (s *CallContext) Expr(i int) IExprContext {
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

func (s *CallContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterCall(s)
	}
}

func (s *CallContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitCall(s)
	}
}

func (s *CallContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitCall(s)

	default:
		return t.VisitChildren(s)
	}
}

type STCrossesContext struct {
	ExprContext
}

func NewSTCrossesContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *STCrossesContext {
	var p = new(STCrossesContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *STCrossesContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *STCrossesContext) STCrosses() antlr.TerminalNode {
	return s.GetToken(PlanParserSTCrosses, 0)
}

func (s *STCrossesContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *STCrossesContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *STCrossesContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterSTCrosses(s)
	}
}

func (s *STCrossesContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitSTCrosses(s)
	}
}

func (s *STCrossesContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTCrosses(s)

	default:
		return t.VisitChildren(s)
	}
}

type ReverseRangeContext struct {
	ExprContext
	op1 antlr.Token
	op2 antlr.Token
}

func NewReverseRangeContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ReverseRangeContext {
	var p = new(ReverseRangeContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *ReverseRangeContext) GetOp1() antlr.Token { return s.op1 }

func (s *ReverseRangeContext) GetOp2() antlr.Token { return s.op2 }

func (s *ReverseRangeContext) SetOp1(v antlr.Token) { s.op1 = v }

func (s *ReverseRangeContext) SetOp2(v antlr.Token) { s.op2 = v }

func (s *ReverseRangeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ReverseRangeContext) AllExpr() []IExprContext {
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

func (s *ReverseRangeContext) Expr(i int) IExprContext {
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

func (s *ReverseRangeContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *ReverseRangeContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *ReverseRangeContext) AllGT() []antlr.TerminalNode {
	return s.GetTokens(PlanParserGT)
}

func (s *ReverseRangeContext) GT(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserGT, i)
}

func (s *ReverseRangeContext) AllGE() []antlr.TerminalNode {
	return s.GetTokens(PlanParserGE)
}

func (s *ReverseRangeContext) GE(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserGE, i)
}

func (s *ReverseRangeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterReverseRange(s)
	}
}

func (s *ReverseRangeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitReverseRange(s)
	}
}

func (s *ReverseRangeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitReverseRange(s)

	default:
		return t.VisitChildren(s)
	}
}

type BitOrContext struct {
	ExprContext
}

func NewBitOrContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BitOrContext {
	var p = new(BitOrContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *BitOrContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BitOrContext) AllExpr() []IExprContext {
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

func (s *BitOrContext) Expr(i int) IExprContext {
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

func (s *BitOrContext) BOR() antlr.TerminalNode {
	return s.GetToken(PlanParserBOR, 0)
}

func (s *BitOrContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterBitOr(s)
	}
}

func (s *BitOrContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitBitOr(s)
	}
}

func (s *BitOrContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitBitOr(s)

	default:
		return t.VisitChildren(s)
	}
}

type EmptyArrayContext struct {
	ExprContext
}

func NewEmptyArrayContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *EmptyArrayContext {
	var p = new(EmptyArrayContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *EmptyArrayContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *EmptyArrayContext) EmptyArray() antlr.TerminalNode {
	return s.GetToken(PlanParserEmptyArray, 0)
}

func (s *EmptyArrayContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterEmptyArray(s)
	}
}

func (s *EmptyArrayContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitEmptyArray(s)
	}
}

func (s *EmptyArrayContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitEmptyArray(s)

	default:
		return t.VisitChildren(s)
	}
}

type AddSubContext struct {
	ExprContext
	op antlr.Token
}

func NewAddSubContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *AddSubContext {
	var p = new(AddSubContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *AddSubContext) GetOp() antlr.Token { return s.op }

func (s *AddSubContext) SetOp(v antlr.Token) { s.op = v }

func (s *AddSubContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AddSubContext) AllExpr() []IExprContext {
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

func (s *AddSubContext) Expr(i int) IExprContext {
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

func (s *AddSubContext) ADD() antlr.TerminalNode {
	return s.GetToken(PlanParserADD, 0)
}

func (s *AddSubContext) SUB() antlr.TerminalNode {
	return s.GetToken(PlanParserSUB, 0)
}

func (s *AddSubContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterAddSub(s)
	}
}

func (s *AddSubContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitAddSub(s)
	}
}

func (s *AddSubContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitAddSub(s)

	default:
		return t.VisitChildren(s)
	}
}

type PhraseMatchContext struct {
	ExprContext
}

func NewPhraseMatchContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PhraseMatchContext {
	var p = new(PhraseMatchContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

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

func (s *PhraseMatchContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterPhraseMatch(s)
	}
}

func (s *PhraseMatchContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitPhraseMatch(s)
	}
}

func (s *PhraseMatchContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitPhraseMatch(s)

	default:
		return t.VisitChildren(s)
	}
}

type RelationalContext struct {
	ExprContext
	op antlr.Token
}

func NewRelationalContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *RelationalContext {
	var p = new(RelationalContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *RelationalContext) GetOp() antlr.Token { return s.op }

func (s *RelationalContext) SetOp(v antlr.Token) { s.op = v }

func (s *RelationalContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RelationalContext) AllExpr() []IExprContext {
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

func (s *RelationalContext) Expr(i int) IExprContext {
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

func (s *RelationalContext) LT() antlr.TerminalNode {
	return s.GetToken(PlanParserLT, 0)
}

func (s *RelationalContext) LE() antlr.TerminalNode {
	return s.GetToken(PlanParserLE, 0)
}

func (s *RelationalContext) GT() antlr.TerminalNode {
	return s.GetToken(PlanParserGT, 0)
}

func (s *RelationalContext) GE() antlr.TerminalNode {
	return s.GetToken(PlanParserGE, 0)
}

func (s *RelationalContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterRelational(s)
	}
}

func (s *RelationalContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitRelational(s)
	}
}

func (s *RelationalContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitRelational(s)

	default:
		return t.VisitChildren(s)
	}
}

type ArrayLengthContext struct {
	ExprContext
}

func NewArrayLengthContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ArrayLengthContext {
	var p = new(ArrayLengthContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

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

func (s *ArrayLengthContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterArrayLength(s)
	}
}

func (s *ArrayLengthContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitArrayLength(s)
	}
}

func (s *ArrayLengthContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitArrayLength(s)

	default:
		return t.VisitChildren(s)
	}
}

type TextMatchContext struct {
	ExprContext
}

func NewTextMatchContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TextMatchContext {
	var p = new(TextMatchContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

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

func (s *TextMatchContext) TextMatchOption() ITextMatchOptionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITextMatchOptionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITextMatchOptionContext)
}

func (s *TextMatchContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterTextMatch(s)
	}
}

func (s *TextMatchContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitTextMatch(s)
	}
}

func (s *TextMatchContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTextMatch(s)

	default:
		return t.VisitChildren(s)
	}
}

type STTouchesContext struct {
	ExprContext
}

func NewSTTouchesContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *STTouchesContext {
	var p = new(STTouchesContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *STTouchesContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *STTouchesContext) STTouches() antlr.TerminalNode {
	return s.GetToken(PlanParserSTTouches, 0)
}

func (s *STTouchesContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *STTouchesContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *STTouchesContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterSTTouches(s)
	}
}

func (s *STTouchesContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitSTTouches(s)
	}
}

func (s *STTouchesContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTTouches(s)

	default:
		return t.VisitChildren(s)
	}
}

type STContainsContext struct {
	ExprContext
}

func NewSTContainsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *STContainsContext {
	var p = new(STContainsContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *STContainsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *STContainsContext) STContains() antlr.TerminalNode {
	return s.GetToken(PlanParserSTContains, 0)
}

func (s *STContainsContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *STContainsContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *STContainsContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterSTContains(s)
	}
}

func (s *STContainsContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitSTContains(s)
	}
}

func (s *STContainsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTContains(s)

	default:
		return t.VisitChildren(s)
	}
}

type TermContext struct {
	ExprContext
	op antlr.Token
}

func NewTermContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TermContext {
	var p = new(TermContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *TermContext) GetOp() antlr.Token { return s.op }

func (s *TermContext) SetOp(v antlr.Token) { s.op = v }

func (s *TermContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TermContext) AllExpr() []IExprContext {
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

func (s *TermContext) Expr(i int) IExprContext {
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

func (s *TermContext) IN() antlr.TerminalNode {
	return s.GetToken(PlanParserIN, 0)
}

func (s *TermContext) NOT() antlr.TerminalNode {
	return s.GetToken(PlanParserNOT, 0)
}

func (s *TermContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterTerm(s)
	}
}

func (s *TermContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitTerm(s)
	}
}

func (s *TermContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTerm(s)

	default:
		return t.VisitChildren(s)
	}
}

type JSONContainsContext struct {
	ExprContext
}

func NewJSONContainsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *JSONContainsContext {
	var p = new(JSONContainsContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

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

func (s *JSONContainsContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterJSONContains(s)
	}
}

func (s *JSONContainsContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitJSONContains(s)
	}
}

func (s *JSONContainsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitJSONContains(s)

	default:
		return t.VisitChildren(s)
	}
}

type STWithinContext struct {
	ExprContext
}

func NewSTWithinContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *STWithinContext {
	var p = new(STWithinContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *STWithinContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *STWithinContext) STWithin() antlr.TerminalNode {
	return s.GetToken(PlanParserSTWithin, 0)
}

func (s *STWithinContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *STWithinContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *STWithinContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterSTWithin(s)
	}
}

func (s *STWithinContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitSTWithin(s)
	}
}

func (s *STWithinContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTWithin(s)

	default:
		return t.VisitChildren(s)
	}
}

type RangeContext struct {
	ExprContext
	op1 antlr.Token
	op2 antlr.Token
}

func NewRangeContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *RangeContext {
	var p = new(RangeContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *RangeContext) GetOp1() antlr.Token { return s.op1 }

func (s *RangeContext) GetOp2() antlr.Token { return s.op2 }

func (s *RangeContext) SetOp1(v antlr.Token) { s.op1 = v }

func (s *RangeContext) SetOp2(v antlr.Token) { s.op2 = v }

func (s *RangeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RangeContext) AllExpr() []IExprContext {
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

func (s *RangeContext) Expr(i int) IExprContext {
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

func (s *RangeContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *RangeContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *RangeContext) AllLT() []antlr.TerminalNode {
	return s.GetTokens(PlanParserLT)
}

func (s *RangeContext) LT(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserLT, i)
}

func (s *RangeContext) AllLE() []antlr.TerminalNode {
	return s.GetTokens(PlanParserLE)
}

func (s *RangeContext) LE(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserLE, i)
}

func (s *RangeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterRange(s)
	}
}

func (s *RangeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitRange(s)
	}
}

func (s *RangeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitRange(s)

	default:
		return t.VisitChildren(s)
	}
}

type UnaryContext struct {
	ExprContext
	op antlr.Token
}

func NewUnaryContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *UnaryContext {
	var p = new(UnaryContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *UnaryContext) GetOp() antlr.Token { return s.op }

func (s *UnaryContext) SetOp(v antlr.Token) { s.op = v }

func (s *UnaryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UnaryContext) Expr() IExprContext {
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

func (s *UnaryContext) ADD() antlr.TerminalNode {
	return s.GetToken(PlanParserADD, 0)
}

func (s *UnaryContext) SUB() antlr.TerminalNode {
	return s.GetToken(PlanParserSUB, 0)
}

func (s *UnaryContext) BNOT() antlr.TerminalNode {
	return s.GetToken(PlanParserBNOT, 0)
}

func (s *UnaryContext) NOT() antlr.TerminalNode {
	return s.GetToken(PlanParserNOT, 0)
}

func (s *UnaryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterUnary(s)
	}
}

func (s *UnaryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitUnary(s)
	}
}

func (s *UnaryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitUnary(s)

	default:
		return t.VisitChildren(s)
	}
}

type TimestamptzCompareContext struct {
	ExprContext
	op1             antlr.Token
	interval_string antlr.Token
	op2             antlr.Token
	compare_string  antlr.Token
}

func NewTimestamptzCompareContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TimestamptzCompareContext {
	var p = new(TimestamptzCompareContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *TimestamptzCompareContext) GetOp1() antlr.Token { return s.op1 }

func (s *TimestamptzCompareContext) GetInterval_string() antlr.Token { return s.interval_string }

func (s *TimestamptzCompareContext) GetOp2() antlr.Token { return s.op2 }

func (s *TimestamptzCompareContext) GetCompare_string() antlr.Token { return s.compare_string }

func (s *TimestamptzCompareContext) SetOp1(v antlr.Token) { s.op1 = v }

func (s *TimestamptzCompareContext) SetInterval_string(v antlr.Token) { s.interval_string = v }

func (s *TimestamptzCompareContext) SetOp2(v antlr.Token) { s.op2 = v }

func (s *TimestamptzCompareContext) SetCompare_string(v antlr.Token) { s.compare_string = v }

func (s *TimestamptzCompareContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TimestamptzCompareContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *TimestamptzCompareContext) ISO() antlr.TerminalNode {
	return s.GetToken(PlanParserISO, 0)
}

func (s *TimestamptzCompareContext) AllStringLiteral() []antlr.TerminalNode {
	return s.GetTokens(PlanParserStringLiteral)
}

func (s *TimestamptzCompareContext) StringLiteral(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, i)
}

func (s *TimestamptzCompareContext) LT() antlr.TerminalNode {
	return s.GetToken(PlanParserLT, 0)
}

func (s *TimestamptzCompareContext) LE() antlr.TerminalNode {
	return s.GetToken(PlanParserLE, 0)
}

func (s *TimestamptzCompareContext) GT() antlr.TerminalNode {
	return s.GetToken(PlanParserGT, 0)
}

func (s *TimestamptzCompareContext) GE() antlr.TerminalNode {
	return s.GetToken(PlanParserGE, 0)
}

func (s *TimestamptzCompareContext) EQ() antlr.TerminalNode {
	return s.GetToken(PlanParserEQ, 0)
}

func (s *TimestamptzCompareContext) NE() antlr.TerminalNode {
	return s.GetToken(PlanParserNE, 0)
}

func (s *TimestamptzCompareContext) INTERVAL() antlr.TerminalNode {
	return s.GetToken(PlanParserINTERVAL, 0)
}

func (s *TimestamptzCompareContext) ADD() antlr.TerminalNode {
	return s.GetToken(PlanParserADD, 0)
}

func (s *TimestamptzCompareContext) SUB() antlr.TerminalNode {
	return s.GetToken(PlanParserSUB, 0)
}

func (s *TimestamptzCompareContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterTimestamptzCompare(s)
	}
}

func (s *TimestamptzCompareContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitTimestamptzCompare(s)
	}
}

func (s *TimestamptzCompareContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTimestamptzCompare(s)

	default:
		return t.VisitChildren(s)
	}
}

type IntegerContext struct {
	ExprContext
}

func NewIntegerContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IntegerContext {
	var p = new(IntegerContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *IntegerContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IntegerContext) IntegerConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserIntegerConstant, 0)
}

func (s *IntegerContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterInteger(s)
	}
}

func (s *IntegerContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitInteger(s)
	}
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
	ExprContext
}

func NewArrayContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ArrayContext {
	var p = new(ArrayContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

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

func (s *ArrayContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterArray(s)
	}
}

func (s *ArrayContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitArray(s)
	}
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
	ExprContext
}

func NewJSONContainsAnyContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *JSONContainsAnyContext {
	var p = new(JSONContainsAnyContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

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

func (s *JSONContainsAnyContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterJSONContainsAny(s)
	}
}

func (s *JSONContainsAnyContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitJSONContainsAny(s)
	}
}

func (s *JSONContainsAnyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitJSONContainsAny(s)

	default:
		return t.VisitChildren(s)
	}
}

type BitXorContext struct {
	ExprContext
}

func NewBitXorContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BitXorContext {
	var p = new(BitXorContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *BitXorContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BitXorContext) AllExpr() []IExprContext {
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

func (s *BitXorContext) Expr(i int) IExprContext {
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

func (s *BitXorContext) BXOR() antlr.TerminalNode {
	return s.GetToken(PlanParserBXOR, 0)
}

func (s *BitXorContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterBitXor(s)
	}
}

func (s *BitXorContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitBitXor(s)
	}
}

func (s *BitXorContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitBitXor(s)

	default:
		return t.VisitChildren(s)
	}
}

type ExistsContext struct {
	ExprContext
}

func NewExistsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExistsContext {
	var p = new(ExistsContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *ExistsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExistsContext) EXISTS() antlr.TerminalNode {
	return s.GetToken(PlanParserEXISTS, 0)
}

func (s *ExistsContext) Expr() IExprContext {
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

func (s *ExistsContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterExists(s)
	}
}

func (s *ExistsContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitExists(s)
	}
}

func (s *ExistsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitExists(s)

	default:
		return t.VisitChildren(s)
	}
}

type BitAndContext struct {
	ExprContext
}

func NewBitAndContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BitAndContext {
	var p = new(BitAndContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *BitAndContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BitAndContext) AllExpr() []IExprContext {
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

func (s *BitAndContext) Expr(i int) IExprContext {
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

func (s *BitAndContext) BAND() antlr.TerminalNode {
	return s.GetToken(PlanParserBAND, 0)
}

func (s *BitAndContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterBitAnd(s)
	}
}

func (s *BitAndContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitBitAnd(s)
	}
}

func (s *BitAndContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitBitAnd(s)

	default:
		return t.VisitChildren(s)
	}
}

type STEuqalsContext struct {
	ExprContext
}

func NewSTEuqalsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *STEuqalsContext {
	var p = new(STEuqalsContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *STEuqalsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *STEuqalsContext) STEuqals() antlr.TerminalNode {
	return s.GetToken(PlanParserSTEuqals, 0)
}

func (s *STEuqalsContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *STEuqalsContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *STEuqalsContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterSTEuqals(s)
	}
}

func (s *STEuqalsContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitSTEuqals(s)
	}
}

func (s *STEuqalsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTEuqals(s)

	default:
		return t.VisitChildren(s)
	}
}

type IsNullContext struct {
	ExprContext
}

func NewIsNullContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IsNullContext {
	var p = new(IsNullContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *IsNullContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IsNullContext) ISNULL() antlr.TerminalNode {
	return s.GetToken(PlanParserISNULL, 0)
}

func (s *IsNullContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *IsNullContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *IsNullContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterIsNull(s)
	}
}

func (s *IsNullContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitIsNull(s)
	}
}

func (s *IsNullContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitIsNull(s)

	default:
		return t.VisitChildren(s)
	}
}

type PowerContext struct {
	ExprContext
}

func NewPowerContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PowerContext {
	var p = new(PowerContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *PowerContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PowerContext) AllExpr() []IExprContext {
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

func (s *PowerContext) Expr(i int) IExprContext {
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

func (s *PowerContext) POW() antlr.TerminalNode {
	return s.GetToken(PlanParserPOW, 0)
}

func (s *PowerContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterPower(s)
	}
}

func (s *PowerContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitPower(s)
	}
}

func (s *PowerContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitPower(s)

	default:
		return t.VisitChildren(s)
	}
}

type STOverlapsContext struct {
	ExprContext
}

func NewSTOverlapsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *STOverlapsContext {
	var p = new(STOverlapsContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *STOverlapsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *STOverlapsContext) STOverlaps() antlr.TerminalNode {
	return s.GetToken(PlanParserSTOverlaps, 0)
}

func (s *STOverlapsContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *STOverlapsContext) StringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, 0)
}

func (s *STOverlapsContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterSTOverlaps(s)
	}
}

func (s *STOverlapsContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitSTOverlaps(s)
	}
}

func (s *STOverlapsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTOverlaps(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) Expr() (localctx IExprContext) {
	return p.expr(0)
}

func (p *PlanParser) expr(_p int) (localctx IExprContext) {
	var _parentctx antlr.ParserRuleContext = p.GetParserRuleContext()

	_parentState := p.GetState()
	localctx = NewExprContext(p, p.GetParserRuleContext(), _parentState)
	var _prevctx IExprContext = localctx
	var _ antlr.ParserRuleContext = _prevctx // TODO: To prevent unused variable warning.
	_startState := 0
	p.EnterRecursionRule(localctx, 0, PlanParserRULE_expr, _p)
	var _la int

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(167)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 8, p.GetParserRuleContext()) {
	case 1:
		localctx = NewTimestamptzCompareContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx

		{
			p.SetState(5)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(9)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserADD || _la == PlanParserSUB {
			{
				p.SetState(6)

				var _lt = p.GetTokenStream().LT(1)

				localctx.(*TimestamptzCompareContext).op1 = _lt

				_la = p.GetTokenStream().LA(1)

				if !(_la == PlanParserADD || _la == PlanParserSUB) {
					var _ri = p.GetErrorHandler().RecoverInline(p)

					localctx.(*TimestamptzCompareContext).op1 = _ri
				} else {
					p.GetErrorHandler().ReportMatch(p)
					p.Consume()
				}
			}
			{
				p.SetState(7)
				p.Match(PlanParserINTERVAL)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(8)

				var _m = p.Match(PlanParserStringLiteral)

				localctx.(*TimestamptzCompareContext).interval_string = _m
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		{
			p.SetState(11)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*TimestamptzCompareContext).op2 = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&16128) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*TimestamptzCompareContext).op2 = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(12)
			p.Match(PlanParserISO)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(13)

			var _m = p.Match(PlanParserStringLiteral)

			localctx.(*TimestamptzCompareContext).compare_string = _m
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		localctx = NewIntegerContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(14)
			p.Match(PlanParserIntegerConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 3:
		localctx = NewFloatingContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(15)
			p.Match(PlanParserFloatingConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 4:
		localctx = NewBooleanContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(16)
			p.Match(PlanParserBooleanConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 5:
		localctx = NewStringContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(17)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 6:
		localctx = NewIdentifierContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(18)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserIdentifier || _la == PlanParserMeta) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}

	case 7:
		localctx = NewJSONIdentifierContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(19)
			p.Match(PlanParserJSONIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 8:
		localctx = NewTemplateVariableContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(20)
			p.Match(PlanParserLBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(21)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(22)
			p.Match(PlanParserRBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 9:
		localctx = NewParensContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(23)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(24)
			p.expr(0)
		}
		{
			p.SetState(25)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 10:
		localctx = NewArrayContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(27)
			p.Match(PlanParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(28)
			p.expr(0)
		}
		p.SetState(33)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 1, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
		for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
			if _alt == 1 {
				{
					p.SetState(29)
					p.Match(PlanParserT__3)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(30)
					p.expr(0)
				}

			}
			p.SetState(35)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 1, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
		}
		p.SetState(37)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__3 {
			{
				p.SetState(36)
				p.Match(PlanParserT__3)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		{
			p.SetState(39)
			p.Match(PlanParserT__4)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 11:
		localctx = NewEmptyArrayContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(41)
			p.Match(PlanParserEmptyArray)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 12:
		localctx = NewExistsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(42)
			p.Match(PlanParserEXISTS)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(43)
			p.expr(35)
		}

	case 13:
		localctx = NewTextMatchContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(44)
			p.Match(PlanParserTEXTMATCH)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(45)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(46)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(47)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(48)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(51)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__3 {
			{
				p.SetState(49)
				p.Match(PlanParserT__3)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(50)
				p.TextMatchOption()
			}

		}
		{
			p.SetState(53)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 14:
		localctx = NewPhraseMatchContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(54)
			p.Match(PlanParserPHRASEMATCH)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(55)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(56)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(57)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(58)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(61)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__3 {
			{
				p.SetState(59)
				p.Match(PlanParserT__3)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(60)
				p.expr(0)
			}

		}
		{
			p.SetState(63)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 15:
		localctx = NewRandomSampleContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(64)
			p.Match(PlanParserRANDOMSAMPLE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(65)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(66)
			p.expr(0)
		}
		{
			p.SetState(67)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 16:
		localctx = NewUnaryContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(69)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*UnaryContext).op = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&824658886656) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*UnaryContext).op = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(70)
			p.expr(29)
		}

	case 17:
		localctx = NewJSONContainsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(71)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContains || _la == PlanParserArrayContains) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(72)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(73)
			p.expr(0)
		}
		{
			p.SetState(74)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(75)
			p.expr(0)
		}
		{
			p.SetState(76)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 18:
		localctx = NewJSONContainsAllContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(78)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContainsAll || _la == PlanParserArrayContainsAll) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(79)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(80)
			p.expr(0)
		}
		{
			p.SetState(81)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(82)
			p.expr(0)
		}
		{
			p.SetState(83)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 19:
		localctx = NewJSONContainsAnyContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(85)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContainsAny || _la == PlanParserArrayContainsAny) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(86)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(87)
			p.expr(0)
		}
		{
			p.SetState(88)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(89)
			p.expr(0)
		}
		{
			p.SetState(90)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 20:
		localctx = NewSTEuqalsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(92)
			p.Match(PlanParserSTEuqals)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(93)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(94)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(95)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(96)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(97)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 21:
		localctx = NewSTTouchesContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(98)
			p.Match(PlanParserSTTouches)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(99)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(100)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(101)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(102)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(103)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 22:
		localctx = NewSTOverlapsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(104)
			p.Match(PlanParserSTOverlaps)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(105)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(106)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(107)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(108)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(109)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 23:
		localctx = NewSTCrossesContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(110)
			p.Match(PlanParserSTCrosses)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(111)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(112)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(113)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(114)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
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

	case 24:
		localctx = NewSTContainsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(116)
			p.Match(PlanParserSTContains)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(117)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(118)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(119)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(120)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(121)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 25:
		localctx = NewSTIntersectsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(122)
			p.Match(PlanParserSTIntersects)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(123)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(124)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(125)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(126)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(127)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 26:
		localctx = NewSTWithinContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(128)
			p.Match(PlanParserSTWithin)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(129)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(130)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(131)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(132)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(133)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 27:
		localctx = NewSTDWithinContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(134)
			p.Match(PlanParserSTDWithin)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(135)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(136)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(137)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(138)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(139)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(140)
			p.expr(0)
		}
		{
			p.SetState(141)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 28:
		localctx = NewArrayLengthContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(143)
			p.Match(PlanParserArrayLength)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(144)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(145)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserIdentifier || _la == PlanParserJSONIdentifier) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(146)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 29:
		localctx = NewCallContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(147)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(148)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(160)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if (int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&-1374363877302) != 0 {
			{
				p.SetState(149)
				p.expr(0)
			}
			p.SetState(154)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 5, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
			for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
				if _alt == 1 {
					{
						p.SetState(150)
						p.Match(PlanParserT__3)
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}
					{
						p.SetState(151)
						p.expr(0)
					}

				}
				p.SetState(156)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 5, p.GetParserRuleContext())
				if p.HasError() {
					goto errorExit
				}
			}
			p.SetState(158)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)

			if _la == PlanParserT__3 {
				{
					p.SetState(157)
					p.Match(PlanParserT__3)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}

			}

		}
		{
			p.SetState(162)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 30:
		localctx = NewIsNullContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(163)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserIdentifier || _la == PlanParserJSONIdentifier) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(164)
			p.Match(PlanParserISNULL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 31:
		localctx = NewIsNotNullContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(165)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserIdentifier || _la == PlanParserJSONIdentifier) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(166)
			p.Match(PlanParserISNOTNULL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case antlr.ATNInvalidAltNumber:
		goto errorExit
	}
	p.GetParserRuleContext().SetStop(p.GetTokenStream().LT(-1))
	p.SetState(223)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 11, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			if p.GetParseListeners() != nil {
				p.TriggerExitRuleEvent()
			}
			_prevctx = localctx
			p.SetState(221)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}

			switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 10, p.GetParserRuleContext()) {
			case 1:
				localctx = NewPowerContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(169)

				if !(p.Precpred(p.GetParserRuleContext(), 30)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 30)", ""))
					goto errorExit
				}
				{
					p.SetState(170)
					p.Match(PlanParserPOW)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(171)
					p.expr(31)
				}

			case 2:
				localctx = NewMulDivModContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(172)

				if !(p.Precpred(p.GetParserRuleContext(), 28)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 28)", ""))
					goto errorExit
				}
				{
					p.SetState(173)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*MulDivModContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&234881024) != 0) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*MulDivModContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(174)
					p.expr(29)
				}

			case 3:
				localctx = NewAddSubContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(175)

				if !(p.Precpred(p.GetParserRuleContext(), 27)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 27)", ""))
					goto errorExit
				}
				{
					p.SetState(176)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*AddSubContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == PlanParserADD || _la == PlanParserSUB) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*AddSubContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(177)
					p.expr(28)
				}

			case 4:
				localctx = NewShiftContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(178)

				if !(p.Precpred(p.GetParserRuleContext(), 26)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 26)", ""))
					goto errorExit
				}
				{
					p.SetState(179)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*ShiftContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == PlanParserSHL || _la == PlanParserSHR) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*ShiftContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(180)
					p.expr(27)
				}

			case 5:
				localctx = NewTermContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(181)

				if !(p.Precpred(p.GetParserRuleContext(), 25)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 25)", ""))
					goto errorExit
				}
				p.SetState(183)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_la = p.GetTokenStream().LA(1)

				if _la == PlanParserNOT {
					{
						p.SetState(182)

						var _m = p.Match(PlanParserNOT)

						localctx.(*TermContext).op = _m
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}

				}
				{
					p.SetState(185)
					p.Match(PlanParserIN)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(186)
					p.expr(26)
				}

			case 6:
				localctx = NewRangeContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(187)

				if !(p.Precpred(p.GetParserRuleContext(), 11)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 11)", ""))
					goto errorExit
				}
				{
					p.SetState(188)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*RangeContext).op1 = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == PlanParserLT || _la == PlanParserLE) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*RangeContext).op1 = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(189)
					_la = p.GetTokenStream().LA(1)

					if !(_la == PlanParserIdentifier || _la == PlanParserJSONIdentifier) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(190)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*RangeContext).op2 = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == PlanParserLT || _la == PlanParserLE) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*RangeContext).op2 = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(191)
					p.expr(12)
				}

			case 7:
				localctx = NewReverseRangeContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(192)

				if !(p.Precpred(p.GetParserRuleContext(), 10)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 10)", ""))
					goto errorExit
				}
				{
					p.SetState(193)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*ReverseRangeContext).op1 = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == PlanParserGT || _la == PlanParserGE) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*ReverseRangeContext).op1 = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(194)
					_la = p.GetTokenStream().LA(1)

					if !(_la == PlanParserIdentifier || _la == PlanParserJSONIdentifier) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(195)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*ReverseRangeContext).op2 = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == PlanParserGT || _la == PlanParserGE) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*ReverseRangeContext).op2 = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(196)
					p.expr(11)
				}

			case 8:
				localctx = NewRelationalContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(197)

				if !(p.Precpred(p.GetParserRuleContext(), 9)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 9)", ""))
					goto errorExit
				}
				{
					p.SetState(198)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*RelationalContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&3840) != 0) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*RelationalContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(199)
					p.expr(10)
				}

			case 9:
				localctx = NewEqualityContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(200)

				if !(p.Precpred(p.GetParserRuleContext(), 8)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 8)", ""))
					goto errorExit
				}
				{
					p.SetState(201)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*EqualityContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == PlanParserEQ || _la == PlanParserNE) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*EqualityContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(202)
					p.expr(9)
				}

			case 10:
				localctx = NewBitAndContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(203)

				if !(p.Precpred(p.GetParserRuleContext(), 7)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 7)", ""))
					goto errorExit
				}
				{
					p.SetState(204)
					p.Match(PlanParserBAND)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(205)
					p.expr(8)
				}

			case 11:
				localctx = NewBitXorContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(206)

				if !(p.Precpred(p.GetParserRuleContext(), 6)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 6)", ""))
					goto errorExit
				}
				{
					p.SetState(207)
					p.Match(PlanParserBXOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(208)
					p.expr(7)
				}

			case 12:
				localctx = NewBitOrContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(209)

				if !(p.Precpred(p.GetParserRuleContext(), 5)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 5)", ""))
					goto errorExit
				}
				{
					p.SetState(210)
					p.Match(PlanParserBOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(211)
					p.expr(6)
				}

			case 13:
				localctx = NewLogicalAndContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(212)

				if !(p.Precpred(p.GetParserRuleContext(), 4)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 4)", ""))
					goto errorExit
				}
				{
					p.SetState(213)
					p.Match(PlanParserAND)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(214)
					p.expr(5)
				}

			case 14:
				localctx = NewLogicalOrContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(215)

				if !(p.Precpred(p.GetParserRuleContext(), 3)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 3)", ""))
					goto errorExit
				}
				{
					p.SetState(216)
					p.Match(PlanParserOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(217)
					p.expr(4)
				}

			case 15:
				localctx = NewLikeContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(218)

				if !(p.Precpred(p.GetParserRuleContext(), 34)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 34)", ""))
					goto errorExit
				}
				{
					p.SetState(219)
					p.Match(PlanParserLIKE)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(220)
					p.Match(PlanParserStringLiteral)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}

			case antlr.ATNInvalidAltNumber:
				goto errorExit
			}

		}
		p.SetState(225)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 11, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
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
	p.UnrollRecursionContexts(_parentctx)
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ITextMatchOptionContext is an interface to support dynamic dispatch.
type ITextMatchOptionContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	MINIMUM_SHOULD_MATCH() antlr.TerminalNode
	ASSIGN() antlr.TerminalNode
	IntegerConstant() antlr.TerminalNode

	// IsTextMatchOptionContext differentiates from other interfaces.
	IsTextMatchOptionContext()
}

type TextMatchOptionContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTextMatchOptionContext() *TextMatchOptionContext {
	var p = new(TextMatchOptionContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_textMatchOption
	return p
}

func InitEmptyTextMatchOptionContext(p *TextMatchOptionContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_textMatchOption
}

func (*TextMatchOptionContext) IsTextMatchOptionContext() {}

func NewTextMatchOptionContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *TextMatchOptionContext {
	var p = new(TextMatchOptionContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_textMatchOption

	return p
}

func (s *TextMatchOptionContext) GetParser() antlr.Parser { return s.parser }

func (s *TextMatchOptionContext) MINIMUM_SHOULD_MATCH() antlr.TerminalNode {
	return s.GetToken(PlanParserMINIMUM_SHOULD_MATCH, 0)
}

func (s *TextMatchOptionContext) ASSIGN() antlr.TerminalNode {
	return s.GetToken(PlanParserASSIGN, 0)
}

func (s *TextMatchOptionContext) IntegerConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserIntegerConstant, 0)
}

func (s *TextMatchOptionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TextMatchOptionContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *TextMatchOptionContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.EnterTextMatchOption(s)
	}
}

func (s *TextMatchOptionContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(PlanListener); ok {
		listenerT.ExitTextMatchOption(s)
	}
}

func (s *TextMatchOptionContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTextMatchOption(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) TextMatchOption() (localctx ITextMatchOptionContext) {
	localctx = NewTextMatchOptionContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 2, PlanParserRULE_textMatchOption)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(226)
		p.Match(PlanParserMINIMUM_SHOULD_MATCH)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(227)
		p.Match(PlanParserASSIGN)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(228)
		p.Match(PlanParserIntegerConstant)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
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

func (p *PlanParser) Sempred(localctx antlr.RuleContext, ruleIndex, predIndex int) bool {
	switch ruleIndex {
	case 0:
		var t *ExprContext = nil
		if localctx != nil {
			t = localctx.(*ExprContext)
		}
		return p.Expr_Sempred(t, predIndex)

	default:
		panic("No predicate with index: " + fmt.Sprint(ruleIndex))
	}
}

func (p *PlanParser) Expr_Sempred(localctx antlr.RuleContext, predIndex int) bool {
	switch predIndex {
	case 0:
		return p.Precpred(p.GetParserRuleContext(), 30)

	case 1:
		return p.Precpred(p.GetParserRuleContext(), 28)

	case 2:
		return p.Precpred(p.GetParserRuleContext(), 27)

	case 3:
		return p.Precpred(p.GetParserRuleContext(), 26)

	case 4:
		return p.Precpred(p.GetParserRuleContext(), 25)

	case 5:
		return p.Precpred(p.GetParserRuleContext(), 11)

	case 6:
		return p.Precpred(p.GetParserRuleContext(), 10)

	case 7:
		return p.Precpred(p.GetParserRuleContext(), 9)

	case 8:
		return p.Precpred(p.GetParserRuleContext(), 8)

	case 9:
		return p.Precpred(p.GetParserRuleContext(), 7)

	case 10:
		return p.Precpred(p.GetParserRuleContext(), 6)

	case 11:
		return p.Precpred(p.GetParserRuleContext(), 5)

	case 12:
		return p.Precpred(p.GetParserRuleContext(), 4)

	case 13:
		return p.Precpred(p.GetParserRuleContext(), 3)

	case 14:
		return p.Precpred(p.GetParserRuleContext(), 34)

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}
