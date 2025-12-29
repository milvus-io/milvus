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
		"'>'", "'>='", "'=='", "'!='", "", "", "", "", "", "", "", "", "", "",
		"", "", "", "", "'='", "'+'", "'-'", "'*'", "'/'", "'%'", "'**'", "'<<'",
		"'>>'", "'&'", "'|'", "'^'", "", "", "", "", "'~'", "", "", "", "",
		"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
		"", "", "", "'$meta'",
	}
	staticData.SymbolicNames = []string{
		"", "", "", "", "", "", "LBRACE", "RBRACE", "LT", "LE", "GT", "GE",
		"EQ", "NE", "LIKE", "EXISTS", "TEXTMATCH", "PHRASEMATCH", "RANDOMSAMPLE",
		"MATCH_ALL", "MATCH_ANY", "MATCH_LEAST", "MATCH_MOST", "MATCH_EXACT",
		"INTERVAL", "ISO", "MINIMUM_SHOULD_MATCH", "THRESHOLD", "ASSIGN", "ADD",
		"SUB", "MUL", "DIV", "MOD", "POW", "SHL", "SHR", "BAND", "BOR", "BXOR",
		"AND", "OR", "ISNULL", "ISNOTNULL", "BNOT", "NOT", "IN", "EmptyArray",
		"JSONContains", "JSONContainsAll", "JSONContainsAny", "ArrayContains",
		"ArrayContainsAll", "ArrayContainsAny", "ArrayLength", "ElementFilter",
		"STEuqals", "STTouches", "STOverlaps", "STCrosses", "STContains", "STIntersects",
		"STWithin", "STDWithin", "STIsValid", "BooleanConstant", "IntegerConstant",
		"FloatingConstant", "Identifier", "Meta", "StringLiteral", "JSONIdentifier",
		"StructSubFieldIdentifier", "Whitespace", "Newline",
	}
	staticData.RuleNames = []string{
		"expr", "textMatchOption",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 1, 74, 299, 2, 0, 7, 0, 2, 1, 7, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3,
		0, 10, 8, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		3, 0, 22, 8, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 5, 0, 42, 8, 0, 10,
		0, 12, 0, 45, 9, 0, 1, 0, 3, 0, 48, 8, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3, 0, 62, 8, 0, 1, 0, 1, 0, 1,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3, 0, 72, 8, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 5, 0,
		221, 8, 0, 10, 0, 12, 0, 224, 9, 0, 1, 0, 3, 0, 227, 8, 0, 3, 0, 229, 8,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3, 0, 236, 8, 0, 1, 0, 1, 0, 1, 0, 1,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3, 0, 252,
		8, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 5, 0, 290, 8, 0, 10, 0, 12, 0, 293, 9, 0, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 0, 1, 0, 2, 0, 2, 0, 15, 1, 0, 29, 30, 1, 0, 8, 13, 1, 0, 68, 69,
		2, 0, 29, 30, 44, 45, 2, 0, 48, 48, 51, 51, 2, 0, 49, 49, 52, 52, 2, 0,
		50, 50, 53, 53, 2, 0, 68, 68, 71, 71, 1, 0, 31, 33, 1, 0, 35, 36, 1, 0,
		8, 9, 2, 0, 68, 68, 71, 72, 1, 0, 10, 11, 1, 0, 8, 11, 1, 0, 12, 13, 360,
		0, 235, 1, 0, 0, 0, 2, 294, 1, 0, 0, 0, 4, 5, 6, 0, -1, 0, 5, 9, 5, 68,
		0, 0, 6, 7, 7, 0, 0, 0, 7, 8, 5, 24, 0, 0, 8, 10, 5, 70, 0, 0, 9, 6, 1,
		0, 0, 0, 9, 10, 1, 0, 0, 0, 10, 11, 1, 0, 0, 0, 11, 12, 7, 1, 0, 0, 12,
		13, 5, 25, 0, 0, 13, 236, 5, 70, 0, 0, 14, 15, 5, 25, 0, 0, 15, 16, 5,
		70, 0, 0, 16, 17, 7, 1, 0, 0, 17, 21, 5, 68, 0, 0, 18, 19, 7, 0, 0, 0,
		19, 20, 5, 24, 0, 0, 20, 22, 5, 70, 0, 0, 21, 18, 1, 0, 0, 0, 21, 22, 1,
		0, 0, 0, 22, 236, 1, 0, 0, 0, 23, 236, 5, 66, 0, 0, 24, 236, 5, 67, 0,
		0, 25, 236, 5, 65, 0, 0, 26, 236, 5, 70, 0, 0, 27, 236, 7, 2, 0, 0, 28,
		236, 5, 71, 0, 0, 29, 236, 5, 72, 0, 0, 30, 31, 5, 6, 0, 0, 31, 32, 5,
		68, 0, 0, 32, 236, 5, 7, 0, 0, 33, 34, 5, 1, 0, 0, 34, 35, 3, 0, 0, 0,
		35, 36, 5, 2, 0, 0, 36, 236, 1, 0, 0, 0, 37, 38, 5, 3, 0, 0, 38, 43, 3,
		0, 0, 0, 39, 40, 5, 4, 0, 0, 40, 42, 3, 0, 0, 0, 41, 39, 1, 0, 0, 0, 42,
		45, 1, 0, 0, 0, 43, 41, 1, 0, 0, 0, 43, 44, 1, 0, 0, 0, 44, 47, 1, 0, 0,
		0, 45, 43, 1, 0, 0, 0, 46, 48, 5, 4, 0, 0, 47, 46, 1, 0, 0, 0, 47, 48,
		1, 0, 0, 0, 48, 49, 1, 0, 0, 0, 49, 50, 5, 5, 0, 0, 50, 236, 1, 0, 0, 0,
		51, 236, 5, 47, 0, 0, 52, 53, 5, 15, 0, 0, 53, 236, 3, 0, 0, 42, 54, 55,
		5, 16, 0, 0, 55, 56, 5, 1, 0, 0, 56, 57, 5, 68, 0, 0, 57, 58, 5, 4, 0,
		0, 58, 61, 5, 70, 0, 0, 59, 60, 5, 4, 0, 0, 60, 62, 3, 2, 1, 0, 61, 59,
		1, 0, 0, 0, 61, 62, 1, 0, 0, 0, 62, 63, 1, 0, 0, 0, 63, 236, 5, 2, 0, 0,
		64, 65, 5, 17, 0, 0, 65, 66, 5, 1, 0, 0, 66, 67, 5, 68, 0, 0, 67, 68, 5,
		4, 0, 0, 68, 71, 5, 70, 0, 0, 69, 70, 5, 4, 0, 0, 70, 72, 3, 0, 0, 0, 71,
		69, 1, 0, 0, 0, 71, 72, 1, 0, 0, 0, 72, 73, 1, 0, 0, 0, 73, 236, 5, 2,
		0, 0, 74, 75, 5, 18, 0, 0, 75, 76, 5, 1, 0, 0, 76, 77, 3, 0, 0, 0, 77,
		78, 5, 2, 0, 0, 78, 236, 1, 0, 0, 0, 79, 80, 5, 55, 0, 0, 80, 81, 5, 1,
		0, 0, 81, 82, 5, 68, 0, 0, 82, 83, 5, 4, 0, 0, 83, 84, 3, 0, 0, 0, 84,
		85, 5, 2, 0, 0, 85, 236, 1, 0, 0, 0, 86, 87, 5, 19, 0, 0, 87, 88, 5, 1,
		0, 0, 88, 89, 5, 68, 0, 0, 89, 90, 5, 4, 0, 0, 90, 91, 3, 0, 0, 0, 91,
		92, 5, 2, 0, 0, 92, 236, 1, 0, 0, 0, 93, 94, 5, 20, 0, 0, 94, 95, 5, 1,
		0, 0, 95, 96, 5, 68, 0, 0, 96, 97, 5, 4, 0, 0, 97, 98, 3, 0, 0, 0, 98,
		99, 5, 2, 0, 0, 99, 236, 1, 0, 0, 0, 100, 101, 5, 21, 0, 0, 101, 102, 5,
		1, 0, 0, 102, 103, 5, 68, 0, 0, 103, 104, 5, 4, 0, 0, 104, 105, 3, 0, 0,
		0, 105, 106, 5, 4, 0, 0, 106, 107, 5, 27, 0, 0, 107, 108, 5, 28, 0, 0,
		108, 109, 5, 66, 0, 0, 109, 110, 5, 2, 0, 0, 110, 236, 1, 0, 0, 0, 111,
		112, 5, 22, 0, 0, 112, 113, 5, 1, 0, 0, 113, 114, 5, 68, 0, 0, 114, 115,
		5, 4, 0, 0, 115, 116, 3, 0, 0, 0, 116, 117, 5, 4, 0, 0, 117, 118, 5, 27,
		0, 0, 118, 119, 5, 28, 0, 0, 119, 120, 5, 66, 0, 0, 120, 121, 5, 2, 0,
		0, 121, 236, 1, 0, 0, 0, 122, 123, 5, 23, 0, 0, 123, 124, 5, 1, 0, 0, 124,
		125, 5, 68, 0, 0, 125, 126, 5, 4, 0, 0, 126, 127, 3, 0, 0, 0, 127, 128,
		5, 4, 0, 0, 128, 129, 5, 27, 0, 0, 129, 130, 5, 28, 0, 0, 130, 131, 5,
		66, 0, 0, 131, 132, 5, 2, 0, 0, 132, 236, 1, 0, 0, 0, 133, 134, 7, 3, 0,
		0, 134, 236, 3, 0, 0, 30, 135, 136, 7, 4, 0, 0, 136, 137, 5, 1, 0, 0, 137,
		138, 3, 0, 0, 0, 138, 139, 5, 4, 0, 0, 139, 140, 3, 0, 0, 0, 140, 141,
		5, 2, 0, 0, 141, 236, 1, 0, 0, 0, 142, 143, 7, 5, 0, 0, 143, 144, 5, 1,
		0, 0, 144, 145, 3, 0, 0, 0, 145, 146, 5, 4, 0, 0, 146, 147, 3, 0, 0, 0,
		147, 148, 5, 2, 0, 0, 148, 236, 1, 0, 0, 0, 149, 150, 7, 6, 0, 0, 150,
		151, 5, 1, 0, 0, 151, 152, 3, 0, 0, 0, 152, 153, 5, 4, 0, 0, 153, 154,
		3, 0, 0, 0, 154, 155, 5, 2, 0, 0, 155, 236, 1, 0, 0, 0, 156, 157, 5, 56,
		0, 0, 157, 158, 5, 1, 0, 0, 158, 159, 5, 68, 0, 0, 159, 160, 5, 4, 0, 0,
		160, 161, 5, 70, 0, 0, 161, 236, 5, 2, 0, 0, 162, 163, 5, 57, 0, 0, 163,
		164, 5, 1, 0, 0, 164, 165, 5, 68, 0, 0, 165, 166, 5, 4, 0, 0, 166, 167,
		5, 70, 0, 0, 167, 236, 5, 2, 0, 0, 168, 169, 5, 58, 0, 0, 169, 170, 5,
		1, 0, 0, 170, 171, 5, 68, 0, 0, 171, 172, 5, 4, 0, 0, 172, 173, 5, 70,
		0, 0, 173, 236, 5, 2, 0, 0, 174, 175, 5, 59, 0, 0, 175, 176, 5, 1, 0, 0,
		176, 177, 5, 68, 0, 0, 177, 178, 5, 4, 0, 0, 178, 179, 5, 70, 0, 0, 179,
		236, 5, 2, 0, 0, 180, 181, 5, 60, 0, 0, 181, 182, 5, 1, 0, 0, 182, 183,
		5, 68, 0, 0, 183, 184, 5, 4, 0, 0, 184, 185, 5, 70, 0, 0, 185, 236, 5,
		2, 0, 0, 186, 187, 5, 61, 0, 0, 187, 188, 5, 1, 0, 0, 188, 189, 5, 68,
		0, 0, 189, 190, 5, 4, 0, 0, 190, 191, 5, 70, 0, 0, 191, 236, 5, 2, 0, 0,
		192, 193, 5, 62, 0, 0, 193, 194, 5, 1, 0, 0, 194, 195, 5, 68, 0, 0, 195,
		196, 5, 4, 0, 0, 196, 197, 5, 70, 0, 0, 197, 236, 5, 2, 0, 0, 198, 199,
		5, 63, 0, 0, 199, 200, 5, 1, 0, 0, 200, 201, 5, 68, 0, 0, 201, 202, 5,
		4, 0, 0, 202, 203, 5, 70, 0, 0, 203, 204, 5, 4, 0, 0, 204, 205, 3, 0, 0,
		0, 205, 206, 5, 2, 0, 0, 206, 236, 1, 0, 0, 0, 207, 208, 5, 64, 0, 0, 208,
		209, 5, 1, 0, 0, 209, 210, 5, 68, 0, 0, 210, 236, 5, 2, 0, 0, 211, 212,
		5, 54, 0, 0, 212, 213, 5, 1, 0, 0, 213, 214, 7, 7, 0, 0, 214, 236, 5, 2,
		0, 0, 215, 216, 5, 68, 0, 0, 216, 228, 5, 1, 0, 0, 217, 222, 3, 0, 0, 0,
		218, 219, 5, 4, 0, 0, 219, 221, 3, 0, 0, 0, 220, 218, 1, 0, 0, 0, 221,
		224, 1, 0, 0, 0, 222, 220, 1, 0, 0, 0, 222, 223, 1, 0, 0, 0, 223, 226,
		1, 0, 0, 0, 224, 222, 1, 0, 0, 0, 225, 227, 5, 4, 0, 0, 226, 225, 1, 0,
		0, 0, 226, 227, 1, 0, 0, 0, 227, 229, 1, 0, 0, 0, 228, 217, 1, 0, 0, 0,
		228, 229, 1, 0, 0, 0, 229, 230, 1, 0, 0, 0, 230, 236, 5, 2, 0, 0, 231,
		232, 7, 7, 0, 0, 232, 236, 5, 42, 0, 0, 233, 234, 7, 7, 0, 0, 234, 236,
		5, 43, 0, 0, 235, 4, 1, 0, 0, 0, 235, 14, 1, 0, 0, 0, 235, 23, 1, 0, 0,
		0, 235, 24, 1, 0, 0, 0, 235, 25, 1, 0, 0, 0, 235, 26, 1, 0, 0, 0, 235,
		27, 1, 0, 0, 0, 235, 28, 1, 0, 0, 0, 235, 29, 1, 0, 0, 0, 235, 30, 1, 0,
		0, 0, 235, 33, 1, 0, 0, 0, 235, 37, 1, 0, 0, 0, 235, 51, 1, 0, 0, 0, 235,
		52, 1, 0, 0, 0, 235, 54, 1, 0, 0, 0, 235, 64, 1, 0, 0, 0, 235, 74, 1, 0,
		0, 0, 235, 79, 1, 0, 0, 0, 235, 86, 1, 0, 0, 0, 235, 93, 1, 0, 0, 0, 235,
		100, 1, 0, 0, 0, 235, 111, 1, 0, 0, 0, 235, 122, 1, 0, 0, 0, 235, 133,
		1, 0, 0, 0, 235, 135, 1, 0, 0, 0, 235, 142, 1, 0, 0, 0, 235, 149, 1, 0,
		0, 0, 235, 156, 1, 0, 0, 0, 235, 162, 1, 0, 0, 0, 235, 168, 1, 0, 0, 0,
		235, 174, 1, 0, 0, 0, 235, 180, 1, 0, 0, 0, 235, 186, 1, 0, 0, 0, 235,
		192, 1, 0, 0, 0, 235, 198, 1, 0, 0, 0, 235, 207, 1, 0, 0, 0, 235, 211,
		1, 0, 0, 0, 235, 215, 1, 0, 0, 0, 235, 231, 1, 0, 0, 0, 235, 233, 1, 0,
		0, 0, 236, 291, 1, 0, 0, 0, 237, 238, 10, 31, 0, 0, 238, 239, 5, 34, 0,
		0, 239, 290, 3, 0, 0, 32, 240, 241, 10, 29, 0, 0, 241, 242, 7, 8, 0, 0,
		242, 290, 3, 0, 0, 30, 243, 244, 10, 28, 0, 0, 244, 245, 7, 0, 0, 0, 245,
		290, 3, 0, 0, 29, 246, 247, 10, 27, 0, 0, 247, 248, 7, 9, 0, 0, 248, 290,
		3, 0, 0, 28, 249, 251, 10, 26, 0, 0, 250, 252, 5, 45, 0, 0, 251, 250, 1,
		0, 0, 0, 251, 252, 1, 0, 0, 0, 252, 253, 1, 0, 0, 0, 253, 254, 5, 46, 0,
		0, 254, 290, 3, 0, 0, 27, 255, 256, 10, 11, 0, 0, 256, 257, 7, 10, 0, 0,
		257, 258, 7, 11, 0, 0, 258, 259, 7, 10, 0, 0, 259, 290, 3, 0, 0, 12, 260,
		261, 10, 10, 0, 0, 261, 262, 7, 12, 0, 0, 262, 263, 7, 11, 0, 0, 263, 264,
		7, 12, 0, 0, 264, 290, 3, 0, 0, 11, 265, 266, 10, 9, 0, 0, 266, 267, 7,
		13, 0, 0, 267, 290, 3, 0, 0, 10, 268, 269, 10, 8, 0, 0, 269, 270, 7, 14,
		0, 0, 270, 290, 3, 0, 0, 9, 271, 272, 10, 7, 0, 0, 272, 273, 5, 37, 0,
		0, 273, 290, 3, 0, 0, 8, 274, 275, 10, 6, 0, 0, 275, 276, 5, 39, 0, 0,
		276, 290, 3, 0, 0, 7, 277, 278, 10, 5, 0, 0, 278, 279, 5, 38, 0, 0, 279,
		290, 3, 0, 0, 6, 280, 281, 10, 4, 0, 0, 281, 282, 5, 40, 0, 0, 282, 290,
		3, 0, 0, 5, 283, 284, 10, 3, 0, 0, 284, 285, 5, 41, 0, 0, 285, 290, 3,
		0, 0, 4, 286, 287, 10, 41, 0, 0, 287, 288, 5, 14, 0, 0, 288, 290, 5, 70,
		0, 0, 289, 237, 1, 0, 0, 0, 289, 240, 1, 0, 0, 0, 289, 243, 1, 0, 0, 0,
		289, 246, 1, 0, 0, 0, 289, 249, 1, 0, 0, 0, 289, 255, 1, 0, 0, 0, 289,
		260, 1, 0, 0, 0, 289, 265, 1, 0, 0, 0, 289, 268, 1, 0, 0, 0, 289, 271,
		1, 0, 0, 0, 289, 274, 1, 0, 0, 0, 289, 277, 1, 0, 0, 0, 289, 280, 1, 0,
		0, 0, 289, 283, 1, 0, 0, 0, 289, 286, 1, 0, 0, 0, 290, 293, 1, 0, 0, 0,
		291, 289, 1, 0, 0, 0, 291, 292, 1, 0, 0, 0, 292, 1, 1, 0, 0, 0, 293, 291,
		1, 0, 0, 0, 294, 295, 5, 26, 0, 0, 295, 296, 5, 28, 0, 0, 296, 297, 5,
		66, 0, 0, 297, 3, 1, 0, 0, 0, 13, 9, 21, 43, 47, 61, 71, 222, 226, 228,
		235, 251, 289, 291,
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
	PlanParserEOF                      = antlr.TokenEOF
	PlanParserT__0                     = 1
	PlanParserT__1                     = 2
	PlanParserT__2                     = 3
	PlanParserT__3                     = 4
	PlanParserT__4                     = 5
	PlanParserLBRACE                   = 6
	PlanParserRBRACE                   = 7
	PlanParserLT                       = 8
	PlanParserLE                       = 9
	PlanParserGT                       = 10
	PlanParserGE                       = 11
	PlanParserEQ                       = 12
	PlanParserNE                       = 13
	PlanParserLIKE                     = 14
	PlanParserEXISTS                   = 15
	PlanParserTEXTMATCH                = 16
	PlanParserPHRASEMATCH              = 17
	PlanParserRANDOMSAMPLE             = 18
	PlanParserMATCH_ALL                = 19
	PlanParserMATCH_ANY                = 20
	PlanParserMATCH_LEAST              = 21
	PlanParserMATCH_MOST               = 22
	PlanParserMATCH_EXACT              = 23
	PlanParserINTERVAL                 = 24
	PlanParserISO                      = 25
	PlanParserMINIMUM_SHOULD_MATCH     = 26
	PlanParserTHRESHOLD                = 27
	PlanParserASSIGN                   = 28
	PlanParserADD                      = 29
	PlanParserSUB                      = 30
	PlanParserMUL                      = 31
	PlanParserDIV                      = 32
	PlanParserMOD                      = 33
	PlanParserPOW                      = 34
	PlanParserSHL                      = 35
	PlanParserSHR                      = 36
	PlanParserBAND                     = 37
	PlanParserBOR                      = 38
	PlanParserBXOR                     = 39
	PlanParserAND                      = 40
	PlanParserOR                       = 41
	PlanParserISNULL                   = 42
	PlanParserISNOTNULL                = 43
	PlanParserBNOT                     = 44
	PlanParserNOT                      = 45
	PlanParserIN                       = 46
	PlanParserEmptyArray               = 47
	PlanParserJSONContains             = 48
	PlanParserJSONContainsAll          = 49
	PlanParserJSONContainsAny          = 50
	PlanParserArrayContains            = 51
	PlanParserArrayContainsAll         = 52
	PlanParserArrayContainsAny         = 53
	PlanParserArrayLength              = 54
	PlanParserElementFilter            = 55
	PlanParserSTEuqals                 = 56
	PlanParserSTTouches                = 57
	PlanParserSTOverlaps               = 58
	PlanParserSTCrosses                = 59
	PlanParserSTContains               = 60
	PlanParserSTIntersects             = 61
	PlanParserSTWithin                 = 62
	PlanParserSTDWithin                = 63
	PlanParserSTIsValid                = 64
	PlanParserBooleanConstant          = 65
	PlanParserIntegerConstant          = 66
	PlanParserFloatingConstant         = 67
	PlanParserIdentifier               = 68
	PlanParserMeta                     = 69
	PlanParserStringLiteral            = 70
	PlanParserJSONIdentifier           = 71
	PlanParserStructSubFieldIdentifier = 72
	PlanParserWhitespace               = 73
	PlanParserNewline                  = 74
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

func (s *StringContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitString(s)

	default:
		return t.VisitChildren(s)
	}
}

type MatchAnyContext struct {
	ExprContext
}

func NewMatchAnyContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MatchAnyContext {
	var p = new(MatchAnyContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *MatchAnyContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MatchAnyContext) MATCH_ANY() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_ANY, 0)
}

func (s *MatchAnyContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *MatchAnyContext) Expr() IExprContext {
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

func (s *MatchAnyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMatchAny(s)

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

func (s *FloatingContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitFloating(s)

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

func (s *IsNotNullContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitIsNotNull(s)

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

func (s *LikeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitLike(s)

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

func (s *BooleanContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitBoolean(s)

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

func (s *ShiftContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitShift(s)

	default:
		return t.VisitChildren(s)
	}
}

type TimestamptzCompareForwardContext struct {
	ExprContext
	op1             antlr.Token
	interval_string antlr.Token
	op2             antlr.Token
	compare_string  antlr.Token
}

func NewTimestamptzCompareForwardContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TimestamptzCompareForwardContext {
	var p = new(TimestamptzCompareForwardContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *TimestamptzCompareForwardContext) GetOp1() antlr.Token { return s.op1 }

func (s *TimestamptzCompareForwardContext) GetInterval_string() antlr.Token { return s.interval_string }

func (s *TimestamptzCompareForwardContext) GetOp2() antlr.Token { return s.op2 }

func (s *TimestamptzCompareForwardContext) GetCompare_string() antlr.Token { return s.compare_string }

func (s *TimestamptzCompareForwardContext) SetOp1(v antlr.Token) { s.op1 = v }

func (s *TimestamptzCompareForwardContext) SetInterval_string(v antlr.Token) { s.interval_string = v }

func (s *TimestamptzCompareForwardContext) SetOp2(v antlr.Token) { s.op2 = v }

func (s *TimestamptzCompareForwardContext) SetCompare_string(v antlr.Token) { s.compare_string = v }

func (s *TimestamptzCompareForwardContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TimestamptzCompareForwardContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *TimestamptzCompareForwardContext) ISO() antlr.TerminalNode {
	return s.GetToken(PlanParserISO, 0)
}

func (s *TimestamptzCompareForwardContext) AllStringLiteral() []antlr.TerminalNode {
	return s.GetTokens(PlanParserStringLiteral)
}

func (s *TimestamptzCompareForwardContext) StringLiteral(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, i)
}

func (s *TimestamptzCompareForwardContext) LT() antlr.TerminalNode {
	return s.GetToken(PlanParserLT, 0)
}

func (s *TimestamptzCompareForwardContext) LE() antlr.TerminalNode {
	return s.GetToken(PlanParserLE, 0)
}

func (s *TimestamptzCompareForwardContext) GT() antlr.TerminalNode {
	return s.GetToken(PlanParserGT, 0)
}

func (s *TimestamptzCompareForwardContext) GE() antlr.TerminalNode {
	return s.GetToken(PlanParserGE, 0)
}

func (s *TimestamptzCompareForwardContext) EQ() antlr.TerminalNode {
	return s.GetToken(PlanParserEQ, 0)
}

func (s *TimestamptzCompareForwardContext) NE() antlr.TerminalNode {
	return s.GetToken(PlanParserNE, 0)
}

func (s *TimestamptzCompareForwardContext) INTERVAL() antlr.TerminalNode {
	return s.GetToken(PlanParserINTERVAL, 0)
}

func (s *TimestamptzCompareForwardContext) ADD() antlr.TerminalNode {
	return s.GetToken(PlanParserADD, 0)
}

func (s *TimestamptzCompareForwardContext) SUB() antlr.TerminalNode {
	return s.GetToken(PlanParserSUB, 0)
}

func (s *TimestamptzCompareForwardContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTimestamptzCompareForward(s)

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

func (s *ReverseRangeContext) StructSubFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructSubFieldIdentifier, 0)
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

func (s *ReverseRangeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitReverseRange(s)

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

func (s *EmptyArrayContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitEmptyArray(s)

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

func (s *PhraseMatchContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitPhraseMatch(s)

	default:
		return t.VisitChildren(s)
	}
}

type MatchLeastContext struct {
	ExprContext
}

func NewMatchLeastContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MatchLeastContext {
	var p = new(MatchLeastContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *MatchLeastContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MatchLeastContext) MATCH_LEAST() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_LEAST, 0)
}

func (s *MatchLeastContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *MatchLeastContext) Expr() IExprContext {
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

func (s *MatchLeastContext) THRESHOLD() antlr.TerminalNode {
	return s.GetToken(PlanParserTHRESHOLD, 0)
}

func (s *MatchLeastContext) ASSIGN() antlr.TerminalNode {
	return s.GetToken(PlanParserASSIGN, 0)
}

func (s *MatchLeastContext) IntegerConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserIntegerConstant, 0)
}

func (s *MatchLeastContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMatchLeast(s)

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

func (s *ArrayLengthContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitArrayLength(s)

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

func (s *STTouchesContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTTouches(s)

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

func (s *RangeContext) StructSubFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructSubFieldIdentifier, 0)
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

func (s *RangeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitRange(s)

	default:
		return t.VisitChildren(s)
	}
}

type MatchAllContext struct {
	ExprContext
}

func NewMatchAllContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MatchAllContext {
	var p = new(MatchAllContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *MatchAllContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MatchAllContext) MATCH_ALL() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_ALL, 0)
}

func (s *MatchAllContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *MatchAllContext) Expr() IExprContext {
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

func (s *MatchAllContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMatchAll(s)

	default:
		return t.VisitChildren(s)
	}
}

type STIsValidContext struct {
	ExprContext
}

func NewSTIsValidContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *STIsValidContext {
	var p = new(STIsValidContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *STIsValidContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *STIsValidContext) STIsValid() antlr.TerminalNode {
	return s.GetToken(PlanParserSTIsValid, 0)
}

func (s *STIsValidContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *STIsValidContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTIsValid(s)

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

func (s *BitXorContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitBitXor(s)

	default:
		return t.VisitChildren(s)
	}
}

type ElementFilterContext struct {
	ExprContext
}

func NewElementFilterContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ElementFilterContext {
	var p = new(ElementFilterContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *ElementFilterContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ElementFilterContext) ElementFilter() antlr.TerminalNode {
	return s.GetToken(PlanParserElementFilter, 0)
}

func (s *ElementFilterContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *ElementFilterContext) Expr() IExprContext {
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

func (s *ElementFilterContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitElementFilter(s)

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

func (s *BitAndContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitBitAnd(s)

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

func (s *STOverlapsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTOverlaps(s)

	default:
		return t.VisitChildren(s)
	}
}

type MatchMostContext struct {
	ExprContext
}

func NewMatchMostContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MatchMostContext {
	var p = new(MatchMostContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *MatchMostContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MatchMostContext) MATCH_MOST() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_MOST, 0)
}

func (s *MatchMostContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *MatchMostContext) Expr() IExprContext {
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

func (s *MatchMostContext) THRESHOLD() antlr.TerminalNode {
	return s.GetToken(PlanParserTHRESHOLD, 0)
}

func (s *MatchMostContext) ASSIGN() antlr.TerminalNode {
	return s.GetToken(PlanParserASSIGN, 0)
}

func (s *MatchMostContext) IntegerConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserIntegerConstant, 0)
}

func (s *MatchMostContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMatchMost(s)

	default:
		return t.VisitChildren(s)
	}
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

func (s *ParensContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitParens(s)

	default:
		return t.VisitChildren(s)
	}
}

type MatchExactContext struct {
	ExprContext
}

func NewMatchExactContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MatchExactContext {
	var p = new(MatchExactContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *MatchExactContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MatchExactContext) MATCH_EXACT() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_EXACT, 0)
}

func (s *MatchExactContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *MatchExactContext) Expr() IExprContext {
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

func (s *MatchExactContext) THRESHOLD() antlr.TerminalNode {
	return s.GetToken(PlanParserTHRESHOLD, 0)
}

func (s *MatchExactContext) ASSIGN() antlr.TerminalNode {
	return s.GetToken(PlanParserASSIGN, 0)
}

func (s *MatchExactContext) IntegerConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserIntegerConstant, 0)
}

func (s *MatchExactContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMatchExact(s)

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

func (s *LogicalOrContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitLogicalOr(s)

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

func (s *MulDivModContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMulDivMod(s)

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

func (s *TemplateVariableContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTemplateVariable(s)

	default:
		return t.VisitChildren(s)
	}
}

type TimestamptzCompareReverseContext struct {
	ExprContext
	compare_string  antlr.Token
	op2             antlr.Token
	op1             antlr.Token
	interval_string antlr.Token
}

func NewTimestamptzCompareReverseContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TimestamptzCompareReverseContext {
	var p = new(TimestamptzCompareReverseContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *TimestamptzCompareReverseContext) GetCompare_string() antlr.Token { return s.compare_string }

func (s *TimestamptzCompareReverseContext) GetOp2() antlr.Token { return s.op2 }

func (s *TimestamptzCompareReverseContext) GetOp1() antlr.Token { return s.op1 }

func (s *TimestamptzCompareReverseContext) GetInterval_string() antlr.Token { return s.interval_string }

func (s *TimestamptzCompareReverseContext) SetCompare_string(v antlr.Token) { s.compare_string = v }

func (s *TimestamptzCompareReverseContext) SetOp2(v antlr.Token) { s.op2 = v }

func (s *TimestamptzCompareReverseContext) SetOp1(v antlr.Token) { s.op1 = v }

func (s *TimestamptzCompareReverseContext) SetInterval_string(v antlr.Token) { s.interval_string = v }

func (s *TimestamptzCompareReverseContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TimestamptzCompareReverseContext) ISO() antlr.TerminalNode {
	return s.GetToken(PlanParserISO, 0)
}

func (s *TimestamptzCompareReverseContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *TimestamptzCompareReverseContext) AllStringLiteral() []antlr.TerminalNode {
	return s.GetTokens(PlanParserStringLiteral)
}

func (s *TimestamptzCompareReverseContext) StringLiteral(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserStringLiteral, i)
}

func (s *TimestamptzCompareReverseContext) LT() antlr.TerminalNode {
	return s.GetToken(PlanParserLT, 0)
}

func (s *TimestamptzCompareReverseContext) LE() antlr.TerminalNode {
	return s.GetToken(PlanParserLE, 0)
}

func (s *TimestamptzCompareReverseContext) GT() antlr.TerminalNode {
	return s.GetToken(PlanParserGT, 0)
}

func (s *TimestamptzCompareReverseContext) GE() antlr.TerminalNode {
	return s.GetToken(PlanParserGE, 0)
}

func (s *TimestamptzCompareReverseContext) EQ() antlr.TerminalNode {
	return s.GetToken(PlanParserEQ, 0)
}

func (s *TimestamptzCompareReverseContext) NE() antlr.TerminalNode {
	return s.GetToken(PlanParserNE, 0)
}

func (s *TimestamptzCompareReverseContext) INTERVAL() antlr.TerminalNode {
	return s.GetToken(PlanParserINTERVAL, 0)
}

func (s *TimestamptzCompareReverseContext) ADD() antlr.TerminalNode {
	return s.GetToken(PlanParserADD, 0)
}

func (s *TimestamptzCompareReverseContext) SUB() antlr.TerminalNode {
	return s.GetToken(PlanParserSUB, 0)
}

func (s *TimestamptzCompareReverseContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTimestamptzCompareReverse(s)

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

func (s *STDWithinContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTDWithin(s)

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

func (s *STCrossesContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTCrosses(s)

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

func (s *BitOrContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitBitOr(s)

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

func (s *AddSubContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitAddSub(s)

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

func (s *RelationalContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitRelational(s)

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

func (s *TextMatchContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTextMatch(s)

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

func (s *STContainsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTContains(s)

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

func (s *UnaryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitUnary(s)

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

func (s *JSONContainsAnyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitJSONContainsAny(s)

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

func (s *ExistsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitExists(s)

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

func (s *IsNullContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitIsNull(s)

	default:
		return t.VisitChildren(s)
	}
}

type StructSubFieldContext struct {
	ExprContext
}

func NewStructSubFieldContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *StructSubFieldContext {
	var p = new(StructSubFieldContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *StructSubFieldContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StructSubFieldContext) StructSubFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructSubFieldIdentifier, 0)
}

func (s *StructSubFieldContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitStructSubField(s)

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

func (s *PowerContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitPower(s)

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
	p.SetState(235)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 9, p.GetParserRuleContext()) {
	case 1:
		localctx = NewTimestamptzCompareForwardContext(p, localctx)
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

				localctx.(*TimestamptzCompareForwardContext).op1 = _lt

				_la = p.GetTokenStream().LA(1)

				if !(_la == PlanParserADD || _la == PlanParserSUB) {
					var _ri = p.GetErrorHandler().RecoverInline(p)

					localctx.(*TimestamptzCompareForwardContext).op1 = _ri
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

				localctx.(*TimestamptzCompareForwardContext).interval_string = _m
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		{
			p.SetState(11)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*TimestamptzCompareForwardContext).op2 = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&16128) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*TimestamptzCompareForwardContext).op2 = _ri
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

			localctx.(*TimestamptzCompareForwardContext).compare_string = _m
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		localctx = NewTimestamptzCompareReverseContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(14)
			p.Match(PlanParserISO)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(15)

			var _m = p.Match(PlanParserStringLiteral)

			localctx.(*TimestamptzCompareReverseContext).compare_string = _m
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(16)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*TimestamptzCompareReverseContext).op2 = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&16128) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*TimestamptzCompareReverseContext).op2 = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(17)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(21)
		p.GetErrorHandler().Sync(p)

		if p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 1, p.GetParserRuleContext()) == 1 {
			{
				p.SetState(18)

				var _lt = p.GetTokenStream().LT(1)

				localctx.(*TimestamptzCompareReverseContext).op1 = _lt

				_la = p.GetTokenStream().LA(1)

				if !(_la == PlanParserADD || _la == PlanParserSUB) {
					var _ri = p.GetErrorHandler().RecoverInline(p)

					localctx.(*TimestamptzCompareReverseContext).op1 = _ri
				} else {
					p.GetErrorHandler().ReportMatch(p)
					p.Consume()
				}
			}
			{
				p.SetState(19)
				p.Match(PlanParserINTERVAL)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(20)

				var _m = p.Match(PlanParserStringLiteral)

				localctx.(*TimestamptzCompareReverseContext).interval_string = _m
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		} else if p.HasError() { // JIM
			goto errorExit
		}

	case 3:
		localctx = NewIntegerContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(23)
			p.Match(PlanParserIntegerConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 4:
		localctx = NewFloatingContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(24)
			p.Match(PlanParserFloatingConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 5:
		localctx = NewBooleanContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(25)
			p.Match(PlanParserBooleanConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 6:
		localctx = NewStringContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(26)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 7:
		localctx = NewIdentifierContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(27)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserIdentifier || _la == PlanParserMeta) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}

	case 8:
		localctx = NewJSONIdentifierContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(28)
			p.Match(PlanParserJSONIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 9:
		localctx = NewStructSubFieldContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(29)
			p.Match(PlanParserStructSubFieldIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 10:
		localctx = NewTemplateVariableContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(30)
			p.Match(PlanParserLBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(31)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(32)
			p.Match(PlanParserRBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 11:
		localctx = NewParensContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(33)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(34)
			p.expr(0)
		}
		{
			p.SetState(35)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 12:
		localctx = NewArrayContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(37)
			p.Match(PlanParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(38)
			p.expr(0)
		}
		p.SetState(43)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 2, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
		for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
			if _alt == 1 {
				{
					p.SetState(39)
					p.Match(PlanParserT__3)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(40)
					p.expr(0)
				}

			}
			p.SetState(45)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 2, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
		}
		p.SetState(47)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__3 {
			{
				p.SetState(46)
				p.Match(PlanParserT__3)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		{
			p.SetState(49)
			p.Match(PlanParserT__4)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 13:
		localctx = NewEmptyArrayContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(51)
			p.Match(PlanParserEmptyArray)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 14:
		localctx = NewExistsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(52)
			p.Match(PlanParserEXISTS)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(53)
			p.expr(42)
		}

	case 15:
		localctx = NewTextMatchContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(54)
			p.Match(PlanParserTEXTMATCH)
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
				p.TextMatchOption()
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

	case 16:
		localctx = NewPhraseMatchContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(64)
			p.Match(PlanParserPHRASEMATCH)
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
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(67)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(68)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(71)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__3 {
			{
				p.SetState(69)
				p.Match(PlanParserT__3)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(70)
				p.expr(0)
			}

		}
		{
			p.SetState(73)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 17:
		localctx = NewRandomSampleContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(74)
			p.Match(PlanParserRANDOMSAMPLE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(75)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(76)
			p.expr(0)
		}
		{
			p.SetState(77)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 18:
		localctx = NewElementFilterContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(79)
			p.Match(PlanParserElementFilter)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(80)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(81)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(82)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(83)
			p.expr(0)
		}
		{
			p.SetState(84)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 19:
		localctx = NewMatchAllContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(86)
			p.Match(PlanParserMATCH_ALL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(87)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(88)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(89)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(90)
			p.expr(0)
		}
		{
			p.SetState(91)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 20:
		localctx = NewMatchAnyContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(93)
			p.Match(PlanParserMATCH_ANY)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(94)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(95)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(96)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(97)
			p.expr(0)
		}
		{
			p.SetState(98)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 21:
		localctx = NewMatchLeastContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(100)
			p.Match(PlanParserMATCH_LEAST)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(101)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(102)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(103)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(104)
			p.expr(0)
		}
		{
			p.SetState(105)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(106)
			p.Match(PlanParserTHRESHOLD)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(107)
			p.Match(PlanParserASSIGN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(108)
			p.Match(PlanParserIntegerConstant)
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

	case 22:
		localctx = NewMatchMostContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(111)
			p.Match(PlanParserMATCH_MOST)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(112)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(113)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(114)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(115)
			p.expr(0)
		}
		{
			p.SetState(116)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(117)
			p.Match(PlanParserTHRESHOLD)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(118)
			p.Match(PlanParserASSIGN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(119)
			p.Match(PlanParserIntegerConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(120)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 23:
		localctx = NewMatchExactContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(122)
			p.Match(PlanParserMATCH_EXACT)
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
			p.expr(0)
		}
		{
			p.SetState(127)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(128)
			p.Match(PlanParserTHRESHOLD)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(129)
			p.Match(PlanParserASSIGN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(130)
			p.Match(PlanParserIntegerConstant)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(131)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 24:
		localctx = NewUnaryContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(133)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*UnaryContext).op = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&52778168745984) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*UnaryContext).op = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(134)
			p.expr(30)
		}

	case 25:
		localctx = NewJSONContainsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(135)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContains || _la == PlanParserArrayContains) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(136)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(137)
			p.expr(0)
		}
		{
			p.SetState(138)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(139)
			p.expr(0)
		}
		{
			p.SetState(140)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 26:
		localctx = NewJSONContainsAllContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(142)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContainsAll || _la == PlanParserArrayContainsAll) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(143)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(144)
			p.expr(0)
		}
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
			p.expr(0)
		}
		{
			p.SetState(147)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 27:
		localctx = NewJSONContainsAnyContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(149)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContainsAny || _la == PlanParserArrayContainsAny) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(150)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(151)
			p.expr(0)
		}
		{
			p.SetState(152)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(153)
			p.expr(0)
		}
		{
			p.SetState(154)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 28:
		localctx = NewSTEuqalsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(156)
			p.Match(PlanParserSTEuqals)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(157)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(158)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(159)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(160)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(161)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 29:
		localctx = NewSTTouchesContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(162)
			p.Match(PlanParserSTTouches)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(163)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(164)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(165)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(166)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(167)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 30:
		localctx = NewSTOverlapsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(168)
			p.Match(PlanParserSTOverlaps)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(169)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(170)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(171)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(172)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(173)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 31:
		localctx = NewSTCrossesContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(174)
			p.Match(PlanParserSTCrosses)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(175)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(176)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(177)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(178)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(179)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 32:
		localctx = NewSTContainsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(180)
			p.Match(PlanParserSTContains)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(181)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(182)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(183)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(184)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(185)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 33:
		localctx = NewSTIntersectsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(186)
			p.Match(PlanParserSTIntersects)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(187)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(188)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(189)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(190)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(191)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 34:
		localctx = NewSTWithinContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(192)
			p.Match(PlanParserSTWithin)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(193)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(194)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(195)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(196)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(197)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 35:
		localctx = NewSTDWithinContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(198)
			p.Match(PlanParserSTDWithin)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(199)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(200)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(201)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(202)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(203)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(204)
			p.expr(0)
		}
		{
			p.SetState(205)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 36:
		localctx = NewSTIsValidContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(207)
			p.Match(PlanParserSTIsValid)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(208)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(209)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(210)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 37:
		localctx = NewArrayLengthContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(211)
			p.Match(PlanParserArrayLength)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(212)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(213)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserIdentifier || _la == PlanParserJSONIdentifier) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(214)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 38:
		localctx = NewCallContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(215)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(216)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(228)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&-87959269310390) != 0) || ((int64((_la-64)) & ^0x3f) == 0 && ((int64(1)<<(_la-64))&511) != 0) {
			{
				p.SetState(217)
				p.expr(0)
			}
			p.SetState(222)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 6, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
			for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
				if _alt == 1 {
					{
						p.SetState(218)
						p.Match(PlanParserT__3)
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}
					{
						p.SetState(219)
						p.expr(0)
					}

				}
				p.SetState(224)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 6, p.GetParserRuleContext())
				if p.HasError() {
					goto errorExit
				}
			}
			p.SetState(226)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)

			if _la == PlanParserT__3 {
				{
					p.SetState(225)
					p.Match(PlanParserT__3)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}

			}

		}
		{
			p.SetState(230)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 39:
		localctx = NewIsNullContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(231)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserIdentifier || _la == PlanParserJSONIdentifier) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(232)
			p.Match(PlanParserISNULL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 40:
		localctx = NewIsNotNullContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(233)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserIdentifier || _la == PlanParserJSONIdentifier) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(234)
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
	p.SetState(291)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 12, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			if p.GetParseListeners() != nil {
				p.TriggerExitRuleEvent()
			}
			_prevctx = localctx
			p.SetState(289)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}

			switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 11, p.GetParserRuleContext()) {
			case 1:
				localctx = NewPowerContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(237)

				if !(p.Precpred(p.GetParserRuleContext(), 31)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 31)", ""))
					goto errorExit
				}
				{
					p.SetState(238)
					p.Match(PlanParserPOW)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(239)
					p.expr(32)
				}

			case 2:
				localctx = NewMulDivModContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(240)

				if !(p.Precpred(p.GetParserRuleContext(), 29)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 29)", ""))
					goto errorExit
				}
				{
					p.SetState(241)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*MulDivModContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&15032385536) != 0) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*MulDivModContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(242)
					p.expr(30)
				}

			case 3:
				localctx = NewAddSubContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(243)

				if !(p.Precpred(p.GetParserRuleContext(), 28)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 28)", ""))
					goto errorExit
				}
				{
					p.SetState(244)

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
					p.SetState(245)
					p.expr(29)
				}

			case 4:
				localctx = NewShiftContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(246)

				if !(p.Precpred(p.GetParserRuleContext(), 27)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 27)", ""))
					goto errorExit
				}
				{
					p.SetState(247)

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
					p.SetState(248)
					p.expr(28)
				}

			case 5:
				localctx = NewTermContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(249)

				if !(p.Precpred(p.GetParserRuleContext(), 26)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 26)", ""))
					goto errorExit
				}
				p.SetState(251)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_la = p.GetTokenStream().LA(1)

				if _la == PlanParserNOT {
					{
						p.SetState(250)

						var _m = p.Match(PlanParserNOT)

						localctx.(*TermContext).op = _m
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}

				}
				{
					p.SetState(253)
					p.Match(PlanParserIN)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(254)
					p.expr(27)
				}

			case 6:
				localctx = NewRangeContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(255)

				if !(p.Precpred(p.GetParserRuleContext(), 11)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 11)", ""))
					goto errorExit
				}
				{
					p.SetState(256)

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
					p.SetState(257)
					_la = p.GetTokenStream().LA(1)

					if !((int64((_la-68)) & ^0x3f) == 0 && ((int64(1)<<(_la-68))&25) != 0) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(258)

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
					p.SetState(259)
					p.expr(12)
				}

			case 7:
				localctx = NewReverseRangeContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(260)

				if !(p.Precpred(p.GetParserRuleContext(), 10)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 10)", ""))
					goto errorExit
				}
				{
					p.SetState(261)

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
					p.SetState(262)
					_la = p.GetTokenStream().LA(1)

					if !((int64((_la-68)) & ^0x3f) == 0 && ((int64(1)<<(_la-68))&25) != 0) {
						p.GetErrorHandler().RecoverInline(p)
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(263)

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
					p.SetState(264)
					p.expr(11)
				}

			case 8:
				localctx = NewRelationalContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(265)

				if !(p.Precpred(p.GetParserRuleContext(), 9)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 9)", ""))
					goto errorExit
				}
				{
					p.SetState(266)

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
					p.SetState(267)
					p.expr(10)
				}

			case 9:
				localctx = NewEqualityContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(268)

				if !(p.Precpred(p.GetParserRuleContext(), 8)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 8)", ""))
					goto errorExit
				}
				{
					p.SetState(269)

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
					p.SetState(270)
					p.expr(9)
				}

			case 10:
				localctx = NewBitAndContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(271)

				if !(p.Precpred(p.GetParserRuleContext(), 7)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 7)", ""))
					goto errorExit
				}
				{
					p.SetState(272)
					p.Match(PlanParserBAND)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(273)
					p.expr(8)
				}

			case 11:
				localctx = NewBitXorContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(274)

				if !(p.Precpred(p.GetParserRuleContext(), 6)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 6)", ""))
					goto errorExit
				}
				{
					p.SetState(275)
					p.Match(PlanParserBXOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(276)
					p.expr(7)
				}

			case 12:
				localctx = NewBitOrContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(277)

				if !(p.Precpred(p.GetParserRuleContext(), 5)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 5)", ""))
					goto errorExit
				}
				{
					p.SetState(278)
					p.Match(PlanParserBOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(279)
					p.expr(6)
				}

			case 13:
				localctx = NewLogicalAndContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(280)

				if !(p.Precpred(p.GetParserRuleContext(), 4)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 4)", ""))
					goto errorExit
				}
				{
					p.SetState(281)
					p.Match(PlanParserAND)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(282)
					p.expr(5)
				}

			case 14:
				localctx = NewLogicalOrContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(283)

				if !(p.Precpred(p.GetParserRuleContext(), 3)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 3)", ""))
					goto errorExit
				}
				{
					p.SetState(284)
					p.Match(PlanParserOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(285)
					p.expr(4)
				}

			case 15:
				localctx = NewLikeContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(286)

				if !(p.Precpred(p.GetParserRuleContext(), 41)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 41)", ""))
					goto errorExit
				}
				{
					p.SetState(287)
					p.Match(PlanParserLIKE)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(288)
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
		p.SetState(293)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 12, p.GetParserRuleContext())
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
		p.SetState(294)
		p.Match(PlanParserMINIMUM_SHOULD_MATCH)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(295)
		p.Match(PlanParserASSIGN)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(296)
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
		return p.Precpred(p.GetParserRuleContext(), 31)

	case 1:
		return p.Precpred(p.GetParserRuleContext(), 29)

	case 2:
		return p.Precpred(p.GetParserRuleContext(), 28)

	case 3:
		return p.Precpred(p.GetParserRuleContext(), 27)

	case 4:
		return p.Precpred(p.GetParserRuleContext(), 26)

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
		return p.Precpred(p.GetParserRuleContext(), 41)

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}
