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
		"", "", "", "", "", "", "'=~'", "'!~'", "'='", "'+'", "'-'", "'*'",
		"'/'", "'%'", "'**'", "'<<'", "'>>'", "'&'", "'|'", "'^'", "", "", "",
		"", "'~'", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
		"", "", "", "", "", "", "", "", "", "'$meta'", "'$score'", "'$distance'",
		"'$rank'", "'$seg_id'", "'$seg_offset'",
	}
	staticData.SymbolicNames = []string{
		"", "", "", "", "", "", "LBRACE", "RBRACE", "LT", "LE", "GT", "GE",
		"EQ", "NE", "LIKE", "EXISTS", "TEXTMATCH", "TEXTMATCHFUZZY", "PHRASEMATCH",
		"RANDOMSAMPLE", "MATCH_ALL", "MATCH_ANY", "MATCH_LEAST", "MATCH_MOST",
		"MATCH_EXACT", "INTERVAL", "ISO", "MINIMUM_SHOULD_MATCH", "THRESHOLD",
		"AS", "REGEXMATCH", "REGEXNOTMATCH", "ASSIGN", "ADD", "SUB", "MUL",
		"DIV", "MOD", "POW", "SHL", "SHR", "BAND", "BOR", "BXOR", "AND", "OR",
		"ISNULL", "ISNOTNULL", "BNOT", "NOT", "IN", "EmptyArray", "JSONContains",
		"JSONContainsAll", "JSONContainsAny", "ArrayContains", "ArrayContainsAll",
		"ArrayContainsAny", "ArrayLength", "ElementFilter", "STEuqals", "STTouches",
		"STOverlaps", "STCrosses", "STContains", "STIntersects", "STWithin",
		"STDWithin", "STIsValid", "BooleanConstant", "IntegerConstant", "FloatingConstant",
		"Identifier", "Meta", "VSCORE", "VDISTANCE", "VRANK", "VSEGID", "VSEGOFFSET",
		"StringLiteral", "RawStringLiteral", "JSONIdentifier", "StructIndexFieldIdentifier",
		"StructFieldIdentifier", "StructSubFieldIdentifier", "Whitespace", "Newline",
	}
	staticData.RuleNames = []string{
		"selectItem", "aggregateCall", "expr", "fieldName", "fieldReference",
		"textMatchOption",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 1, 86, 350, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7,
		4, 2, 5, 7, 5, 1, 0, 1, 0, 1, 0, 1, 0, 3, 0, 17, 8, 0, 1, 0, 1, 0, 1, 0,
		3, 0, 22, 8, 0, 3, 0, 24, 8, 0, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 30, 8, 1,
		1, 1, 1, 1, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 39, 8, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 52, 8, 2, 1, 2,
		1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 61, 8, 2, 1, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 3, 2, 68, 8, 2, 1, 2, 3, 2, 71, 8, 2, 1, 2, 1, 2, 1, 2, 1, 2,
		1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 5, 2,
		88, 8, 2, 10, 2, 12, 2, 91, 9, 2, 1, 2, 3, 2, 94, 8, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 108, 8,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 130, 8, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 215,
		8, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2,
		1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 5, 2, 234, 8, 2, 10, 2, 12, 2, 237,
		9, 2, 1, 2, 3, 2, 240, 8, 2, 3, 2, 242, 8, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1,
		2, 1, 2, 3, 2, 250, 8, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 258,
		8, 2, 1, 2, 3, 2, 261, 8, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2,
		1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2,
		1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 286, 8, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2,
		1, 2, 1, 2, 1, 2, 3, 2, 296, 8, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2,
		1, 2, 1, 2, 3, 2, 306, 8, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2,
		1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2,
		1, 2, 1, 2, 1, 2, 1, 2, 5, 2, 331, 8, 2, 10, 2, 12, 2, 334, 9, 2, 1, 3,
		1, 3, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 3, 4, 344, 8, 4, 1, 5, 1, 5,
		1, 5, 1, 5, 1, 5, 0, 1, 4, 6, 0, 2, 4, 6, 8, 10, 0, 17, 1, 0, 33, 34, 1,
		0, 8, 13, 1, 0, 74, 78, 1, 0, 20, 21, 1, 0, 22, 24, 2, 0, 33, 34, 48, 49,
		2, 0, 52, 52, 55, 55, 2, 0, 53, 53, 56, 56, 2, 0, 54, 54, 57, 57, 1, 0,
		60, 66, 1, 0, 35, 37, 1, 0, 39, 40, 1, 0, 8, 9, 1, 0, 10, 11, 1, 0, 8,
		11, 1, 0, 12, 13, 4, 0, 17, 17, 20, 29, 59, 68, 72, 72, 438, 0, 23, 1,
		0, 0, 0, 2, 25, 1, 0, 0, 0, 4, 260, 1, 0, 0, 0, 6, 335, 1, 0, 0, 0, 8,
		343, 1, 0, 0, 0, 10, 345, 1, 0, 0, 0, 12, 24, 5, 35, 0, 0, 13, 16, 3, 2,
		1, 0, 14, 15, 5, 29, 0, 0, 15, 17, 3, 6, 3, 0, 16, 14, 1, 0, 0, 0, 16,
		17, 1, 0, 0, 0, 17, 24, 1, 0, 0, 0, 18, 21, 3, 4, 2, 0, 19, 20, 5, 29,
		0, 0, 20, 22, 3, 6, 3, 0, 21, 19, 1, 0, 0, 0, 21, 22, 1, 0, 0, 0, 22, 24,
		1, 0, 0, 0, 23, 12, 1, 0, 0, 0, 23, 13, 1, 0, 0, 0, 23, 18, 1, 0, 0, 0,
		24, 1, 1, 0, 0, 0, 25, 26, 5, 72, 0, 0, 26, 29, 5, 1, 0, 0, 27, 30, 3,
		6, 3, 0, 28, 30, 5, 35, 0, 0, 29, 27, 1, 0, 0, 0, 29, 28, 1, 0, 0, 0, 29,
		30, 1, 0, 0, 0, 30, 31, 1, 0, 0, 0, 31, 32, 5, 2, 0, 0, 32, 3, 1, 0, 0,
		0, 33, 34, 6, 2, -1, 0, 34, 38, 3, 6, 3, 0, 35, 36, 7, 0, 0, 0, 36, 37,
		5, 25, 0, 0, 37, 39, 5, 79, 0, 0, 38, 35, 1, 0, 0, 0, 38, 39, 1, 0, 0,
		0, 39, 40, 1, 0, 0, 0, 40, 41, 7, 1, 0, 0, 41, 42, 5, 26, 0, 0, 42, 43,
		5, 79, 0, 0, 43, 261, 1, 0, 0, 0, 44, 45, 5, 26, 0, 0, 45, 46, 5, 79, 0,
		0, 46, 47, 7, 1, 0, 0, 47, 51, 3, 6, 3, 0, 48, 49, 7, 0, 0, 0, 49, 50,
		5, 25, 0, 0, 50, 52, 5, 79, 0, 0, 51, 48, 1, 0, 0, 0, 51, 52, 1, 0, 0,
		0, 52, 261, 1, 0, 0, 0, 53, 261, 5, 70, 0, 0, 54, 261, 5, 71, 0, 0, 55,
		261, 5, 69, 0, 0, 56, 261, 5, 79, 0, 0, 57, 261, 5, 80, 0, 0, 58, 61, 3,
		6, 3, 0, 59, 61, 5, 73, 0, 0, 60, 58, 1, 0, 0, 0, 60, 59, 1, 0, 0, 0, 61,
		261, 1, 0, 0, 0, 62, 261, 5, 81, 0, 0, 63, 70, 7, 2, 0, 0, 64, 67, 5, 1,
		0, 0, 65, 68, 3, 6, 3, 0, 66, 68, 5, 70, 0, 0, 67, 65, 1, 0, 0, 0, 67,
		66, 1, 0, 0, 0, 68, 69, 1, 0, 0, 0, 69, 71, 5, 2, 0, 0, 70, 64, 1, 0, 0,
		0, 70, 71, 1, 0, 0, 0, 71, 261, 1, 0, 0, 0, 72, 261, 5, 83, 0, 0, 73, 261,
		5, 82, 0, 0, 74, 261, 5, 84, 0, 0, 75, 76, 5, 6, 0, 0, 76, 77, 3, 6, 3,
		0, 77, 78, 5, 7, 0, 0, 78, 261, 1, 0, 0, 0, 79, 80, 5, 1, 0, 0, 80, 81,
		3, 4, 2, 0, 81, 82, 5, 2, 0, 0, 82, 261, 1, 0, 0, 0, 83, 84, 5, 3, 0, 0,
		84, 89, 3, 4, 2, 0, 85, 86, 5, 4, 0, 0, 86, 88, 3, 4, 2, 0, 87, 85, 1,
		0, 0, 0, 88, 91, 1, 0, 0, 0, 89, 87, 1, 0, 0, 0, 89, 90, 1, 0, 0, 0, 90,
		93, 1, 0, 0, 0, 91, 89, 1, 0, 0, 0, 92, 94, 5, 4, 0, 0, 93, 92, 1, 0, 0,
		0, 93, 94, 1, 0, 0, 0, 94, 95, 1, 0, 0, 0, 95, 96, 5, 5, 0, 0, 96, 261,
		1, 0, 0, 0, 97, 261, 5, 51, 0, 0, 98, 99, 5, 15, 0, 0, 99, 261, 3, 4, 2,
		37, 100, 101, 5, 16, 0, 0, 101, 102, 5, 1, 0, 0, 102, 103, 3, 6, 3, 0,
		103, 104, 5, 4, 0, 0, 104, 107, 3, 4, 2, 0, 105, 106, 5, 4, 0, 0, 106,
		108, 3, 10, 5, 0, 107, 105, 1, 0, 0, 0, 107, 108, 1, 0, 0, 0, 108, 109,
		1, 0, 0, 0, 109, 110, 5, 2, 0, 0, 110, 261, 1, 0, 0, 0, 111, 112, 5, 17,
		0, 0, 112, 113, 5, 1, 0, 0, 113, 114, 3, 6, 3, 0, 114, 115, 5, 4, 0, 0,
		115, 116, 3, 4, 2, 0, 116, 117, 5, 4, 0, 0, 117, 118, 5, 72, 0, 0, 118,
		119, 5, 32, 0, 0, 119, 120, 5, 70, 0, 0, 120, 121, 5, 2, 0, 0, 121, 261,
		1, 0, 0, 0, 122, 123, 5, 18, 0, 0, 123, 124, 5, 1, 0, 0, 124, 125, 3, 6,
		3, 0, 125, 126, 5, 4, 0, 0, 126, 129, 3, 4, 2, 0, 127, 128, 5, 4, 0, 0,
		128, 130, 3, 4, 2, 0, 129, 127, 1, 0, 0, 0, 129, 130, 1, 0, 0, 0, 130,
		131, 1, 0, 0, 0, 131, 132, 5, 2, 0, 0, 132, 261, 1, 0, 0, 0, 133, 134,
		5, 19, 0, 0, 134, 135, 5, 1, 0, 0, 135, 136, 3, 4, 2, 0, 136, 137, 5, 2,
		0, 0, 137, 261, 1, 0, 0, 0, 138, 139, 5, 59, 0, 0, 139, 140, 5, 1, 0, 0,
		140, 141, 3, 6, 3, 0, 141, 142, 5, 4, 0, 0, 142, 143, 3, 4, 2, 0, 143,
		144, 5, 2, 0, 0, 144, 261, 1, 0, 0, 0, 145, 146, 7, 3, 0, 0, 146, 147,
		5, 1, 0, 0, 147, 148, 3, 6, 3, 0, 148, 149, 5, 4, 0, 0, 149, 150, 3, 4,
		2, 0, 150, 151, 5, 2, 0, 0, 151, 261, 1, 0, 0, 0, 152, 153, 7, 4, 0, 0,
		153, 154, 5, 1, 0, 0, 154, 155, 3, 6, 3, 0, 155, 156, 5, 4, 0, 0, 156,
		157, 3, 4, 2, 0, 157, 158, 5, 4, 0, 0, 158, 159, 5, 28, 0, 0, 159, 160,
		5, 32, 0, 0, 160, 161, 5, 70, 0, 0, 161, 162, 5, 2, 0, 0, 162, 261, 1,
		0, 0, 0, 163, 164, 7, 5, 0, 0, 164, 261, 3, 4, 2, 25, 165, 166, 7, 6, 0,
		0, 166, 167, 5, 1, 0, 0, 167, 168, 3, 4, 2, 0, 168, 169, 5, 4, 0, 0, 169,
		170, 3, 4, 2, 0, 170, 171, 5, 2, 0, 0, 171, 261, 1, 0, 0, 0, 172, 173,
		7, 7, 0, 0, 173, 174, 5, 1, 0, 0, 174, 175, 3, 4, 2, 0, 175, 176, 5, 4,
		0, 0, 176, 177, 3, 4, 2, 0, 177, 178, 5, 2, 0, 0, 178, 261, 1, 0, 0, 0,
		179, 180, 7, 8, 0, 0, 180, 181, 5, 1, 0, 0, 181, 182, 3, 4, 2, 0, 182,
		183, 5, 4, 0, 0, 183, 184, 3, 4, 2, 0, 184, 185, 5, 2, 0, 0, 185, 261,
		1, 0, 0, 0, 186, 187, 7, 9, 0, 0, 187, 188, 5, 1, 0, 0, 188, 189, 3, 6,
		3, 0, 189, 190, 5, 4, 0, 0, 190, 191, 3, 4, 2, 0, 191, 192, 5, 2, 0, 0,
		192, 261, 1, 0, 0, 0, 193, 194, 5, 67, 0, 0, 194, 195, 5, 1, 0, 0, 195,
		196, 3, 6, 3, 0, 196, 197, 5, 4, 0, 0, 197, 198, 3, 4, 2, 0, 198, 199,
		5, 4, 0, 0, 199, 200, 3, 4, 2, 0, 200, 201, 5, 2, 0, 0, 201, 261, 1, 0,
		0, 0, 202, 203, 5, 68, 0, 0, 203, 204, 5, 1, 0, 0, 204, 205, 3, 6, 3, 0,
		205, 206, 5, 2, 0, 0, 206, 261, 1, 0, 0, 0, 207, 208, 5, 58, 0, 0, 208,
		214, 5, 1, 0, 0, 209, 215, 3, 6, 3, 0, 210, 215, 5, 81, 0, 0, 211, 215,
		5, 83, 0, 0, 212, 215, 5, 84, 0, 0, 213, 215, 5, 82, 0, 0, 214, 209, 1,
		0, 0, 0, 214, 210, 1, 0, 0, 0, 214, 211, 1, 0, 0, 0, 214, 212, 1, 0, 0,
		0, 214, 213, 1, 0, 0, 0, 215, 216, 1, 0, 0, 0, 216, 261, 5, 2, 0, 0, 217,
		218, 5, 72, 0, 0, 218, 219, 5, 1, 0, 0, 219, 220, 3, 8, 4, 0, 220, 221,
		5, 4, 0, 0, 221, 222, 3, 4, 2, 0, 222, 223, 5, 4, 0, 0, 223, 224, 5, 72,
		0, 0, 224, 225, 5, 32, 0, 0, 225, 226, 5, 72, 0, 0, 226, 227, 5, 2, 0,
		0, 227, 261, 1, 0, 0, 0, 228, 229, 5, 72, 0, 0, 229, 241, 5, 1, 0, 0, 230,
		235, 3, 4, 2, 0, 231, 232, 5, 4, 0, 0, 232, 234, 3, 4, 2, 0, 233, 231,
		1, 0, 0, 0, 234, 237, 1, 0, 0, 0, 235, 233, 1, 0, 0, 0, 235, 236, 1, 0,
		0, 0, 236, 239, 1, 0, 0, 0, 237, 235, 1, 0, 0, 0, 238, 240, 5, 4, 0, 0,
		239, 238, 1, 0, 0, 0, 239, 240, 1, 0, 0, 0, 240, 242, 1, 0, 0, 0, 241,
		230, 1, 0, 0, 0, 241, 242, 1, 0, 0, 0, 242, 243, 1, 0, 0, 0, 243, 261,
		5, 2, 0, 0, 244, 250, 3, 6, 3, 0, 245, 250, 5, 81, 0, 0, 246, 250, 5, 83,
		0, 0, 247, 250, 5, 82, 0, 0, 248, 250, 5, 84, 0, 0, 249, 244, 1, 0, 0,
		0, 249, 245, 1, 0, 0, 0, 249, 246, 1, 0, 0, 0, 249, 247, 1, 0, 0, 0, 249,
		248, 1, 0, 0, 0, 250, 251, 1, 0, 0, 0, 251, 261, 5, 46, 0, 0, 252, 258,
		3, 6, 3, 0, 253, 258, 5, 81, 0, 0, 254, 258, 5, 83, 0, 0, 255, 258, 5,
		82, 0, 0, 256, 258, 5, 84, 0, 0, 257, 252, 1, 0, 0, 0, 257, 253, 1, 0,
		0, 0, 257, 254, 1, 0, 0, 0, 257, 255, 1, 0, 0, 0, 257, 256, 1, 0, 0, 0,
		258, 259, 1, 0, 0, 0, 259, 261, 5, 47, 0, 0, 260, 33, 1, 0, 0, 0, 260,
		44, 1, 0, 0, 0, 260, 53, 1, 0, 0, 0, 260, 54, 1, 0, 0, 0, 260, 55, 1, 0,
		0, 0, 260, 56, 1, 0, 0, 0, 260, 57, 1, 0, 0, 0, 260, 60, 1, 0, 0, 0, 260,
		62, 1, 0, 0, 0, 260, 63, 1, 0, 0, 0, 260, 72, 1, 0, 0, 0, 260, 73, 1, 0,
		0, 0, 260, 74, 1, 0, 0, 0, 260, 75, 1, 0, 0, 0, 260, 79, 1, 0, 0, 0, 260,
		83, 1, 0, 0, 0, 260, 97, 1, 0, 0, 0, 260, 98, 1, 0, 0, 0, 260, 100, 1,
		0, 0, 0, 260, 111, 1, 0, 0, 0, 260, 122, 1, 0, 0, 0, 260, 133, 1, 0, 0,
		0, 260, 138, 1, 0, 0, 0, 260, 145, 1, 0, 0, 0, 260, 152, 1, 0, 0, 0, 260,
		163, 1, 0, 0, 0, 260, 165, 1, 0, 0, 0, 260, 172, 1, 0, 0, 0, 260, 179,
		1, 0, 0, 0, 260, 186, 1, 0, 0, 0, 260, 193, 1, 0, 0, 0, 260, 202, 1, 0,
		0, 0, 260, 207, 1, 0, 0, 0, 260, 217, 1, 0, 0, 0, 260, 228, 1, 0, 0, 0,
		260, 249, 1, 0, 0, 0, 260, 257, 1, 0, 0, 0, 261, 332, 1, 0, 0, 0, 262,
		263, 10, 36, 0, 0, 263, 264, 5, 14, 0, 0, 264, 331, 3, 4, 2, 37, 265, 266,
		10, 35, 0, 0, 266, 267, 5, 30, 0, 0, 267, 331, 3, 4, 2, 36, 268, 269, 10,
		34, 0, 0, 269, 270, 5, 31, 0, 0, 270, 331, 3, 4, 2, 35, 271, 272, 10, 26,
		0, 0, 272, 273, 5, 38, 0, 0, 273, 331, 3, 4, 2, 27, 274, 275, 10, 24, 0,
		0, 275, 276, 7, 10, 0, 0, 276, 331, 3, 4, 2, 25, 277, 278, 10, 23, 0, 0,
		278, 279, 7, 0, 0, 0, 279, 331, 3, 4, 2, 24, 280, 281, 10, 22, 0, 0, 281,
		282, 7, 11, 0, 0, 282, 331, 3, 4, 2, 23, 283, 285, 10, 21, 0, 0, 284, 286,
		5, 49, 0, 0, 285, 284, 1, 0, 0, 0, 285, 286, 1, 0, 0, 0, 286, 287, 1, 0,
		0, 0, 287, 288, 5, 50, 0, 0, 288, 331, 3, 4, 2, 22, 289, 290, 10, 11, 0,
		0, 290, 295, 7, 12, 0, 0, 291, 296, 3, 6, 3, 0, 292, 296, 5, 81, 0, 0,
		293, 296, 5, 84, 0, 0, 294, 296, 5, 82, 0, 0, 295, 291, 1, 0, 0, 0, 295,
		292, 1, 0, 0, 0, 295, 293, 1, 0, 0, 0, 295, 294, 1, 0, 0, 0, 296, 297,
		1, 0, 0, 0, 297, 298, 7, 12, 0, 0, 298, 331, 3, 4, 2, 12, 299, 300, 10,
		10, 0, 0, 300, 305, 7, 13, 0, 0, 301, 306, 3, 6, 3, 0, 302, 306, 5, 81,
		0, 0, 303, 306, 5, 84, 0, 0, 304, 306, 5, 82, 0, 0, 305, 301, 1, 0, 0,
		0, 305, 302, 1, 0, 0, 0, 305, 303, 1, 0, 0, 0, 305, 304, 1, 0, 0, 0, 306,
		307, 1, 0, 0, 0, 307, 308, 7, 13, 0, 0, 308, 331, 3, 4, 2, 11, 309, 310,
		10, 9, 0, 0, 310, 311, 7, 14, 0, 0, 311, 331, 3, 4, 2, 10, 312, 313, 10,
		8, 0, 0, 313, 314, 7, 15, 0, 0, 314, 331, 3, 4, 2, 9, 315, 316, 10, 7,
		0, 0, 316, 317, 5, 41, 0, 0, 317, 331, 3, 4, 2, 8, 318, 319, 10, 6, 0,
		0, 319, 320, 5, 43, 0, 0, 320, 331, 3, 4, 2, 7, 321, 322, 10, 5, 0, 0,
		322, 323, 5, 42, 0, 0, 323, 331, 3, 4, 2, 6, 324, 325, 10, 4, 0, 0, 325,
		326, 5, 44, 0, 0, 326, 331, 3, 4, 2, 5, 327, 328, 10, 3, 0, 0, 328, 329,
		5, 45, 0, 0, 329, 331, 3, 4, 2, 4, 330, 262, 1, 0, 0, 0, 330, 265, 1, 0,
		0, 0, 330, 268, 1, 0, 0, 0, 330, 271, 1, 0, 0, 0, 330, 274, 1, 0, 0, 0,
		330, 277, 1, 0, 0, 0, 330, 280, 1, 0, 0, 0, 330, 283, 1, 0, 0, 0, 330,
		289, 1, 0, 0, 0, 330, 299, 1, 0, 0, 0, 330, 309, 1, 0, 0, 0, 330, 312,
		1, 0, 0, 0, 330, 315, 1, 0, 0, 0, 330, 318, 1, 0, 0, 0, 330, 321, 1, 0,
		0, 0, 330, 324, 1, 0, 0, 0, 330, 327, 1, 0, 0, 0, 331, 334, 1, 0, 0, 0,
		332, 330, 1, 0, 0, 0, 332, 333, 1, 0, 0, 0, 333, 5, 1, 0, 0, 0, 334, 332,
		1, 0, 0, 0, 335, 336, 7, 16, 0, 0, 336, 7, 1, 0, 0, 0, 337, 344, 3, 6,
		3, 0, 338, 344, 5, 73, 0, 0, 339, 344, 5, 81, 0, 0, 340, 344, 5, 83, 0,
		0, 341, 344, 5, 82, 0, 0, 342, 344, 5, 84, 0, 0, 343, 337, 1, 0, 0, 0,
		343, 338, 1, 0, 0, 0, 343, 339, 1, 0, 0, 0, 343, 340, 1, 0, 0, 0, 343,
		341, 1, 0, 0, 0, 343, 342, 1, 0, 0, 0, 344, 9, 1, 0, 0, 0, 345, 346, 5,
		27, 0, 0, 346, 347, 5, 32, 0, 0, 347, 348, 5, 70, 0, 0, 348, 11, 1, 0,
		0, 0, 26, 16, 21, 23, 29, 38, 51, 60, 67, 70, 89, 93, 107, 129, 214, 235,
		239, 241, 249, 257, 260, 285, 295, 305, 330, 332, 343,
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
	PlanParserEOF                        = antlr.TokenEOF
	PlanParserT__0                       = 1
	PlanParserT__1                       = 2
	PlanParserT__2                       = 3
	PlanParserT__3                       = 4
	PlanParserT__4                       = 5
	PlanParserLBRACE                     = 6
	PlanParserRBRACE                     = 7
	PlanParserLT                         = 8
	PlanParserLE                         = 9
	PlanParserGT                         = 10
	PlanParserGE                         = 11
	PlanParserEQ                         = 12
	PlanParserNE                         = 13
	PlanParserLIKE                       = 14
	PlanParserEXISTS                     = 15
	PlanParserTEXTMATCH                  = 16
	PlanParserTEXTMATCHFUZZY             = 17
	PlanParserPHRASEMATCH                = 18
	PlanParserRANDOMSAMPLE               = 19
	PlanParserMATCH_ALL                  = 20
	PlanParserMATCH_ANY                  = 21
	PlanParserMATCH_LEAST                = 22
	PlanParserMATCH_MOST                 = 23
	PlanParserMATCH_EXACT                = 24
	PlanParserINTERVAL                   = 25
	PlanParserISO                        = 26
	PlanParserMINIMUM_SHOULD_MATCH       = 27
	PlanParserTHRESHOLD                  = 28
	PlanParserAS                         = 29
	PlanParserREGEXMATCH                 = 30
	PlanParserREGEXNOTMATCH              = 31
	PlanParserASSIGN                     = 32
	PlanParserADD                        = 33
	PlanParserSUB                        = 34
	PlanParserMUL                        = 35
	PlanParserDIV                        = 36
	PlanParserMOD                        = 37
	PlanParserPOW                        = 38
	PlanParserSHL                        = 39
	PlanParserSHR                        = 40
	PlanParserBAND                       = 41
	PlanParserBOR                        = 42
	PlanParserBXOR                       = 43
	PlanParserAND                        = 44
	PlanParserOR                         = 45
	PlanParserISNULL                     = 46
	PlanParserISNOTNULL                  = 47
	PlanParserBNOT                       = 48
	PlanParserNOT                        = 49
	PlanParserIN                         = 50
	PlanParserEmptyArray                 = 51
	PlanParserJSONContains               = 52
	PlanParserJSONContainsAll            = 53
	PlanParserJSONContainsAny            = 54
	PlanParserArrayContains              = 55
	PlanParserArrayContainsAll           = 56
	PlanParserArrayContainsAny           = 57
	PlanParserArrayLength                = 58
	PlanParserElementFilter              = 59
	PlanParserSTEuqals                   = 60
	PlanParserSTTouches                  = 61
	PlanParserSTOverlaps                 = 62
	PlanParserSTCrosses                  = 63
	PlanParserSTContains                 = 64
	PlanParserSTIntersects               = 65
	PlanParserSTWithin                   = 66
	PlanParserSTDWithin                  = 67
	PlanParserSTIsValid                  = 68
	PlanParserBooleanConstant            = 69
	PlanParserIntegerConstant            = 70
	PlanParserFloatingConstant           = 71
	PlanParserIdentifier                 = 72
	PlanParserMeta                       = 73
	PlanParserVSCORE                     = 74
	PlanParserVDISTANCE                  = 75
	PlanParserVRANK                      = 76
	PlanParserVSEGID                     = 77
	PlanParserVSEGOFFSET                 = 78
	PlanParserStringLiteral              = 79
	PlanParserRawStringLiteral           = 80
	PlanParserJSONIdentifier             = 81
	PlanParserStructIndexFieldIdentifier = 82
	PlanParserStructFieldIdentifier      = 83
	PlanParserStructSubFieldIdentifier   = 84
	PlanParserWhitespace                 = 85
	PlanParserNewline                    = 86
)

// PlanParser rules.
const (
	PlanParserRULE_selectItem      = 0
	PlanParserRULE_aggregateCall   = 1
	PlanParserRULE_expr            = 2
	PlanParserRULE_fieldName       = 3
	PlanParserRULE_fieldReference  = 4
	PlanParserRULE_textMatchOption = 5
)

// ISelectItemContext is an interface to support dynamic dispatch.
type ISelectItemContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsSelectItemContext differentiates from other interfaces.
	IsSelectItemContext()
}

type SelectItemContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySelectItemContext() *SelectItemContext {
	var p = new(SelectItemContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_selectItem
	return p
}

func InitEmptySelectItemContext(p *SelectItemContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_selectItem
}

func (*SelectItemContext) IsSelectItemContext() {}

func NewSelectItemContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SelectItemContext {
	var p = new(SelectItemContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_selectItem

	return p
}

func (s *SelectItemContext) GetParser() antlr.Parser { return s.parser }

func (s *SelectItemContext) CopyAll(ctx *SelectItemContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *SelectItemContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SelectItemContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type SelectStarContext struct {
	SelectItemContext
}

func NewSelectStarContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SelectStarContext {
	var p = new(SelectStarContext)

	InitEmptySelectItemContext(&p.SelectItemContext)
	p.parser = parser
	p.CopyAll(ctx.(*SelectItemContext))

	return p
}

func (s *SelectStarContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SelectStarContext) MUL() antlr.TerminalNode {
	return s.GetToken(PlanParserMUL, 0)
}

func (s *SelectStarContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSelectStar(s)

	default:
		return t.VisitChildren(s)
	}
}

type SelectExprContext struct {
	SelectItemContext
	alias IFieldNameContext
}

func NewSelectExprContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SelectExprContext {
	var p = new(SelectExprContext)

	InitEmptySelectItemContext(&p.SelectItemContext)
	p.parser = parser
	p.CopyAll(ctx.(*SelectItemContext))

	return p
}

func (s *SelectExprContext) GetAlias() IFieldNameContext { return s.alias }

func (s *SelectExprContext) SetAlias(v IFieldNameContext) { s.alias = v }

func (s *SelectExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SelectExprContext) Expr() IExprContext {
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

func (s *SelectExprContext) AS() antlr.TerminalNode {
	return s.GetToken(PlanParserAS, 0)
}

func (s *SelectExprContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *SelectExprContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSelectExpr(s)

	default:
		return t.VisitChildren(s)
	}
}

type SelectAggregateContext struct {
	SelectItemContext
	alias IFieldNameContext
}

func NewSelectAggregateContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SelectAggregateContext {
	var p = new(SelectAggregateContext)

	InitEmptySelectItemContext(&p.SelectItemContext)
	p.parser = parser
	p.CopyAll(ctx.(*SelectItemContext))

	return p
}

func (s *SelectAggregateContext) GetAlias() IFieldNameContext { return s.alias }

func (s *SelectAggregateContext) SetAlias(v IFieldNameContext) { s.alias = v }

func (s *SelectAggregateContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SelectAggregateContext) AggregateCall() IAggregateCallContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IAggregateCallContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IAggregateCallContext)
}

func (s *SelectAggregateContext) AS() antlr.TerminalNode {
	return s.GetToken(PlanParserAS, 0)
}

func (s *SelectAggregateContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *SelectAggregateContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSelectAggregate(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) SelectItem() (localctx ISelectItemContext) {
	localctx = NewSelectItemContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, PlanParserRULE_selectItem)
	var _la int

	p.SetState(23)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 2, p.GetParserRuleContext()) {
	case 1:
		localctx = NewSelectStarContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(12)
			p.Match(PlanParserMUL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		localctx = NewSelectAggregateContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(13)
			p.AggregateCall()
		}
		p.SetState(16)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserAS {
			{
				p.SetState(14)
				p.Match(PlanParserAS)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(15)

				var _x = p.FieldName()

				localctx.(*SelectAggregateContext).alias = _x
			}

		}

	case 3:
		localctx = NewSelectExprContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(18)
			p.expr(0)
		}
		p.SetState(21)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserAS {
			{
				p.SetState(19)
				p.Match(PlanParserAS)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(20)

				var _x = p.FieldName()

				localctx.(*SelectExprContext).alias = _x
			}

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

// IAggregateCallContext is an interface to support dynamic dispatch.
type IAggregateCallContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// GetFunction returns the function token.
	GetFunction() antlr.Token

	// GetStar returns the star token.
	GetStar() antlr.Token

	// SetFunction sets the function token.
	SetFunction(antlr.Token)

	// SetStar sets the star token.
	SetStar(antlr.Token)

	// GetArg returns the arg rule contexts.
	GetArg() IFieldNameContext

	// SetArg sets the arg rule contexts.
	SetArg(IFieldNameContext)

	// Getter signatures
	Identifier() antlr.TerminalNode
	FieldName() IFieldNameContext
	MUL() antlr.TerminalNode

	// IsAggregateCallContext differentiates from other interfaces.
	IsAggregateCallContext()
}

type AggregateCallContext struct {
	antlr.BaseParserRuleContext
	parser   antlr.Parser
	function antlr.Token
	arg      IFieldNameContext
	star     antlr.Token
}

func NewEmptyAggregateCallContext() *AggregateCallContext {
	var p = new(AggregateCallContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_aggregateCall
	return p
}

func InitEmptyAggregateCallContext(p *AggregateCallContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_aggregateCall
}

func (*AggregateCallContext) IsAggregateCallContext() {}

func NewAggregateCallContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *AggregateCallContext {
	var p = new(AggregateCallContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_aggregateCall

	return p
}

func (s *AggregateCallContext) GetParser() antlr.Parser { return s.parser }

func (s *AggregateCallContext) GetFunction() antlr.Token { return s.function }

func (s *AggregateCallContext) GetStar() antlr.Token { return s.star }

func (s *AggregateCallContext) SetFunction(v antlr.Token) { s.function = v }

func (s *AggregateCallContext) SetStar(v antlr.Token) { s.star = v }

func (s *AggregateCallContext) GetArg() IFieldNameContext { return s.arg }

func (s *AggregateCallContext) SetArg(v IFieldNameContext) { s.arg = v }

func (s *AggregateCallContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *AggregateCallContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *AggregateCallContext) MUL() antlr.TerminalNode {
	return s.GetToken(PlanParserMUL, 0)
}

func (s *AggregateCallContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AggregateCallContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *AggregateCallContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitAggregateCall(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) AggregateCall() (localctx IAggregateCallContext) {
	localctx = NewAggregateCallContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 2, PlanParserRULE_aggregateCall)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(25)

		var _m = p.Match(PlanParserIdentifier)

		localctx.(*AggregateCallContext).function = _m
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(26)
		p.Match(PlanParserT__0)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(29)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	switch p.GetTokenStream().LA(1) {
	case PlanParserTEXTMATCHFUZZY, PlanParserMATCH_ALL, PlanParserMATCH_ANY, PlanParserMATCH_LEAST, PlanParserMATCH_MOST, PlanParserMATCH_EXACT, PlanParserINTERVAL, PlanParserISO, PlanParserMINIMUM_SHOULD_MATCH, PlanParserTHRESHOLD, PlanParserAS, PlanParserElementFilter, PlanParserSTEuqals, PlanParserSTTouches, PlanParserSTOverlaps, PlanParserSTCrosses, PlanParserSTContains, PlanParserSTIntersects, PlanParserSTWithin, PlanParserSTDWithin, PlanParserSTIsValid, PlanParserIdentifier:
		{
			p.SetState(27)

			var _x = p.FieldName()

			localctx.(*AggregateCallContext).arg = _x
		}

	case PlanParserMUL:
		{
			p.SetState(28)

			var _m = p.Match(PlanParserMUL)

			localctx.(*AggregateCallContext).star = _m
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserT__1:

	default:
	}
	{
		p.SetState(31)
		p.Match(PlanParserT__1)
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

type TextMatchFuzzyContext struct {
	ExprContext
}

func NewTextMatchFuzzyContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TextMatchFuzzyContext {
	var p = new(TextMatchFuzzyContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *TextMatchFuzzyContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TextMatchFuzzyContext) TEXTMATCHFUZZY() antlr.TerminalNode {
	return s.GetToken(PlanParserTEXTMATCHFUZZY, 0)
}

func (s *TextMatchFuzzyContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *TextMatchFuzzyContext) Expr() IExprContext {
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

func (s *TextMatchFuzzyContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *TextMatchFuzzyContext) ASSIGN() antlr.TerminalNode {
	return s.GetToken(PlanParserASSIGN, 0)
}

func (s *TextMatchFuzzyContext) IntegerConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserIntegerConstant, 0)
}

func (s *TextMatchFuzzyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitTextMatchFuzzy(s)

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

func (s *IsNotNullContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *IsNotNullContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *IsNotNullContext) StructFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructFieldIdentifier, 0)
}

func (s *IsNotNullContext) StructIndexFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructIndexFieldIdentifier, 0)
}

func (s *IsNotNullContext) StructSubFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructSubFieldIdentifier, 0)
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

func (s *IdentifierContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
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

func (s *LikeContext) AllExpr() []IExprContext {
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

func (s *LikeContext) Expr(i int) IExprContext {
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

func (s *LikeContext) LIKE() antlr.TerminalNode {
	return s.GetToken(PlanParserLIKE, 0)
}

func (s *LikeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitLike(s)

	default:
		return t.VisitChildren(s)
	}
}

type RegexNotMatchContext struct {
	ExprContext
}

func NewRegexNotMatchContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *RegexNotMatchContext {
	var p = new(RegexNotMatchContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *RegexNotMatchContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RegexNotMatchContext) AllExpr() []IExprContext {
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

func (s *RegexNotMatchContext) Expr(i int) IExprContext {
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

func (s *RegexNotMatchContext) REGEXNOTMATCH() antlr.TerminalNode {
	return s.GetToken(PlanParserREGEXNOTMATCH, 0)
}

func (s *RegexNotMatchContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitRegexNotMatch(s)

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

func (s *TimestamptzCompareForwardContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
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

func (s *ReverseRangeContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *ReverseRangeContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *ReverseRangeContext) StructSubFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructSubFieldIdentifier, 0)
}

func (s *ReverseRangeContext) StructIndexFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructIndexFieldIdentifier, 0)
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

func (s *PhraseMatchContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *PhraseMatchContext) AllExpr() []IExprContext {
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

func (s *PhraseMatchContext) Expr(i int) IExprContext {
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

func (s *PhraseMatchContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitPhraseMatch(s)

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

func (s *ArrayLengthContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *ArrayLengthContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *ArrayLengthContext) StructFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructFieldIdentifier, 0)
}

func (s *ArrayLengthContext) StructSubFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructSubFieldIdentifier, 0)
}

func (s *ArrayLengthContext) StructIndexFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructIndexFieldIdentifier, 0)
}

func (s *ArrayLengthContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitArrayLength(s)

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

func (s *RangeContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *RangeContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *RangeContext) StructSubFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructSubFieldIdentifier, 0)
}

func (s *RangeContext) StructIndexFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructIndexFieldIdentifier, 0)
}

func (s *RangeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitRange(s)

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

func (s *STIsValidContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *STIsValidContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSTIsValid(s)

	default:
		return t.VisitChildren(s)
	}
}

type MatchThresholdContext struct {
	ExprContext
	op antlr.Token
}

func NewMatchThresholdContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MatchThresholdContext {
	var p = new(MatchThresholdContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *MatchThresholdContext) GetOp() antlr.Token { return s.op }

func (s *MatchThresholdContext) SetOp(v antlr.Token) { s.op = v }

func (s *MatchThresholdContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MatchThresholdContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *MatchThresholdContext) Expr() IExprContext {
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

func (s *MatchThresholdContext) THRESHOLD() antlr.TerminalNode {
	return s.GetToken(PlanParserTHRESHOLD, 0)
}

func (s *MatchThresholdContext) ASSIGN() antlr.TerminalNode {
	return s.GetToken(PlanParserASSIGN, 0)
}

func (s *MatchThresholdContext) IntegerConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserIntegerConstant, 0)
}

func (s *MatchThresholdContext) MATCH_LEAST() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_LEAST, 0)
}

func (s *MatchThresholdContext) MATCH_MOST() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_MOST, 0)
}

func (s *MatchThresholdContext) MATCH_EXACT() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_EXACT, 0)
}

func (s *MatchThresholdContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMatchThreshold(s)

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

func (s *ElementFilterContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
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

type StructIndexFieldContext struct {
	ExprContext
}

func NewStructIndexFieldContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *StructIndexFieldContext {
	var p = new(StructIndexFieldContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *StructIndexFieldContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StructIndexFieldContext) StructIndexFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructIndexFieldIdentifier, 0)
}

func (s *StructIndexFieldContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitStructIndexField(s)

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

type RawStringContext struct {
	ExprContext
}

func NewRawStringContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *RawStringContext {
	var p = new(RawStringContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *RawStringContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RawStringContext) RawStringLiteral() antlr.TerminalNode {
	return s.GetToken(PlanParserRawStringLiteral, 0)
}

func (s *RawStringContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitRawString(s)

	default:
		return t.VisitChildren(s)
	}
}

type SpatialBinaryContext struct {
	ExprContext
	op antlr.Token
}

func NewSpatialBinaryContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SpatialBinaryContext {
	var p = new(SpatialBinaryContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *SpatialBinaryContext) GetOp() antlr.Token { return s.op }

func (s *SpatialBinaryContext) SetOp(v antlr.Token) { s.op = v }

func (s *SpatialBinaryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SpatialBinaryContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *SpatialBinaryContext) Expr() IExprContext {
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

func (s *SpatialBinaryContext) STEuqals() antlr.TerminalNode {
	return s.GetToken(PlanParserSTEuqals, 0)
}

func (s *SpatialBinaryContext) STTouches() antlr.TerminalNode {
	return s.GetToken(PlanParserSTTouches, 0)
}

func (s *SpatialBinaryContext) STOverlaps() antlr.TerminalNode {
	return s.GetToken(PlanParserSTOverlaps, 0)
}

func (s *SpatialBinaryContext) STCrosses() antlr.TerminalNode {
	return s.GetToken(PlanParserSTCrosses, 0)
}

func (s *SpatialBinaryContext) STContains() antlr.TerminalNode {
	return s.GetToken(PlanParserSTContains, 0)
}

func (s *SpatialBinaryContext) STIntersects() antlr.TerminalNode {
	return s.GetToken(PlanParserSTIntersects, 0)
}

func (s *SpatialBinaryContext) STWithin() antlr.TerminalNode {
	return s.GetToken(PlanParserSTWithin, 0)
}

func (s *SpatialBinaryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitSpatialBinary(s)

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

type StructFieldContext struct {
	ExprContext
}

func NewStructFieldContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *StructFieldContext {
	var p = new(StructFieldContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *StructFieldContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StructFieldContext) StructFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructFieldIdentifier, 0)
}

func (s *StructFieldContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitStructField(s)

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

func (s *TemplateVariableContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
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

type MembershipMatchWithOptionContext struct {
	ExprContext
	function antlr.Token
	field    IFieldReferenceContext
	option   antlr.Token
	kind     antlr.Token
}

func NewMembershipMatchWithOptionContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MembershipMatchWithOptionContext {
	var p = new(MembershipMatchWithOptionContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *MembershipMatchWithOptionContext) GetFunction() antlr.Token { return s.function }

func (s *MembershipMatchWithOptionContext) GetOption() antlr.Token { return s.option }

func (s *MembershipMatchWithOptionContext) GetKind() antlr.Token { return s.kind }

func (s *MembershipMatchWithOptionContext) SetFunction(v antlr.Token) { s.function = v }

func (s *MembershipMatchWithOptionContext) SetOption(v antlr.Token) { s.option = v }

func (s *MembershipMatchWithOptionContext) SetKind(v antlr.Token) { s.kind = v }

func (s *MembershipMatchWithOptionContext) GetField() IFieldReferenceContext { return s.field }

func (s *MembershipMatchWithOptionContext) SetField(v IFieldReferenceContext) { s.field = v }

func (s *MembershipMatchWithOptionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MembershipMatchWithOptionContext) Expr() IExprContext {
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

func (s *MembershipMatchWithOptionContext) ASSIGN() antlr.TerminalNode {
	return s.GetToken(PlanParserASSIGN, 0)
}

func (s *MembershipMatchWithOptionContext) AllIdentifier() []antlr.TerminalNode {
	return s.GetTokens(PlanParserIdentifier)
}

func (s *MembershipMatchWithOptionContext) Identifier(i int) antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, i)
}

func (s *MembershipMatchWithOptionContext) FieldReference() IFieldReferenceContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldReferenceContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldReferenceContext)
}

func (s *MembershipMatchWithOptionContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMembershipMatchWithOption(s)

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

func (s *TimestamptzCompareReverseContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
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

func (s *STDWithinContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *STDWithinContext) AllExpr() []IExprContext {
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

func (s *STDWithinContext) Expr(i int) IExprContext {
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

func (s *TextMatchContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *TextMatchContext) Expr() IExprContext {
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

type MatchSimpleContext struct {
	ExprContext
	op antlr.Token
}

func NewMatchSimpleContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *MatchSimpleContext {
	var p = new(MatchSimpleContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *MatchSimpleContext) GetOp() antlr.Token { return s.op }

func (s *MatchSimpleContext) SetOp(v antlr.Token) { s.op = v }

func (s *MatchSimpleContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MatchSimpleContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *MatchSimpleContext) Expr() IExprContext {
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

func (s *MatchSimpleContext) MATCH_ALL() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_ALL, 0)
}

func (s *MatchSimpleContext) MATCH_ANY() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_ANY, 0)
}

func (s *MatchSimpleContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitMatchSimple(s)

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

func (s *IsNullContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *IsNullContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *IsNullContext) StructFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructFieldIdentifier, 0)
}

func (s *IsNullContext) StructIndexFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructIndexFieldIdentifier, 0)
}

func (s *IsNullContext) StructSubFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructSubFieldIdentifier, 0)
}

func (s *IsNullContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitIsNull(s)

	default:
		return t.VisitChildren(s)
	}
}

type RegexMatchContext struct {
	ExprContext
}

func NewRegexMatchContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *RegexMatchContext {
	var p = new(RegexMatchContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *RegexMatchContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RegexMatchContext) AllExpr() []IExprContext {
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

func (s *RegexMatchContext) Expr(i int) IExprContext {
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

func (s *RegexMatchContext) REGEXMATCH() antlr.TerminalNode {
	return s.GetToken(PlanParserREGEXMATCH, 0)
}

func (s *RegexMatchContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitRegexMatch(s)

	default:
		return t.VisitChildren(s)
	}
}

type VirtualColumnContext struct {
	ExprContext
	name     antlr.Token
	argField IFieldNameContext
	argIndex antlr.Token
}

func NewVirtualColumnContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *VirtualColumnContext {
	var p = new(VirtualColumnContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *VirtualColumnContext) GetName() antlr.Token { return s.name }

func (s *VirtualColumnContext) GetArgIndex() antlr.Token { return s.argIndex }

func (s *VirtualColumnContext) SetName(v antlr.Token) { s.name = v }

func (s *VirtualColumnContext) SetArgIndex(v antlr.Token) { s.argIndex = v }

func (s *VirtualColumnContext) GetArgField() IFieldNameContext { return s.argField }

func (s *VirtualColumnContext) SetArgField(v IFieldNameContext) { s.argField = v }

func (s *VirtualColumnContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *VirtualColumnContext) VSCORE() antlr.TerminalNode {
	return s.GetToken(PlanParserVSCORE, 0)
}

func (s *VirtualColumnContext) VDISTANCE() antlr.TerminalNode {
	return s.GetToken(PlanParserVDISTANCE, 0)
}

func (s *VirtualColumnContext) VRANK() antlr.TerminalNode {
	return s.GetToken(PlanParserVRANK, 0)
}

func (s *VirtualColumnContext) VSEGID() antlr.TerminalNode {
	return s.GetToken(PlanParserVSEGID, 0)
}

func (s *VirtualColumnContext) VSEGOFFSET() antlr.TerminalNode {
	return s.GetToken(PlanParserVSEGOFFSET, 0)
}

func (s *VirtualColumnContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *VirtualColumnContext) IntegerConstant() antlr.TerminalNode {
	return s.GetToken(PlanParserIntegerConstant, 0)
}

func (s *VirtualColumnContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitVirtualColumn(s)

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
	_startState := 4
	p.EnterRecursionRule(localctx, 4, PlanParserRULE_expr, _p)
	var _la int

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(260)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 19, p.GetParserRuleContext()) {
	case 1:
		localctx = NewTimestamptzCompareForwardContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx

		{
			p.SetState(34)
			p.FieldName()
		}
		p.SetState(38)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserADD || _la == PlanParserSUB {
			{
				p.SetState(35)

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
				p.SetState(36)
				p.Match(PlanParserINTERVAL)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(37)

				var _m = p.Match(PlanParserStringLiteral)

				localctx.(*TimestamptzCompareForwardContext).interval_string = _m
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		{
			p.SetState(40)

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
			p.SetState(41)
			p.Match(PlanParserISO)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(42)

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
			p.SetState(44)
			p.Match(PlanParserISO)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(45)

			var _m = p.Match(PlanParserStringLiteral)

			localctx.(*TimestamptzCompareReverseContext).compare_string = _m
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(46)

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
			p.SetState(47)
			p.FieldName()
		}
		p.SetState(51)
		p.GetErrorHandler().Sync(p)

		if p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 5, p.GetParserRuleContext()) == 1 {
			{
				p.SetState(48)

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
				p.SetState(49)
				p.Match(PlanParserINTERVAL)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(50)

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
			p.SetState(53)
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
			p.SetState(54)
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
			p.SetState(55)
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
			p.SetState(56)
			p.Match(PlanParserStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 7:
		localctx = NewRawStringContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(57)
			p.Match(PlanParserRawStringLiteral)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 8:
		localctx = NewIdentifierContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		p.SetState(60)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}

		switch p.GetTokenStream().LA(1) {
		case PlanParserTEXTMATCHFUZZY, PlanParserMATCH_ALL, PlanParserMATCH_ANY, PlanParserMATCH_LEAST, PlanParserMATCH_MOST, PlanParserMATCH_EXACT, PlanParserINTERVAL, PlanParserISO, PlanParserMINIMUM_SHOULD_MATCH, PlanParserTHRESHOLD, PlanParserAS, PlanParserElementFilter, PlanParserSTEuqals, PlanParserSTTouches, PlanParserSTOverlaps, PlanParserSTCrosses, PlanParserSTContains, PlanParserSTIntersects, PlanParserSTWithin, PlanParserSTDWithin, PlanParserSTIsValid, PlanParserIdentifier:
			{
				p.SetState(58)
				p.FieldName()
			}

		case PlanParserMeta:
			{
				p.SetState(59)
				p.Match(PlanParserMeta)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		default:
			p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
			goto errorExit
		}

	case 9:
		localctx = NewJSONIdentifierContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(62)
			p.Match(PlanParserJSONIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 10:
		localctx = NewVirtualColumnContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(63)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*VirtualColumnContext).name = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64((_la-74)) & ^0x3f) == 0 && ((int64(1)<<(_la-74))&31) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*VirtualColumnContext).name = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		p.SetState(70)
		p.GetErrorHandler().Sync(p)

		if p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 8, p.GetParserRuleContext()) == 1 {
			{
				p.SetState(64)
				p.Match(PlanParserT__0)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			p.SetState(67)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}

			switch p.GetTokenStream().LA(1) {
			case PlanParserTEXTMATCHFUZZY, PlanParserMATCH_ALL, PlanParserMATCH_ANY, PlanParserMATCH_LEAST, PlanParserMATCH_MOST, PlanParserMATCH_EXACT, PlanParserINTERVAL, PlanParserISO, PlanParserMINIMUM_SHOULD_MATCH, PlanParserTHRESHOLD, PlanParserAS, PlanParserElementFilter, PlanParserSTEuqals, PlanParserSTTouches, PlanParserSTOverlaps, PlanParserSTCrosses, PlanParserSTContains, PlanParserSTIntersects, PlanParserSTWithin, PlanParserSTDWithin, PlanParserSTIsValid, PlanParserIdentifier:
				{
					p.SetState(65)

					var _x = p.FieldName()

					localctx.(*VirtualColumnContext).argField = _x
				}

			case PlanParserIntegerConstant:
				{
					p.SetState(66)

					var _m = p.Match(PlanParserIntegerConstant)

					localctx.(*VirtualColumnContext).argIndex = _m
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}

			default:
				p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
				goto errorExit
			}
			{
				p.SetState(69)
				p.Match(PlanParserT__1)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		} else if p.HasError() { // JIM
			goto errorExit
		}

	case 11:
		localctx = NewStructFieldContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(72)
			p.Match(PlanParserStructFieldIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 12:
		localctx = NewStructIndexFieldContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(73)
			p.Match(PlanParserStructIndexFieldIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 13:
		localctx = NewStructSubFieldContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(74)
			p.Match(PlanParserStructSubFieldIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 14:
		localctx = NewTemplateVariableContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(75)
			p.Match(PlanParserLBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(76)
			p.FieldName()
		}
		{
			p.SetState(77)
			p.Match(PlanParserRBRACE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 15:
		localctx = NewParensContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
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
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 16:
		localctx = NewArrayContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(83)
			p.Match(PlanParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(84)
			p.expr(0)
		}
		p.SetState(89)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 9, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
		for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
			if _alt == 1 {
				{
					p.SetState(85)
					p.Match(PlanParserT__3)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(86)
					p.expr(0)
				}

			}
			p.SetState(91)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 9, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
		}
		p.SetState(93)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__3 {
			{
				p.SetState(92)
				p.Match(PlanParserT__3)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		{
			p.SetState(95)
			p.Match(PlanParserT__4)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 17:
		localctx = NewEmptyArrayContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(97)
			p.Match(PlanParserEmptyArray)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 18:
		localctx = NewExistsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(98)
			p.Match(PlanParserEXISTS)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(99)
			p.expr(37)
		}

	case 19:
		localctx = NewTextMatchContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(100)
			p.Match(PlanParserTEXTMATCH)
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
			p.FieldName()
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
		p.SetState(107)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__3 {
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
				p.TextMatchOption()
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

	case 20:
		localctx = NewTextMatchFuzzyContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(111)
			p.Match(PlanParserTEXTMATCHFUZZY)
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
			p.FieldName()
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
			p.Match(PlanParserIdentifier)
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

	case 21:
		localctx = NewPhraseMatchContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(122)
			p.Match(PlanParserPHRASEMATCH)
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
			p.FieldName()
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
		p.SetState(129)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == PlanParserT__3 {
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
				p.expr(0)
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

	case 22:
		localctx = NewRandomSampleContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(133)
			p.Match(PlanParserRANDOMSAMPLE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(134)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(135)
			p.expr(0)
		}
		{
			p.SetState(136)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 23:
		localctx = NewElementFilterContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(138)
			p.Match(PlanParserElementFilter)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(139)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(140)
			p.FieldName()
		}
		{
			p.SetState(141)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(142)
			p.expr(0)
		}
		{
			p.SetState(143)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 24:
		localctx = NewMatchSimpleContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(145)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*MatchSimpleContext).op = _lt

			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserMATCH_ALL || _la == PlanParserMATCH_ANY) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*MatchSimpleContext).op = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(146)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(147)
			p.FieldName()
		}
		{
			p.SetState(148)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(149)
			p.expr(0)
		}
		{
			p.SetState(150)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 25:
		localctx = NewMatchThresholdContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(152)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*MatchThresholdContext).op = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&29360128) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*MatchThresholdContext).op = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(153)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(154)
			p.FieldName()
		}
		{
			p.SetState(155)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(156)
			p.expr(0)
		}
		{
			p.SetState(157)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(158)
			p.Match(PlanParserTHRESHOLD)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(159)
			p.Match(PlanParserASSIGN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(160)
			p.Match(PlanParserIntegerConstant)
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

	case 26:
		localctx = NewUnaryContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(163)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*UnaryContext).op = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&844450699935744) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*UnaryContext).op = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(164)
			p.expr(25)
		}

	case 27:
		localctx = NewJSONContainsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(165)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContains || _la == PlanParserArrayContains) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(166)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(167)
			p.expr(0)
		}
		{
			p.SetState(168)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(169)
			p.expr(0)
		}
		{
			p.SetState(170)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 28:
		localctx = NewJSONContainsAllContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(172)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContainsAll || _la == PlanParserArrayContainsAll) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(173)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(174)
			p.expr(0)
		}
		{
			p.SetState(175)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(176)
			p.expr(0)
		}
		{
			p.SetState(177)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 29:
		localctx = NewJSONContainsAnyContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(179)
			_la = p.GetTokenStream().LA(1)

			if !(_la == PlanParserJSONContainsAny || _la == PlanParserArrayContainsAny) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(180)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(181)
			p.expr(0)
		}
		{
			p.SetState(182)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(183)
			p.expr(0)
		}
		{
			p.SetState(184)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 30:
		localctx = NewSpatialBinaryContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(186)

			var _lt = p.GetTokenStream().LT(1)

			localctx.(*SpatialBinaryContext).op = _lt

			_la = p.GetTokenStream().LA(1)

			if !((int64((_la-60)) & ^0x3f) == 0 && ((int64(1)<<(_la-60))&127) != 0) {
				var _ri = p.GetErrorHandler().RecoverInline(p)

				localctx.(*SpatialBinaryContext).op = _ri
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
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
			p.FieldName()
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
			p.expr(0)
		}
		{
			p.SetState(191)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 31:
		localctx = NewSTDWithinContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(193)
			p.Match(PlanParserSTDWithin)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(194)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(195)
			p.FieldName()
		}
		{
			p.SetState(196)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(197)
			p.expr(0)
		}
		{
			p.SetState(198)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(199)
			p.expr(0)
		}
		{
			p.SetState(200)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 32:
		localctx = NewSTIsValidContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(202)
			p.Match(PlanParserSTIsValid)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(203)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(204)
			p.FieldName()
		}
		{
			p.SetState(205)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 33:
		localctx = NewArrayLengthContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(207)
			p.Match(PlanParserArrayLength)
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
		p.SetState(214)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}

		switch p.GetTokenStream().LA(1) {
		case PlanParserTEXTMATCHFUZZY, PlanParserMATCH_ALL, PlanParserMATCH_ANY, PlanParserMATCH_LEAST, PlanParserMATCH_MOST, PlanParserMATCH_EXACT, PlanParserINTERVAL, PlanParserISO, PlanParserMINIMUM_SHOULD_MATCH, PlanParserTHRESHOLD, PlanParserAS, PlanParserElementFilter, PlanParserSTEuqals, PlanParserSTTouches, PlanParserSTOverlaps, PlanParserSTCrosses, PlanParserSTContains, PlanParserSTIntersects, PlanParserSTWithin, PlanParserSTDWithin, PlanParserSTIsValid, PlanParserIdentifier:
			{
				p.SetState(209)
				p.FieldName()
			}

		case PlanParserJSONIdentifier:
			{
				p.SetState(210)
				p.Match(PlanParserJSONIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		case PlanParserStructFieldIdentifier:
			{
				p.SetState(211)
				p.Match(PlanParserStructFieldIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		case PlanParserStructSubFieldIdentifier:
			{
				p.SetState(212)
				p.Match(PlanParserStructSubFieldIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		case PlanParserStructIndexFieldIdentifier:
			{
				p.SetState(213)
				p.Match(PlanParserStructIndexFieldIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		default:
			p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
			goto errorExit
		}
		{
			p.SetState(216)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 34:
		localctx = NewMembershipMatchWithOptionContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(217)

			var _m = p.Match(PlanParserIdentifier)

			localctx.(*MembershipMatchWithOptionContext).function = _m
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(218)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(219)

			var _x = p.FieldReference()

			localctx.(*MembershipMatchWithOptionContext).field = _x
		}
		{
			p.SetState(220)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(221)
			p.expr(0)
		}
		{
			p.SetState(222)
			p.Match(PlanParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(223)

			var _m = p.Match(PlanParserIdentifier)

			localctx.(*MembershipMatchWithOptionContext).option = _m
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(224)
			p.Match(PlanParserASSIGN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(225)

			var _m = p.Match(PlanParserIdentifier)

			localctx.(*MembershipMatchWithOptionContext).kind = _m
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(226)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 35:
		localctx = NewCallContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(228)
			p.Match(PlanParserIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(229)
			p.Match(PlanParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(241)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if ((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&-1407348040040374) != 0) || ((int64((_la-64)) & ^0x3f) == 0 && ((int64(1)<<(_la-64))&2097151) != 0) {
			{
				p.SetState(230)
				p.expr(0)
			}
			p.SetState(235)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 14, p.GetParserRuleContext())
			if p.HasError() {
				goto errorExit
			}
			for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
				if _alt == 1 {
					{
						p.SetState(231)
						p.Match(PlanParserT__3)
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}
					{
						p.SetState(232)
						p.expr(0)
					}

				}
				p.SetState(237)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 14, p.GetParserRuleContext())
				if p.HasError() {
					goto errorExit
				}
			}
			p.SetState(239)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)

			if _la == PlanParserT__3 {
				{
					p.SetState(238)
					p.Match(PlanParserT__3)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}

			}

		}
		{
			p.SetState(243)
			p.Match(PlanParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 36:
		localctx = NewIsNullContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		p.SetState(249)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}

		switch p.GetTokenStream().LA(1) {
		case PlanParserTEXTMATCHFUZZY, PlanParserMATCH_ALL, PlanParserMATCH_ANY, PlanParserMATCH_LEAST, PlanParserMATCH_MOST, PlanParserMATCH_EXACT, PlanParserINTERVAL, PlanParserISO, PlanParserMINIMUM_SHOULD_MATCH, PlanParserTHRESHOLD, PlanParserAS, PlanParserElementFilter, PlanParserSTEuqals, PlanParserSTTouches, PlanParserSTOverlaps, PlanParserSTCrosses, PlanParserSTContains, PlanParserSTIntersects, PlanParserSTWithin, PlanParserSTDWithin, PlanParserSTIsValid, PlanParserIdentifier:
			{
				p.SetState(244)
				p.FieldName()
			}

		case PlanParserJSONIdentifier:
			{
				p.SetState(245)
				p.Match(PlanParserJSONIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		case PlanParserStructFieldIdentifier:
			{
				p.SetState(246)
				p.Match(PlanParserStructFieldIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		case PlanParserStructIndexFieldIdentifier:
			{
				p.SetState(247)
				p.Match(PlanParserStructIndexFieldIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		case PlanParserStructSubFieldIdentifier:
			{
				p.SetState(248)
				p.Match(PlanParserStructSubFieldIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		default:
			p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
			goto errorExit
		}
		{
			p.SetState(251)
			p.Match(PlanParserISNULL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 37:
		localctx = NewIsNotNullContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		p.SetState(257)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}

		switch p.GetTokenStream().LA(1) {
		case PlanParserTEXTMATCHFUZZY, PlanParserMATCH_ALL, PlanParserMATCH_ANY, PlanParserMATCH_LEAST, PlanParserMATCH_MOST, PlanParserMATCH_EXACT, PlanParserINTERVAL, PlanParserISO, PlanParserMINIMUM_SHOULD_MATCH, PlanParserTHRESHOLD, PlanParserAS, PlanParserElementFilter, PlanParserSTEuqals, PlanParserSTTouches, PlanParserSTOverlaps, PlanParserSTCrosses, PlanParserSTContains, PlanParserSTIntersects, PlanParserSTWithin, PlanParserSTDWithin, PlanParserSTIsValid, PlanParserIdentifier:
			{
				p.SetState(252)
				p.FieldName()
			}

		case PlanParserJSONIdentifier:
			{
				p.SetState(253)
				p.Match(PlanParserJSONIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		case PlanParserStructFieldIdentifier:
			{
				p.SetState(254)
				p.Match(PlanParserStructFieldIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		case PlanParserStructIndexFieldIdentifier:
			{
				p.SetState(255)
				p.Match(PlanParserStructIndexFieldIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		case PlanParserStructSubFieldIdentifier:
			{
				p.SetState(256)
				p.Match(PlanParserStructSubFieldIdentifier)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		default:
			p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
			goto errorExit
		}
		{
			p.SetState(259)
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
	p.SetState(332)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 24, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			if p.GetParseListeners() != nil {
				p.TriggerExitRuleEvent()
			}
			_prevctx = localctx
			p.SetState(330)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}

			switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 23, p.GetParserRuleContext()) {
			case 1:
				localctx = NewLikeContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(262)

				if !(p.Precpred(p.GetParserRuleContext(), 36)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 36)", ""))
					goto errorExit
				}
				{
					p.SetState(263)
					p.Match(PlanParserLIKE)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(264)
					p.expr(37)
				}

			case 2:
				localctx = NewRegexMatchContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(265)

				if !(p.Precpred(p.GetParserRuleContext(), 35)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 35)", ""))
					goto errorExit
				}
				{
					p.SetState(266)
					p.Match(PlanParserREGEXMATCH)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(267)
					p.expr(36)
				}

			case 3:
				localctx = NewRegexNotMatchContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(268)

				if !(p.Precpred(p.GetParserRuleContext(), 34)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 34)", ""))
					goto errorExit
				}
				{
					p.SetState(269)
					p.Match(PlanParserREGEXNOTMATCH)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(270)
					p.expr(35)
				}

			case 4:
				localctx = NewPowerContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(271)

				if !(p.Precpred(p.GetParserRuleContext(), 26)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 26)", ""))
					goto errorExit
				}
				{
					p.SetState(272)
					p.Match(PlanParserPOW)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(273)
					p.expr(27)
				}

			case 5:
				localctx = NewMulDivModContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(274)

				if !(p.Precpred(p.GetParserRuleContext(), 24)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 24)", ""))
					goto errorExit
				}
				{
					p.SetState(275)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*MulDivModContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&240518168576) != 0) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*MulDivModContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(276)
					p.expr(25)
				}

			case 6:
				localctx = NewAddSubContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(277)

				if !(p.Precpred(p.GetParserRuleContext(), 23)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 23)", ""))
					goto errorExit
				}
				{
					p.SetState(278)

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
					p.SetState(279)
					p.expr(24)
				}

			case 7:
				localctx = NewShiftContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(280)

				if !(p.Precpred(p.GetParserRuleContext(), 22)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 22)", ""))
					goto errorExit
				}
				{
					p.SetState(281)

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
					p.SetState(282)
					p.expr(23)
				}

			case 8:
				localctx = NewTermContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(283)

				if !(p.Precpred(p.GetParserRuleContext(), 21)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 21)", ""))
					goto errorExit
				}
				p.SetState(285)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_la = p.GetTokenStream().LA(1)

				if _la == PlanParserNOT {
					{
						p.SetState(284)

						var _m = p.Match(PlanParserNOT)

						localctx.(*TermContext).op = _m
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}

				}
				{
					p.SetState(287)
					p.Match(PlanParserIN)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(288)
					p.expr(22)
				}

			case 9:
				localctx = NewRangeContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(289)

				if !(p.Precpred(p.GetParserRuleContext(), 11)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 11)", ""))
					goto errorExit
				}
				{
					p.SetState(290)

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
				p.SetState(295)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}

				switch p.GetTokenStream().LA(1) {
				case PlanParserTEXTMATCHFUZZY, PlanParserMATCH_ALL, PlanParserMATCH_ANY, PlanParserMATCH_LEAST, PlanParserMATCH_MOST, PlanParserMATCH_EXACT, PlanParserINTERVAL, PlanParserISO, PlanParserMINIMUM_SHOULD_MATCH, PlanParserTHRESHOLD, PlanParserAS, PlanParserElementFilter, PlanParserSTEuqals, PlanParserSTTouches, PlanParserSTOverlaps, PlanParserSTCrosses, PlanParserSTContains, PlanParserSTIntersects, PlanParserSTWithin, PlanParserSTDWithin, PlanParserSTIsValid, PlanParserIdentifier:
					{
						p.SetState(291)
						p.FieldName()
					}

				case PlanParserJSONIdentifier:
					{
						p.SetState(292)
						p.Match(PlanParserJSONIdentifier)
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}

				case PlanParserStructSubFieldIdentifier:
					{
						p.SetState(293)
						p.Match(PlanParserStructSubFieldIdentifier)
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}

				case PlanParserStructIndexFieldIdentifier:
					{
						p.SetState(294)
						p.Match(PlanParserStructIndexFieldIdentifier)
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}

				default:
					p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
					goto errorExit
				}
				{
					p.SetState(297)

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
					p.SetState(298)
					p.expr(12)
				}

			case 10:
				localctx = NewReverseRangeContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(299)

				if !(p.Precpred(p.GetParserRuleContext(), 10)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 10)", ""))
					goto errorExit
				}
				{
					p.SetState(300)

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
				p.SetState(305)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}

				switch p.GetTokenStream().LA(1) {
				case PlanParserTEXTMATCHFUZZY, PlanParserMATCH_ALL, PlanParserMATCH_ANY, PlanParserMATCH_LEAST, PlanParserMATCH_MOST, PlanParserMATCH_EXACT, PlanParserINTERVAL, PlanParserISO, PlanParserMINIMUM_SHOULD_MATCH, PlanParserTHRESHOLD, PlanParserAS, PlanParserElementFilter, PlanParserSTEuqals, PlanParserSTTouches, PlanParserSTOverlaps, PlanParserSTCrosses, PlanParserSTContains, PlanParserSTIntersects, PlanParserSTWithin, PlanParserSTDWithin, PlanParserSTIsValid, PlanParserIdentifier:
					{
						p.SetState(301)
						p.FieldName()
					}

				case PlanParserJSONIdentifier:
					{
						p.SetState(302)
						p.Match(PlanParserJSONIdentifier)
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}

				case PlanParserStructSubFieldIdentifier:
					{
						p.SetState(303)
						p.Match(PlanParserStructSubFieldIdentifier)
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}

				case PlanParserStructIndexFieldIdentifier:
					{
						p.SetState(304)
						p.Match(PlanParserStructIndexFieldIdentifier)
						if p.HasError() {
							// Recognition error - abort rule
							goto errorExit
						}
					}

				default:
					p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
					goto errorExit
				}
				{
					p.SetState(307)

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
					p.SetState(308)
					p.expr(11)
				}

			case 11:
				localctx = NewRelationalContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(309)

				if !(p.Precpred(p.GetParserRuleContext(), 9)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 9)", ""))
					goto errorExit
				}
				{
					p.SetState(310)

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
					p.SetState(311)
					p.expr(10)
				}

			case 12:
				localctx = NewEqualityContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(312)

				if !(p.Precpred(p.GetParserRuleContext(), 8)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 8)", ""))
					goto errorExit
				}
				{
					p.SetState(313)

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
					p.SetState(314)
					p.expr(9)
				}

			case 13:
				localctx = NewBitAndContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(315)

				if !(p.Precpred(p.GetParserRuleContext(), 7)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 7)", ""))
					goto errorExit
				}
				{
					p.SetState(316)
					p.Match(PlanParserBAND)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(317)
					p.expr(8)
				}

			case 14:
				localctx = NewBitXorContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(318)

				if !(p.Precpred(p.GetParserRuleContext(), 6)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 6)", ""))
					goto errorExit
				}
				{
					p.SetState(319)
					p.Match(PlanParserBXOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(320)
					p.expr(7)
				}

			case 15:
				localctx = NewBitOrContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(321)

				if !(p.Precpred(p.GetParserRuleContext(), 5)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 5)", ""))
					goto errorExit
				}
				{
					p.SetState(322)
					p.Match(PlanParserBOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(323)
					p.expr(6)
				}

			case 16:
				localctx = NewLogicalAndContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(324)

				if !(p.Precpred(p.GetParserRuleContext(), 4)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 4)", ""))
					goto errorExit
				}
				{
					p.SetState(325)
					p.Match(PlanParserAND)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(326)
					p.expr(5)
				}

			case 17:
				localctx = NewLogicalOrContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, PlanParserRULE_expr)
				p.SetState(327)

				if !(p.Precpred(p.GetParserRuleContext(), 3)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 3)", ""))
					goto errorExit
				}
				{
					p.SetState(328)
					p.Match(PlanParserOR)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(329)
					p.expr(4)
				}

			case antlr.ATNInvalidAltNumber:
				goto errorExit
			}

		}
		p.SetState(334)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 24, p.GetParserRuleContext())
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

// IFieldNameContext is an interface to support dynamic dispatch.
type IFieldNameContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Identifier() antlr.TerminalNode
	AS() antlr.TerminalNode
	ISO() antlr.TerminalNode
	INTERVAL() antlr.TerminalNode
	MINIMUM_SHOULD_MATCH() antlr.TerminalNode
	THRESHOLD() antlr.TerminalNode
	TEXTMATCHFUZZY() antlr.TerminalNode
	MATCH_ALL() antlr.TerminalNode
	MATCH_ANY() antlr.TerminalNode
	MATCH_LEAST() antlr.TerminalNode
	MATCH_MOST() antlr.TerminalNode
	MATCH_EXACT() antlr.TerminalNode
	ElementFilter() antlr.TerminalNode
	STEuqals() antlr.TerminalNode
	STTouches() antlr.TerminalNode
	STOverlaps() antlr.TerminalNode
	STCrosses() antlr.TerminalNode
	STContains() antlr.TerminalNode
	STIntersects() antlr.TerminalNode
	STWithin() antlr.TerminalNode
	STDWithin() antlr.TerminalNode
	STIsValid() antlr.TerminalNode

	// IsFieldNameContext differentiates from other interfaces.
	IsFieldNameContext()
}

type FieldNameContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFieldNameContext() *FieldNameContext {
	var p = new(FieldNameContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_fieldName
	return p
}

func InitEmptyFieldNameContext(p *FieldNameContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_fieldName
}

func (*FieldNameContext) IsFieldNameContext() {}

func NewFieldNameContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FieldNameContext {
	var p = new(FieldNameContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_fieldName

	return p
}

func (s *FieldNameContext) GetParser() antlr.Parser { return s.parser }

func (s *FieldNameContext) Identifier() antlr.TerminalNode {
	return s.GetToken(PlanParserIdentifier, 0)
}

func (s *FieldNameContext) AS() antlr.TerminalNode {
	return s.GetToken(PlanParserAS, 0)
}

func (s *FieldNameContext) ISO() antlr.TerminalNode {
	return s.GetToken(PlanParserISO, 0)
}

func (s *FieldNameContext) INTERVAL() antlr.TerminalNode {
	return s.GetToken(PlanParserINTERVAL, 0)
}

func (s *FieldNameContext) MINIMUM_SHOULD_MATCH() antlr.TerminalNode {
	return s.GetToken(PlanParserMINIMUM_SHOULD_MATCH, 0)
}

func (s *FieldNameContext) THRESHOLD() antlr.TerminalNode {
	return s.GetToken(PlanParserTHRESHOLD, 0)
}

func (s *FieldNameContext) TEXTMATCHFUZZY() antlr.TerminalNode {
	return s.GetToken(PlanParserTEXTMATCHFUZZY, 0)
}

func (s *FieldNameContext) MATCH_ALL() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_ALL, 0)
}

func (s *FieldNameContext) MATCH_ANY() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_ANY, 0)
}

func (s *FieldNameContext) MATCH_LEAST() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_LEAST, 0)
}

func (s *FieldNameContext) MATCH_MOST() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_MOST, 0)
}

func (s *FieldNameContext) MATCH_EXACT() antlr.TerminalNode {
	return s.GetToken(PlanParserMATCH_EXACT, 0)
}

func (s *FieldNameContext) ElementFilter() antlr.TerminalNode {
	return s.GetToken(PlanParserElementFilter, 0)
}

func (s *FieldNameContext) STEuqals() antlr.TerminalNode {
	return s.GetToken(PlanParserSTEuqals, 0)
}

func (s *FieldNameContext) STTouches() antlr.TerminalNode {
	return s.GetToken(PlanParserSTTouches, 0)
}

func (s *FieldNameContext) STOverlaps() antlr.TerminalNode {
	return s.GetToken(PlanParserSTOverlaps, 0)
}

func (s *FieldNameContext) STCrosses() antlr.TerminalNode {
	return s.GetToken(PlanParserSTCrosses, 0)
}

func (s *FieldNameContext) STContains() antlr.TerminalNode {
	return s.GetToken(PlanParserSTContains, 0)
}

func (s *FieldNameContext) STIntersects() antlr.TerminalNode {
	return s.GetToken(PlanParserSTIntersects, 0)
}

func (s *FieldNameContext) STWithin() antlr.TerminalNode {
	return s.GetToken(PlanParserSTWithin, 0)
}

func (s *FieldNameContext) STDWithin() antlr.TerminalNode {
	return s.GetToken(PlanParserSTDWithin, 0)
}

func (s *FieldNameContext) STIsValid() antlr.TerminalNode {
	return s.GetToken(PlanParserSTIsValid, 0)
}

func (s *FieldNameContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FieldNameContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FieldNameContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitFieldName(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) FieldName() (localctx IFieldNameContext) {
	localctx = NewFieldNameContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, PlanParserRULE_fieldName)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(335)
		_la = p.GetTokenStream().LA(1)

		if !((int64((_la-17)) & ^0x3f) == 0 && ((int64(1)<<(_la-17))&40527998599831545) != 0) {
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

// IFieldReferenceContext is an interface to support dynamic dispatch.
type IFieldReferenceContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	FieldName() IFieldNameContext
	Meta() antlr.TerminalNode
	JSONIdentifier() antlr.TerminalNode
	StructFieldIdentifier() antlr.TerminalNode
	StructIndexFieldIdentifier() antlr.TerminalNode
	StructSubFieldIdentifier() antlr.TerminalNode

	// IsFieldReferenceContext differentiates from other interfaces.
	IsFieldReferenceContext()
}

type FieldReferenceContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFieldReferenceContext() *FieldReferenceContext {
	var p = new(FieldReferenceContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_fieldReference
	return p
}

func InitEmptyFieldReferenceContext(p *FieldReferenceContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = PlanParserRULE_fieldReference
}

func (*FieldReferenceContext) IsFieldReferenceContext() {}

func NewFieldReferenceContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FieldReferenceContext {
	var p = new(FieldReferenceContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = PlanParserRULE_fieldReference

	return p
}

func (s *FieldReferenceContext) GetParser() antlr.Parser { return s.parser }

func (s *FieldReferenceContext) FieldName() IFieldNameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFieldNameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFieldNameContext)
}

func (s *FieldReferenceContext) Meta() antlr.TerminalNode {
	return s.GetToken(PlanParserMeta, 0)
}

func (s *FieldReferenceContext) JSONIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserJSONIdentifier, 0)
}

func (s *FieldReferenceContext) StructFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructFieldIdentifier, 0)
}

func (s *FieldReferenceContext) StructIndexFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructIndexFieldIdentifier, 0)
}

func (s *FieldReferenceContext) StructSubFieldIdentifier() antlr.TerminalNode {
	return s.GetToken(PlanParserStructSubFieldIdentifier, 0)
}

func (s *FieldReferenceContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FieldReferenceContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FieldReferenceContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case PlanVisitor:
		return t.VisitFieldReference(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *PlanParser) FieldReference() (localctx IFieldReferenceContext) {
	localctx = NewFieldReferenceContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 8, PlanParserRULE_fieldReference)
	p.SetState(343)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case PlanParserTEXTMATCHFUZZY, PlanParserMATCH_ALL, PlanParserMATCH_ANY, PlanParserMATCH_LEAST, PlanParserMATCH_MOST, PlanParserMATCH_EXACT, PlanParserINTERVAL, PlanParserISO, PlanParserMINIMUM_SHOULD_MATCH, PlanParserTHRESHOLD, PlanParserAS, PlanParserElementFilter, PlanParserSTEuqals, PlanParserSTTouches, PlanParserSTOverlaps, PlanParserSTCrosses, PlanParserSTContains, PlanParserSTIntersects, PlanParserSTWithin, PlanParserSTDWithin, PlanParserSTIsValid, PlanParserIdentifier:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(337)
			p.FieldName()
		}

	case PlanParserMeta:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(338)
			p.Match(PlanParserMeta)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserJSONIdentifier:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(339)
			p.Match(PlanParserJSONIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserStructFieldIdentifier:
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(340)
			p.Match(PlanParserStructFieldIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserStructIndexFieldIdentifier:
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(341)
			p.Match(PlanParserStructIndexFieldIdentifier)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case PlanParserStructSubFieldIdentifier:
		p.EnterOuterAlt(localctx, 6)
		{
			p.SetState(342)
			p.Match(PlanParserStructSubFieldIdentifier)
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
	p.EnterRule(localctx, 10, PlanParserRULE_textMatchOption)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(345)
		p.Match(PlanParserMINIMUM_SHOULD_MATCH)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(346)
		p.Match(PlanParserASSIGN)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(347)
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
	case 2:
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
		return p.Precpred(p.GetParserRuleContext(), 36)

	case 1:
		return p.Precpred(p.GetParserRuleContext(), 35)

	case 2:
		return p.Precpred(p.GetParserRuleContext(), 34)

	case 3:
		return p.Precpred(p.GetParserRuleContext(), 26)

	case 4:
		return p.Precpred(p.GetParserRuleContext(), 24)

	case 5:
		return p.Precpred(p.GetParserRuleContext(), 23)

	case 6:
		return p.Precpred(p.GetParserRuleContext(), 22)

	case 7:
		return p.Precpred(p.GetParserRuleContext(), 21)

	case 8:
		return p.Precpred(p.GetParserRuleContext(), 11)

	case 9:
		return p.Precpred(p.GetParserRuleContext(), 10)

	case 10:
		return p.Precpred(p.GetParserRuleContext(), 9)

	case 11:
		return p.Precpred(p.GetParserRuleContext(), 8)

	case 12:
		return p.Precpred(p.GetParserRuleContext(), 7)

	case 13:
		return p.Precpred(p.GetParserRuleContext(), 6)

	case 14:
		return p.Precpred(p.GetParserRuleContext(), 5)

	case 15:
		return p.Precpred(p.GetParserRuleContext(), 4)

	case 16:
		return p.Precpred(p.GetParserRuleContext(), 3)

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}
