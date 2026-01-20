package planparserv2

import (
	"fmt"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"

	antlrparser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Comprehensive benchmark expressions covering all parser features
var optimizationBenchExprs = []struct {
	name string
	expr string
}{
	// === Basic Operations ===
	{"int_eq", "Int64Field == 100"},
	{"int_lt", "Int64Field < 100"},
	{"int_gt", "Int64Field > 100"},
	{"int_le", "Int64Field <= 100"},
	{"int_ge", "Int64Field >= 100"},
	{"int_ne", "Int64Field != 100"},
	{"float_eq", "FloatField == 1.5"},
	{"float_lt", "FloatField < 1.5"},

	// === String Operations ===
	{"string_eq", `StringField == "hello"`},
	{"string_ne", `StringField != "world"`},
	{"string_like", `StringField like "prefix%"`},
	{"string_like_suffix", `StringField like "%suffix"`},
	{"string_like_contains", `StringField like "%middle%"`},

	// === Boolean Operations (case-insensitive keywords) ===
	{"bool_and_symbol", "Int64Field > 10 && Int64Field < 100"},
	{"bool_or_symbol", "Int64Field < 10 || Int64Field > 100"},
	{"bool_and_keyword", "Int64Field > 10 and Int64Field < 100"},
	{"bool_or_keyword", "Int64Field < 10 or Int64Field > 100"},
	{"bool_AND_upper", "Int64Field > 10 AND Int64Field < 100"},
	{"bool_OR_upper", "Int64Field < 10 OR Int64Field > 100"},
	{"bool_not", "not (Int64Field > 100)"},
	{"bool_NOT_upper", "NOT (Int64Field > 100)"},

	// === IN Operations ===
	{"in_3", "Int64Field in [1, 2, 3]"},
	{"in_5", "Int64Field in [1, 2, 3, 4, 5]"},
	{"in_10", "Int64Field in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"},
	{"not_in", "Int64Field not in [1, 2, 3]"},
	{"NOT_IN_upper", "Int64Field NOT IN [1, 2, 3]"},
	{"string_in", `StringField in ["a", "b", "c"]`},

	// === NULL Checks (case-insensitive) ===
	{"is_null_lower", "Int64Field is null"},
	{"is_null_upper", "Int64Field IS NULL"},
	{"is_not_null_lower", "Int64Field is not null"},
	{"is_not_null_upper", "Int64Field IS NOT NULL"},

	// === Range Expressions ===
	{"range_lt_lt", "10 < Int64Field < 100"},
	{"range_le_le", "10 <= Int64Field <= 100"},
	{"range_gt_gt", "100 > Int64Field > 10"},
	{"range_ge_ge", "100 >= Int64Field >= 10"},

	// === Arithmetic ===
	{"arith_add", "Int64Field + 5 == 100"},
	{"arith_sub", "Int64Field - 5 == 95"},
	{"arith_mul", "Int64Field * 2 < 200"},
	{"arith_div", "Int64Field / 2 > 25"},
	{"arith_mod", "Int64Field % 10 == 0"},

	// === Array Operations (case-insensitive) ===
	{"array_contains", "array_contains(ArrayField, 1)"},
	{"ARRAY_CONTAINS_upper", "ARRAY_CONTAINS(ArrayField, 1)"},
	{"array_contains_all", "array_contains_all(ArrayField, [1, 2, 3])"},
	{"array_contains_any", "array_contains_any(ArrayField, [1, 2, 3])"},
	{"array_length", "array_length(ArrayField) == 10"},
	{"ARRAY_LENGTH_upper", "ARRAY_LENGTH(ArrayField) == 10"},

	// === JSON Operations (case-insensitive) ===
	{"json_access", `JSONField["key"] == 100`},
	{"json_nested", `JSONField["a"]["b"] == "value"`},
	{"json_contains", `json_contains(JSONField["arr"], 1)`},
	{"JSON_CONTAINS_upper", `JSON_CONTAINS(JSONField["arr"], 1)`},
	{"json_contains_all", `json_contains_all(JSONField["arr"], [1, 2])`},
	{"json_contains_any", `json_contains_any(JSONField["arr"], [1, 2])`},

	// === EXISTS (case-insensitive) ===
	{"exists_lower", `exists JSONField["key"]`},
	{"EXISTS_upper", `EXISTS JSONField["key"]`},

	// === LIKE (case-insensitive) ===
	{"like_lower", `StringField like "test%"`},
	{"LIKE_upper", `StringField LIKE "test%"`},

	// === Complex Expressions ===
	{"complex_and_chain", "Int64Field > 0 && Int64Field < 100 && FloatField > 1.0"},
	{"complex_or_chain", "Int64Field < 0 || Int64Field > 100 || FloatField < 0"},
	{"complex_mixed", "(Int64Field > 10 && Int64Field < 100) || (FloatField > 1.0 && FloatField < 10.0)"},
	{"complex_nested", "((Int64Field > 10 && Int64Field < 50) || (Int64Field > 60 && Int64Field < 100)) && FloatField > 0"},
	{"complex_with_in", `(Int64Field in [1,2,3] || FloatField > 1.5) && StringField != "exclude"`},
	{"complex_with_json", `JSONField["status"] == "active" && Int64Field > 0 && Int64Field is not null`},
	{"complex_with_array", `Int64Field > 10 && array_length(ArrayField) > 0 && array_contains(ArrayField, 1)`},
	{"complex_full", `Int64Field > 10 && StringField like "test%" && array_length(ArrayField) > 0 && JSONField["active"] == true`},
}

func getOptBenchSchemaHelper(b *testing.B) *typeutil.SchemaHelper {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(b, err)
	return schemaHelper
}

// BenchmarkOptimizationComparison benchmarks full parsing pipeline without cache
func BenchmarkOptimizationComparison(b *testing.B) {
	schemaHelper := getOptBenchSchemaHelper(b)

	for _, tc := range optimizationBenchExprs {
		b.Run(tc.name, func(b *testing.B) {
			// Purge cache before each benchmark to measure raw parsing performance
			exprCache.Purge()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Purge cache on each iteration to prevent caching
				exprCache.Purge()
				_, err := ParseExpr(schemaHelper, tc.expr, nil)
				if err != nil {
					b.Fatalf("failed to parse %s: %v", tc.expr, err)
				}
			}
		})
	}
}

// BenchmarkLexerComparison benchmarks only the lexer stage
func BenchmarkLexerComparison(b *testing.B) {
	for _, tc := range optimizationBenchExprs {
		b.Run(tc.name, func(b *testing.B) {
			exprNormal := convertHanToASCII(tc.expr)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				listener := &errorListenerImpl{}
				inputStream := antlr.NewInputStream(exprNormal)
				lexer := getLexer(inputStream, listener)

				// Consume all tokens
				for {
					tok := lexer.NextToken()
					if tok.GetTokenType() == antlr.TokenEOF {
						break
					}
				}

				putLexer(lexer)
			}
		})
	}
}

// BenchmarkParserComparison benchmarks lexer + parser (without visitor)
func BenchmarkParserComparison(b *testing.B) {
	for _, tc := range optimizationBenchExprs {
		b.Run(tc.name, func(b *testing.B) {
			exprNormal := convertHanToASCII(tc.expr)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				listener := &errorListenerImpl{}
				inputStream := antlr.NewInputStream(exprNormal)
				lexer := getLexer(inputStream, listener)
				parser := getParser(lexer, listener)

				_ = parser.Expr()

				putLexer(lexer)
				putParser(parser)
			}
		})
	}
}

// BenchmarkConvertHanToASCII benchmarks the Han character conversion
func BenchmarkConvertHanToASCII(b *testing.B) {
	testCases := []struct {
		name string
		expr string
	}{
		{"short_ascii", "a == 1"},
		{"medium_ascii", "Int64Field > 10 && Int64Field < 100"},
		{"long_ascii", `(Int64Field in [1,2,3,4,5] || FloatField > 1.5) && StringField != "exclude" && JSONField["status"] == "active"`},
		{"with_han_chars", "字段名 == 100 && 另一个字段 > 50"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = convertHanToASCII(tc.expr)
			}
		})
	}
}

// BenchmarkDecodeUnicode benchmarks unicode decoding
func BenchmarkDecodeUnicode(b *testing.B) {
	testCases := []struct {
		name string
		expr string
	}{
		{"no_unicode", "Int64Field == 100"},
		{"with_unicode", `field\u0041\u0042 == 100`},
		{"multiple_unicode", `\u0041\u0042\u0043\u0044 == "test"`},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = decodeUnicode(tc.expr)
			}
		})
	}
}

// BenchmarkPoolOverhead measures sync.Pool get/put overhead
func BenchmarkPoolOverhead(b *testing.B) {
	b.Run("lexer_get_put", func(b *testing.B) {
		inputStream := antlr.NewInputStream("a == 1")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			lexer := getLexer(inputStream)
			putLexer(lexer)
		}
	})

	b.Run("parser_get_put", func(b *testing.B) {
		inputStream := antlr.NewInputStream("a == 1")
		lexer := antlrparser.NewPlanLexer(inputStream)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			parser := getParser(lexer)
			putParser(parser)
		}
	})
}

// BenchmarkCacheHitRatio compares cached vs uncached performance
func BenchmarkCacheEffect(b *testing.B) {
	schemaHelper := getOptBenchSchemaHelper(b)
	expr := "Int64Field > 10 && Int64Field < 100"

	b.Run("without_cache", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			exprCache.Purge()
			_, _ = ParseExpr(schemaHelper, expr, nil)
		}
	})

	b.Run("with_cache", func(b *testing.B) {
		// Warm up cache
		exprCache.Purge()
		_, _ = ParseExpr(schemaHelper, expr, nil)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = ParseExpr(schemaHelper, expr, nil)
		}
	})
}

// BenchmarkKeywordCaseSensitivity tests case-insensitive keyword performance
func BenchmarkKeywordCaseSensitivity(b *testing.B) {
	schemaHelper := getOptBenchSchemaHelper(b)

	keywords := []struct {
		name  string
		lower string
		upper string
	}{
		{"and", "a > 1 and a < 10", "a > 1 AND a < 10"},
		{"or", "a < 1 or a > 10", "a < 1 OR a > 10"},
		{"not", "not (a > 10)", "NOT (a > 10)"},
		{"in", "a in [1,2,3]", "a IN [1,2,3]"},
		{"like", `b like "test%"`, `b LIKE "test%"`},
		{"is_null", "a is null", "a IS NULL"},
		{"is_not_null", "a is not null", "a IS NOT NULL"},
		{"exists", `exists c["key"]`, `EXISTS c["key"]`},
		{"array_contains", "array_contains(d, 1)", "ARRAY_CONTAINS(d, 1)"},
		{"json_contains", `json_contains(c["arr"], 1)`, `JSON_CONTAINS(c["arr"], 1)`},
	}

	for _, kw := range keywords {
		b.Run(kw.name+"_lower", func(b *testing.B) {
			exprCache.Purge()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				exprCache.Purge()
				_, _ = ParseExpr(schemaHelper, kw.lower, nil)
			}
		})
		b.Run(kw.name+"_upper", func(b *testing.B) {
			exprCache.Purge()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				exprCache.Purge()
				_, _ = ParseExpr(schemaHelper, kw.upper, nil)
			}
		})
	}
}

// BenchmarkExpressionComplexity tests how performance scales with expression complexity
func BenchmarkExpressionComplexity(b *testing.B) {
	schemaHelper := getOptBenchSchemaHelper(b)

	// Generate expressions with increasing complexity
	for _, andCount := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("and_chain_%d", andCount), func(b *testing.B) {
			expr := "Int64Field > 0"
			for i := 1; i < andCount; i++ {
				expr += fmt.Sprintf(" && Int64Field < %d", 1000+i)
			}

			exprCache.Purge()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				exprCache.Purge()
				_, _ = ParseExpr(schemaHelper, expr, nil)
			}
		})
	}

	// Test with increasing IN list size
	for _, inCount := range []int{5, 10, 25, 50, 100} {
		b.Run(fmt.Sprintf("in_list_%d", inCount), func(b *testing.B) {
			expr := "Int64Field in ["
			for i := 0; i < inCount; i++ {
				if i > 0 {
					expr += ","
				}
				expr += fmt.Sprintf("%d", i)
			}
			expr += "]"

			exprCache.Purge()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				exprCache.Purge()
				_, _ = ParseExpr(schemaHelper, expr, nil)
			}
		})
	}
}
