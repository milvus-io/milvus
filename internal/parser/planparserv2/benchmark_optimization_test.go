package planparserv2

import (
	"fmt"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Benchmark test cases covering various expression patterns
var benchmarkExprs = []struct {
	name string
	expr string
}{
	// Simple comparisons
	{"simple_eq", "Int64Field == 100"},
	{"simple_lt", "Int64Field < 100"},
	{"simple_ne", "Int64Field != 100"},

	// Boolean operations
	{"bool_and", "Int64Field > 10 && Int64Field < 100"},
	{"bool_or", "Int64Field < 10 || Int64Field > 100"},
	{"bool_and_text", "Int64Field > 10 and Int64Field < 100"},
	{"bool_or_text", "Int64Field < 10 or Int64Field > 100"},
	{"bool_complex", "(Int64Field > 10 && Int64Field < 100) || (FloatField > 1.0 && FloatField < 10.0)"},

	// Arithmetic
	{"arith_add", "Int64Field + 5 == 100"},
	{"arith_mul", "Int64Field * 2 < 200"},

	// IN expressions
	{"in_small", "Int64Field in [1, 2, 3]"},
	{"in_medium", "Int64Field in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"},
	{"not_in", "Int64Field not in [1, 2, 3]"},

	// String operations
	{"string_eq", `StringField == "hello"`},
	{"string_like", `StringField like "hello%"`},
	{"string_in", `StringField in ["a", "b", "c"]`},

	// Array operations
	{"array_contains", "array_contains(ArrayField, 1)"},
	{"array_contains_all", "array_contains_all(ArrayField, [1, 2, 3])"},
	{"array_contains_any", "array_contains_any(ArrayField, [1, 2, 3])"},
	{"array_length", "array_length(ArrayField) == 10"},

	// JSON field access
	{"json_simple", `JSONField["key"] == 100`},
	{"json_nested", `JSONField["a"]["b"] == "value"`},
	{"json_contains", `json_contains(JSONField["arr"], 1)`},

	// NULL checks
	{"is_null", "Int64Field is null"},
	{"is_not_null", "Int64Field is not null"},
	{"is_null_upper", "Int64Field IS NULL"},
	{"is_not_null_upper", "Int64Field IS NOT NULL"},

	// Range expressions
	{"range_lt_lt", "10 < Int64Field < 100"},
	{"range_le_le", "10 <= Int64Field <= 100"},
	{"range_gt_gt", "100 > Int64Field > 10"},

	// EXISTS
	{"exists", `exists JSONField["key"]`},

	// Complex mixed expressions
	{"complex_1", `Int64Field > 10 && StringField like "test%" && array_length(ArrayField) > 0`},
	{"complex_2", `(Int64Field in [1,2,3] || FloatField > 1.5) && StringField != "exclude"`},
	{"complex_3", `JSONField["status"] == "active" && Int64Field > 0 && Int64Field is not null`},
}

func getTestSchemaHelper(b *testing.B) *typeutil.SchemaHelper {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(b, err)
	return schemaHelper
}

// BenchmarkParserOverall tests overall parser performance
func BenchmarkParserOverall(b *testing.B) {
	schemaHelper := getTestSchemaHelper(b)

	for _, tc := range benchmarkExprs {
		b.Run(tc.name, func(b *testing.B) {
			// Clear cache to test raw parsing performance
			exprCache.Purge()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := ParseExpr(schemaHelper, tc.expr, nil)
				if err != nil {
					b.Fatalf("failed to parse %s: %v", tc.expr, err)
				}
			}
		})
	}
}

// BenchmarkLexerOnly tests lexer performance
func BenchmarkLexerOnly(b *testing.B) {
	for _, tc := range benchmarkExprs {
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

// BenchmarkParseOnly tests parsing without visitor
func BenchmarkParseOnly(b *testing.B) {
	for _, tc := range benchmarkExprs {
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

// BenchmarkPoolPerformance tests object pool overhead
func BenchmarkPoolPerformance(b *testing.B) {
	b.Run("lexer_pool", func(b *testing.B) {
		inputStream := antlr.NewInputStream("Int64Field > 10")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			lexer := getLexer(inputStream)
			putLexer(lexer)
		}
	})

	b.Run("parser_pool", func(b *testing.B) {
		inputStream := antlr.NewInputStream("Int64Field > 10")
		lexer := getLexer(inputStream)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			parser := getParser(lexer)
			putParser(parser)
		}
		putLexer(lexer)
	})
}

// BenchmarkScalability tests performance with increasing expression complexity
func BenchmarkScalability(b *testing.B) {
	schemaHelper := getTestSchemaHelper(b)

	// Test with increasing number of AND conditions
	for _, count := range []int{1, 5, 10, 20} {
		b.Run(fmt.Sprintf("and_chain_%d", count), func(b *testing.B) {
			expr := "Int64Field > 0"
			for i := 1; i < count; i++ {
				expr += fmt.Sprintf(" && Int64Field < %d", 1000+i)
			}

			exprCache.Purge()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := ParseExpr(schemaHelper, expr, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	// Test with increasing IN list size
	for _, count := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("in_list_%d", count), func(b *testing.B) {
			expr := "Int64Field in ["
			for i := 0; i < count; i++ {
				if i > 0 {
					expr += ","
				}
				expr += fmt.Sprintf("%d", i)
			}
			expr += "]"

			exprCache.Purge()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := ParseExpr(schemaHelper, expr, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
