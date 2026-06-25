package planparserv2

import (
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"

	gen "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
)

// benchLexParseLL parses a single expression with the original single-pass full
// LL strategy (the pre-optimization baseline). It deliberately bypasses the
// expr AST cache so the benchmark measures lexing + parsing only.
func benchLexParseLL(expr string) (gen.IExprContext, error) {
	listener := &errorListenerImpl{}
	is := antlr.NewInputStream(convertHanToASCII(expr))
	lexer := getLexer(is, listener)
	parser := getParser(lexer, listener)
	parser.SetErrorHandler(antlr.NewDefaultErrorStrategy())
	parser.GetInterpreter().SetPredictionMode(antlr.PredictionModeLL)
	ast := parser.Expr()
	err := eofErr(parser, listener)
	putLexer(lexer)
	putParser(parser)
	return ast, err
}

// eofErr mirrors handleInternal: a parse is only accepted if there was no syntax
// error AND the whole input was consumed (trailing tokens => invalid).
func eofErr(parser *gen.PlanParser, listener *errorListenerImpl) error {
	if e := listener.Error(); e != nil {
		return e
	}
	if parser.GetCurrentToken().GetTokenType() != antlr.TokenEOF {
		return errTrailingTokens
	}
	return nil
}

var errTrailingTokens = &trailingTokenError{}

type trailingTokenError struct{}

func (*trailingTokenError) Error() string { return "invalid expression: trailing tokens" }

// benchLexParseTwoStage parses a single expression with the new SLL-first
// two-stage strategy (parseExpr). Also bypasses the AST cache.
func benchLexParseTwoStage(expr string) (gen.IExprContext, error) {
	listener := &errorListenerImpl{}
	is := antlr.NewInputStream(convertHanToASCII(expr))
	lexer := getLexer(is, listener)
	parser := getParser(lexer, listener)
	ast := parseExpr(parser, listener)
	err := eofErr(parser, listener)
	putLexer(lexer)
	putParser(parser)
	return ast, err
}

// TestTwoStageMatchesLL guarantees the SLL-first two-stage parse yields exactly
// the same tree (and the same accept/reject decision) as a pure LL parse for the
// whole benchmark corpus — the correctness contract behind the optimization.
func TestTwoStageMatchesLL(t *testing.T) {
	for _, tc := range benchmarkExprs {
		llAst, llErr := benchLexParseLL(tc.expr)
		tsAst, tsErr := benchLexParseTwoStage(tc.expr)
		require.Equal(t, llErr == nil, tsErr == nil, "accept/reject differs for %q (LL err=%v, two-stage err=%v)", tc.expr, llErr, tsErr)
		if llErr == nil && tsErr == nil {
			require.Equal(t, llAst.GetText(), tsAst.GetText(), "parse tree differs for %q", tc.expr)
		}
	}
}

// identifierLed expressions begin with a column identifier, so under the current
// grammar they pay the prediction cost of the two leading Timestamptz* expr
// alternatives (#4). literalLed begin with a constant and do not.
var (
	identifierLedExprs = []string{
		"Int64Field == 100",
		"Int64Field > 10 && Int64Field < 100",
		"Int64Field in [1, 2, 3, 4, 5]",
		"FloatField >= 1.5",
		"VarCharField == \"abc\"",
	}
	literalLedExprs = []string{
		"100 == Int64Field",
		"10 < Int64Field && 100 > Int64Field",
		"1.5 <= FloatField",
	}
)

func benchParseSet(b *testing.B, exprs []string, parse func(string) (gen.IExprContext, error)) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parse(exprs[i%len(exprs)])
	}
}

// BenchmarkParse_LL vs BenchmarkParse_TwoStage: overall corpus, lex+parse only.
func BenchmarkParse_LL(b *testing.B) {
	exprs := make([]string, len(benchmarkExprs))
	for i, tc := range benchmarkExprs {
		exprs[i] = tc.expr
	}
	benchParseSet(b, exprs, benchLexParseLL)
}

func BenchmarkParse_TwoStage(b *testing.B) {
	exprs := make([]string, len(benchmarkExprs))
	for i, tc := range benchmarkExprs {
		exprs[i] = tc.expr
	}
	benchParseSet(b, exprs, benchLexParseTwoStage)
}

// Per-expression detail so we can see which shapes benefit most.
func BenchmarkParseEach_LL(b *testing.B) {
	for _, tc := range benchmarkExprs {
		b.Run(tc.name, func(b *testing.B) { benchParseSet(b, []string{tc.expr}, benchLexParseLL) })
	}
}

func BenchmarkParseEach_TwoStage(b *testing.B) {
	for _, tc := range benchmarkExprs {
		b.Run(tc.name, func(b *testing.B) { benchParseSet(b, []string{tc.expr}, benchLexParseTwoStage) })
	}
}

// Quantify the #4 Timestamptz-prefix tax: identifier-led vs literal-led, in both
// prediction strategies.
func BenchmarkIdentifierLed_LL(b *testing.B) { benchParseSet(b, identifierLedExprs, benchLexParseLL) }

func BenchmarkIdentifierLed_TwoStage(b *testing.B) {
	benchParseSet(b, identifierLedExprs, benchLexParseTwoStage)
}
func BenchmarkLiteralLed_LL(b *testing.B) { benchParseSet(b, literalLedExprs, benchLexParseLL) }
func BenchmarkLiteralLed_TwoStage(b *testing.B) {
	benchParseSet(b, literalLedExprs, benchLexParseTwoStage)
}
