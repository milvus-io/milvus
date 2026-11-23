package planparserv2

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"weak"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	antlrparser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
)

func genNaiveInputStream() *antlr.InputStream {
	return antlr.NewInputStream("a > 2")
}

func Test_getLexer(t *testing.T) {
	var lexer *antlrparser.PlanLexer
	resetLexerPool()

	lexer = getLexer(genNaiveInputStream(), &errorListenerImpl{})
	assert.NotNil(t, lexer)

	lexer2 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	assert.NotNil(t, lexer2)

	// Return lexers to the pool
	putLexer(lexer)
	putLexer(lexer2)

	// Get from pool again - should reuse
	lexer3 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	assert.NotNil(t, lexer3)
	putLexer(lexer3)
}

func Test_getParser(t *testing.T) {
	var lexer *antlrparser.PlanLexer
	var parser *antlrparser.PlanParser

	resetParserPool()
	resetLexerPool()

	lexer = getLexer(genNaiveInputStream(), &errorListenerImpl{})
	assert.NotNil(t, lexer)

	parser = getParser(lexer, &errorListenerImpl{})
	assert.NotNil(t, parser)

	parser2 := getParser(lexer, &errorListenerImpl{})
	assert.NotNil(t, parser2)

	// Return parsers to the pool
	putParser(parser)
	putParser(parser2)

	// Get from pool again - should reuse
	parser3 := getParser(lexer, &errorListenerImpl{})
	assert.NotNil(t, parser3)
	putParser(parser3)
	putLexer(lexer)
}

func Test_poolConcurrency(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	// Test concurrent access
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			lexer := getLexer(genNaiveInputStream(), &errorListenerImpl{})
			parser := getParser(lexer, &errorListenerImpl{})
			_ = parser.Expr()
			putParser(parser)
			putLexer(lexer)
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// Test_lexerPoolReuse verifies that lexers are properly reused from pool
// This ensures the pool optimization actually works to reduce allocations
func Test_lexerPoolReuse(t *testing.T) {
	resetLexerPool()

	// Get a lexer and put it back
	lexer1 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	assert.NotNil(t, lexer1)
	putLexer(lexer1)

	// Get another lexer - it should be the same instance from pool
	lexer2 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	assert.NotNil(t, lexer2)

	// The lexer should work correctly after being reused
	tokens := antlr.NewCommonTokenStream(lexer2, antlr.TokenDefaultChannel)
	tokens.Fill()
	// Verify tokens are available by checking the token stream size
	assert.Greater(t, tokens.Size(), 0)

	putLexer(lexer2)
}

// Test_parserPoolReuse verifies that parsers are properly reused from pool
// This ensures the pool optimization actually works to reduce allocations
func Test_parserPoolReuse(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	// Get a parser and put it back
	lexer1 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	parser1 := getParser(lexer1, &errorListenerImpl{})
	assert.NotNil(t, parser1)
	putParser(parser1)
	putLexer(lexer1)

	// Get another parser - it should work correctly after being reused
	lexer2 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	parser2 := getParser(lexer2, &errorListenerImpl{})
	assert.NotNil(t, parser2)

	// The parser should correctly parse expressions after reuse
	expr := parser2.Expr()
	assert.NotNil(t, expr)

	putParser(parser2)
	putLexer(lexer2)
}

// Test_poolWithMultipleErrorListeners tests that error listeners are properly
// managed when getting/putting lexers and parsers
func Test_poolWithMultipleErrorListeners(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	// Create multiple error listeners
	listener1 := &errorListenerImpl{}
	listener2 := &errorListenerImpl{}

	// Get lexer with multiple listeners
	lexer := getLexer(genNaiveInputStream(), listener1, listener2)
	assert.NotNil(t, lexer)

	// Get parser with multiple listeners
	parser := getParser(lexer, listener1, listener2)
	assert.NotNil(t, parser)

	// Return to pool - listeners should be removed
	putParser(parser)
	putLexer(lexer)

	// Get again with different listeners - old listeners should not persist
	newListener := &errorListenerImpl{}
	lexer2 := getLexer(genNaiveInputStream(), newListener)
	parser2 := getParser(lexer2, newListener)

	// Should still work correctly
	expr := parser2.Expr()
	assert.NotNil(t, expr)

	putParser(parser2)
	putLexer(lexer2)
}

// Test_poolWithVariousExpressions tests pool with different expression types
// This ensures pooled lexers/parsers work correctly across various input patterns
func Test_poolWithVariousExpressions(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	expressions := []string{
		"a > 2",
		"b < 10 && c > 5",
		"name == 'test'",
		"x + y > z",
		"arr[0] == 1",
		"json_field['key'] > 100",
		"a in [1, 2, 3]",
		"1 < x < 10",
		"not (a > b)",
	}

	for _, expr := range expressions {
		stream := antlr.NewInputStream(expr)
		lexer := getLexer(stream, &errorListenerImpl{})
		parser := getParser(lexer, &errorListenerImpl{})

		result := parser.Expr()
		assert.NotNil(t, result, "Expression '%s' should parse successfully", expr)

		putParser(parser)
		putLexer(lexer)
	}
}

// Test_poolHighConcurrency tests the pool under high concurrent load
// This ensures thread safety of the pool implementation
func Test_poolHighConcurrency(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	const numGoroutines = 100
	const numIterations = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				stream := antlr.NewInputStream("field > " + string(rune('0'+j)))
				lexer := getLexer(stream, &errorListenerImpl{})
				parser := getParser(lexer, &errorListenerImpl{})

				_ = parser.Expr()

				putParser(parser)
				putLexer(lexer)
			}
		}(i)
	}

	wg.Wait()
}

// Test_resetLexerPool verifies that resetLexerPool creates a fresh pool
func Test_resetLexerPool(t *testing.T) {
	// Get a lexer from the current pool
	lexer1 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	putLexer(lexer1)

	// Reset the pool
	resetLexerPool()

	// Get a new lexer - should be a fresh one from the new pool
	lexer2 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	assert.NotNil(t, lexer2)
	putLexer(lexer2)
}

// Test_resetParserPool verifies that resetParserPool creates a fresh pool
func Test_resetParserPool(t *testing.T) {
	resetLexerPool()

	// Get a parser from the current pool
	lexer1 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	parser1 := getParser(lexer1, &errorListenerImpl{})
	putParser(parser1)
	putLexer(lexer1)

	// Reset the pool
	resetParserPool()

	// Get a new parser - should be a fresh one from the new pool
	lexer2 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	parser2 := getParser(lexer2, &errorListenerImpl{})
	assert.NotNil(t, parser2)
	putParser(parser2)
	putLexer(lexer2)
}

// Test_getParserResetsPredictionMode verifies the pool always hands out a parser
// in the default LL prediction mode, even after a previous borrower left it stuck
// in SLL (as parseExpr's stage-1 fast path does). Without the reset in getParser,
// a reused parser would run SLL-only and could reject inputs that full LL accepts.
func Test_getParserResetsPredictionMode(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	// Borrow a parser, force it into SLL (mimicking parseExpr's stage-1 fast path
	// returning without restoring LL), and return it to the pool.
	lexer1 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	parser1 := getParser(lexer1, &errorListenerImpl{})
	parser1.GetInterpreter().SetPredictionMode(antlr.PredictionModeSLL)
	putParser(parser1)
	putLexer(lexer1)

	// The next borrow must come back in LL mode regardless of how it was left.
	lexer2 := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	parser2 := getParser(lexer2, &errorListenerImpl{})
	assert.Equal(t, antlr.PredictionModeLL, parser2.GetInterpreter().GetPredictionMode(),
		"pool must hand out a parser in default LL mode, not a leaked SLL state")
	assert.NotNil(t, parser2.Expr())
	putParser(parser2)
	putLexer(lexer2)
}

// Test_poolParserBuildParseTrees verifies that BuildParseTrees is set correctly
// This is important for the parser to generate the parse tree
func Test_poolParserBuildParseTrees(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	lexer := getLexer(genNaiveInputStream(), &errorListenerImpl{})
	parser := getParser(lexer, &errorListenerImpl{})

	// BuildParseTrees should be true after getParser
	assert.True(t, parser.BuildParseTrees)

	putParser(parser)
	putLexer(lexer)
}

func waitWeakCollected[T any](t *testing.T, w weak.Pointer[T], label string) {
	t.Helper()
	for range 10 {
		runtime.GC()
		if w.Value() == nil {
			return
		}
	}
	require.Nil(t, w.Value(), "%s remained reachable after GC", label)
}

// Test_putParserDetachesSimulatorReferences pins a pooled parser across a large
// SLL/LL parse and verifies putParser detaches the input stream, parse tree, and
// recognition errors so nothing from the completed request stays reachable through
// the simulator.
func Test_putParserDetachesSimulatorReferences(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	largeExpr := `userId NOT IN [` + strings.Repeat(`"1000000000000000001",`, 5000) + `"1000000000000000002"]`
	var pinnedParser *antlrparser.PlanParser
	var pinnedLexer *antlrparser.PlanLexer
	var weakStream weak.Pointer[antlr.InputStream]
	var weakAST weak.Pointer[antlrparser.ExprContext]

	func() {
		stream := antlr.NewInputStream(largeExpr)
		weakStream = weak.Make(stream)
		listener := &errorListenerImpl{}
		pinnedLexer = getLexer(stream, listener)
		pinnedParser = getParser(pinnedLexer, listener)

		ast := parseExpr(pinnedParser, listener)
		require.NoError(t, listener.Error())
		if exprCtx, ok := ast.(*antlrparser.ExprContext); ok {
			weakAST = weak.Make(exprCtx)
		}
		putParser(pinnedParser)
		putLexer(pinnedLexer)
	}()

	waitWeakCollected(t, weakStream, "large InputStream after putParser")
	if weakAST != (weak.Pointer[antlrparser.ExprContext]{}) {
		waitWeakCollected(t, weakAST, "large AST after putParser")
	}
	runtime.KeepAlive(pinnedParser)
	runtime.KeepAlive(pinnedLexer)
}

// Test_putParserDetachesSyntaxErrorReferences verifies that returning a parser
// after a syntax error (including Stage-2 LL fallback) detaches the exception's
// offending token, input stream, and rule context.
func Test_putParserDetachesSyntaxErrorReferences(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	largeInvalidExpr := `userId NOT IN [` + strings.Repeat(`"1000000000000000001",`, 2000) + `== ]`
	var pinnedParser *antlrparser.PlanParser
	var pinnedLexer *antlrparser.PlanLexer
	var weakStream weak.Pointer[antlr.InputStream]

	func() {
		stream := antlr.NewInputStream(largeInvalidExpr)
		weakStream = weak.Make(stream)
		listener := &errorListenerImpl{}
		pinnedLexer = getLexer(stream, listener)
		pinnedParser = getParser(pinnedLexer, listener)

		_ = parseExpr(pinnedParser, listener)
		require.Error(t, listener.Error())

		putParser(pinnedParser)
		putLexer(pinnedLexer)
	}()

	waitWeakCollected(t, weakStream, "invalid InputStream after putParser")
	require.Nil(t, pinnedParser.GetError())
	require.Nil(t, pinnedLexer.GetError())
	runtime.KeepAlive(pinnedParser)
	runtime.KeepAlive(pinnedLexer)
}

// Test_cachedASTDoesNotRetainSubsequentLargeParse reproduces the cross-root interaction
// between exprCache (#53754) and parser pooling (#55605): a cached small AST keeps its
// creating PlanParser alive via ExprContext.parser. Reusing that PlanParser for an
// uncached large expression must not keep the large expression alive through the cached AST.
func Test_cachedASTDoesNotRetainSubsequentLargeParse(t *testing.T) {
	resetLexerPool()
	resetParserPool()
	configureExprCacheWithBytes(t, "true", "16", "1048576", "4096")

	smallAST, err := handleInternal("id == 1")
	require.NoError(t, err)
	require.True(t, exprCache().cache.Contains("id == 1"))

	// Reuse the exact parser referenced by the cached small AST for an oversized
	// expression that is too large to enter exprCache.
	reusedParser, ok := smallAST.GetParser().(*antlrparser.PlanParser)
	require.True(t, ok)

	largeExpr := `userId NOT IN [` + strings.Repeat(`"1000000000000000001",`, 5000) + `"1000000000000000002"]`
	var weakLargeStream weak.Pointer[antlr.InputStream]
	func() {
		stream := antlr.NewInputStream(largeExpr)
		weakLargeStream = weak.Make(stream)
		listener := &errorListenerImpl{}
		lexer := getLexer(stream, listener)
		defer putLexer(lexer)

		reusedParser.RemoveErrorListeners()
		reusedParser.AddErrorListener(listener)
		reusedParser.BuildParseTrees = true
		reusedParser.SetInputStream(antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel))
		reusedParser.GetInterpreter().SetPredictionMode(antlr.PredictionModeLL)

		_ = parseExpr(reusedParser, listener)
		require.NoError(t, listener.Error())
		putParser(reusedParser)
	}()

	waitWeakCollected(t, weakLargeStream, "large InputStream must not be pinned by cached small AST")

	// The cached small AST must remain valid and reusable.
	cachedAgain, err := handleInternal("id == 1")
	require.NoError(t, err)
	require.Same(t, smallAST, cachedAgain)
}
