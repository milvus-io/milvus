package planparserv2

import (
	"runtime"
	"strings"
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

	lexer = getLexer(genNaiveInputStream(), &errorListenerImpl{})
	assert.NotNil(t, lexer)

	pool := getLexerPool()
	assert.Equal(t, pool.GetNumActive(), 2)
	assert.Equal(t, pool.GetNumIdle(), 0)

	putLexer(lexer)
	assert.Equal(t, pool.GetNumActive(), 1)
	assert.Equal(t, pool.GetNumIdle(), 1)
}

func Test_getParser(t *testing.T) {
	var lexer *antlrparser.PlanLexer
	var parser *antlrparser.PlanParser

	resetParserPool()
	lexer = getLexer(genNaiveInputStream(), &errorListenerImpl{})
	assert.NotNil(t, lexer)

	parser = getParser(lexer, &errorListenerImpl{})
	assert.NotNil(t, parser)

	parser = getParser(lexer, &errorListenerImpl{})
	assert.NotNil(t, parser)

	pool := getParserPool()
	assert.Equal(t, pool.GetNumActive(), 2)
	assert.Equal(t, pool.GetNumIdle(), 0)

	putParser(parser)
	assert.Equal(t, pool.GetNumActive(), 1)
	assert.Equal(t, pool.GetNumIdle(), 1)
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
// parse and verifies putParser detaches the input stream, parse tree, and
// recognition errors so nothing from the completed request stays reachable through
// the simulator.
func Test_putParserDetachesSimulatorReferences(t *testing.T) {
	resetLexerPool()
	resetParserPool()

	largeExpr := `userId NOT IN [` + strings.Repeat(`"1000000000000000001",`, 5000) + `"1000000000000000002"]`
	var pinnedParser *antlrparser.PlanParser
	var pinnedLexer *antlrparser.PlanLexer
	var weakStream weak.Pointer[antlr.InputStream]
	var weakAST weak.Pointer[antlrparser.TermContext]

	func() {
		stream := antlr.NewInputStream(largeExpr)
		weakStream = weak.Make(stream)
		listener := &errorListenerImpl{}
		pinnedLexer = getLexer(stream, listener)
		pinnedParser = getParser(pinnedLexer, listener)

		ast := pinnedParser.Expr()
		require.NoError(t, listener.Error())
		term, ok := ast.(*antlrparser.TermContext)
		require.True(t, ok)
		weakAST = weak.Make(term)
		putParser(pinnedParser)
		putLexer(pinnedLexer)
	}()

	waitWeakCollected(t, weakStream, "large InputStream after putParser")
	waitWeakCollected(t, weakAST, "large AST after putParser")
	runtime.KeepAlive(pinnedParser)
	runtime.KeepAlive(pinnedLexer)
}

// Test_putParserDetachesSyntaxErrorReferences verifies that returning a parser
// after a syntax error detaches the exception's
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

		_ = pinnedParser.Expr()
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

		parser := getParser(lexer, listener)
		defer putParser(parser)
		require.Same(t, reusedParser, parser)
		_ = parser.Expr()
		require.NoError(t, listener.Error())
	}()

	waitWeakCollected(t, weakLargeStream, "large InputStream must not be pinned by cached small AST")

	// The cached small AST must remain valid and reusable.
	cachedAgain, err := handleInternal("id == 1")
	require.NoError(t, err)
	require.Same(t, smallAST, cachedAgain)
}

func TestHandleInternalReturnsBorrowedObjectsOnError(t *testing.T) {
	resetLexerPool()
	resetParserPool()
	configureExprCache(t, "false", "16")
	for _, expr := range []string{"id ==", "id id"} {
		_, err := handleInternal(expr)
		require.Error(t, err)
		require.Zero(t, getLexerPool().GetNumActive())
		require.Zero(t, getParserPool().GetNumActive())
	}
}
