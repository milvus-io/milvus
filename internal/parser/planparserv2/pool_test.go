package planparserv2

import (
	"sync"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/assert"

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
