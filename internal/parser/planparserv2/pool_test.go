package planparserv2

import (
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
