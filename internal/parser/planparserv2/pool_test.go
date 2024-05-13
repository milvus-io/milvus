package planparserv2

import (
	"testing"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/stretchr/testify/assert"

	antlrparser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
)

func genNaiveInputStream() *antlr.InputStream {
	return antlr.NewInputStream("a > 2")
}

func Test_getLexer(t *testing.T) {
	var lexer *antlrparser.PlanLexer
	resetLexerPool()
	lexer = getLexer(genNaiveInputStream(), &errorListener{})
	assert.NotNil(t, lexer)

	lexer = getLexer(genNaiveInputStream(), &errorListener{})
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
	lexer = getLexer(genNaiveInputStream(), &errorListener{})
	assert.NotNil(t, lexer)

	parser = getParser(lexer, &errorListener{})
	assert.NotNil(t, parser)

	parser = getParser(lexer, &errorListener{})
	assert.NotNil(t, parser)

	pool := getParserPool()
	assert.Equal(t, pool.GetNumActive(), 2)
	assert.Equal(t, pool.GetNumIdle(), 0)

	putParser(parser)
	assert.Equal(t, pool.GetNumActive(), 1)
	assert.Equal(t, pool.GetNumIdle(), 1)
}
