package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/antlr/antlr4/runtime/Go/antlr"

	antlrparser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
)

func genNaiveInputStream() *antlr.InputStream {
	return antlr.NewInputStream("a > 2")
}

func Test_getLexer(t *testing.T) {
	var lexer *antlrparser.PlanLexer

	lexer = getLexer(genNaiveInputStream(), &errorListener{})
	assert.NotNil(t, lexer)

	lexer = getLexer(genNaiveInputStream(), &errorListener{})
	assert.NotNil(t, lexer)
}

func Test_getParser(t *testing.T) {
	var lexer *antlrparser.PlanLexer
	var parser *antlrparser.PlanParser

	lexer = getLexer(genNaiveInputStream(), &errorListener{})
	assert.NotNil(t, lexer)

	parser = getParser(lexer, &errorListener{})
	assert.NotNil(t, parser)

	parser = getParser(lexer, &errorListener{})
	assert.NotNil(t, parser)
}
