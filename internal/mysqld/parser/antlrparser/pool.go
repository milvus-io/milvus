package antlrparser

import (
	"sync"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	parsergen "github.com/milvus-io/milvus/internal/mysqld/parser/antlrparser/parser"
)

var (
	lexerPool = sync.Pool{
		New: func() interface{} {
			return parsergen.NewMySqlLexer(nil)
		},
	}
	parserPool = sync.Pool{
		New: func() interface{} {
			return parsergen.NewMySqlParser(nil)
		},
	}
)

func getLexer(stream *antlr.InputStream, listeners ...antlr.ErrorListener) *parsergen.MySqlLexer {
	lexer, ok := lexerPool.Get().(*parsergen.MySqlLexer)
	if !ok {
		lexer = parsergen.NewMySqlLexer(nil)
	}
	for _, listener := range listeners {
		lexer.AddErrorListener(listener)
	}
	lexer.SetInputStream(stream)
	return lexer
}

func getParser(lexer *parsergen.MySqlLexer, listeners ...antlr.ErrorListener) *parsergen.MySqlParser {
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser, ok := parserPool.Get().(*parsergen.MySqlParser)
	if !ok {
		parser = parsergen.NewMySqlParser(nil)
	}
	for _, listener := range listeners {
		parser.AddErrorListener(listener)
	}
	parser.BuildParseTrees = true
	parser.SetInputStream(tokenStream)
	return parser
}

func putLexer(lexer *parsergen.MySqlLexer) {
	lexer.SetInputStream(nil)
	lexerPool.Put(lexer)
}

func putParser(parser *parsergen.MySqlParser) {
	parser.SetInputStream(nil)
	parserPool.Put(parser)
}
