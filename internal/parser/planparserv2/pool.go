package planparserv2

import (
	"sync"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	antlrparser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
)

var (
	lexerPool = sync.Pool{
		New: func() interface{} {
			return antlrparser.NewPlanLexer(nil)
		},
	}
	parserPool = sync.Pool{
		New: func() interface{} {
			return antlrparser.NewPlanParser(nil)
		},
	}
)

func getLexer(stream *antlr.InputStream, listeners ...antlr.ErrorListener) *antlrparser.PlanLexer {
	lexer, ok := lexerPool.Get().(*antlrparser.PlanLexer)
	if !ok {
		lexer = antlrparser.NewPlanLexer(nil)
	}
	lexer.SetInputStream(stream)
	for _, listener := range listeners {
		lexer.AddErrorListener(listener)
	}
	lexerPool.Put(lexer)
	return lexer
}

func getParser(stream *antlr.InputStream, listeners ...antlr.ErrorListener) *antlrparser.PlanParser {
	lexer := getLexer(stream, listeners...)
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser, ok := parserPool.Get().(*antlrparser.PlanParser)
	if !ok {
		parser = antlrparser.NewPlanParser(nil)
	}
	parser.SetInputStream(tokenStream)
	parser.BuildParseTrees = true
	for _, listener := range listeners {
		parser.AddErrorListener(listener)
	}
	parserPool.Put(parser)
	return parser
}
