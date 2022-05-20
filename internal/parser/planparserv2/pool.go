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
	for _, listener := range listeners {
		lexer.AddErrorListener(listener)
	}
	lexer.SetInputStream(stream)
	return lexer
}

func getParser(lexer *antlrparser.PlanLexer, listeners ...antlr.ErrorListener) *antlrparser.PlanParser {
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser, ok := parserPool.Get().(*antlrparser.PlanParser)
	if !ok {
		parser = antlrparser.NewPlanParser(nil)
	}
	for _, listener := range listeners {
		parser.AddErrorListener(listener)
	}
	parser.BuildParseTrees = true
	parser.SetInputStream(tokenStream)
	return parser
}

func putLexer(lexer *antlrparser.PlanLexer) {
	lexer.SetInputStream(nil)
	lexerPool.Put(lexer)
}

func putParser(parser *antlrparser.PlanParser) {
	parser.SetInputStream(nil)
	parserPool.Put(parser)
}
