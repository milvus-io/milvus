package planparserv2

import (
	"sync"

	"github.com/antlr4-go/antlr/v4"

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
	lexer := lexerPool.Get().(*antlrparser.PlanLexer)
	for _, listener := range listeners {
		lexer.AddErrorListener(listener)
	}
	lexer.SetInputStream(stream)
	return lexer
}

func getParser(lexer *antlrparser.PlanLexer, listeners ...antlr.ErrorListener) *antlrparser.PlanParser {
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := parserPool.Get().(*antlrparser.PlanParser)
	for _, listener := range listeners {
		parser.AddErrorListener(listener)
	}
	parser.BuildParseTrees = true
	parser.SetInputStream(tokenStream)
	return parser
}

func putLexer(lexer *antlrparser.PlanLexer) {
	lexer.SetInputStream(nil)
	lexer.RemoveErrorListeners()
	lexerPool.Put(lexer)
}

func putParser(parser *antlrparser.PlanParser) {
	parser.SetInputStream(nil)
	parser.RemoveErrorListeners()
	parserPool.Put(parser)
}

// resetLexerPool resets the lexer pool (for testing)
func resetLexerPool() {
	lexerPool = sync.Pool{
		New: func() interface{} {
			return antlrparser.NewPlanLexer(nil)
		},
	}
}

// resetParserPool resets the parser pool (for testing)
func resetParserPool() {
	parserPool = sync.Pool{
		New: func() interface{} {
			return antlrparser.NewPlanParser(nil)
		},
	}
}
