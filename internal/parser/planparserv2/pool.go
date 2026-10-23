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
	// Drop ANTLR's default ConsoleErrorListener (see getParser).
	lexer.RemoveErrorListeners()
	for _, listener := range listeners {
		lexer.AddErrorListener(listener)
	}
	lexer.SetInputStream(stream)
	return lexer
}

func getParser(lexer *antlrparser.PlanLexer, listeners ...antlr.ErrorListener) *antlrparser.PlanParser {
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := parserPool.Get().(*antlrparser.PlanParser)
	// Drop ANTLR's default ConsoleErrorListener so a syntax error does not do
	// stderr I/O on the hot failure path; only our own listener stays attached.
	parser.RemoveErrorListeners()
	for _, listener := range listeners {
		parser.AddErrorListener(listener)
	}
	parser.BuildParseTrees = true
	parser.SetInputStream(tokenStream)
	// Hand the parser out in the default LL prediction mode. parseExpr flips it to
	// SLL for stage 1 and only restores LL on the (rare) stage-2 path, so a parser
	// returned to the pool after a clean stage-1 parse is still in SLL mode —
	// antlr-go's SetInputStream/reset does not restore predictionMode. Reset it
	// here so the pool always yields an LL parser and any caller that runs the
	// pooled parser directly (e.g. parser.Expr() in tests/benchmarks) never
	// inherits SLL-only behavior that could reject inputs full LL accepts.
	parser.GetInterpreter().SetPredictionMode(antlr.PredictionModeLL)
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
	// Every context of a parse tree keeps a pointer to the parser that built
	// it, and the parser's ATN simulator keeps the LAST parse it ran — its
	// token stream and its whole tree — because antlr-go's simulator reset is
	// a no-op. So a small cached tree used to pin whatever its parser parsed
	// next, cached or not, and eviction freed nothing that was still pinned:
	// the parse cache retained far more than its entries (#53754). A fresh
	// simulator drops that; the DFA and prediction-context caches it is built
	// from are shared and carry over.
	old := parser.Interpreter
	parser.Interpreter = antlr.NewParserATNSimulator(parser, old.ATN(), old.DecisionToDFA(), old.SharedContextCache())
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
