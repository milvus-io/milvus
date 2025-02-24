package planparserv2

import (
	"context"

	"github.com/antlr4-go/antlr/v4"
	pool "github.com/jolestar/go-commons-pool/v2"

	antlrparser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

var (
	config = &pool.ObjectPoolConfig{
		LIFO:                     pool.DefaultLIFO,
		MaxTotal:                 hardware.GetCPUNum() * 8,
		MaxIdle:                  hardware.GetCPUNum() * 8,
		MinIdle:                  pool.DefaultMinIdle,
		MinEvictableIdleTime:     pool.DefaultMinEvictableIdleTime,
		SoftMinEvictableIdleTime: pool.DefaultSoftMinEvictableIdleTime,
		NumTestsPerEvictionRun:   pool.DefaultNumTestsPerEvictionRun,
		EvictionPolicyName:       pool.DefaultEvictionPolicyName,
		EvictionContext:          context.Background(),
		TestOnCreate:             pool.DefaultTestOnCreate,
		TestOnBorrow:             pool.DefaultTestOnBorrow,
		TestOnReturn:             pool.DefaultTestOnReturn,
		TestWhileIdle:            pool.DefaultTestWhileIdle,
		TimeBetweenEvictionRuns:  pool.DefaultTimeBetweenEvictionRuns,
		BlockWhenExhausted:       false,
	}
	ctx              = context.Background()
	lexerPoolFactory = pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return antlrparser.NewPlanLexer(nil), nil
		})
	lexerPool = pool.NewObjectPool(ctx, lexerPoolFactory, config)

	parserPoolFactory = pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return antlrparser.NewPlanParser(nil), nil
		})
	parserPool = pool.NewObjectPool(ctx, parserPoolFactory, config)
)

func getLexer(stream *antlr.InputStream, listeners ...antlr.ErrorListener) *antlrparser.PlanLexer {
	cached, _ := lexerPool.BorrowObject(context.Background())
	lexer, ok := cached.(*antlrparser.PlanLexer)
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
	cached, _ := parserPool.BorrowObject(context.Background())
	parser, ok := cached.(*antlrparser.PlanParser)
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
	lexer.RemoveErrorListeners()
	lexerPool.ReturnObject(context.TODO(), lexer)
}

func putParser(parser *antlrparser.PlanParser) {
	parser.SetInputStream(nil)
	parser.RemoveErrorListeners()
	parserPool.ReturnObject(context.TODO(), parser)
}

func getLexerPool() *pool.ObjectPool {
	return lexerPool
}

// only for test
func resetLexerPool() {
	ctx = context.Background()
	lexerPoolFactory = pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return antlrparser.NewPlanLexer(nil), nil
		})
	lexerPool = pool.NewObjectPool(ctx, lexerPoolFactory, config)
}

func getParserPool() *pool.ObjectPool {
	return parserPool
}

// only for test
func resetParserPool() {
	ctx = context.Background()
	parserPoolFactory = pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return antlrparser.NewPlanParser(nil), nil
		})
	parserPool = pool.NewObjectPool(ctx, parserPoolFactory, config)
}
