package mysqld

import (
	"github.com/milvus-io/milvus/internal/mysqld/executor"
	"github.com/milvus-io/milvus/internal/mysqld/optimizer"
	parserimpl "github.com/milvus-io/milvus/internal/mysqld/parser"
	"github.com/milvus-io/milvus/internal/mysqld/parser/antlrparser"
	"github.com/milvus-io/milvus/internal/types"
)

type sqlFactory interface {
	NewParser() parserimpl.Parser
	NewOptimizer() (optimizer.RuleBasedOptimizer, optimizer.CostBasedOptimizer)
	NewCompiler() executor.Compiler
	NewExecutor() executor.Executor
}

type defaultFactory struct {
	s types.ProxyComponent
}

func (f defaultFactory) NewParser() parserimpl.Parser {
	return antlrparser.NewAntlrParser()
}

func (f defaultFactory) NewOptimizer() (optimizer.RuleBasedOptimizer, optimizer.CostBasedOptimizer) {
	return optimizer.NewDefaultRBO(), optimizer.NewDefaultCBO()
}

func (f defaultFactory) NewCompiler() executor.Compiler {
	return executor.NewDefaultCompiler()
}

func (f defaultFactory) NewExecutor() executor.Executor {
	return executor.NewDefaultExecutor(f.s)
}

func newDefaultFactory(s types.ProxyComponent) sqlFactory {
	return &defaultFactory{s: s}
}
