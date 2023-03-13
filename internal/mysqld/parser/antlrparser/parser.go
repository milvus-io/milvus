package antlrparser

import (
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/milvus-io/milvus/internal/mysqld/parser"
	"github.com/milvus-io/milvus/internal/mysqld/planner"
)

type antlrParser struct{}

func (p antlrParser) Parse(sql string, opts ...parser.Option) (*planner.LogicalPlan, []error, error) {
	el := &ErrorListener{}

	inputStream := antlr.NewInputStream(sql)
	lexer := getLexer(inputStream, el)
	defer putLexer(lexer)

	if el.err != nil {
		return nil, nil, el.err
	}

	parserFromPool := getParser(lexer, el)
	defer putParser(parserFromPool)

	if el.err != nil {
		return nil, nil, el.err
	}

	ast := parserFromPool.Root()
	n, err := NewAstBuilder().Build(ast)
	if err != nil {
		return nil, nil, err
	}

	return &planner.LogicalPlan{
		Node: n,
	}, nil, nil
}

func NewAntlrParser() parser.Parser {
	return &antlrParser{}
}
