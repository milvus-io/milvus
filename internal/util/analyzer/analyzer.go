package analyzer

import (
	"github.com/milvus-io/milvus/internal/util/analyzer/canalyzer"
	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
)

type (
	Analyzer    interfaces.Analyzer
	TokenStream interfaces.TokenStream
)

func NewAnalyzer(param string) (Analyzer, error) {
	return canalyzer.NewAnalyzer(param)
}

func ValidateAnalyzer(param string) error {
	return canalyzer.ValidateAnalyzer(param)
}
