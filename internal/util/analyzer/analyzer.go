package analyzer

import (
	"github.com/milvus-io/milvus/internal/util/analyzer/canalyzer"
	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
)

type (
	Analyzer    interfaces.Analyzer
	TokenStream interfaces.TokenStream
)

func NewAnalyzer(param string, extraInfo string) (Analyzer, error) {
	return canalyzer.NewAnalyzer(param, extraInfo)
}

func ValidateAnalyzer(param string, extraInfo string) ([]int64, error) {
	return canalyzer.ValidateAnalyzer(param, extraInfo)
}

func UpdateGlobalResourceInfo(resourceMap map[string]int64) error {
	return canalyzer.UpdateGlobalResourceInfo(resourceMap)
}

func InitOptions() {
	canalyzer.InitOptions()
}
