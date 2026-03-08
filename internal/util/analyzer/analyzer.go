package analyzer

import (
	"github.com/milvus-io/milvus/internal/util/analyzer/canalyzer"
	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
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

func BuildExtraResourceInfo(storage string, resources []*internalpb.FileResourceInfo) (string, error) {
	return canalyzer.BuildExtraResourceInfo(storage, resources)
}

func InitOptions() {
	canalyzer.InitOptions()
}
