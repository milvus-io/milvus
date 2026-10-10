package function

import "github.com/milvus-io/milvus/pkg/v3/util/merr"

func NormalizeAnalyzerNames(analyzerNames []string, textNum int) ([]string, error) {
	if textNum == 0 {
		return []string{}, nil
	}

	switch len(analyzerNames) {
	case 0:
		names := make([]string, textNum)
		for i := range names {
			names[i] = "default"
		}
		return names, nil
	case 1:
		name := analyzerNames[0]
		if name == "" {
			name = "default"
		}
		names := make([]string, textNum)
		for i := range names {
			names[i] = name
		}
		return names, nil
	case textNum:
		names := append([]string(nil), analyzerNames...)
		for i, name := range names {
			if name == "" {
				names[i] = "default"
			}
		}
		return names, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("analyzer names size must be 0, 1, or equal to text size, got analyzer names size [%d], text size [%d]", len(analyzerNames), textNum)
	}
}
