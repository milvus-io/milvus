package fileresource

import "github.com/milvus-io/milvus/internal/json"

// analyzerResourceNames extracts the remote resource names from analyzer configs.
// All remote file options use the same object shape, including nested tokenizer,
// filter and multi-analyzer configs. Walk objects/arrays without reproducing the
// analyzer grammar or interpreting local paths and built-in dictionary names.
func analyzerResourceNames(params []string) map[string]struct{} {
	names := make(map[string]struct{})
	var visit func(any)
	visit = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			if kind, _ := value["type"].(string); kind == "remote" {
				if name, ok := value["resource_name"].(string); ok {
					names[name] = struct{}{}
				}
			}
			for _, child := range value {
				visit(child)
			}
		case []any:
			for _, child := range value {
				visit(child)
			}
		}
	}
	for _, params := range params {
		var value any
		if json.Unmarshal([]byte(params), &value) == nil {
			visit(value)
		}
		// Invalid JSON is rejected by the existing analyzer constructor. It
		// cannot require remote I/O merely to report the same parameter error.
	}
	return names
}
