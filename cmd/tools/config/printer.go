package main

import (
	"fmt"
	"os"
	"sort"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

func ShowYaml(filepath string) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		log.Warn("read config failed", zap.Error(err))
		os.Exit(-3)
	}

	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		log.Warn("parse config failed", zap.Error(err))
		os.Exit(-3)
	}

	flat := make(map[string]string)
	flattenYaml("", raw, flat)

	keys := make([]string, 0, len(flat))
	for k := range flat {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintln(os.Stdout, key, "=", flat[key])
	}
}

func flattenYaml(prefix string, m map[string]interface{}, out map[string]string) {
	for k, v := range m {
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}
		switch val := v.(type) {
		case map[string]interface{}:
			flattenYaml(fullKey, val, out)
		case []interface{}:
			for i, item := range val {
				childKey := fmt.Sprintf("%s.%d", fullKey, i)
				if nested, ok := item.(map[string]interface{}); ok {
					flattenYaml(childKey, nested, out)
				} else {
					out[childKey] = fmt.Sprintf("%v", item)
				}
			}
		default:
			if val == nil {
				out[fullKey] = ""
			} else {
				out[fullKey] = fmt.Sprintf("%v", val)
			}
		}
	}
}
