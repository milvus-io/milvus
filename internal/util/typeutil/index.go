package typeutil

import "github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

func CompareIndexParams(indexParam1 []*commonpb.KeyValuePair, indexParam2 []*commonpb.KeyValuePair) bool {
	if indexParam1 == nil && indexParam2 == nil {
		return true
	}
	if indexParam1 == nil || indexParam2 == nil {
		return false
	}
	if len(indexParam1) != len(indexParam2) {
		return false
	}
	paramMap1 := make(map[string]string)
	paramMap2 := make(map[string]string)

	for _, kv := range indexParam1 {
		paramMap1[kv.Key] = kv.Value
	}
	for _, kv := range indexParam2 {
		paramMap2[kv.Key] = kv.Value
	}

	for k, v := range paramMap1 {
		if _, ok := paramMap2[k]; !ok {
			return false
		}
		if v != paramMap2[k] {
			return false
		}
	}

	return true
}
