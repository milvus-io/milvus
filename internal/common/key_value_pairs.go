package common

import "github.com/milvus-io/milvus/internal/proto/commonpb"

type KeyValuePairs []*commonpb.KeyValuePair

func (pairs KeyValuePairs) Clone() KeyValuePairs {
	clone := make(KeyValuePairs, 0, len(pairs))
	for _, pair := range pairs {
		clone = append(clone, &commonpb.KeyValuePair{
			Key:   pair.GetKey(),
			Value: pair.GetValue(),
		})
	}
	return clone
}

func CloneKeyValuePairs(pairs KeyValuePairs) KeyValuePairs {
	return pairs.Clone()
}
