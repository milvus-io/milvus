package common

import (
	"reflect"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type KeyValuePairs []*commonpb.KeyValuePair

func (pairs KeyValuePairs) Clone() KeyValuePairs {
	if pairs == nil {
		return nil
	}
	clone := make(KeyValuePairs, 0, len(pairs))
	for _, pair := range pairs {
		clone = append(clone, &commonpb.KeyValuePair{
			Key:   pair.GetKey(),
			Value: pair.GetValue(),
		})
	}
	return clone
}

func (pairs KeyValuePairs) ToMap() map[string]string {
	ret := make(map[string]string)
	for _, pair := range pairs {
		ret[pair.GetKey()] = pair.GetValue()
	}
	return ret
}

func (pairs KeyValuePairs) Equal(other KeyValuePairs) bool {
	return reflect.DeepEqual(pairs.ToMap(), other.ToMap())
}

func CloneKeyValuePairs(pairs KeyValuePairs) KeyValuePairs {
	return pairs.Clone()
}
