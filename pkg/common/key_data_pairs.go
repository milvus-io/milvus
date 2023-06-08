package common

import (
	"reflect"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type KeyDataPairs []*commonpb.KeyDataPair

func (pairs KeyDataPairs) Clone() KeyDataPairs {
	clone := make(KeyDataPairs, 0, len(pairs))
	for _, pair := range pairs {
		clone = append(clone, &commonpb.KeyDataPair{
			Key:  pair.GetKey(),
			Data: CloneByteSlice(pair.GetData()),
		})
	}
	return clone
}

func (pairs KeyDataPairs) ToMap() map[string][]byte {
	ret := make(map[string][]byte)
	for _, pair := range pairs {
		ret[pair.GetKey()] = CloneByteSlice(pair.GetData())
	}
	return ret
}

func (pairs KeyDataPairs) Equal(other KeyDataPairs) bool {
	return reflect.DeepEqual(pairs.ToMap(), other.ToMap())
}

func CloneKeyDataPairs(pairs KeyDataPairs) KeyDataPairs {
	return pairs.Clone()
}
