package common

import "github.com/milvus-io/milvus/internal/proto/commonpb"

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

func CloneKeyDataPairs(pairs KeyDataPairs) KeyDataPairs {
	return pairs.Clone()
}
