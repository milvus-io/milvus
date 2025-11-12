package typeutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type kvPairsHelper[K comparable, V any] struct {
	kvPairs map[K]V
}

func (h *kvPairsHelper[K, V]) Get(k K) (V, error) {
	v, ok := h.kvPairs[k]
	if !ok {
		return v, merr.WrapErrParameterInvalidMsg("%v not found", k)
	}
	return v, nil
}

func (h *kvPairsHelper[K, V]) GetAll() map[K]V {
	return h.kvPairs
}

func NewKvPairs(pairs []*commonpb.KeyValuePair) *kvPairsHelper[string, string] {
	helper := &kvPairsHelper[string, string]{
		kvPairs: make(map[string]string),
	}

	for _, pair := range pairs {
		helper.kvPairs[pair.GetKey()] = pair.GetValue()
	}

	return helper
}
