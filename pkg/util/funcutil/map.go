package funcutil

import (
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func MapReduce(results []map[string]string, method map[string]func(string) error) error {
	// TODO: use generic type to reconstruct map[string]string -> [T any] map[string]T
	for _, result := range results {
		for k, v := range result {
			fn, ok := method[k]
			if !ok {
				return merr.WrapErrServiceInternalMsg("unknown field %s", k)
			}
			if err := fn(v); err != nil {
				return err
			}
		}
	}
	return nil
}
