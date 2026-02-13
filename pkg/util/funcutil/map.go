package funcutil

import "fmt"

func MapReduce[T any](results []map[string]T, method map[string]func(T) error) error {
	for _, result := range results {
		for k, v := range result {
			fn, ok := method[k]
			if !ok {
				return fmt.Errorf("unknown field %s", k)
			}
			if err := fn(v); err != nil {
				return err
			}
		}
	}
	return nil
}
