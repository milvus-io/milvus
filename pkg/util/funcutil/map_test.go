package funcutil

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapReduce(t *testing.T) {
	t.Run("map[string]string", func(t *testing.T) {
		results := []map[string]string{
			{"a": "1", "b": "2"},
			{"a": "3", "b": "4"},
		}

		sum := 0
		method := map[string]func(string) error{
			"a": func(s string) error {
				val, err := strconv.Atoi(s)
				if err != nil {
					return err
				}
				sum += val
				return nil
			},
			"b": func(s string) error {
				val, err := strconv.Atoi(s)
				if err != nil {
					return err
				}
				sum += val
				return nil
			},
		}

		err := MapReduce(results, method)
		assert.NoError(t, err)
		assert.Equal(t, 10, sum) // 1+2+3+4 = 10
	})

	t.Run("map[string]int", func(t *testing.T) {
		results := []map[string]int{
			{"a": 1, "b": 2},
			{"a": 3, "b": 4},
		}

		sum := 0
		method := map[string]func(int) error{
			"a": func(i int) error {
				sum += i
				return nil
			},
			"b": func(i int) error {
				sum += i
				return nil
			},
		}

		err := MapReduce(results, method)
		assert.NoError(t, err)
		assert.Equal(t, 10, sum)
	})

	t.Run("unknown field", func(t *testing.T) {
		results := []map[string]string{
			{"c": "1"},
		}
		method := map[string]func(string) error{
			"a": func(s string) error { return nil },
		}
		err := MapReduce(results, method)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown field c")
	})

	t.Run("method error", func(t *testing.T) {
		results := []map[string]string{
			{"a": "invalid"},
		}
		method := map[string]func(string) error{
			"a": func(s string) error {
				_, err := strconv.Atoi(s)
				return err
			},
		}
		err := MapReduce(results, method)
		assert.Error(t, err)
	})
}
