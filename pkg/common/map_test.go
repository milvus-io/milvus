package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapEqual(t *testing.T) {
	t.Run("int64 to int64", func(t *testing.T) {
		m1 := map[int64]int64{1: 11, 2: 22, 3: 33}
		m2 := map[int64]int64{1: 11, 2: 22, 3: 33}
		assert.True(t, MapEqual(m1, m2))

		m3 := map[int64]int64{1: 11, 2: 23, 3: 33}
		assert.False(t, MapEqual(m1, m3))

		m4 := map[int64]int64{1: 11, 2: 22}
		assert.False(t, MapEqual(m1, m4))

		assert.False(t, MapEqual(m1, nil))
		assert.True(t, MapEqual(map[int64]int64(nil), map[int64]int64(nil)))
	})

	t.Run("string to string", func(t *testing.T) {
		m1 := map[string]string{"a": "1", "b": "2"}
		m2 := map[string]string{"a": "1", "b": "2"}
		assert.True(t, MapEqual(m1, m2))

		m3 := map[string]string{"a": "1", "b": "3"}
		assert.False(t, MapEqual(m1, m3))
	})

	t.Run("string to int", func(t *testing.T) {
		m1 := map[string]int{"a": 1, "b": 2}
		m2 := map[string]int{"a": 1, "b": 2}
		assert.True(t, MapEqual(m1, m2))

		m3 := map[string]int{"a": 1, "b": 3}
		assert.False(t, MapEqual(m1, m3))
	})
}

func TestCloneMap(t *testing.T) {
	t.Run("nil map", func(t *testing.T) {
		var m map[string]string = nil
		cloned := CloneMap(m)
		assert.Nil(t, cloned)
	})

	t.Run("empty map", func(t *testing.T) {
		m := map[string]string{}
		cloned := CloneMap(m)
		assert.NotNil(t, cloned)
		assert.Equal(t, 0, len(cloned))
		assert.True(t, MapEqual(m, cloned))
	})

	t.Run("string to string", func(t *testing.T) {
		m := map[string]string{"k1": "v1", "k2": "v2"}
		cloned := CloneMap(m)
		assert.True(t, MapEqual(m, cloned))
		// Ensure it's a deep copy
		cloned["k3"] = "v3"
		assert.NotEqual(t, len(m), len(cloned))
	})

	t.Run("int64 to int64", func(t *testing.T) {
		m := map[int64]int64{1: 11, 2: 22, 3: 33}
		cloned := CloneMap(m)
		assert.True(t, MapEqual(m, cloned))
		// Ensure it's a deep copy
		cloned[4] = 44
		assert.NotEqual(t, len(m), len(cloned))
	})

	t.Run("string to uint64", func(t *testing.T) {
		m := map[string]uint64{"ch1": 100, "ch2": 200}
		cloned := CloneMap(m)
		assert.True(t, MapEqual(m, cloned))
	})
}
