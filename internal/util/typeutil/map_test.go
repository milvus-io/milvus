package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeMap(t *testing.T) {
	src := make(map[string]string)
	src["Alice"] = "female"
	src["Bob"] = "male"
	dst := make(map[string]string)
	dst = MergeMap(src, dst)
	assert.EqualValues(t, dst, src)

	src = nil
	dst = nil
	dst = MergeMap(src, dst)
	assert.Nil(t, dst)
}

func TestGetMapKeys(t *testing.T) {
	src := make(map[string]string)
	src["Alice"] = "female"
	src["Bob"] = "male"
	keys := GetMapKeys(src)
	assert.EqualValues(t, keys, []string{"Alice", "Bob"})
}
