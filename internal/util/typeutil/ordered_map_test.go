package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderedMap(t *testing.T) {
	o := NewOrderedMap[string, int]()
	// number
	o.Set("number", 3)
	v, _ := o.Get("number")
	assert.Equal(t, 3, v)

	// overriding existing key
	o.Set("number", 4)
	v, _ = o.Get("number")
	assert.Equal(t, 4, v)

	o.Set("number2", 2)
	o.Set("number3", 3)
	o.Set("number4", 4)
	// Keys method
	keys := o.Keys()
	expectedKeys := []string{
		"number",
		"number2",
		"number3",
		"number4",
	}

	for i, key := range keys {
		assert.Equal(t, expectedKeys[i], key, "Keys method %s != %s", key, expectedKeys[i])
	}
	for i, key := range expectedKeys {
		assert.Equal(t, key, expectedKeys[i], "Keys method %s != %s", key, expectedKeys[i])
	}
	// delete
	o.Delete("number2")
	o.Delete("not a key being used")
	assert.Equal(t, 3, len(o.Keys()))

	_, ok := o.Get("number2")
	assert.False(t, ok, "Delete did not remove 'number2' key")
}
