package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoles(t *testing.T) {
	r := MilvusRoles{}

	assert.True(t, r.envValue("1"))
	assert.True(t, r.envValue(" 1 "))
	assert.True(t, r.envValue("True"))
	assert.True(t, r.envValue(" True "))
	assert.True(t, r.envValue(" TRue "))
	assert.False(t, r.envValue("0"))
	assert.False(t, r.envValue(" 0 "))
	assert.False(t, r.envValue(" false "))
	assert.False(t, r.envValue(" False "))
	assert.False(t, r.envValue(" abc "))

	ss := strings.SplitN("abcdef", "=", 2)
	assert.Equal(t, len(ss), 1)
	ss = strings.SplitN("adb=def", "=", 2)
	assert.Equal(t, len(ss), 2)
}
