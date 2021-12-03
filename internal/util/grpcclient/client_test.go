package grpcclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientBase_SetRole(t *testing.T) {
	base := ClientBase{}
	expect := "abc"
	base.SetRole("abc")
	assert.Equal(t, expect, base.GetRole())
}

func TestClientBase_GetRole(t *testing.T) {
	base := ClientBase{}
	assert.Equal(t, "", base.GetRole())
}
