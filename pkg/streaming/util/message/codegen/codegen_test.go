package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodegen(t *testing.T) {
	_, err := codegen()
	assert.NoError(t, err)
}
