package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddOne(t *testing.T) {
	input := ""
	output := AddOne(input)
	assert.Equal(t, output, "")

	input = "a"
	output = AddOne(input)
	assert.Equal(t, output, "b")

	input = "aaa="
	output = AddOne(input)
	assert.Equal(t, output, "aaa>")

	// test the increate case
	binary := []byte{1, 20, 255}
	input = string(binary)
	output = AddOne(input)
	assert.Equal(t, len(output), 4)
	resultb := []byte(output)
	assert.Equal(t, resultb, []byte{1, 20, 255, 0})
}
