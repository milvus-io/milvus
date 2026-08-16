package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsFieldNameKeywordRejectsNullCaseInsensitive(t *testing.T) {
	for _, name := range []string{"null", "Null", "NULL", "nUlL", "NuLL"} {
		assert.True(t, IsFieldNameKeyword(name), name)
	}
}
