package logutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestName(t *testing.T) {
	assert.Equal(t, 1.5, ToMB(1024*1024+512*1024))
	assert.Equal(t, int64(2), ToMB(int64(2*1024*1024)))
}
