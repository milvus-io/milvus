package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultipartQueue(t *testing.T) {
	q := NewMultipartQueue[int]()
	for i := 0; i < 100; i++ {
		q.AddOne(i)
		assert.Equal(t, i+1, q.Len())
	}
	for i := 100; i > 0; i-- {
		assert.NotNil(t, q.Next())
		q.UnsafeAdvance()
		assert.Equal(t, i-1, q.Len())
	}
}
