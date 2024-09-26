package netutil

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListener(t *testing.T) {
	l, err := NewListener(
		OptIP("127.0.0.1"),
		OptPort(0),
	)
	assert.NoError(t, err)
	assert.NotNil(t, l)
	assert.NotZero(t, l.Port())
	assert.Equal(t, l.Address(), fmt.Sprintf("127.0.0.1:%d", l.Port()))

	l2, err := NewListener(
		OptIP("127.0.0.1"),
		OptPort(l.Port()),
	)
	assert.Error(t, err)
	assert.Nil(t, l2)

	l3, err := NewListener(
		OptIP("127.0.0.1"),
		OptHighPriorityToUsePort(l.Port()),
	)
	assert.NoError(t, err)
	assert.NotNil(t, l3)
	assert.NotZero(t, l3.Port())
	assert.Equal(t, l3.Address(), fmt.Sprintf("127.0.0.1:%d", l3.Port()))

	l3.Close()
	l.Close()
}
