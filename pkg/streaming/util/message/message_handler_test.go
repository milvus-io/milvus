package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageHandler(t *testing.T) {
	ch := make(chan ImmutableMessage, 100)
	h := ChanMessageHandler(ch)
	h.Handle(nil)
	assert.Nil(t, <-ch)
	h.Close()
	_, ok := <-ch
	assert.False(t, ok)
}
