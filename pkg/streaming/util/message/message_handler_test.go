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

	ch = make(chan ImmutableMessage, 100)
	hNop := NopCloseHandler{
		Handler: ChanMessageHandler(ch),
	}
	hNop.Handle(nil)
	assert.Nil(t, <-ch)
	hNop.Close()
	select {
	case <-ch:
		panic("should not be closed")
	default:
	}
}
