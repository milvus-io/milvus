package message

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageHandler(t *testing.T) {
	ch := make(chan ImmutableMessage, 1)
	h := ChanMessageHandler(ch)
	ok, err := h.Handle(context.Background(), nil)
	assert.NoError(t, err)
	assert.True(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, err = h.Handle(ctx, nil)
	assert.ErrorIs(t, err, ctx.Err())
	assert.False(t, ok)

	assert.Nil(t, <-ch)
	h.Close()
	_, ok = <-ch
	assert.False(t, ok)
}
