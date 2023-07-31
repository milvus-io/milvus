package concurrency

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolOption(t *testing.T) {
	opt := &poolOption{}

	o := WithPreAlloc(true)
	o(opt)
	assert.True(t, opt.preAlloc)

	o = WithNonBlocking(true)
	o(opt)
	assert.True(t, opt.nonBlocking)

	o = WithDisablePurge(true)
	o(opt)
	assert.True(t, opt.disablePurge)

	o = WithExpiryDuration(time.Second)
	o(opt)
	assert.Equal(t, time.Second, opt.expiryDuration)

	o = WithConcealPanic(true)
	o(opt)
	assert.True(t, opt.concealPanic)
}
