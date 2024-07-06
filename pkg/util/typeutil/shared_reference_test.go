package typeutil

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

type closer struct {
	t      *testing.T
	closed atomic.Bool
}

func (c *closer) Close() {
	assert.True(c.t, c.closed.CompareAndSwap(false, true))
}

func TestSharedReference(t *testing.T) {
	// test downgrade and upgrade
	closer1 := &closer{t: t}
	r := NewSharedReference(closer1)
	w := r.Downgrade()
	assert.Nil(t, w.Upgrade())
	assert.True(t, closer1.closed.Load())

	// test concurrent
	closer1 = &closer{t: t}

	r = NewSharedReference(closer1)

	wg := &sync.WaitGroup{}
	wg.Add(4)
	r1 := r.Clone()
	go func() {
		defer wg.Done()
		defer r1.Close()
		for i := 0; i < 1000; i++ {
			r3 := r1.Clone()
			r3.Close()
		}
	}()
	r2 := r.Clone()
	go func() {
		defer wg.Done()
		defer r2.Close()
		for i := 0; i < 1000; i++ {
			r := r2.Clone()
			r.Close()
		}
	}()
	r3 := r.Clone().Downgrade()
	go func() {
		defer wg.Done()
		r := r3.Upgrade()
		r.Close()
	}()
	r4 := r.Clone().Downgrade()
	go func() {
		defer wg.Done()
		r := r4.Upgrade()
		r.Close()
	}()
	wg.Wait()

	assert.False(t, r.Deref().closed.Load())
	r.Close()
	assert.True(t, closer1.closed.Load())

	assert.Panics(t, func() {
		r.Clone()
	})

	closer1 = &closer{t: t}
	r = NewSharedReference(closer1)
	r.Close()
	assert.Panics(t, func() {
		r.Close()
	})
}
