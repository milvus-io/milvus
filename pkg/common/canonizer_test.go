package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCanonizer(t *testing.T) {
	type foo struct {
		a int
		b int
	}

	canonizer := NewCanonizer[foo, *foo](10, 10*time.Minute, func(o foo) *foo { return &o })

	foo1 := foo{1, 2}
	fooc, ok := canonizer.Canonize(foo1)
	assert.Equal(t, false, ok)
	assert.Equal(t, foo1, *fooc)
	foo2 := foo{1, 2}
	fooc, ok = canonizer.Canonize(foo2)
	assert.Equal(t, true, ok)
	assert.Equal(t, foo1, *fooc)
}

func BenchmarkCanonizer(b *testing.B) {
	type foo struct {
		payload string
	}

	n := func() *foo {
		return &foo{payload: "foobar"}
	}

	canonizer := NewCanonizer[string, *foo](10, 10*time.Minute, func(s string) *foo { return &foo{payload: s} })
	nc := func() *foo {
		canonized, _ := canonizer.Canonize("foobar")
		return canonized
	}

	// payload := strings.Repeat("#", 500)

	b.Run("normal", func(b *testing.B) {
		foos := make([]*foo, b.N)
		for i := 0; i < b.N; i++ {
			foos[i] = n()
		}
	})

	b.Run("canonized", func(b *testing.B) {
		foos := make([]*foo, b.N)
		for i := 0; i < b.N; i++ {
			foos[i] = nc()
		}
	})

	assert.Equal(b, 1, canonizer.cache.Len())
}
