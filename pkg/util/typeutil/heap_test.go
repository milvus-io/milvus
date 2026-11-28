package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinimumHeap(t *testing.T) {
	h := []int{4, 5, 2}
	heap := NewArrayBasedMinimumHeap(h)
	assert.Equal(t, 2, heap.Peek())
	assert.Equal(t, 3, heap.Len())
	heap.Push(3)
	assert.Equal(t, 2, heap.Peek())
	assert.Equal(t, 4, heap.Len())
	heap.Push(1)
	assert.Equal(t, 1, heap.Peek())
	assert.Equal(t, 5, heap.Len())
	for i := 1; i <= 5; i++ {
		assert.Equal(t, i, heap.Peek())
		assert.Equal(t, i, heap.Pop())
	}
}

func TestMaximumHeap(t *testing.T) {
	h := []int{4, 1, 2}
	heap := NewArrayBasedMaximumHeap(h)
	assert.Equal(t, 4, heap.Peek())
	assert.Equal(t, 3, heap.Len())
	heap.Push(3)
	assert.Equal(t, 4, heap.Peek())
	assert.Equal(t, 4, heap.Len())
	heap.Push(5)
	assert.Equal(t, 5, heap.Peek())
	assert.Equal(t, 5, heap.Len())
	for i := 5; i >= 1; i-- {
		assert.Equal(t, i, heap.Peek())
		assert.Equal(t, i, heap.Pop())
	}
}

type FooHeapObject struct {
	value int
}

func GetFooHeapObjectOrderFunc(obj *FooHeapObject) int {
	return obj.value
}

func TestMinimumObjectHeap(t *testing.T) {
	h := []*FooHeapObject{
		{value: 4},
		{value: 5},
		{value: 2},
	}
	heap := NewObjectArrayBasedMinimumHeap(h, GetFooHeapObjectOrderFunc)
	assert.Equal(t, 2, heap.Peek().value)
	assert.Equal(t, 3, heap.Len())
	heap.Push(&FooHeapObject{value: 3})
	assert.Equal(t, 2, heap.Peek().value)
	assert.Equal(t, 4, heap.Len())
	heap.Push(&FooHeapObject{value: 1})
	assert.Equal(t, 1, heap.Peek().value)
	assert.Equal(t, 5, heap.Len())
	for i := 1; i <= 5; i++ {
		assert.Equal(t, i, heap.Peek().value)
		assert.Equal(t, i, heap.Pop().value)
	}
}

func TestMaximumObjectHeap(t *testing.T) {
	h := []*FooHeapObject{
		{value: 4},
		{value: 1},
		{value: 2},
	}
	heap := NewObjectArrayBasedMaximumHeap(h, GetFooHeapObjectOrderFunc)
	assert.Equal(t, 4, heap.Peek().value)
	assert.Equal(t, 3, heap.Len())
	heap.Push(&FooHeapObject{value: 3})
	assert.Equal(t, 4, heap.Peek().value)
	assert.Equal(t, 4, heap.Len())
	heap.Push(&FooHeapObject{value: 5})
	assert.Equal(t, 5, heap.Peek().value)
	assert.Equal(t, 5, heap.Len())
	for i := 5; i >= 1; i-- {
		assert.Equal(t, i, heap.Peek().value)
		assert.Equal(t, i, heap.Pop().value)
	}
}

func TestIndexedHeap(t *testing.T) {
	type item struct{ value, index int }
	h := NewIndexedHeap(func(a, b *item) bool { return a.value > b.value },
		func(entry *item, i int) { entry.index = i })
	a, b, c := &item{value: -3}, &item{value: 0}, &item{value: 10}
	h.Push(a)
	h.Push(b)
	h.Push(c)
	assert.Same(t, c, h.Peek())
	a.value = 12
	h.Fix(a.index)
	assert.Same(t, a, h.Peek())
	a.value = -5
	h.Fix(a.index)
	assert.Same(t, b, h.Remove(b.index))
	assert.Equal(t, -1, b.index)
	assert.Same(t, c, h.Pop())
	assert.Equal(t, -1, c.index)
	assert.Equal(t, 0, a.index)
	assert.Same(t, a, h.Pop())
	assert.Zero(t, h.Len())
}
