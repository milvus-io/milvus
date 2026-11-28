package typeutil

import (
	"container/heap"

	"golang.org/x/exp/constraints"
)

var _ HeapInterface = (*heapArray[int])(nil)

// HeapInterface is the interface that a heap must implement.
type HeapInterface interface {
	heap.Interface
	Peek() interface{}
}

// Heap is a heap of E.
// Use `golang.org/x/exp/constraints` directly if you want to change any element.
type Heap[E any] interface {
	// Len returns the size of the heap.
	Len() int

	// Push pushes an element onto the heap.
	Push(x E)

	// Pop returns the element at the top of the heap.
	// Panics if the heap is empty.
	Pop() E

	// Peek returns the element at the top of the heap.
	// Panics if the heap is empty.
	Peek() E
}

// heapArray is a heap backed by an array.
type heapArray[E constraints.Ordered] []E

// Len returns the length of the heap.
func (h heapArray[E]) Len() int {
	return len(h)
}

// Less returns true if the element at index i is less than the element at index j.
func (h heapArray[E]) Less(i, j int) bool {
	return h[i] < h[j]
}

// Swap swaps the elements at indexes i and j.
func (h heapArray[E]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes the last one at len.
func (h *heapArray[E]) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(E))
}

// Pop pop the last one at len.
func (h *heapArray[E]) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	var zero E
	old[n-1] = zero // release the memory of underlying array.
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
func (h *heapArray[E]) Peek() interface{} {
	return (*h)[0]
}

type objectHeapArray[O any] struct {
	objects  []O
	less     func(O, O) bool
	setIndex func(O, int)
}

func (h *objectHeapArray[O]) Len() int {
	return len(h.objects)
}

func (h *objectHeapArray[O]) Less(i, j int) bool {
	return h.less(h.objects[i], h.objects[j])
}

func (h *objectHeapArray[O]) Swap(i, j int) {
	h.objects[i], h.objects[j] = h.objects[j], h.objects[i]
	if h.setIndex != nil {
		h.setIndex(h.objects[i], i)
		h.setIndex(h.objects[j], j)
	}
}

func (h *objectHeapArray[O]) Push(x interface{}) {
	object := x.(O)
	if h.setIndex != nil {
		h.setIndex(object, len(h.objects))
	}
	h.objects = append(h.objects, object)
}

func (h *objectHeapArray[O]) Pop() interface{} {
	old := h.objects
	n := len(old)
	x := old[n-1]
	if h.setIndex != nil {
		h.setIndex(x, -1)
	}
	var zero O
	old[n-1] = zero // release the memory of underlying array.
	h.objects = old[0 : n-1]
	return x
}

func (h *objectHeapArray[O]) Peek() interface{} {
	return h.objects[0]
}

// reverseOrderedInterface is a heap base interface that reverses the order of the elements.
type reverseOrderedInterface[E constraints.Ordered] struct {
	HeapInterface
}

// Less returns true if the element at index j is less than the element at index i.
func (r reverseOrderedInterface[E]) Less(i, j int) bool {
	return r.HeapInterface.Less(j, i)
}

// NewHeap returns a new heap from a underlying representation.
func NewHeap[E any](inner HeapInterface) Heap[E] {
	return &heapImpl[E, HeapInterface]{
		inner: inner,
	}
}

// NewArrayBasedMaximumHeap returns a new maximum heap.
func NewArrayBasedMaximumHeap[E constraints.Ordered](initial []E) Heap[E] {
	ha := heapArray[E](initial)
	reverse := reverseOrderedInterface[E]{
		HeapInterface: &ha,
	}
	heap.Init(reverse)
	return &heapImpl[E, reverseOrderedInterface[E]]{
		inner: reverse,
	}
}

// NewArrayBasedMinimumHeap returns a new minimum heap.
func NewArrayBasedMinimumHeap[E constraints.Ordered](initial []E) Heap[E] {
	ha := heapArray[E](initial)
	heap.Init(&ha)
	return &heapImpl[E, *heapArray[E]]{
		inner: &ha,
	}
}

func NewObjectArrayBasedMaximumHeap[O any, E constraints.Ordered](initial []O, getOrderFunc func(O) E) Heap[O] {
	return newObjectHeap(initial, func(a, b O) bool { return getOrderFunc(b) < getOrderFunc(a) }, nil)
}

func NewObjectArrayBasedMinimumHeap[O any, E constraints.Ordered](initial []O, getOrderFunc func(O) E) Heap[O] {
	return newObjectHeap(initial, func(a, b O) bool { return getOrderFunc(a) < getOrderFunc(b) }, nil)
}

// IndexedHeap supports updating or removing a resident item by its current index.
type IndexedHeap[E any] interface {
	Heap[E]
	Fix(index int)
	Remove(index int) E
}

// NewIndexedHeap orders distinct items by less. setIndex records the current
// position on each item and receives -1 when the item leaves the heap.
func NewIndexedHeap[E any](less func(E, E) bool, setIndex func(E, int)) IndexedHeap[E] {
	return newObjectHeap(nil, less, setIndex)
}

func newObjectHeap[E any](initial []E, less func(E, E) bool, setIndex func(E, int)) *heapImpl[E, *objectHeapArray[E]] {
	array := &objectHeapArray[E]{objects: initial, less: less, setIndex: setIndex}
	heap.Init(array)
	return &heapImpl[E, *objectHeapArray[E]]{inner: array}
}

// heapImpl is a min-heap of E.
type heapImpl[E any, H HeapInterface] struct {
	inner H
}

// Len returns the length of the heap.
func (h *heapImpl[E, H]) Len() int {
	return h.inner.Len()
}

// Push pushes an element onto the heap.
func (h *heapImpl[E, H]) Push(x E) {
	heap.Push(h.inner, x)
}

// Pop pops an element from the heap.
func (h *heapImpl[E, H]) Pop() E {
	return heap.Pop(h.inner).(E)
}

// Peek returns the element at the top of the heap.
func (h *heapImpl[E, H]) Peek() E {
	return h.inner.Peek().(E)
}

func (h *heapImpl[E, H]) Fix(index int) {
	heap.Fix(h.inner, index)
}

func (h *heapImpl[E, H]) Remove(index int) E {
	return heap.Remove(h.inner, index).(E)
}
