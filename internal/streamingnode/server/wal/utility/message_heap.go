package utility

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ typeutil.HeapInterface = (*immutableMessageHeap)(nil)

// immutableMessageHeap is a heap underlying represent of timestampAck.
type immutableMessageHeap []message.ImmutableMessage

// Len returns the length of the heap.
func (h immutableMessageHeap) Len() int {
	return len(h)
}

// Less returns true if the element at index i is less than the element at index j.
func (h immutableMessageHeap) Less(i, j int) bool {
	return h[i].TimeTick() < h[j].TimeTick()
}

// Swap swaps the elements at indexes i and j.
func (h immutableMessageHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes the last one at len.
func (h *immutableMessageHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(message.ImmutableMessage))
}

// Pop pop the last one at len.
func (h *immutableMessageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // release the memory of underlying array.
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
func (h *immutableMessageHeap) Peek() interface{} {
	return (*h)[0]
}
