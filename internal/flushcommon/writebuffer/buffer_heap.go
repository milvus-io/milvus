package writebuffer

import (
	"container/heap"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// bufferHeapItem represents an item in the buffer heap
type bufferHeapItem struct {
	segmentID int64
	timestamp typeutil.Timestamp
	index     int // index in heap, maintained by heap operations
}

// bufferHeap is a min-heap of bufferHeapItems ordered by timestamp
type bufferHeap []*bufferHeapItem

func (h bufferHeap) Len() int           { return len(h) }
func (h bufferHeap) Less(i, j int) bool { return h[i].timestamp < h[j].timestamp }
func (h bufferHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *bufferHeap) Push(x any) {
	n := len(*h)
	item := x.(*bufferHeapItem)
	item.index = n
	*h = append(*h, item)
}

func (h *bufferHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // mark as removed
	*h = old[0 : n-1]
	return item
}

// BufferTimestampHeap provides O(1) minimum lookup and O(log N) updates.
// NOT thread-safe - caller must hold write_buffer mutex.
type BufferTimestampHeap struct {
	heap  bufferHeap
	index map[int64]*bufferHeapItem // segmentID -> item
}

// NewBufferTimestampHeap creates a new buffer timestamp heap
func NewBufferTimestampHeap() *BufferTimestampHeap {
	return &BufferTimestampHeap{
		heap:  make(bufferHeap, 0),
		index: make(map[int64]*bufferHeapItem),
	}
}

// Update adds or updates a buffer's timestamp in the heap.
// Time complexity: O(log N)
func (h *BufferTimestampHeap) Update(segmentID int64, timestamp typeutil.Timestamp) {
	if item, ok := h.index[segmentID]; ok {
		// Only update if timestamp decreased (MinTimestamp can only decrease)
		if timestamp < item.timestamp {
			item.timestamp = timestamp
			heap.Fix(&h.heap, item.index)
		}
	} else {
		// Add new item
		item := &bufferHeapItem{
			segmentID: segmentID,
			timestamp: timestamp,
		}
		h.index[segmentID] = item
		heap.Push(&h.heap, item)
	}
}

// Remove removes a buffer from the heap.
// Time complexity: O(log N)
func (h *BufferTimestampHeap) Remove(segmentID int64) {
	if item, ok := h.index[segmentID]; ok {
		heap.Remove(&h.heap, item.index)
		delete(h.index, segmentID)
	}
}

// PeekMin returns the minimum timestamp without removing it.
// Time complexity: O(1)
func (h *BufferTimestampHeap) PeekMin() (segmentID int64, timestamp typeutil.Timestamp, ok bool) {
	if len(h.heap) == 0 {
		return 0, 0, false
	}
	item := h.heap[0]
	return item.segmentID, item.timestamp, true
}

// CollectWhile traverses the heap and collects segments where predicate returns true.
// Uses heap property for pruning: if parent doesn't match, children won't either.
// Time complexity: O(K) where K is the number of matching segments.
func (h *BufferTimestampHeap) CollectWhile(predicate func(timestamp typeutil.Timestamp) bool) []int64 {
	if len(h.heap) == 0 {
		return nil
	}

	var result []int64
	// BFS traversal with pruning
	queue := []int{0} // start from root

	for len(queue) > 0 {
		idx := queue[0]
		queue = queue[1:]

		if idx >= len(h.heap) {
			continue
		}

		item := h.heap[idx]
		if !predicate(item.timestamp) {
			// This node doesn't match, skip its subtree (children have larger timestamps)
			continue
		}

		// This node matches
		result = append(result, item.segmentID)

		// Check children
		left := 2*idx + 1
		right := 2*idx + 2
		if left < len(h.heap) {
			queue = append(queue, left)
		}
		if right < len(h.heap) {
			queue = append(queue, right)
		}
	}

	return result
}

// Len returns the number of items in the heap.
func (h *BufferTimestampHeap) Len() int {
	return len(h.heap)
}

// GetOldestN returns the N segments with the smallest timestamps.
// Uses BFS traversal to collect up to N items from the min-heap.
// Time complexity: O(N) where N is the requested count.
func (h *BufferTimestampHeap) GetOldestN(n int) []int64 {
	if n <= 0 || len(h.heap) == 0 {
		return nil
	}

	if n >= len(h.heap) {
		// Return all segment IDs
		result := make([]int64, len(h.heap))
		for i, item := range h.heap {
			result[i] = item.segmentID
		}
		return result
	}

	// Use a secondary min-heap to extract N smallest elements
	// This is more efficient than sorting when N << len(heap)
	type candidate struct {
		idx       int
		timestamp typeutil.Timestamp
	}

	// Priority queue ordered by timestamp
	candidates := make([]candidate, 0, n+2)
	candidates = append(candidates, candidate{0, h.heap[0].timestamp})

	result := make([]int64, 0, n)

	for len(result) < n && len(candidates) > 0 {
		// Find minimum in candidates (linear scan is fine for small N)
		minIdx := 0
		for i := 1; i < len(candidates); i++ {
			if candidates[i].timestamp < candidates[minIdx].timestamp {
				minIdx = i
			}
		}

		// Extract minimum
		best := candidates[minIdx]
		candidates[minIdx] = candidates[len(candidates)-1]
		candidates = candidates[:len(candidates)-1]

		result = append(result, h.heap[best.idx].segmentID)

		// Add children to candidates
		left := 2*best.idx + 1
		right := 2*best.idx + 2
		if left < len(h.heap) {
			candidates = append(candidates, candidate{left, h.heap[left].timestamp})
		}
		if right < len(h.heap) {
			candidates = append(candidates, candidate{right, h.heap[right].timestamp})
		}
	}

	return result
}
