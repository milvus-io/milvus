package typeutil

// NewMultipartQueue create a new multi-part queue.
func NewMultipartQueue[T any]() *MultipartQueue[T] {
	return &MultipartQueue[T]{
		pendings: make([][]T, 0),
		cnt:      0,
	}
}

// MultipartQueue is a multi-part queue.
type MultipartQueue[T any] struct {
	pendings [][]T
	cnt      int
}

// Len return the queue size.
func (pq *MultipartQueue[T]) Len() int {
	return pq.cnt
}

// AddOne add a message as pending one
func (pq *MultipartQueue[T]) AddOne(msg T) {
	pq.Add([]T{msg})
}

// Add add a slice of message as pending one
func (pq *MultipartQueue[T]) Add(msgs []T) {
	if len(msgs) == 0 {
		return
	}
	pq.pendings = append(pq.pendings, msgs)
	pq.cnt += len(msgs)
}

// Next return the next message in pending queue.
func (pq *MultipartQueue[T]) Next() T {
	if len(pq.pendings) != 0 && len(pq.pendings[0]) != 0 {
		return pq.pendings[0][0]
	}
	var val T
	return val
}

// UnsafeAdvance do a advance without check.
// !!! Should only be called `Next` do not return nil.
func (pq *MultipartQueue[T]) UnsafeAdvance() {
	if len(pq.pendings[0]) == 1 {
		// release the memory after advance.
		pq.pendings[0] = nil

		pq.pendings = pq.pendings[1:]
		pq.cnt--
		return
	}
	// release the memory after advance.
	var nop T
	pq.pendings[0][0] = nop

	pq.pendings[0] = pq.pendings[0][1:]
	pq.cnt--
}
