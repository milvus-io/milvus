package typeutil

import (
	"go.uber.org/atomic"
)

// NewIDAllocator creates a new IDAllocator.
func NewIDAllocator() *IDAllocator {
	return &IDAllocator{}
}

// IDAllocator is a thread-safe ID allocator.
type IDAllocator struct {
	underlying atomic.Int64
}

// Allocate allocates a new ID.
func (ida *IDAllocator) Allocate() int64 {
	return ida.underlying.Inc()
}
