package allocator

import (
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	defaultInitializedValue = 0
	defaultDelta            = 1
)

type AtomicAllocator struct {
	now   atomic.Int64
	delta int64
}

type Option func(allocator *AtomicAllocator)

func WithInitializedValue(value int64) Option {
	return func(allocator *AtomicAllocator) {
		allocator.now.Store(value)
	}
}

func WithDelta(delta int64) Option {
	return func(allocator *AtomicAllocator) {
		allocator.delta = delta
	}
}

func (alloc *AtomicAllocator) apply(opts ...Option) {
	for _, opt := range opts {
		opt(alloc)
	}
}

func (alloc *AtomicAllocator) AllocID() (typeutil.UniqueID, error) {
	id := alloc.now.Add(alloc.delta)
	return id, nil
}

func NewAllocator(opts ...Option) *AtomicAllocator {
	alloc := &AtomicAllocator{
		now:   *atomic.NewInt64(defaultInitializedValue),
		delta: defaultDelta,
	}
	alloc.apply(opts...)
	return alloc
}
