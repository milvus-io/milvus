package util

import (
	"go.uber.org/atomic"
)

func NewIDAllocator() *IDAllocator {
	return &IDAllocator{}
}

type IDAllocator struct {
	underlying atomic.Int64
}

func (ida *IDAllocator) Allocate() int64 {
	return ida.underlying.Inc()
}
