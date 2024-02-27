package memory

import "sync/atomic"

type MemoryTracker interface {
	Consume(size int64)
	Return(bytes int64)
	TotalConsumed() int64
	CreateChild(name string) MemoryTracker
	Drop()
	Name() string
}

type GoMemoryTracker struct {
	name     string
	consumed int64
	parent   MemoryTracker
	limit    int64
	onExceed func(tracker MemoryTracker)
}

func NewGoMemoryTracker(name string, parent MemoryTracker) MemoryTracker {
	return &GoMemoryTracker{
		name:     name,
		consumed: 0,
		parent:   parent,
		limit:    -1,
	}
}

func NewGoMemoryTrackerWithLimit(name string, parent MemoryTracker, limit int64, onExceed func(tracker MemoryTracker)) MemoryTracker {
	return &GoMemoryTracker{
		name:     name,
		consumed: 0,
		parent:   parent,
		limit:    limit,
		onExceed: onExceed,
	}
}

func (mt *GoMemoryTracker) Consume(bytes int64) {
	atomic.AddInt64(&mt.consumed, bytes)
	if atomic.LoadInt64(&mt.consumed) > mt.limit && mt.onExceed != nil {
		mt.onExceed(mt)
	}
	if mt.parent != nil {
		mt.parent.Consume(bytes)
	}
}

func (mt *GoMemoryTracker) Return(bytes int64) {
	atomic.AddInt64(&mt.consumed, -bytes)
	if mt.parent != nil {
		mt.parent.Return(bytes)
	}
}

func (mt *GoMemoryTracker) Name() string {
	return mt.name
}

func (mt *GoMemoryTracker) TotalConsumed() int64 {
	return atomic.LoadInt64(&mt.consumed)
}

func (mt *GoMemoryTracker) CreateChild(name string) MemoryTracker {
	child := NewGoMemoryTracker(name, mt)
	return child
}

func (mt *GoMemoryTracker) Drop() {
	if mt.parent != nil {
		mt.parent.Return(atomic.LoadInt64(&mt.consumed))
	}
}
