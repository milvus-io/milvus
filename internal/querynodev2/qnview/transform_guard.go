package qnview

import "sync/atomic"

// A view and its still-unregistered segments share one buffer range pin. Each
// owner releases exactly once; registration takes over retention for a segment.
type retainedTransformGuard struct {
	TransformLogGuard
	refs atomic.Int32
}

func newRetainedTransformGuard(guard TransformLogGuard) *retainedTransformGuard {
	g := &retainedTransformGuard{TransformLogGuard: guard}
	g.refs.Store(1)
	return g
}

func (g *retainedTransformGuard) retain() *retainedTransformGuard {
	g.refs.Add(1)
	return g
}

func (g *retainedTransformGuard) Release() {
	if g.refs.Add(-1) == 0 {
		g.TransformLogGuard.Release()
	}
}
