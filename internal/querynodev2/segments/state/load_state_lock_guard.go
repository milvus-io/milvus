package state

// newLoadStateLockGuard creates a LoadStateGuard.
func newLoadStateLockGuard(ls *LoadStateLock, original loadStateEnum, target loadStateEnum) *LoadStateLockGuard {
	return &LoadStateLockGuard{
		ls:       ls,
		original: original,
		target:   target,
	}
}

// LoadStateLockGuard is a guard to update the state of LoadState.
type LoadStateLockGuard struct {
	ls       *LoadStateLock
	original loadStateEnum
	target   loadStateEnum
}

// Done updates the state of LoadState to target state.
func (g *LoadStateLockGuard) Done(err error) {
	g.ls.cv.L.Lock()
	g.ls.cv.Broadcast()
	defer g.ls.cv.L.Unlock()

	if err != nil {
		g.ls.state = g.original
		return
	}
	g.ls.state = g.target
}
