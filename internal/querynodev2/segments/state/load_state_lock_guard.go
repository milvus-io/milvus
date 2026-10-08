package state

type LoadStateLockGuard interface {
	Done(err error)
}

// newLoadStateLockGuard creates a LoadStateGuard.
func newLoadStateLockGuard(ls *LoadStateLock, original loadStateEnum, target loadStateEnum) *loadStateLockGuard {
	return &loadStateLockGuard{
		ls:       ls,
		original: original,
		target:   target,
	}
}

// loadStateLockGuard is a guard to update the state of LoadState.
type loadStateLockGuard struct {
	ls       *LoadStateLock
	original loadStateEnum
	target   loadStateEnum
}

// Done updates the state of LoadState to target state.
func (g *loadStateLockGuard) Done(err error) {
	g.ls.cv.L.Lock()
	g.ls.cv.Broadcast()
	defer g.ls.cv.L.Unlock()

	if err != nil {
		g.ls.state = g.original
		return
	}
	g.ls.state = g.target
}

// newNopLoadStateLockGuard creates a LoadStateLockGuard that does nothing.
func newNopLoadStateLockGuard() LoadStateLockGuard {
	return nopLockGuard{}
}

// nopLockGuard is a guard that does nothing.
type nopLockGuard struct{}

// Done does nothing.
func (nopLockGuard) Done(err error) {}
