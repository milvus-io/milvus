package typeutil

import "sync"

type LifetimeState int

var (
	LifetimeStateWorking LifetimeState = 0
	LifetimeStateStopped LifetimeState = 1
)

// NewLifetime returns a new instance of Lifetime with default state logic.
func NewLifetime() *Lifetime {
	return NewGenericLifetime(LifetimeStateWorking)
}

// NewGenericLifetime returns a new instance of Lifetime with init state and isHealthy logic.
// WARNING: This type is a unsafe type, the lifetime state transfer should never be a loop.
// The state is controlled by the user, and the user should ensure the state transfer is correct.
func NewGenericLifetime[State comparable](initState State) *GenericLifetime[State] {
	return &GenericLifetime[State]{
		mu:    sync.Mutex{},
		wg:    sync.WaitGroup{},
		state: initState,
	}
}

type Lifetime = GenericLifetime[LifetimeState]

// GenericLifetime is a common component lifetime control logic.
type GenericLifetime[State comparable] struct {
	mu    sync.Mutex
	wg    sync.WaitGroup
	state State
}

func (l *GenericLifetime[State]) GetState() State {
	return l.state
}

func (l *GenericLifetime[State]) SetState(s State) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state = s
}

func (l *GenericLifetime[State]) Add(s State) bool {
	return l.AddIf(func(s2 State) bool { return s == s2 })
}

func (l *GenericLifetime[State]) AddIf(pred func(s State) bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !pred(l.state) {
		return false
	}
	l.wg.Add(1)
	return true
}

func (l *GenericLifetime[State]) Done() {
	l.wg.Done()
}

func (l *GenericLifetime[State]) Wait() {
	l.wg.Wait()
}
