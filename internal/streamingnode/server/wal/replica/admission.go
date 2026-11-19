package replica

import (
	"context"
	"sync"
)

// Admission counts operations which may change external state. A handoff
// permanently closes admission for this writer and waits for those operations
// to return. A caller timeout does not reopen admission.
type Admission struct {
	mu      sync.Mutex
	active  int
	drained chan struct{}
}

func (g *Admission) Enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.drained != nil {
		return false
	}
	g.active++
	return true
}

func (g *Admission) Leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if g.active == 0 && g.drained != nil {
		close(g.drained)
	}
}

func (g *Admission) Drain(ctx context.Context) error {
	g.mu.Lock()
	if g.drained == nil {
		g.drained = make(chan struct{})
		if g.active == 0 {
			close(g.drained)
		}
	}
	drained := g.drained
	g.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
