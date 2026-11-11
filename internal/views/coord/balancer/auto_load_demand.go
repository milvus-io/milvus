package balancer

import "sync"

// AutoLoadDemand tracks outstanding demand, not a permanent collection property.
// Its zero value is ready to use. It owns no goroutine or timer.
type AutoLoadDemand struct {
	mu      sync.Mutex
	waiting map[int64]int
}

// Begin keeps AutoLoad urgency until the shared readiness operation finishes.
// The returned release function must be called exactly once.
func (d *AutoLoadDemand) Begin(collectionID int64) func() {
	d.mu.Lock()
	if d.waiting == nil {
		d.waiting = make(map[int64]int)
	}
	d.waiting[collectionID]++
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.waiting[collectionID]--
		if d.waiting[collectionID] == 0 {
			delete(d.waiting, collectionID)
		}
	}
}

func (d *AutoLoadDemand) Pending(collectionID int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.waiting[collectionID] > 0
}

func WithAutoLoadDemand(demand *AutoLoadDemand) PolicyOption {
	return func(p *DefaultBalancePolicy) { p.autoLoad = demand }
}
