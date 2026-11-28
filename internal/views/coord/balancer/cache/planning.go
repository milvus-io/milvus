package cache

import (
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
)

// ReplicaLayout is the immutable target ownership for one collection and RG.
type ReplicaLayout struct {
	Replicas, Nodes []int64
	Owners          map[int64]int64
	Targets         map[int64][]int64
}

// CollectionPlanning is pinned with its collection facts. Its membership is
// immutable; layout and shard-history updates never republish collection facts.
type CollectionPlanning struct {
	layouts  map[string]*atomic.Pointer[ReplicaLayout]
	replicas map[int64]*ReplicaPlanning
}

func (c *CollectionEntry) Planning() *CollectionPlanning { return c.planning }

func (old *CollectionPlanning) withMembership(cfg *loadmgr.LoadConfig) *CollectionPlanning {
	next := &CollectionPlanning{
		layouts:  make(map[string]*atomic.Pointer[ReplicaLayout]),
		replicas: make(map[int64]*ReplicaPlanning, len(cfg.Replicas)),
	}
	for _, r := range cfg.Replicas {
		previous := old.Replica(r.ReplicaID)
		history := previous
		// Pre-upgrade WAL records may still reassign an ID across groups.
		if history == nil || history.group != r.ResourceGroup {
			history = &ReplicaPlanning{group: r.ResourceGroup}
		}
		next.replicas[r.ReplicaID] = history
		slot := next.layouts[r.ResourceGroup]
		if slot == nil {
			slot = &atomic.Pointer[ReplicaLayout]{}
			next.layouts[r.ResourceGroup] = slot
		}
		// Carry a seed only through a surviving member of the same group.
		// A late layout computation can still write only to its old slot.
		if history == previous && slot.Load() == nil {
			if prior := old.Layout(r.ResourceGroup); prior != nil {
				if _, ok := prior.Targets[r.ReplicaID]; ok {
					slot.Store(prior)
				}
			}
		}
	}
	return next
}

func (p *CollectionPlanning) Layout(group string) *ReplicaLayout {
	if p != nil && p.layouts[group] != nil {
		return p.layouts[group].Load()
	}
	return nil
}

// StoreLayout publishes a fully built, immutable target for a pinned group.
func (p *CollectionPlanning) StoreLayout(group string, layout *ReplicaLayout) {
	p.layouts[group].Store(layout)
}

func (p *CollectionPlanning) Replica(id int64) *ReplicaPlanning {
	if p == nil {
		return nil
	}
	return p.replicas[id]
}

// SameLoad identifies both the pinned requirements and the load lifetime.
func (c *CollectionEntry) SameLoad(other *CollectionEntry) bool {
	return other != nil && c.config == other.config && c.configVersion == other.configVersion && c.planning == other.planning
}

type ReplicaPlanning struct {
	group  string
	mu     sync.Mutex
	shards map[string]int
}

func (r *ReplicaPlanning) Fanout(channel string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.shards[channel]
}

func (r *ReplicaPlanning) StoreFanout(channel string, fanout int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shards == nil {
		r.shards = make(map[string]int)
	}
	r.shards[channel] = fanout
}
