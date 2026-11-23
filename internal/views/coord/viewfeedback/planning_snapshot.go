package viewfeedback

import (
	"encoding/binary"

	iradix "github.com/hashicorp/go-immutable-radix"
)

// PlanningSnapshot pins replica feedback and RG totals at the same publication.
// Its indexes are immutable: retaining a snapshot does not block writers or copy
// all replicas. The zero value is empty.
type PlanningSnapshot struct {
	replicas *iradix.Tree
	groups   *iradix.Tree
}

type planningReplica struct {
	group    string
	feedback *Snapshot
}

type planningGroup struct {
	rates        RequestRates
	contributors int
}

// GetReplicaFeedback returns a numerator and denominator from the same snapshot.
// A mismatched RG means LoadConfig and feedback publication raced; treat that
// feedback as unavailable instead of combining unrelated resource groups.
func (p *PlanningSnapshot) GetReplicaFeedback(id int64, group string) (*Snapshot, RequestRates) {
	if p.replicas == nil {
		return nil, RequestRates{}
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(id))
	value, ok := p.replicas.Get(key[:])
	if !ok {
		return nil, RequestRates{}
	}
	replica := value.(planningReplica)
	if replica.group != group {
		return nil, RequestRates{}
	}
	var rates RequestRates
	if p.groups != nil {
		if value, ok := p.groups.Get([]byte(group)); ok {
			rates = value.(planningGroup).rates
		}
	}
	return replica.feedback, rates
}

// replaceReplica copies only changed radix paths, never a whole replica map.
// The store holds its write lock across all replacements in a publication.
func (p *PlanningSnapshot) replaceReplica(id int64, group string, feedback *Snapshot) *PlanningSnapshot {
	next := *p
	var old planningReplica
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(id))
	if next.replicas != nil {
		if value, ok := next.replicas.Get(key[:]); ok {
			old = value.(planningReplica)
		}
	}
	if old.feedback == feedback && (feedback == nil || old.group == group) {
		return p
	}
	var before, after RequestRates
	if old.feedback != nil {
		before = old.feedback.RequestRates
	}
	if feedback != nil {
		after = feedback.RequestRates
	}
	if old.group == group {
		next.replaceGroup(group, before, after)
	} else {
		next.replaceGroup(old.group, before, RequestRates{})
		next.replaceGroup(group, RequestRates{}, after)
	}
	if feedback == nil {
		next.replicas, _, _ = next.replicas.Delete(key[:])
	} else {
		if next.replicas == nil {
			next.replicas = iradix.New()
		}
		next.replicas, _, _ = next.replicas.Insert(key[:], planningReplica{group: group, feedback: feedback})
	}
	return &next
}

func (p *PlanningSnapshot) replaceGroup(group string, before, after RequestRates) {
	if before == after {
		return
	}
	var aggregate planningGroup
	key := []byte(group)
	if p.groups != nil {
		if value, ok := p.groups.Get(key); ok {
			aggregate = value.(planningGroup)
		}
	} else {
		p.groups = iradix.New()
	}
	if before != (RequestRates{}) {
		aggregate.contributors--
	}
	if after != (RequestRates{}) {
		aggregate.contributors++
	}
	if aggregate.contributors == 0 {
		// Drop empty groups and floating-point subtraction residue together.
		p.groups, _, _ = p.groups.Delete(key)
		return
	}
	aggregate.rates.Last2Windows += after.Last2Windows - before.Last2Windows
	aggregate.rates.Last6Windows += after.Last6Windows - before.Last6Windows
	aggregate.rates.Last12Windows += after.Last12Windows - before.Last12Windows
	aggregate.rates.Last24Windows += after.Last24Windows - before.Last24Windows
	p.groups, _, _ = p.groups.Insert(key, aggregate)
}
