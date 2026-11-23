package loadmgr

type pendingPlacement struct {
	group string
	since uint64 // Earlier Views of a moved replica may keep serving.
}

// Config publication and its placement changes share the same lock.
func (s *LoadConfigStore) updatePendingVisibility(cfg *LoadConfig, newPlacements []*ReplicaAssignment) {
	held := s.pendingVisibility[cfg.CollectionID]
	if len(held) == 0 && len(newPlacements) == 0 {
		return
	}
	next := make(map[int64]pendingPlacement)
	for _, r := range cfg.Replicas {
		if p, ok := held[r.ReplicaID]; ok && p.group == r.ResourceGroup {
			next[r.ReplicaID] = p
		}
	}
	for _, r := range newPlacements {
		next[r.ReplicaID] = pendingPlacement{group: r.ResourceGroup, since: cfg.Version}
	}
	if len(next) == 0 {
		delete(s.pendingVisibility, cfg.CollectionID)
	} else {
		s.pendingVisibility[cfg.CollectionID] = next
	}
}

// HoldReplicaUp lets an earlier View of a moved replica keep serving.
func (s *LoadConfigStore) HoldReplicaUp(collectionID, replicaID int64, version uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, held := s.pendingVisibility[collectionID][replicaID]
	return held && version >= p.since
}

// PendingPlacements groups held assignments by the immutable config they were checked against.
type PendingPlacements map[*LoadConfig][]*ReplicaAssignment

// PendingVisibility snapshots the held assignments by immutable collection config.
func (s *LoadConfigStore) PendingVisibility() PendingPlacements {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pending := make(PendingPlacements, len(s.pendingVisibility))
	for id, held := range s.pendingVisibility {
		cfg := s.configs[id]
		for _, replica := range cfg.Replicas {
			if _, ok := held[replica.ReplicaID]; ok {
				pending[cfg] = append(pending[cfg], replica)
			}
		}
	}
	return pending
}

// AdmitResourceGroup releases all checked assignments in one in-memory update.
// The cluster watcher is the sole publisher of new holds.
func (s *LoadConfigStore) AdmitResourceGroup(checked PendingPlacements) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for cfg := range checked {
		if s.configs[cfg.CollectionID] != cfg {
			return false
		}
	}
	for cfg, replicas := range checked {
		held := s.pendingVisibility[cfg.CollectionID]
		for _, r := range replicas {
			delete(held, r.ReplicaID)
		}
		if len(held) == 0 {
			delete(s.pendingVisibility, cfg.CollectionID)
		}
	}
	return true
}
