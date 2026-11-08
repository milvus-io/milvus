package cache

import (
	"io"
	"sync"
	"sync/atomic"
)

// Scanner visits collections in registration order, retaining only the directory
// slice for one round. Collection contents remain live, immutable publications.
// Calls on one Scanner must be serialized; cache publication may run concurrently.
type Scanner interface {
	// Scan examines at most maxSlots directory slots, including deleted slots.
	// maxSlots must be positive. Returning false from visit consumes that
	// collection and stops this call. Callbacks run without cache locks.
	// io.EOF ends the round, possibly after callbacks delivered a final batch;
	// the next call starts a new round against the current directory.
	Scan(maxSlots int, visit func(*CollectionEntry) bool) error
}

type collectionScanSlot struct {
	slot atomic.Pointer[collectionSlot]
}

// Existing slice elements never change. Retirement clears the shared slot;
// compaction publishes a new backing array without disturbing active scanners.
type collectionScanIndex struct {
	slots      []*collectionScanSlot
	revision   uint64
	deleted    int
	compacting bool
	closed     bool
	workers    sync.WaitGroup
}

type collectionScanner struct {
	cache  *Cache
	slots  []*collectionScanSlot
	offset int
	active bool
}

func (c *Cache) NewScanner() Scanner {
	return &collectionScanner{cache: c}
}

func (s *collectionScanner) Scan(maxSlots int, visit func(*CollectionEntry) bool) error {
	if !s.active {
		s.cache.mu.RLock()
		s.slots = s.cache.scanIndex.slots[:len(s.cache.scanIndex.slots):len(s.cache.scanIndex.slots)]
		s.cache.mu.RUnlock()
		s.offset, s.active = 0, true
	}
	for examined := 0; examined < maxSlots && s.offset < len(s.slots); examined++ {
		slot := s.slots[s.offset].slot.Load()
		s.offset++
		if slot != nil && !visit(slot.value.Load()) {
			break
		}
	}
	if s.offset == len(s.slots) {
		s.slots, s.offset, s.active = nil, 0, false
		return io.EOF
	}
	return nil
}

// Called under c.mu. The slot handle avoids fixing collection positions when
// compaction publishes its replacement directory.
func (c *Cache) registerCollectionLocked(slot *collectionSlot) {
	slot.scanSlot = &collectionScanSlot{}
	slot.scanSlot.slot.Store(slot)
	c.scanIndex.slots = append(c.scanIndex.slots, slot.scanSlot)
	c.scanIndex.revision++
}

const minScanTombstones = 1024

func (c *Cache) retireCollectionLocked(slot *collectionSlot) {
	slot.scanSlot.slot.Store(nil)
	c.scanIndex.deleted++
	if c.scanIndex.closed || c.scanIndex.compacting || c.scanIndex.deleted < minScanTombstones || c.scanIndex.deleted < len(c.scanIndex.slots)/2 {
		return
	}
	c.scanIndex.compacting = true
	c.scanIndex.workers.Add(1)
	go func() {
		defer c.scanIndex.workers.Done()
		c.compactCollections()
		c.mu.Lock()
		c.scanIndex.compacting = false
		c.mu.Unlock()
	}()
}

// compactCollections does all O(C) work outside the directory lock. Concurrent
// additions invalidate the result; lazy reclamation can wait for a later attempt.
// Deletions share atomic tombstones and therefore cannot be resurrected.
func (c *Cache) compactCollections() {
	c.mu.RLock()
	slots, revision := c.scanIndex.slots, c.scanIndex.revision
	c.mu.RUnlock()
	next, removed := compactCollectionSlots(slots)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.scanIndex.revision != revision {
		return
	}
	c.scanIndex.slots = next
	c.scanIndex.deleted -= removed
	c.scanIndex.revision++
}

func compactCollectionSlots(slots []*collectionScanSlot) ([]*collectionScanSlot, int) {
	next := make([]*collectionScanSlot, 0, len(slots)/2)
	for _, slot := range slots {
		if slot.slot.Load() != nil {
			next = append(next, slot)
		}
	}
	return next, len(slots) - len(next)
}
