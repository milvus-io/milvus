// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"slices"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

// Segment writes and their locks.
//
// A write of segment metadata holds the writer lock of every segment it reads
// to decide what to write and of every segment it writes, from its first read
// until the write is stored in the catalog and published to the cache. Writes
// of different segments run in parallel; readers take no segment lock.
//
// A write waits for a segment lock only when every lock it holds has a
// smaller segment ID, so writes never deadlock on them. A write that finds
// out which segments it needs while it runs (an update pack running
// operators) takes each lock as it reaches it; when it reaches a segment with
// a smaller ID than one it holds and another write holds that segment, the
// run is discarded before any I/O and runs again with every lock it reached
// taken in ID order. The rest of that run reads the segment as absent (Get
// and Peek return nil), so operators never read a segment another write
// holds. A discarded run must leave no trace: operators change only the pack,
// never their inputs or state shared between runs.
//
// A write that selects segments by a range (a partition, a channel) locks the
// segments selected and selects again, until the selection is all locked. A
// write that creates segments from others (compaction, external refresh)
// holds the locks of those sources until it publishes the new segments, so a
// range write either sees the new segments or makes the creator fail.
//
// Lock order: SegmentManager channel lock -> segment manifest locks ->
// indexMeta key lock -> DataView collection lock -> segment writer locks ->
// segment change groups (groupMu) -> MetaStore shards.

// segmentLockSet is the segment writer locks one write holds.
type segmentLockSet struct {
	locks  *lock.KeyLock[int64]
	held   map[int64]struct{}
	maxID  int64
	missed map[int64]struct{}
}

func newSegmentLockSet(locks *lock.KeyLock[int64]) *segmentLockSet {
	return &segmentLockSet{
		locks:  locks,
		held:   make(map[int64]struct{}),
		missed: make(map[int64]struct{}),
	}
}

// lockAll waits until it holds the locks of segmentIDs. The set must hold no
// lock yet.
func (s *segmentLockSet) lockAll(segmentIDs []int64) {
	lock.LockManyOrdered(s.locks, segmentIDs)
	for _, id := range segmentIDs {
		s.add(id)
	}
}

func (s *segmentLockSet) add(segmentID int64) {
	if len(s.held) == 0 || segmentID > s.maxID {
		s.maxID = segmentID
	}
	s.held[segmentID] = struct{}{}
}

// lock takes the lock of segmentID and reports whether the set holds it: it
// waits for it when the set holds only smaller IDs, and otherwise only takes
// it if no other write holds it, recording a miss.
func (s *segmentLockSet) lock(segmentID int64) bool {
	if _, ok := s.held[segmentID]; ok {
		return true
	}
	if len(s.held) == 0 || segmentID > s.maxID {
		s.locks.Lock(segmentID)
		s.add(segmentID)
		return true
	}
	if s.locks.TryLock(segmentID) {
		s.add(segmentID)
		return true
	}
	s.missed[segmentID] = struct{}{}
	return false
}

// complete reports whether the set holds every lock it was asked for.
func (s *segmentLockSet) complete() bool {
	return len(s.missed) == 0
}

// relock releases the locks and waits until it holds them and the missed ones.
func (s *segmentLockSet) relock() {
	ids := append(lo.Keys(s.held), lo.Keys(s.missed)...)
	s.release()
	s.lockAll(ids)
}

func (s *segmentLockSet) release() {
	s.locks.UnlockMany(lo.Keys(s.held))
	clear(s.held)
	clear(s.missed)
}

func (s *segmentLockSet) holds(segmentIDs []int64) bool {
	for _, id := range segmentIDs {
		if _, ok := s.held[id]; !ok {
			return false
		}
	}
	return true
}

func (m *meta) getSegmentLocks() *lock.KeyLock[int64] {
	m.segmentLocksOnce.Do(func() {
		if m.segmentLocks == nil {
			m.segmentLocks = lock.NewKeyLock[int64]()
		}
	})
	return m.segmentLocks
}

// lockSegments waits until it holds the writer locks of segmentIDs and
// returns the function that releases them, for a write that knows its segments.
func (m *meta) lockSegments(segmentIDs ...int64) func() {
	locks := newSegmentLockSet(m.getSegmentLocks())
	locks.lockAll(segmentIDs)
	return locks.release
}

// lockSelection locks the segments selectIDs selects and selects again,
// until every segment selected is locked, and returns that selection.
func lockSelection(locks *segmentLockSet, selectIDs func() []int64) []int64 {
	selected := selectIDs()
	for {
		slices.Sort(selected)
		for _, id := range selected {
			locks.lock(id)
		}
		if !locks.complete() {
			locks.relock()
		}
		again := selectIDs()
		if locks.holds(again) {
			return again
		}
		selected = again
	}
}
