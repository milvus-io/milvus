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

package dedup

import (
	"context"
	"slices"
	"sync"
)

// inflightSet holds the writes of one vchannel that were decided but are not
// yet applied to the engine by observe. It answers one question, whether a key
// exists, and doubles as the exact primary key lock of the write path.
//
// One mutex guards the whole set. It is held only while a map or a time tick is
// read or changed, never across an append or a wait.
//
// Two ordering rules make the set and the engine together complete: every
// appended write is in the set, in the engine, or in both.
//   - Rule A (observe): write the engine first, then remove the entries.
//   - Rule B (writer): check the set first, then the engine.
//
// A miss in the set means the entry was removed, so the engine was written before
// the removal, so the engine probe that follows finds the key.
//
// A write leaves the set when observe has passed its time tick. Observe raises
// the observed time tick and removes entries under the mutex, and a writer sets
// its time tick and reads the observed time tick under the mutex, so the side
// that comes second removes the write.
type inflightSet struct {
	mu sync.Mutex
	// keys is nil when empty, because a Go map never shrinks after deletes.
	keys map[uint64][]*inflightWrite // lock key -> writes in registration order
	// observed is the highest time tick that observe has applied to the engine.
	observed uint64
	// raised is closed and replaced by nil when observed rises, which wakes the
	// waits of waitObserved. It is nil while nobody waits.
	raised chan struct{}
	// inDoubt holds the writes whose append failed with an unknown outcome.
	// Observe removes them by time tick, because such a write may never reach
	// the WAL and then observe never meets its keys.
	inDoubt []*inflightWrite
}

// inflightWrite is one write of one or more keys. All keys of the write share it.
type inflightWrite struct {
	keys []uint64 // distinct lock keys, ascending
	// tt is 0 while the append has not returned: the write then is the lock of
	// its keys, and a later write of the same key waits for it. Otherwise it is
	// the time tick of the write, or for a write with an unknown outcome the
	// largest time tick at which it may be in the WAL. It is guarded by
	// inflightSet.mu.
	tt uint64
	// done is closed when tt is set or the write is removed.
	done chan struct{}
}

// enqueue adds one write of pks to the end of the queue of each key in one step
// and returns it with the pending writes ahead of it. The step is atomic under
// the mutex, so the enqueues of overlapping writes are totally ordered and a write waits
// only for writes enqueued before it: the waits form no cycle. Per key it is
// enough to wait for the last pending write, because that write waits itself for
// the ones before it.
//
// TODO: lock escalation, a vchannel-wide exclusion for a write above some number
// of keys, is open, and so is the threshold (write-path.md, open items).
// TODO: the total budget of the set and what to do above it (back pressure or a
// degraded write) are open.
func (s *inflightSet) enqueue(pks []PK) (*inflightWrite, []*inflightWrite) {
	w := &inflightWrite{keys: lockKeys(pks), done: make(chan struct{})}

	// A key without other writes, the common case, gets a queue of one slot of
	// this array, so a write allocates once and not once per key. Each slot has
	// capacity 1, so an append to a queue never writes into its neighbor.
	slots := make([]*inflightWrite, len(w.keys))
	var ahead []*inflightWrite
	s.mu.Lock()
	for i, key := range w.keys {
		queue := s.keys[key]
		if len(queue) == 0 {
			queue = slots[i : i : i+1]
		}
		for j := len(queue) - 1; j >= 0; j-- {
			if queue[j].tt == 0 {
				if !slices.Contains(ahead, queue[j]) {
					ahead = append(ahead, queue[j])
				}
				break
			}
		}
		if s.keys == nil {
			s.keys = make(map[uint64][]*inflightWrite)
		}
		s.keys[key] = append(queue, w)
	}
	s.mu.Unlock()
	return w, ahead
}

// waitAhead blocks until every write in ahead is committed, aborted or in doubt. When ctx
// ends first, w keeps its place in line until those writes are done and is
// removed after them: a later write that found w waits only for w, not for the
// writes w was waiting for.
func (s *inflightSet) waitAhead(ctx context.Context, w *inflightWrite, ahead []*inflightWrite) error {
	for _, e := range ahead {
		select {
		case <-e.done:
		case <-ctx.Done():
			go func() {
				for _, e := range ahead {
					<-e.done
				}
				s.abort(w)
			}()
			return ctx.Err()
		}
	}
	return nil
}

// keysAhead returns the lock keys of w that have a write enqueued before w,
// nil when none has. Such a write is ahead of w in the queue of the key, and
// w is still in each of its queues. After waitAhead no such write is pending, so each
// one is committed or in doubt: both count as present. Writes
// enqueued after w are ignored, they wait for w and take a larger time tick.
func (s *inflightSet) keysAhead(w *inflightWrite) map[uint64]struct{} {
	var present map[uint64]struct{}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range w.keys {
		if slices.Index(s.keys[key], w) > 0 {
			if present == nil {
				present = make(map[uint64]struct{})
			}
			present[key] = struct{}{}
		}
	}
	return present
}

// commit records the time tick of w after its append returned and wakes the
// writes that wait for it. When observe already passed tt, observe may have
// skipped w, so w removes itself.
func (s *inflightSet) commit(w *inflightWrite, tt uint64) {
	s.mu.Lock()
	w.tt = tt
	if tt <= s.observed {
		s.removeLocked(w)
	}
	s.mu.Unlock()
	close(w.done)
}

// abort drops a pending write that is known not to be in the WAL.
func (s *inflightSet) abort(w *inflightWrite) {
	s.mu.Lock()
	s.removeLocked(w)
	s.mu.Unlock()
	close(w.done)
}

// markInDoubt keeps a pending write whose append failed but may be in the WAL
// at a time tick up to tt. Its keys count as present until observe passes tt,
// and it no longer blocks other writes.
func (s *inflightSet) markInDoubt(w *inflightWrite, tt uint64) {
	s.mu.Lock()
	w.tt = tt
	if tt <= s.observed {
		s.removeLocked(w)
	} else {
		s.inDoubt = append(s.inDoubt, w)
	}
	s.mu.Unlock()
	close(w.done)
}

// observe is called after every write up to tt was applied to the engine
// (rule A). pks are the keys of the write at tt, none for a time tick that
// writes no key. It raises the observed time tick, removes the in-doubt writes
// up to it, then removes the committed writes of pks up to tt.
//
// TODO: rule A needs an engine write to be visible to reads as soon as it
// returns. A Pebble batch commit should give that; confirm it with a test when
// observe writes the Pebble engine.
func (s *inflightSet) observe(tt uint64, pks []PK) {
	keys := lockKeys(pks)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raiseObservedLocked(tt)
	s.removeInDoubtLocked()
	for _, key := range keys {
		queue, ok := s.keys[key]
		if !ok {
			continue
		}
		kept := queue[:0]
		for _, w := range queue {
			if w.tt == 0 || w.tt > tt {
				kept = append(kept, w)
			}
		}
		clear(queue[len(kept):])
		s.setQueue(key, kept)
	}
}

// raiseObservedLocked stores tt as the observed time tick unless it is lower,
// and wakes the waits of waitObserved.
func (s *inflightSet) raiseObservedLocked(tt uint64) {
	if tt > s.observed {
		s.observed = tt
		if s.raised != nil {
			close(s.raised)
			s.raised = nil
		}
	}
}

// waitObserved blocks until the observed time tick is at least tt, ctx ends or
// stop is closed.
func (s *inflightSet) waitObserved(ctx context.Context, tt uint64, stop <-chan struct{}, vchannel string) error {
	for {
		s.mu.Lock()
		if s.observed >= tt {
			s.mu.Unlock()
			return nil
		}
		if s.raised == nil {
			s.raised = make(chan struct{})
		}
		raised := s.raised
		s.mu.Unlock()
		select {
		case <-raised:
		case <-ctx.Done():
			return ctx.Err()
		case <-stop:
			return errClosed(vchannel)
		}
	}
}

// removeInDoubtLocked removes the in-doubt writes whose time tick observe
// passed.
func (s *inflightSet) removeInDoubtLocked() {
	kept := s.inDoubt[:0]
	for _, w := range s.inDoubt {
		if w.tt <= s.observed {
			s.removeLocked(w)
		} else {
			kept = append(kept, w)
		}
	}
	clear(s.inDoubt[len(kept):])
	s.inDoubt = kept
}

// lockKeys returns the distinct lock keys of pks, ascending.
func lockKeys(pks []PK) []uint64 {
	keys := make([]uint64, len(pks))
	for i, pk := range pks {
		keys[i] = pk.lockKey()
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func (s *inflightSet) observedTimeTick() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observed
}

// len returns the number of key entries in the set.
func (s *inflightSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, queue := range s.keys {
		n += len(queue)
	}
	return n
}

// removeLocked drops w from the queue of each of its keys that still holds it.
func (s *inflightSet) removeLocked(w *inflightWrite) {
	for _, key := range w.keys {
		queue := s.keys[key]
		if i := slices.Index(queue, w); i >= 0 {
			s.setQueue(key, slices.Delete(queue, i, i+1))
		}
	}
}

func (s *inflightSet) setQueue(key uint64, queue []*inflightWrite) {
	if len(queue) > 0 {
		s.keys[key] = queue
		return
	}
	delete(s.keys, key)
	if len(s.keys) == 0 {
		s.keys = nil
	}
}
