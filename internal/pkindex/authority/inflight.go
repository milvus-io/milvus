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

package authority

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
)

// inflightShardCount is the number of shards of an in-flight set. A shard is a
// mutex and a map. It is held only while a map is read or changed, never across
// an append, so the count only spreads mutex contention and has no effect on
// correctness.
const inflightShardCount = 64

// inflightSet holds the writes of one vchannel that were decided but are not
// yet applied to the engine by observe. It answers one question, whether a key
// exists, and doubles as the exact primary key lock of the write path.
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
// the observed time tick before it removes entries, and a writer sets its time
// tick before it reads the observed time tick. Both use sequentially consistent
// atomics, so at least one side sees the other and removes the write.
type inflightSet struct {
	shards [inflightShardCount]inflightShard
	// observed is the highest time tick that observe has applied to the engine.
	observed atomic.Uint64
	// seq orders registrations. It is taken under the shard mutexes of the write,
	// so two writes of one key always see their seq in registration order.
	seq atomic.Uint64

	// uncertainMu guards uncertain: the writes whose append failed with an
	// unknown outcome. Observe removes them by time tick, because such a write
	// may never reach the WAL and then observe never meets its keys.
	uncertainMu sync.Mutex
	uncertain   []*inflightWrite

	// Test hooks: afterObservedStore runs in observe between the store of the
	// observed time tick and the removal, afterFill runs between the store of a
	// write's time tick, by a fill-in or an uncertain mark, and the read of the
	// observed time tick.
	afterObservedStore func()
	afterFill          func()
}

type inflightShard struct {
	mu sync.Mutex
	// keys is nil when empty, because a Go map never shrinks after deletes.
	keys map[uint64][]*inflightWrite // lock key -> writes in registration order
}

// inflightWrite is one write of one or more keys. All keys of the write share it.
type inflightWrite struct {
	seq       uint64
	keys      []uint64 // distinct lock keys, sorted by shard
	keyShards []int    // the shard of each key
	shards    []int    // the distinct shards of keys, ascending
	// tt is 0 while the append has not returned: the write then is the lock of
	// its keys, and a later write of the same key waits for it. Otherwise it is
	// the time tick of the write, or for a write with an unknown outcome the
	// largest time tick at which it may be in the WAL.
	tt atomic.Uint64
	// done is closed when tt is set or the write is removed.
	done chan struct{}
}

// shardOf maps a lock key to a shard. An int64 key is its own lock key, so the
// splitmix64 finalizer mixes the high bits into the low bits first: keys that
// are multiples of the shard count would all land on one shard otherwise.
func shardOf(key uint64) int {
	z := key + 0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return int((z ^ (z >> 31)) % inflightShardCount)
}

// register adds one write of pks to the set in one step and returns it with the
// pending writes it must wait for. All involved shard mutexes are taken together
// in ascending order, so registrations of overlapping writes are totally ordered
// and a write waits only for writes registered before it: the waits form no
// cycle. Per key it is enough to wait for the last pending write, because that
// write waits itself for the ones before it.
//
// TODO: lock escalation, a vchannel-wide exclusion for a write above some number
// of keys, is open, and so is the threshold (write-path.md, open items).
// TODO: the total budget of the set and what to do above it (back pressure or a
// degraded write) are open.
func (s *inflightSet) register(pks []PK) (*inflightWrite, []*inflightWrite) {
	w := &inflightWrite{done: make(chan struct{})}
	w.keys, w.keyShards = groupByShard(pks)
	for _, shard := range w.keyShards {
		if n := len(w.shards); n == 0 || w.shards[n-1] != shard {
			w.shards = append(w.shards, shard)
		}
	}

	// A key without other writes, the common case, gets a queue of one slot of
	// this array, so a write allocates once and not once per key. Each slot has
	// capacity 1, so an append to a queue never writes into its neighbor.
	slots := make([]*inflightWrite, len(w.keys))
	var earlier []*inflightWrite
	s.lock(w)
	w.seq = s.seq.Add(1)
	for i, key := range w.keys {
		shard := &s.shards[w.keyShards[i]]
		queue := shard.keys[key]
		if len(queue) == 0 {
			queue = slots[i : i : i+1]
		}
		for j := len(queue) - 1; j >= 0; j-- {
			if queue[j].tt.Load() == 0 {
				if !slices.Contains(earlier, queue[j]) {
					earlier = append(earlier, queue[j])
				}
				break
			}
		}
		if shard.keys == nil {
			shard.keys = make(map[uint64][]*inflightWrite)
		}
		shard.keys[key] = append(queue, w)
	}
	s.unlock(w)
	return w, earlier
}

// wait blocks until every write in earlier is filled in or removed. When ctx
// ends first, w keeps its place in line until those writes are done and is
// removed after them: a later write that found w waits only for w, not for the
// writes w was waiting for.
func (s *inflightSet) wait(ctx context.Context, w *inflightWrite, earlier []*inflightWrite) error {
	for _, e := range earlier {
		select {
		case <-e.done:
		case <-ctx.Done():
			go func() {
				for _, e := range earlier {
					<-e.done
				}
				s.remove(w)
			}()
			return ctx.Err()
		}
	}
	return nil
}

// presentBefore returns the lock keys of w that have a write registered before
// w, nil when none has. After wait no such write is pending, so each one is
// filled in or has an unknown outcome: both count as present. Writes registered
// after w are ignored, they wait for w and take a larger time tick.
func (s *inflightSet) presentBefore(w *inflightWrite) map[uint64]struct{} {
	var present map[uint64]struct{}
	for start := 0; start < len(w.keys); {
		end := start
		for end < len(w.keys) && w.keyShards[end] == w.keyShards[start] {
			end++
		}
		shard := &s.shards[w.keyShards[start]]
		shard.mu.Lock()
		for _, key := range w.keys[start:end] {
			for _, other := range shard.keys[key] {
				if other.seq < w.seq {
					if present == nil {
						present = make(map[uint64]struct{})
					}
					present[key] = struct{}{}
					break
				}
			}
		}
		shard.mu.Unlock()
		start = end
	}
	return present
}

// fill records the time tick of w after its append returned and wakes the
// writes that wait for it. It takes no shard mutex, except when observe already
// passed tt: observe may have skipped w, so w removes itself.
func (s *inflightSet) fill(w *inflightWrite, tt uint64) {
	w.tt.Store(tt)
	close(w.done)
	s.removeIfObserved(w, tt)
}

// remove drops a pending write that is known not to be in the WAL.
func (s *inflightSet) remove(w *inflightWrite) {
	s.lock(w)
	s.removeLocked(w)
	s.unlock(w)
	close(w.done)
}

// markUncertain keeps a pending write whose append failed but may be in the WAL
// at a time tick up to tt. Its keys count as present until observe passes tt,
// and it no longer blocks other writes.
func (s *inflightSet) markUncertain(w *inflightWrite, tt uint64) {
	w.tt.Store(tt)
	s.uncertainMu.Lock()
	s.uncertain = append(s.uncertain, w)
	s.uncertainMu.Unlock()
	close(w.done)
	s.removeIfObserved(w, tt)
}

// removeIfObserved removes w when observe already passed tt, the time tick that
// the caller stored in w.
func (s *inflightSet) removeIfObserved(w *inflightWrite, tt uint64) {
	if s.afterFill != nil {
		s.afterFill()
	}
	if tt > s.observed.Load() {
		return
	}
	s.lock(w)
	s.removeLocked(w)
	s.unlock(w)
}

// observe is called after the writes of pks with time tick tt were applied to
// the engine (rule A). It raises the observed time tick, then removes the
// filled-in writes of those keys up to tt.
//
// TODO: rule A needs an engine write to be visible to reads as soon as it
// returns. A Pebble batch commit should give that; confirm it with a test when
// observe writes the Pebble engine.
func (s *inflightSet) observe(tt uint64, pks []PK) {
	s.raiseObserved(tt)
	if s.afterObservedStore != nil {
		s.afterObservedStore()
	}
	s.removeUncertain()
	s.removeFilled(tt, pks)
}

// removeFilled removes the filled-in writes of pks up to tt.
func (s *inflightSet) removeFilled(tt uint64, pks []PK) {
	keys, keyShards := groupByShard(pks)
	for start := 0; start < len(keys); {
		end := start
		for end < len(keys) && keyShards[end] == keyShards[start] {
			end++
		}
		shard := &s.shards[keyShards[start]]
		shard.mu.Lock()
		for _, key := range keys[start:end] {
			queue, ok := shard.keys[key]
			if !ok {
				continue
			}
			kept := queue[:0]
			for _, w := range queue {
				if wtt := w.tt.Load(); wtt == 0 || wtt > tt {
					kept = append(kept, w)
				}
			}
			clear(queue[len(kept):])
			shard.setQueue(key, kept)
		}
		shard.mu.Unlock()
		start = end
	}
}

// observeTimeTick is observe of a time tick that writes no key: it raises the
// observed time tick to tt and removes the writes with an unknown outcome up to
// it. Observe calls it after it applied every write up to tt to the engine.
func (s *inflightSet) observeTimeTick(tt uint64) {
	s.raiseObserved(tt)
	if s.afterObservedStore != nil {
		s.afterObservedStore()
	}
	s.removeUncertain()
}

// raiseObserved stores tt as the observed time tick unless it is lower. Observe
// must call it before it removes any write of tt, see inflightSet.
func (s *inflightSet) raiseObserved(tt uint64) {
	for {
		cur := s.observed.Load()
		if tt <= cur || s.observed.CompareAndSwap(cur, tt) {
			return
		}
	}
}

// removeUncertain removes the writes with an unknown outcome whose time tick
// observe passed.
func (s *inflightSet) removeUncertain() {
	observed := s.observed.Load()
	var expired []*inflightWrite
	s.uncertainMu.Lock()
	kept := s.uncertain[:0]
	for _, w := range s.uncertain {
		if w.tt.Load() <= observed {
			expired = append(expired, w)
		} else {
			kept = append(kept, w)
		}
	}
	clear(s.uncertain[len(kept):])
	s.uncertain = kept
	s.uncertainMu.Unlock()
	for _, w := range expired {
		s.lock(w)
		s.removeLocked(w)
		s.unlock(w)
	}
}

// groupByShard returns the distinct lock keys of pks ordered by shard, and the
// shard of each, with a counting sort over the shards.
func groupByShard(pks []PK) ([]uint64, []int) {
	var counts [inflightShardCount + 1]int
	keys := make([]uint64, len(pks))
	shards := make([]int, len(pks))
	for i, pk := range pks {
		keys[i] = pk.lockKey()
		shards[i] = shardOf(keys[i])
		counts[shards[i]+1]++
	}
	for i := 1; i <= inflightShardCount; i++ {
		counts[i] += counts[i-1]
	}
	sorted := make([]uint64, len(pks))
	next := counts
	for i, key := range keys {
		sorted[next[shards[i]]] = key
		next[shards[i]]++
	}
	// Remove the duplicates of each shard. The keys of one shard are few, so a
	// sort per shard is cheap.
	out := sorted[:0]
	keyShards := shards[:0]
	for shard := 0; shard < inflightShardCount; shard++ {
		bucket := sorted[counts[shard]:counts[shard+1]]
		if len(bucket) == 0 {
			continue
		}
		slices.Sort(bucket)
		for j, key := range bucket {
			if j > 0 && key == bucket[j-1] {
				continue
			}
			out = append(out, key)
			keyShards = append(keyShards, shard)
		}
	}
	return out, keyShards
}

func (s *inflightSet) observedTimeTick() uint64 {
	return s.observed.Load()
}

// len returns the number of key entries in the set.
func (s *inflightSet) len() int {
	n := 0
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for _, queue := range shard.keys {
			n += len(queue)
		}
		shard.mu.Unlock()
	}
	return n
}

func (s *inflightSet) lock(w *inflightWrite) {
	for _, shard := range w.shards {
		s.shards[shard].mu.Lock()
	}
}

func (s *inflightSet) unlock(w *inflightWrite) {
	for i := len(w.shards) - 1; i >= 0; i-- {
		s.shards[w.shards[i]].mu.Unlock()
	}
}

// removeLocked drops w from the queue of each of its keys that still holds it.
func (s *inflightSet) removeLocked(w *inflightWrite) {
	for i, key := range w.keys {
		shard := &s.shards[w.keyShards[i]]
		queue := shard.keys[key]
		if i := slices.Index(queue, w); i >= 0 {
			shard.setQueue(key, slices.Delete(queue, i, i+1))
		}
	}
}

func (sh *inflightShard) setQueue(key uint64, queue []*inflightWrite) {
	if len(queue) > 0 {
		sh.keys[key] = queue
		return
	}
	delete(sh.keys, key)
	if len(sh.keys) == 0 {
		sh.keys = nil
	}
}
