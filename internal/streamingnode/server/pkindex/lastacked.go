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

package pkindex

import (
	"container/heap"
	"math"

	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
)

// LastAcked returns the time tick through which the primary key indexes of
// this pchannel are committed. A vchannel without unpublished writes does not
// hold it back. It returns math.MaxUint64 when the pchannel has no index or
// the node has no Committer. In the no-Committer case it returns before it
// reads or updates ackedMax.
//
// The value is min(lastApplied of the pchannel watermark, LastCommittedTimeTick
// of every dirty Index). An Index is dirty when it has writes since its last
// rotate, frozen generations, or a pending handover. An Index of a node
// without a Committer never joins the heap.
//
// The value does not go down while the pchannel has an index: LastAcked keeps
// the largest value it returned in ackedMax and never returns less. The clamp
// never reports too much, for two reasons. First, every value returned before
// is at most the lastApplied of the watermark at that time. Second, when
// lastApplied is V, every writer that entered the watermark before tick V has
// ended, and markDirty, which adds the Index to the heap through OnDirtied,
// runs inside the write before the writer ends. So every write with a time
// tick at or below V either joined the heap already or is committed, and a
// writer that enters after tick V gets a time tick above V. An Index that
// joins the heap with a key below ackedMax therefore has only writes above
// ackedMax, and ackedMax stays a safe answer.
func (c *PChannelIndexManager) LastAcked() uint64 {
	if c.m.cfg.Committer == nil {
		// Nothing ever commits, so the index never holds the WAL truncation back.
		return math.MaxUint64
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.indexes) == 0 {
		return math.MaxUint64
	}
	v := c.lastAckedLocked()
	if v < c.ackedMax {
		return c.ackedMax
	}
	c.ackedMax = v
	return v
}

// lastAckedUnclamped returns LastAcked without the clamp to ackedMax, and
// does not change ackedMax. It sees an Index whose key is below ackedMax,
// which the clamp hides: the truncation does not wait for that Index, its
// uncommitted writes are above ackedMax; the handover triggers that compare
// with the WAL tail use this value. The caller makes sure that the
// PChannelIndexManager has an index.
func (c *PChannelIndexManager) lastAckedUnclamped() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAckedLocked()
}

// lastAckedLocked computes LastAcked from the heap, without the clamp. The
// caller holds mu.
func (c *PChannelIndexManager) lastAckedLocked() uint64 {
	v := c.wm.lastApplied()
	if k, ok := c.acked.min(); ok && k < v {
		v = k
	}
	return v
}

// The dirty Indexes are kept in a min-heap by LastCommittedTimeTick.
// Membership changes at three places:
//
//   - The first write after the dirty flag is cleared adds the Index
//     (trackDirty). The writer sets the flag first and then takes mu.
//   - The end of every handover and of the manifest lookup refreshes the Index
//     (refreshAcked). It reads the flag under mu. If it reads false, no
//     write set the flag yet, and that write adds the Index later. If it
//     reads true, the Index stays. So a dirty Index is never left out.
//   - Remove and Close delete the Index.
//
// An Index of a node without a Committer never stays in the heap: trackDirty
// skips it, and refreshAcked removes it.
//
// The heap key is the LastCommittedTimeTick at the last refresh. The real
// value only goes up between refreshes, so the key is a lower bound and the
// result errs on the safe side.

// trackDirty adds idx to the heap after its first write since the dirty flag
// was cleared.
func (c *PChannelIndexManager) trackDirty(idx *index.Index) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.attachedLocked(idx) || c.acked.contains(idx) || !idx.HasCommitter() {
		return
	}
	c.acked.update(idx, idx.LastCommittedTimeTick())
}

// refreshAcked removes idx from the heap when it is no longer dirty or not
// fresh, and otherwise updates its key.
func (c *PChannelIndexManager) refreshAcked(idx *index.Index) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.attachedLocked(idx) {
		return
	}
	if !idx.Dirty() || !idx.HasCommitter() {
		c.acked.remove(idx)
		return
	}
	c.acked.update(idx, idx.LastCommittedTimeTick())
}

// dirtyIndexes returns the members of the heap: the dirty and fresh Indexes
// of the pchannel. The driver picks its handover candidates from them, so a
// step costs the number of dirty Indexes, not the number of Indexes.
func (c *PChannelIndexManager) dirtyIndexes() []*index.Index {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acked.members()
}

// ackTracker is a min-heap of Indexes by their key, the LastCommittedTimeTick
// at the last refresh. It keeps the position of every member itself, so an
// Index carries nothing of the heap. The PChannelIndexManager guards it with mu.
type ackTracker struct {
	items []ackItem
	pos   map[*index.Index]int
}

type ackItem struct {
	idx *index.Index
	key uint64
}

// contains reports whether idx is a member.
func (t *ackTracker) contains(idx *index.Index) bool {
	_, ok := t.pos[idx]
	return ok
}

// update sets the key of idx, and adds idx when it is not a member.
func (t *ackTracker) update(idx *index.Index, key uint64) {
	if i, ok := t.pos[idx]; ok {
		t.items[i].key = key
		heap.Fix(t, i)
		return
	}
	heap.Push(t, ackItem{idx: idx, key: key})
}

// remove deletes idx if it is a member.
func (t *ackTracker) remove(idx *index.Index) {
	if i, ok := t.pos[idx]; ok {
		heap.Remove(t, i)
	}
}

// min returns the smallest key, and false when the heap is empty.
func (t *ackTracker) min() (uint64, bool) {
	if len(t.items) == 0 {
		return 0, false
	}
	return t.items[0].key, true
}

// members returns the Indexes in the heap, in no particular order.
func (t *ackTracker) members() []*index.Index {
	out := make([]*index.Index, len(t.items))
	for i, it := range t.items {
		out[i] = it.idx
	}
	return out
}

// heap.Interface. Only the methods above call it.

func (t *ackTracker) Len() int           { return len(t.items) }
func (t *ackTracker) Less(i, j int) bool { return t.items[i].key < t.items[j].key }

func (t *ackTracker) Swap(i, j int) {
	t.items[i], t.items[j] = t.items[j], t.items[i]
	t.pos[t.items[i].idx] = i
	t.pos[t.items[j].idx] = j
}

func (t *ackTracker) Push(x any) {
	it := x.(ackItem)
	if t.pos == nil {
		t.pos = make(map[*index.Index]int)
	}
	t.pos[it.idx] = len(t.items)
	t.items = append(t.items, it)
}

func (t *ackTracker) Pop() any {
	n := len(t.items)
	it := t.items[n-1]
	t.items[n-1] = ackItem{}
	t.items = t.items[:n-1]
	delete(t.pos, it.idx)
	return it
}
