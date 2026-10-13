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

package index

import (
	"container/heap"
	"math"
)

// LastAcked returns the time tick through which the primary key indexes of
// this pchannel are committed. A vchannel without unpublished writes does not
// hold it back. It returns math.MaxUint64 when the pchannel has no index.
//
// The value is min(lastAppliedTimeTick, LastCommittedTimeTick of every dirty
// Index). An Index is dirty when it has writes since its last rotate, frozen
// generations, or a pending handover.
//
// The value does not go down while the pchannel has an index. An idle Index
// does not hold the value back, and its first new write makes it dirty again
// with its old LastCommittedTimeTick. That write has a time tick above the
// value that was already returned, and the Index did not change between its
// commit and that write, so the returned value stays correct. LastAcked keeps
// the largest returned value and never returns less.
//
// An Index that is not fresh never joins the heap, so it does not hold the
// value back.
//
// TODO: the clamp to ackedMax can report too much. An Index that joins the
// heap after a larger value was returned, such as one opened while the
// pchannel already had indexes, has a LastCommittedTimeTick below that value.
// Recovery must handle this before the WAL truncation uses LastAcked.
func (c *Channel) LastAcked() uint64 {
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
// does not change ackedMax. The caller makes sure that the Channel has an index.
func (c *Channel) lastAckedUnclamped() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAckedLocked()
}

// lastAckedLocked computes LastAcked from the heap, without the clamp. The
// caller holds mu.
func (c *Channel) lastAckedLocked() uint64 {
	v := c.wm.lastApplied()
	if len(c.acked) > 0 && c.acked[0].heapKey < v {
		v = c.acked[0].heapKey
	}
	return v
}

// The dirty Indexes are kept in a min-heap by LastCommittedTimeTick.
// Membership changes at three places:
//
//   - The first write after the dirty flag is cleared adds the Index
//     (trackDirty). The writer sets the flag first and then takes mu.
//   - The end of every handover and of the claim refreshes the Index
//     (refreshAcked). It reads the flag under mu. If it reads false, no
//     write set the flag yet, and that write adds the Index later. If it
//     reads true, the Index stays. So a dirty Index is never left out.
//   - Remove and Close delete the Index.
//
// An Index that is not fresh never stays in the heap: trackDirty skips it,
// and refreshAcked removes it.
//
// The heap key is the LastCommittedTimeTick at the last refresh. The real
// value only goes up between refreshes, so the key is a lower bound and the
// result errs on the safe side.

// trackDirty adds idx to the heap after its first write since the dirty flag
// was cleared.
func (c *Channel) trackDirty(idx *Index) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx.removed || idx.heapPos >= 0 || !idx.fresh() {
		return
	}
	idx.heapKey = idx.LastCommittedTimeTick()
	heap.Push(&c.acked, idx)
}

// refreshAcked removes idx from the heap when it is no longer dirty or not
// fresh, and otherwise updates its key.
func (c *Channel) refreshAcked(idx *Index) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx.removed {
		return
	}
	if !idx.dirty() || !idx.fresh() {
		c.acked.remove(idx)
		return
	}
	idx.heapKey = idx.LastCommittedTimeTick()
	if idx.heapPos >= 0 {
		heap.Fix(&c.acked, idx.heapPos)
		return
	}
	heap.Push(&c.acked, idx)
}

// ackHeap is a min-heap of Indexes by heapKey. It implements heap.Interface.
type ackHeap []*Index

func (h ackHeap) Len() int           { return len(h) }
func (h ackHeap) Less(i, j int) bool { return h[i].heapKey < h[j].heapKey }

func (h ackHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapPos = i
	h[j].heapPos = j
}

func (h *ackHeap) Push(x any) {
	idx := x.(*Index)
	idx.heapPos = len(*h)
	*h = append(*h, idx)
}

func (h *ackHeap) Pop() any {
	old := *h
	n := len(old)
	idx := old[n-1]
	old[n-1] = nil
	idx.heapPos = -1
	*h = old[:n-1]
	return idx
}

// remove deletes idx from the heap if it is in it.
func (h *ackHeap) remove(idx *Index) {
	if idx.heapPos >= 0 {
		heap.Remove(h, idx.heapPos)
	}
}
