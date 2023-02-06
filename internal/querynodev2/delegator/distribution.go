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

package delegator

import (
	"sync"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	// wildcardNodeID matches any nodeID, used for force distribution correction.
	wildcardNodeID = int64(-1)
)

// distribution is the struct to store segment distribution.
// it contains both growing and sealed segments.
type distribution struct {
	// segments information
	// map[SegmentID]=>segmentEntry
	growingSegments map[UniqueID]SegmentEntry
	sealedSegments  map[UniqueID]SegmentEntry

	// version indicator
	version int64

	snapshots *typeutil.ConcurrentMap[int64, *snapshot]
	// current is the snapshot for quick usage for search/query
	// generated for each change of distribution
	current *snapshot
	// protects current & segments
	mut sync.RWMutex
}

// SegmentEntry stores the segment meta information.
type SegmentEntry struct {
	NodeID      int64
	SegmentID   UniqueID
	PartitionID UniqueID
	Version     int64
}

// NewDistribution creates a new distribution instance with all field initialized.
func NewDistribution() *distribution {
	dist := &distribution{
		growingSegments: make(map[UniqueID]SegmentEntry),
		sealedSegments:  make(map[UniqueID]SegmentEntry),
		snapshots:       typeutil.NewConcurrentMap[int64, *snapshot](),
	}

	dist.genSnapshot()
	return dist
}

// GetCurrent returns current snapshot.
func (d *distribution) GetCurrent(partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry, version int64) {
	d.mut.RLock()
	defer d.mut.RUnlock()

	sealed, growing = d.current.Get(partitions...)
	version = d.current.version
	return
}

// FinishUsage notifies snapshot one reference is released.
func (d *distribution) FinishUsage(version int64) {
	snapshot, ok := d.snapshots.Get(version)
	if ok {
		snapshot.Done(d.getCleanup(snapshot.version))
	}
}

// AddDistributions add multiple segment entries.
func (d *distribution) AddDistributions(entries ...SegmentEntry) []int64 {
	d.mut.Lock()
	defer d.mut.Unlock()

	// remove growing if sealed is loaded
	var removed []int64
	for _, entry := range entries {
		d.sealedSegments[entry.SegmentID] = entry
		_, ok := d.growingSegments[entry.SegmentID]
		if ok {
			removed = append(removed, entry.SegmentID)
			delete(d.growingSegments, entry.SegmentID)
		}
	}

	d.genSnapshot()
	return removed
}

// AddGrowing adds growing segment distribution.
func (d *distribution) AddGrowing(entries ...SegmentEntry) {
	d.mut.Lock()
	defer d.mut.Unlock()

	for _, entry := range entries {
		d.growingSegments[entry.SegmentID] = entry
	}

	d.genSnapshot()
}

// RemoveDistributions remove segments distributions and returns the clear signal channel.
func (d *distribution) RemoveDistributions(sealedSegments []SegmentEntry, growingSegments []SegmentEntry) chan struct{} {
	d.mut.Lock()
	defer d.mut.Unlock()

	changed := false
	for _, sealed := range sealedSegments {
		entry, ok := d.sealedSegments[sealed.SegmentID]
		if !ok {
			continue
		}
		if entry.NodeID == sealed.NodeID || sealed.NodeID == wildcardNodeID {
			delete(d.sealedSegments, sealed.SegmentID)
			changed = true
		}
	}

	for _, growing := range growingSegments {
		_, ok := d.growingSegments[growing.SegmentID]
		if !ok {
			continue
		}

		delete(d.growingSegments, growing.SegmentID)
		changed = true
	}

	if !changed {
		// no change made, return closed signal channel
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return d.genSnapshot()
}

// getSnapshot converts current distribution to snapshot format.
// in which, user could juse found nodeID=>segmentID list.
// mutex RLock is required before calling this method.
func (d *distribution) genSnapshot() chan struct{} {

	nodeSegments := make(map[int64][]SegmentEntry)
	for _, entry := range d.sealedSegments {
		nodeSegments[entry.NodeID] = append(nodeSegments[entry.NodeID], entry)
	}

	dist := make([]SnapshotItem, 0, len(nodeSegments))
	for nodeID, items := range nodeSegments {
		dist = append(dist, SnapshotItem{
			NodeID:   nodeID,
			Segments: items,
		})
	}

	growing := make([]SegmentEntry, 0, len(d.growingSegments))
	for _, entry := range d.growingSegments {
		growing = append(growing, entry)
	}

	// stores last snapshot
	// ok to be nil
	last := d.current
	// increase version
	d.version++
	d.current = NewSnapshot(dist, growing, last, d.version)
	// shall be a new one
	d.snapshots.GetOrInsert(d.version, d.current)

	// first snapshot, return closed chan
	if last == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	last.Expire(d.getCleanup(last.version))

	return last.cleared
}

// getCleanup returns cleanup snapshots function.
func (d *distribution) getCleanup(version int64) snapshotCleanup {
	return func() {
		d.snapshots.GetAndRemove(version)
	}
}
