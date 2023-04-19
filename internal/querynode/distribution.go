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

package querynode

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// wildcardNodeID matches any nodeID, used for force distribution correction.
	wildcardNodeID = int64(-1)
)

// distribution is the struct to store segment distribution.
// it contains both growing and sealed segments.
type distribution struct {
	replicaID int64
	// segments information
	// map[SegmentID]=>segmentEntry
	sealedSegments map[UniqueID]SegmentEntry

	// version indicator
	version int64

	snapshots *typeutil.ConcurrentMap[int64, *snapshot]
	// current is the snapshot for quick usage for search/query
	// generated for each change of distribution
	current *atomic.Pointer[snapshot]
	// protects current & segments
	mut sync.RWMutex
}

// SegmentEntry stores the segment meta information.
type SegmentEntry struct {
	NodeID      int64
	SegmentID   UniqueID
	PartitionID UniqueID
	Version     int64
	State       segmentState
}

// Update set current entry value based on new entry.
func (s *SegmentEntry) Update(new SegmentEntry) {
	s.NodeID = new.NodeID
	s.PartitionID = new.PartitionID
	s.State = new.State
	s.Version = new.Version
	s.SegmentID = new.SegmentID
}

// NewDistribution creates a new distribution instance with all field initialized.
func NewDistribution(replicaID int64) *distribution {
	dist := &distribution{
		replicaID:      replicaID,
		sealedSegments: make(map[UniqueID]SegmentEntry),
		snapshots:      typeutil.NewConcurrentMap[int64, *snapshot](),
		current:        atomic.NewPointer[snapshot](nil),
	}

	dist.genSnapshot()
	return dist
}

func (d *distribution) getLogger() *log.MLogger {
	return log.Ctx(context.Background()).With(zap.Int64("replicaID", d.replicaID))
}

func (d *distribution) Serviceable() bool {
	d.mut.RLock()
	defer d.mut.RUnlock()
	return d.serviceableImpl()
}

// Serviceable returns whether all segment recorded is in loaded state, hold d.mut before call it
func (d *distribution) serviceableImpl() bool {
	for _, entry := range d.sealedSegments {
		if entry.State != segmentStateLoaded {
			return false
		}
	}
	return true
}

// GetCurrent returns current snapshot.
func (d *distribution) GetCurrent(partitions ...int64) (sealed []SnapshotItem, version int64) {
	d.mut.RLock()
	defer d.mut.RUnlock()
	if !d.serviceableImpl() {
		return nil, -1
	}
	current := d.current.Load()
	sealed = current.Get(partitions...)
	version = current.version
	return
}

// Peek returns current snapshot without increasing inuse count
// show only used by GetDataDistribution.
func (d *distribution) Peek(partitions ...int64) []SnapshotItem {
	current := d.current.Load()
	return current.Peek(partitions...)
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
	}

	d.genSnapshot()
	return removed
}

// UpdateDistribution updates sealed segments distribution.
func (d *distribution) UpdateDistribution(entries ...SegmentEntry) {
	d.mut.Lock()
	defer d.mut.Unlock()

	for _, entry := range entries {
		old, ok := d.sealedSegments[entry.SegmentID]
		d.getLogger().Info("Update distribution", zap.Int64("segmentID", entry.SegmentID),
			zap.Int64("node", entry.NodeID),
			zap.Bool("segment exist", ok))
		if !ok {
			d.sealedSegments[entry.SegmentID] = entry
			continue
		}
		old.Update(entry)
		d.sealedSegments[old.SegmentID] = old
	}

	d.genSnapshot()
}

// NodeDown marks segment offline in batch for node down event.
func (d *distribution) NodeDown(nodeID int64) {
	d.mut.Lock()
	defer d.mut.Unlock()

	d.getLogger().Info("handle node down", zap.Int64("node", nodeID))
	for _, entry := range d.sealedSegments {
		if entry.NodeID == nodeID && entry.State != segmentStateOffline {
			entry.State = segmentStateOffline
			d.sealedSegments[entry.SegmentID] = entry
			d.getLogger().Info("update the segment to offline since nodeDown", zap.Int64("nodeID", nodeID), zap.Int64("segmentID", entry.SegmentID))
		}
	}
}

// RemoveDistributions remove segments distributions and returns the clear signal channel,
// requires the read lock of shard cluster mut held
func (d *distribution) RemoveDistributions(releaseFn func(), sealedSegments ...SegmentEntry) {
	d.mut.Lock()
	defer d.mut.Unlock()
	for _, sealed := range sealedSegments {
		entry, ok := d.sealedSegments[sealed.SegmentID]
		d.getLogger().Info("Remove distribution", zap.Int64("segmentID", sealed.SegmentID),
			zap.Int64("node", sealed.NodeID),
			zap.Bool("segment exist", ok))
		if !ok {
			continue
		}
		if entry.NodeID == sealed.NodeID || sealed.NodeID == wildcardNodeID {
			delete(d.sealedSegments, sealed.SegmentID)
		}
	}
	ts := time.Now()
	<-d.genSnapshot()
	releaseFn()
	d.getLogger().Info("successfully remove distribution", zap.Any("segments", sealedSegments), zap.Duration("time", time.Since(ts)))
}

// getSnapshot converts current distribution to snapshot format.
// in which, user could use found nodeID=>segmentID list.
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

	// stores last snapshot
	// ok to be nil
	last := d.current.Load()
	// increase version
	d.version++
	snapshot := NewSnapshot(dist, last, d.version)
	d.current.Store(snapshot)
	// shall be a new one
	d.snapshots.GetOrInsert(d.version, snapshot)

	// first snapshot, return closed chan
	if last == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	d.getLogger().Info("gen snapshot for version", zap.Any("version", d.version), zap.Any("is serviceable", d.serviceableImpl()))
	last.Expire(d.getCleanup(last.version))

	return last.cleared
}

// getCleanup returns cleanup snapshots function.
func (d *distribution) getCleanup(version int64) snapshotCleanup {
	return func() {
		d.snapshots.GetAndRemove(version)
	}
}
