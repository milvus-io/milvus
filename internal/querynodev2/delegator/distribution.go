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

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
)

const (
	// wildcardNodeID matches any nodeID, used for force distribution correction.
	wildcardNodeID       = int64(-1)
	initialTargetVersion = int64(0)
)

var (
	closedCh  chan struct{}
	closeOnce sync.Once
)

func getClosedCh() chan struct{} {
	closeOnce.Do(func() {
		closedCh = make(chan struct{})
		close(closedCh)
	})
	return closedCh
}

// distribution is the struct to store segment distribution.
// it contains both growing and sealed segments.
type distribution struct {
	// segments information
	// map[SegmentID]=>segmentEntry
	targetVersion   *atomic.Int64
	growingSegments map[UniqueID]SegmentEntry
	sealedSegments  map[UniqueID]SegmentEntry

	// snapshotVersion indicator
	snapshotVersion int64
	// quick flag for current snapshot is serviceable
	serviceable *atomic.Bool
	offlines    typeutil.Set[int64]

	snapshots *typeutil.ConcurrentMap[int64, *snapshot]
	// current is the snapshot for quick usage for search/query
	// generated for each change of distribution
	current *atomic.Pointer[snapshot]
	// protects current & segments
	mut sync.RWMutex
}

// SegmentEntry stores the segment meta information.
type SegmentEntry struct {
	NodeID        int64
	SegmentID     UniqueID
	PartitionID   UniqueID
	Version       int64
	TargetVersion int64
}

// NewDistribution creates a new distribution instance with all field initialized.
func NewDistribution() *distribution {
	dist := &distribution{
		serviceable:     atomic.NewBool(false),
		growingSegments: make(map[UniqueID]SegmentEntry),
		sealedSegments:  make(map[UniqueID]SegmentEntry),
		snapshots:       typeutil.NewConcurrentMap[int64, *snapshot](),
		current:         atomic.NewPointer[snapshot](nil),
		offlines:        typeutil.NewSet[int64](),
		targetVersion:   atomic.NewInt64(initialTargetVersion),
	}

	dist.genSnapshot()
	return dist
}

// GetAllSegments returns segments in current snapshot, filter readable segment when readable is true
func (d *distribution) GetSegments(readable bool, partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry, version int64) {
	d.mut.RLock()
	defer d.mut.RUnlock()

	current := d.current.Load()
	sealed, growing = current.Get(partitions...)
	version = current.version

	if readable {
		TargetVersion := current.GetTargetVersion()
		sealed, growing = d.filterReadableSegments(sealed, growing, TargetVersion)
		return
	}

	return
}

func (d *distribution) filterReadableSegments(sealed []SnapshotItem, growing []SegmentEntry, targetVersion int64) ([]SnapshotItem, []SegmentEntry) {
	filterReadable := func(entry SegmentEntry, _ int) bool {
		return entry.TargetVersion == targetVersion || entry.TargetVersion == initialTargetVersion
	}

	growing = lo.Filter(growing, filterReadable)
	sealed = lo.Map(sealed, func(item SnapshotItem, _ int) SnapshotItem {
		return SnapshotItem{
			NodeID:   item.NodeID,
			Segments: lo.Filter(item.Segments, filterReadable),
		}
	})

	return sealed, growing
}

// PeekAllSegments returns current snapshot without increasing inuse count
// show only used by GetDataDistribution.
func (d *distribution) PeekSegments(readable bool, partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry) {
	current := d.current.Load()
	sealed, growing = current.Peek(partitions...)

	if readable {
		TargetVersion := current.GetTargetVersion()
		sealed, growing = d.filterReadableSegments(sealed, growing, TargetVersion)
		return
	}

	return
}

// FinishUsage notifies snapshot one reference is released.
func (d *distribution) FinishUsage(version int64) {
	snapshot, ok := d.snapshots.Get(version)
	if ok {
		snapshot.Done(d.getCleanup(snapshot.version))
	}
}

func (d *distribution) getTargetVersion() int64 {
	current := d.current.Load()
	return current.GetTargetVersion()
}

// Serviceable returns wether current snapshot is serviceable.
func (d *distribution) Serviceable() bool {
	return d.serviceable.Load()
}

// AddDistributions add multiple segment entries.
func (d *distribution) AddDistributions(entries ...SegmentEntry) {
	d.mut.Lock()
	defer d.mut.Unlock()

	for _, entry := range entries {
		d.sealedSegments[entry.SegmentID] = entry
		d.offlines.Remove(entry.SegmentID)
	}

	d.genSnapshot()
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

// AddOffline set segmentIDs to offlines.
func (d *distribution) AddOfflines(segmentIDs ...int64) {
	d.mut.Lock()
	defer d.mut.Unlock()

	updated := false
	for _, segmentID := range segmentIDs {
		_, ok := d.sealedSegments[segmentID]
		if !ok {
			continue
		}
		updated = true
		d.offlines.Insert(segmentID)
	}

	if updated {
		d.genSnapshot()
	}
}

// UpdateTargetVersion update readable segment version
func (d *distribution) SyncTargetVersion(newVersion int64, growingInTarget []int64, sealedInTarget []int64) {
	d.mut.Lock()
	defer d.mut.Unlock()
	for _, segmentID := range growingInTarget {
		entry, ok := d.growingSegments[segmentID]
		if !ok {
			log.Error("readable growing segment lost, make it unserviceable",
				zap.Int64("segmentID", segmentID))
			d.serviceable.Store(false)
			continue
		}
		entry.TargetVersion = newVersion
		d.growingSegments[segmentID] = entry
	}

	for _, segmentID := range sealedInTarget {
		entry, ok := d.sealedSegments[segmentID]
		if !ok {
			log.Error("readable sealed segment lost, make it unserviceable",
				zap.Int64("segmentID", segmentID))
			d.serviceable.Store(false)
			continue
		}
		entry.TargetVersion = newVersion
		d.sealedSegments[segmentID] = entry
	}

	oldValue := d.targetVersion.Load()
	d.targetVersion.Store(newVersion)
	d.genSnapshot()
	log.Info("Update readable segment version",
		zap.Int64("oldVersion", oldValue),
		zap.Int64("newVersion", newVersion),
		zap.Int64s("growing", growingInTarget),
		zap.Int64s("sealed", sealedInTarget),
	)
}

// RemoveDistributions remove segments distributions and returns the clear signal channel.
func (d *distribution) RemoveDistributions(sealedSegments []SegmentEntry, growingSegments []SegmentEntry) chan struct{} {
	d.mut.Lock()
	defer d.mut.Unlock()

	changed := false
	for _, sealed := range sealedSegments {
		if d.offlines.Contain(sealed.SegmentID) {
			d.offlines.Remove(sealed.SegmentID)
			changed = true
		}
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
		return getClosedCh()
	}

	return d.genSnapshot()
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

	growing := make([]SegmentEntry, 0, len(d.growingSegments))
	for _, entry := range d.growingSegments {
		growing = append(growing, entry)
	}

	d.serviceable.Store(d.offlines.Len() == 0)

	// stores last snapshot
	// ok to be nil
	last := d.current.Load()
	// update snapshot version
	d.snapshotVersion++
	newSnapShot := NewSnapshot(dist, growing, last, d.snapshotVersion, d.targetVersion.Load())
	d.current.Store(newSnapShot)
	// shall be a new one
	d.snapshots.GetOrInsert(d.snapshotVersion, newSnapShot)

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
