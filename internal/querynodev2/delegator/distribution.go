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

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	// wildcardNodeID matches any nodeID, used for force distribution correction.
	wildcardNodeID = int64(-1)
	// for growing segment consumed from channel
	initialTargetVersion = int64(0)

	// for growing segment which not exist in target, and it's start position < max sealed dml position
	redundantTargetVersion = int64(-1)

	// for sealed segment which loaded by load segment request, should become readable after sync target version
	unreadableTargetVersion = int64(-2)
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

func (d *distribution) PinReadableSegments(partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry, version int64, err error) {
	d.mut.RLock()
	defer d.mut.RUnlock()

	if !d.Serviceable() {
		return nil, nil, -1, merr.WrapErrServiceInternal("channel distribution is not serviceable")
	}

	current := d.current.Load()
	sealed, growing = current.Get(partitions...)
	version = current.version
	targetVersion := current.GetTargetVersion()
	filterReadable := func(entry SegmentEntry, _ int) bool {
		return entry.TargetVersion == targetVersion || entry.TargetVersion == initialTargetVersion
	}
	sealed, growing = d.filterSegments(sealed, growing, filterReadable)
	return
}

func (d *distribution) PinOnlineSegments(partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry, version int64) {
	d.mut.RLock()
	defer d.mut.RUnlock()

	current := d.current.Load()
	sealed, growing = current.Get(partitions...)
	version = current.version

	filterOnline := func(entry SegmentEntry, _ int) bool {
		return !d.offlines.Contain(entry.SegmentID)
	}
	sealed, growing = d.filterSegments(sealed, growing, filterOnline)

	return
}

func (d *distribution) filterSegments(sealed []SnapshotItem, growing []SegmentEntry, filter func(SegmentEntry, int) bool) ([]SnapshotItem, []SegmentEntry) {
	growing = lo.Filter(growing, filter)
	sealed = lo.Map(sealed, func(item SnapshotItem, _ int) SnapshotItem {
		return SnapshotItem{
			NodeID:   item.NodeID,
			Segments: lo.Filter(item.Segments, filter),
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
		targetVersion := current.GetTargetVersion()
		filterReadable := func(entry SegmentEntry, _ int) bool {
			return entry.TargetVersion == targetVersion || entry.TargetVersion == initialTargetVersion
		}
		sealed, growing = d.filterSegments(sealed, growing, filterReadable)
		return
	}

	return
}

// Unpin notifies snapshot one reference is released.
func (d *distribution) Unpin(version int64) {
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
		oldEntry, ok := d.sealedSegments[entry.SegmentID]
		if ok && oldEntry.Version >= entry.Version {
			log.Warn("Invalid segment distribution changed, skip it",
				zap.Int64("segmentID", entry.SegmentID),
				zap.Int64("oldVersion", oldEntry.Version),
				zap.Int64("oldNode", oldEntry.NodeID),
				zap.Int64("newVersion", entry.Version),
				zap.Int64("newNode", entry.NodeID),
			)
			continue
		}

		if ok {
			// remain the target version for already loaded segment to void skipping this segment when executing search
			entry.TargetVersion = oldEntry.TargetVersion
		} else {
			// waiting for sync target version, to become readable
			entry.TargetVersion = unreadableTargetVersion
		}
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
func (d *distribution) SyncTargetVersion(newVersion int64, growingInTarget []int64, sealedInTarget []int64, redundantGrowings []int64) {
	d.mut.Lock()
	defer d.mut.Unlock()

	for _, segmentID := range growingInTarget {
		entry, ok := d.growingSegments[segmentID]
		if !ok {
			log.Warn("readable growing segment lost, consume from dml seems too slow",
				zap.Int64("segmentID", segmentID))
			continue
		}
		entry.TargetVersion = newVersion
		d.growingSegments[segmentID] = entry
	}

	for _, segmentID := range redundantGrowings {
		entry, ok := d.growingSegments[segmentID]
		if !ok {
			continue
		}
		entry.TargetVersion = redundantTargetVersion
		d.growingSegments[segmentID] = entry
	}

	available := true
	for _, segmentID := range sealedInTarget {
		entry, ok := d.sealedSegments[segmentID]
		if !ok {
			log.Warn("readable sealed segment lost, make it unserviceable", zap.Int64("segmentID", segmentID))
			available = false
			continue
		}
		entry.TargetVersion = newVersion
		d.sealedSegments[segmentID] = entry
	}

	oldValue := d.targetVersion.Load()
	d.targetVersion.Store(newVersion)
	d.genSnapshot()
	// if sealed segment in leader view is less than sealed segment in target, set delegator to unserviceable
	d.serviceable.Store(available)
	log.Info("Update readable segment version",
		zap.Int64("oldVersion", oldValue),
		zap.Int64("newVersion", newVersion),
		zap.Int("growingSegmentNum", len(growingInTarget)),
		zap.Int("sealedSegmentNum", len(sealedInTarget)),
	)
}

// RemoveDistributions remove segments distributions and returns the clear signal channel.
func (d *distribution) RemoveDistributions(sealedSegments []SegmentEntry, growingSegments []SegmentEntry) chan struct{} {
	d.mut.Lock()
	defer d.mut.Unlock()

	for _, sealed := range sealedSegments {
		if d.offlines.Contain(sealed.SegmentID) {
			d.offlines.Remove(sealed.SegmentID)
		}
		entry, ok := d.sealedSegments[sealed.SegmentID]
		if !ok {
			continue
		}
		if entry.NodeID == sealed.NodeID || sealed.NodeID == wildcardNodeID {
			delete(d.sealedSegments, sealed.SegmentID)
		}
	}

	for _, growing := range growingSegments {
		_, ok := d.growingSegments[growing.SegmentID]
		if !ok {
			continue
		}

		delete(d.growingSegments, growing.SegmentID)
	}

	// wait previous read even not distribution changed
	// in case of segment balance caused segment lost track
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
