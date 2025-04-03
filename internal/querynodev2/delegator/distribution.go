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
	"fmt"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

// channelQueryView maintains the sealed segment list which should be used for search/query.
// for new delegator, will got a new channelQueryView from WatchChannel, and get the queryView update from querycoord before it becomes serviceable
// after delegator becomes serviceable, it only update the queryView by SyncTargetVersion
type channelQueryView struct {
	growingSegments       typeutil.UniqueSet // growing segment list which should be used for search/query
	sealedSegmentRowCount map[int64]int64    // sealed segment list which should be used for search/query, segmentID -> row count
	partitions            typeutil.UniqueSet // partitions list which sealed segments belong to
	version               int64              // version of current query view, same as targetVersion in qc

	loadedRatio            *atomic.Float64 // loaded ratio of current query view, set serviceable to true if loadedRatio == 1.0
	unloadedSealedSegments []SegmentEntry  // workerID -> -1
}

func NewChannelQueryView(growings []int64, sealedSegmentRowCount map[int64]int64, partitions []int64, version int64) *channelQueryView {
	return &channelQueryView{
		growingSegments:       typeutil.NewUniqueSet(growings...),
		sealedSegmentRowCount: sealedSegmentRowCount,
		partitions:            typeutil.NewUniqueSet(partitions...),
		version:               version,
		loadedRatio:           atomic.NewFloat64(0),
	}
}

func (q *channelQueryView) GetVersion() int64 {
	return q.version
}

func (q *channelQueryView) Serviceable() bool {
	return q.loadedRatio.Load() >= 1.0
}

func (q *channelQueryView) GetLoadedRatio() float64 {
	return q.loadedRatio.Load()
}

// distribution is the struct to store segment distribution.
// it contains both growing and sealed segments.
type distribution struct {
	// segments information
	// map[SegmentID]=>segmentEntry
	growingSegments map[UniqueID]SegmentEntry
	sealedSegments  map[UniqueID]SegmentEntry

	// snapshotVersion indicator
	snapshotVersion int64
	snapshots       *typeutil.ConcurrentMap[int64, *snapshot]
	// current is the snapshot for quick usage for search/query
	// generated for each change of distribution
	current *atomic.Pointer[snapshot]

	idfOracle IDFOracle
	// protects current & segments
	mut sync.RWMutex

	// distribution info
	channelName string
	queryView   *channelQueryView
}

// SegmentEntry stores the segment meta information.
type SegmentEntry struct {
	NodeID        int64
	SegmentID     UniqueID
	PartitionID   UniqueID
	Version       int64
	TargetVersion int64
	Level         datapb.SegmentLevel
	Offline       bool // if delegator failed to execute forwardDelete/Query/Search on segment, it will be offline
}

func NewDistribution(channelName string, queryView *channelQueryView) *distribution {
	dist := &distribution{
		channelName:     channelName,
		growingSegments: make(map[UniqueID]SegmentEntry),
		sealedSegments:  make(map[UniqueID]SegmentEntry),
		snapshots:       typeutil.NewConcurrentMap[int64, *snapshot](),
		current:         atomic.NewPointer[snapshot](nil),
		queryView:       queryView,
	}
	dist.genSnapshot()
	dist.updateServiceable("NewDistribution")
	return dist
}

func (d *distribution) SetIDFOracle(idfOracle IDFOracle) {
	d.mut.Lock()
	defer d.mut.Unlock()
	d.idfOracle = idfOracle
}

// return segment distribution in query view
func (d *distribution) PinReadableSegments(requiredLoadRatio float64, partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry, version int64, err error) {
	d.mut.RLock()
	defer d.mut.RUnlock()

	if d.queryView.GetLoadedRatio() < requiredLoadRatio {
		return nil, nil, -1, merr.WrapErrChannelNotAvailable(d.channelName,
			fmt.Sprintf("channel distribution is not serviceable, required load ratio is %f, current load ratio is %f", requiredLoadRatio, d.queryView.GetLoadedRatio()))
	}

	current := d.current.Load()
	// snapshot sanity check
	// if user specified a partition id which is not serviceable, return err
	for _, partition := range partitions {
		if !current.partitions.Contain(partition) {
			return nil, nil, -1, merr.WrapErrPartitionNotLoaded(partition)
		}
	}
	sealed, growing = current.Get(partitions...)
	version = current.version
	targetVersion := current.GetTargetVersion()
	filterReadable := d.readableFilter(targetVersion)
	sealed, growing = d.filterSegments(sealed, growing, filterReadable)

	// append distribution of unloaded segment
	sealed = append(sealed, SnapshotItem{
		NodeID:   -1,
		Segments: d.queryView.unloadedSealedSegments,
	})
	return
}

func (d *distribution) PinOnlineSegments(partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry, version int64) {
	d.mut.RLock()
	defer d.mut.RUnlock()

	current := d.current.Load()
	sealed, growing = current.Get(partitions...)
	filterOnline := func(entry SegmentEntry, _ int) bool {
		return !entry.Offline
	}
	sealed, growing = d.filterSegments(sealed, growing, filterOnline)
	version = current.version
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
		filterReadable := d.readableFilter(targetVersion)
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
	return d.queryView.Serviceable()
}

// for now, delegator become serviceable only when watchDmChannel is done
// so we regard all needed growing is loaded and we compute loadRatio based on sealed segments
func (d *distribution) updateServiceable(triggerAction string) {
	loadedSealedSegments := int64(0)
	totalSealedRowCount := int64(0)
	unloadedSealedSegments := make([]SegmentEntry, 0)
	for id, rowCount := range d.queryView.sealedSegmentRowCount {
		if entry, ok := d.sealedSegments[id]; ok && !entry.Offline {
			loadedSealedSegments += rowCount
		} else {
			unloadedSealedSegments = append(unloadedSealedSegments, SegmentEntry{SegmentID: id, NodeID: -1})
		}
		totalSealedRowCount += rowCount
	}

	// unloaded segment entry list for partial result
	d.queryView.unloadedSealedSegments = unloadedSealedSegments

	loadedRatio := 0.0
	if len(d.queryView.sealedSegmentRowCount) == 0 {
		loadedRatio = 1.0
	} else if loadedSealedSegments == 0 {
		loadedRatio = 0.0
	} else {
		loadedRatio = float64(loadedSealedSegments) / float64(totalSealedRowCount)
	}

	serviceable := loadedRatio >= 1.0
	if serviceable != d.queryView.Serviceable() {
		log.Info("channel distribution serviceable changed",
			zap.String("channel", d.channelName),
			zap.Bool("serviceable", serviceable),
			zap.Float64("loadedRatio", loadedRatio),
			zap.Int64("loadedSealedRowCount", loadedSealedSegments),
			zap.Int64("totalSealedRowCount", totalSealedRowCount),
			zap.Int("unloadedSealedSegmentNum", len(unloadedSealedSegments)),
			zap.Int("totalSealedSegmentNum", len(d.queryView.sealedSegmentRowCount)),
			zap.String("action", triggerAction))
	}

	d.queryView.loadedRatio.Store(loadedRatio)
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
			_, ok := d.queryView.sealedSegmentRowCount[entry.SegmentID]
			if ok || d.queryView.growingSegments.Contain(entry.SegmentID) {
				// set segment version to query view version, to support partial result
				entry.TargetVersion = d.queryView.GetVersion()
			} else {
				// set segment version to unreadableTargetVersion, if it's not in query view
				entry.TargetVersion = unreadableTargetVersion
			}
		}
		d.sealedSegments[entry.SegmentID] = entry
	}

	d.genSnapshot()
	d.updateServiceable("AddDistributions")
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
func (d *distribution) MarkOfflineSegments(segmentIDs ...int64) {
	d.mut.Lock()
	defer d.mut.Unlock()

	updated := false
	for _, segmentID := range segmentIDs {
		entry, ok := d.sealedSegments[segmentID]
		if !ok {
			continue
		}
		updated = true
		entry.Offline = true
		entry.Version = unreadableTargetVersion
		entry.NodeID = -1
		d.sealedSegments[segmentID] = entry
	}

	if updated {
		log.Info("mark sealed segment offline from distribution",
			zap.String("channelName", d.channelName),
			zap.Int64s("segmentIDs", segmentIDs))
		d.genSnapshot()
		d.updateServiceable("MarkOfflineSegments")
	}
}

// update readable channel view
// 1. update readable channel view to support partial result before distribution is serviceable
// 2. update readable channel view to support full result after new distribution is serviceable
// Notice: if we don't need to be compatible with 2.5.x, we can just update new query view to support query,
// and new query view will become serviceable automatically, a sync action after distribution is serviceable is unnecessary
func (d *distribution) SyncReadableChannelView(action *querypb.SyncAction, partitions []int64) {
	d.mut.Lock()
	defer d.mut.Unlock()

	oldValue := d.queryView.version
	d.queryView = &channelQueryView{
		growingSegments:       typeutil.NewUniqueSet(action.GetGrowingInTarget()...),
		sealedSegmentRowCount: action.GetSealedSegmentRowCount(),
		partitions:            typeutil.NewUniqueSet(partitions...),
		version:               action.GetTargetVersion(),
		loadedRatio:           atomic.NewFloat64(0),
	}

	sealedSet := typeutil.NewUniqueSet(action.GetSealedInTarget()...)
	droppedSet := typeutil.NewUniqueSet(action.GetDroppedInTarget()...)
	for _, s := range d.growingSegments {
		// sealed segment already exists or dropped, make growing segment redundant
		if sealedSet.Contain(s.SegmentID) || droppedSet.Contain(s.SegmentID) {
			s.TargetVersion = redundantTargetVersion
			d.growingSegments[s.SegmentID] = s
		}
	}

	d.queryView.growingSegments.Range(func(s UniqueID) bool {
		entry, ok := d.growingSegments[s]
		if !ok {
			log.Warn("readable growing segment lost, consume from dml seems too slow",
				zap.Int64("segmentID", s))
			return true
		}
		entry.TargetVersion = action.GetTargetVersion()
		d.growingSegments[s] = entry
		return true
	})

	for id := range d.queryView.sealedSegmentRowCount {
		entry, ok := d.sealedSegments[id]
		if !ok {
			continue
		}
		entry.TargetVersion = action.GetTargetVersion()
		d.sealedSegments[id] = entry
	}

	d.genSnapshot()
	d.updateServiceable("SyncTargetVersion")
	log.Info("Update channel query view",
		zap.String("channel", d.channelName),
		zap.Int64s("partitions", partitions),
		zap.Int64("oldVersion", oldValue),
		zap.Int64("newVersion", action.GetTargetVersion()),
		zap.Bool("serviceable", d.queryView.Serviceable()),
		zap.Float64("loadedRatio", d.queryView.GetLoadedRatio()),
		zap.Int("growingSegmentNum", len(action.GetGrowingInTarget())),
		zap.Int("sealedSegmentNum", len(action.GetSealedInTarget())),
	)
}

// RemoveDistributions remove segments distributions and returns the clear signal channel.
func (d *distribution) RemoveDistributions(sealedSegments []SegmentEntry, growingSegments []SegmentEntry) chan struct{} {
	d.mut.Lock()
	defer d.mut.Unlock()

	for _, sealed := range sealedSegments {
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

	log.Info("remove segments from distribution",
		zap.String("channelName", d.channelName),
		zap.Int64s("growing", lo.Map(growingSegments, func(s SegmentEntry, _ int) int64 { return s.SegmentID })),
		zap.Int64s("sealed", lo.Map(sealedSegments, func(s SegmentEntry, _ int) int64 { return s.SegmentID })),
	)

	d.updateServiceable("RemoveDistributions")
	// wait previous read even not distribution changed
	// in case of segment balance caused segment lost track
	return d.genSnapshot()
}

// getSnapshot converts current distribution to snapshot format.
// in which, user could use found nodeID=>segmentID list.
// mutex RLock is required before calling this method.
func (d *distribution) genSnapshot() chan struct{} {
	// stores last snapshot
	// ok to be nil
	last := d.current.Load()

	nodeSegments := make(map[int64][]SegmentEntry)
	for _, entry := range d.sealedSegments {
		nodeSegments[entry.NodeID] = append(nodeSegments[entry.NodeID], entry)
	}

	// only store working partition entry in snapshot to reduce calculation
	dist := make([]SnapshotItem, 0, len(nodeSegments))
	for nodeID, items := range nodeSegments {
		dist = append(dist, SnapshotItem{
			NodeID: nodeID,
			Segments: lo.Map(items, func(entry SegmentEntry, _ int) SegmentEntry {
				if !d.queryView.partitions.Contain(entry.PartitionID) {
					entry.TargetVersion = unreadableTargetVersion
				}
				return entry
			}),
		})
	}

	growing := make([]SegmentEntry, 0, len(d.growingSegments))
	for _, entry := range d.growingSegments {
		if !d.queryView.partitions.Contain(entry.PartitionID) {
			entry.TargetVersion = unreadableTargetVersion
		}
		growing = append(growing, entry)
	}

	// update snapshot version
	d.snapshotVersion++
	newSnapShot := NewSnapshot(dist, growing, last, d.snapshotVersion, d.queryView.GetVersion())
	newSnapShot.partitions = d.queryView.partitions

	d.current.Store(newSnapShot)
	// shall be a new one
	d.snapshots.GetOrInsert(d.snapshotVersion, newSnapShot)
	if d.idfOracle != nil {
		d.idfOracle.SyncDistribution(newSnapShot)
	}

	// first snapshot, return closed chan
	if last == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	last.Expire(d.getCleanup(last.version))

	return last.cleared
}

func (d *distribution) readableFilter(targetVersion int64) func(entry SegmentEntry, _ int) bool {
	return func(entry SegmentEntry, _ int) bool {
		// segment L0 is not readable for now
		return entry.Level != datapb.SegmentLevel_L0 && (entry.TargetVersion == targetVersion || entry.TargetVersion == initialTargetVersion)
	}
}

// getCleanup returns cleanup snapshots function.
func (d *distribution) getCleanup(version int64) snapshotCleanup {
	return func() {
		d.snapshots.GetAndRemove(version)
	}
}
