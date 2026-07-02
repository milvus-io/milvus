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
	"context"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	loadedSealedRowCount   int64
	totalSealedRowCount    int64
	unloadedSealedSegments map[UniqueID]SegmentEntry // workerID -> -1

	syncedByCoord bool // if the query view is synced by coord
}

func NewChannelQueryView(growings []int64, sealedSegmentRowCount map[int64]int64, partitions []int64, version int64) *channelQueryView {
	return &channelQueryView{
		growingSegments:        typeutil.NewUniqueSet(growings...),
		sealedSegmentRowCount:  sealedSegmentRowCount,
		partitions:             typeutil.NewUniqueSet(partitions...),
		version:                version,
		loadedRatio:            atomic.NewFloat64(0),
		unloadedSealedSegments: make(map[UniqueID]SegmentEntry),
	}
}

func (q *channelQueryView) GetVersion() int64 {
	return q.version
}

func (q *channelQueryView) Serviceable() bool {
	dataReady := q.loadedRatio.Load() >= 1.0
	// for now, we only support collection level target(data view), so we need to wait for the query view is synced by coord
	// incase of delegator become serviceable before current target is ready when memory is not enough.
	// if current target is not ready, segment on delegator will be released at any time, serviceable state is not reliable.
	// Note: after we support channel level target(data view), we can remove this flag
	viewReady := q.syncedByCoord

	return dataReady && viewReady
}

func (q *channelQueryView) GetLoadedRatio() float64 {
	return q.loadedRatio.Load()
}

func (q *channelQueryView) GetUnloadedSealedSegments() []SegmentEntry {
	if len(q.unloadedSealedSegments) == 0 {
		return nil
	}
	return lo.Values(q.unloadedSealedSegments)
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

	pendingSnapshotDelta    snapshotDelta
	snapshotNodeSegments    map[int64][]SegmentEntry
	snapshotSegmentPosition map[UniqueID]snapshotSegmentPosition
	snapshotSegments        map[UniqueID]SegmentEntry
	snapshotGrowings        map[UniqueID]SegmentEntry

	idfOracle IDFOracle
	// protects current & segments
	mut sync.RWMutex

	// async snapshot generation
	snapshotNotifier chan struct{} // capacity 1, notify background goroutine to regenerate snapshot
	snapshotClose    chan struct{} // closed to stop background goroutine
	snapshotDone     chan struct{} // closed when background goroutine exits
	closed           *atomic.Bool
	closeOnce        sync.Once

	// distribution info
	channelName string
	queryView   *channelQueryView
}

type snapshotDelta struct {
	sealedUpserts  map[UniqueID]SegmentEntry
	sealedDeletes  map[UniqueID]struct{}
	growingUpserts map[UniqueID]SegmentEntry
	growingDeletes map[UniqueID]struct{}
}

type snapshotSegmentPosition struct {
	nodeID int64
	index  int
}

func newSnapshotDelta() snapshotDelta {
	return snapshotDelta{
		sealedUpserts:  make(map[UniqueID]SegmentEntry),
		sealedDeletes:  make(map[UniqueID]struct{}),
		growingUpserts: make(map[UniqueID]SegmentEntry),
		growingDeletes: make(map[UniqueID]struct{}),
	}
}

func (d snapshotDelta) empty() bool {
	return len(d.sealedUpserts) == 0 &&
		len(d.sealedDeletes) == 0 &&
		len(d.growingUpserts) == 0 &&
		len(d.growingDeletes) == 0
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

	// Candidate for PK existence check (BF query)
	// - For sealed segments: *pkoracle.BloomFilterSet
	// - For growing segments: segments.Segment (LocalSegment)
	// Note: nil for offline segments or L0 segments
	Candidate pkoracle.Candidate
}

func NewDistribution(channelName string, queryView *channelQueryView) *distribution {
	dist := &distribution{
		channelName:             channelName,
		growingSegments:         make(map[UniqueID]SegmentEntry),
		sealedSegments:          make(map[UniqueID]SegmentEntry),
		snapshots:               typeutil.NewConcurrentMap[int64, *snapshot](),
		current:                 atomic.NewPointer[snapshot](nil),
		pendingSnapshotDelta:    newSnapshotDelta(),
		snapshotNodeSegments:    make(map[int64][]SegmentEntry),
		snapshotSegmentPosition: make(map[UniqueID]snapshotSegmentPosition),
		snapshotSegments:        make(map[UniqueID]SegmentEntry),
		snapshotGrowings:        make(map[UniqueID]SegmentEntry),
		queryView:               queryView,
		snapshotNotifier:        make(chan struct{}, 1),
		snapshotClose:           make(chan struct{}),
		snapshotDone:            make(chan struct{}),
		closed:                  atomic.NewBool(false),
	}
	// generate initial snapshot synchronously
	dist.genSnapshot()
	dist.updateServiceable("NewDistribution")
	// start background snapshot loop
	go dist.snapshotLoop()
	return dist
}

// notifySnapshotUpdate sends a non-blocking notification to regenerate snapshot.
func (d *distribution) notifySnapshotUpdate() {
	if d.closed.Load() {
		return
	}
	select {
	case d.snapshotNotifier <- struct{}{}:
	default:
	}
}

// snapshotLoop runs in a background goroutine, regenerating snapshot on notification.
func (d *distribution) snapshotLoop() {
	defer close(d.snapshotDone)
	for {
		select {
		case <-d.snapshotClose:
			return
		case <-d.snapshotNotifier:
			d.mut.Lock()
			delta := d.takeSnapshotDeltaLocked()
			if delta.empty() {
				d.mut.Unlock()
				continue
			}
			changedSegments, oldSegments := d.applySnapshotDeltaLocked(delta)
			d.updateServiceableByChangedSegments("snapshotLoop", changedSegments, oldSegments)
			d.mut.Unlock()
		}
	}
}

func (d *distribution) takeSnapshotDeltaLocked() snapshotDelta {
	delta := d.pendingSnapshotDelta
	d.pendingSnapshotDelta = newSnapshotDelta()
	return delta
}

func (d *distribution) resetSnapshotDeltaLocked() {
	d.pendingSnapshotDelta = newSnapshotDelta()
}

func (d *distribution) recordSealedSnapshotUpsertLocked(entry SegmentEntry) {
	delete(d.pendingSnapshotDelta.sealedDeletes, entry.SegmentID)
	d.pendingSnapshotDelta.sealedUpserts[entry.SegmentID] = entry
}

func (d *distribution) recordSealedSnapshotDeleteLocked(segmentID UniqueID) {
	delete(d.pendingSnapshotDelta.sealedUpserts, segmentID)
	d.pendingSnapshotDelta.sealedDeletes[segmentID] = struct{}{}
}

func (d *distribution) recordGrowingSnapshotDeleteLocked(segmentID UniqueID) {
	delete(d.pendingSnapshotDelta.growingUpserts, segmentID)
	d.pendingSnapshotDelta.growingDeletes[segmentID] = struct{}{}
}

func (d *distribution) snapshotEntryForQueryView(entry SegmentEntry) SegmentEntry {
	if !d.queryView.partitions.Contain(entry.PartitionID) {
		entry.TargetVersion = unreadableTargetVersion
	}
	return entry
}

func cloneSnapshotNodeSegments(segments []SegmentEntry) []SegmentEntry {
	if len(segments) == 0 {
		return nil
	}
	cloned := make([]SegmentEntry, len(segments))
	copy(cloned, segments)
	return cloned
}

func (d *distribution) cloneSnapshotNodeSegmentsLocked(nodeID int64) []SegmentEntry {
	return cloneSnapshotNodeSegments(d.snapshotNodeSegments[nodeID])
}

func (d *distribution) snapshotNodeSegmentsForUpdateLocked(nodeSegments map[int64][]SegmentEntry, nodeID int64) []SegmentEntry {
	segments, ok := nodeSegments[nodeID]
	if ok {
		return segments
	}
	segments = d.cloneSnapshotNodeSegmentsLocked(nodeID)
	nodeSegments[nodeID] = segments
	return segments
}

func (d *distribution) replaceSnapshotSegmentLocked(nodeSegments map[int64][]SegmentEntry, entry SegmentEntry) {
	position, ok := d.snapshotSegmentPosition[entry.SegmentID]
	if ok && position.nodeID == entry.NodeID {
		segments := d.snapshotNodeSegmentsForUpdateLocked(nodeSegments, entry.NodeID)
		if position.index < len(segments) && segments[position.index].SegmentID == entry.SegmentID {
			segments[position.index] = entry
			d.snapshotSegments[entry.SegmentID] = entry
			return
		}
	}

	d.removeSnapshotSegmentLocked(nodeSegments, entry.SegmentID)
	segments := d.snapshotNodeSegmentsForUpdateLocked(nodeSegments, entry.NodeID)
	d.snapshotSegmentPosition[entry.SegmentID] = snapshotSegmentPosition{
		nodeID: entry.NodeID,
		index:  len(segments),
	}
	nodeSegments[entry.NodeID] = append(segments, entry)
	d.snapshotSegments[entry.SegmentID] = entry
}

func (d *distribution) removeSnapshotSegmentLocked(nodeSegments map[int64][]SegmentEntry, segmentID UniqueID) {
	position, ok := d.snapshotSegmentPosition[segmentID]
	if !ok {
		delete(d.snapshotSegments, segmentID)
		return
	}

	segments := d.snapshotNodeSegmentsForUpdateLocked(nodeSegments, position.nodeID)
	if position.index >= len(segments) || segments[position.index].SegmentID != segmentID {
		for idx, entry := range segments {
			if entry.SegmentID == segmentID {
				position.index = idx
				break
			}
		}
		if position.index >= len(segments) || segments[position.index].SegmentID != segmentID {
			delete(d.snapshotSegmentPosition, segmentID)
			delete(d.snapshotSegments, segmentID)
			return
		}
	}

	lastIdx := len(segments) - 1
	if position.index != lastIdx {
		moved := segments[lastIdx]
		segments[position.index] = moved
		d.snapshotSegmentPosition[moved.SegmentID] = snapshotSegmentPosition{
			nodeID: position.nodeID,
			index:  position.index,
		}
	}
	nodeSegments[position.nodeID] = segments[:lastIdx]
	delete(d.snapshotSegmentPosition, segmentID)
	delete(d.snapshotSegments, segmentID)
}

func (d *distribution) applySnapshotDeltaLocked(delta snapshotDelta) (map[UniqueID]struct{}, map[UniqueID]SegmentEntry) {
	last := d.current.Load()
	if last == nil {
		d.genSnapshot()
		return nil, nil
	}

	changedSegments := make(map[UniqueID]struct{}, len(delta.sealedUpserts)+len(delta.sealedDeletes))
	oldSegments := make(map[UniqueID]SegmentEntry, len(delta.sealedUpserts)+len(delta.sealedDeletes))
	affectedNodes := make(map[int64]struct{})

	for segmentID := range delta.sealedDeletes {
		changedSegments[segmentID] = struct{}{}
		if oldEntry, ok := d.snapshotSegments[segmentID]; ok {
			oldSegments[segmentID] = oldEntry
			affectedNodes[oldEntry.NodeID] = struct{}{}
		}
	}
	for segmentID, entry := range delta.sealedUpserts {
		changedSegments[segmentID] = struct{}{}
		if oldEntry, ok := d.snapshotSegments[segmentID]; ok {
			oldSegments[segmentID] = oldEntry
			affectedNodes[oldEntry.NodeID] = struct{}{}
		}
		affectedNodes[entry.NodeID] = struct{}{}
	}

	nodeSegments := make(map[int64][]SegmentEntry, len(affectedNodes))
	for nodeID := range affectedNodes {
		nodeSegments[nodeID] = d.cloneSnapshotNodeSegmentsLocked(nodeID)
	}

	for segmentID := range delta.sealedDeletes {
		d.removeSnapshotSegmentLocked(nodeSegments, segmentID)
	}
	for _, entry := range delta.sealedUpserts {
		entry = d.snapshotEntryForQueryView(entry)
		d.replaceSnapshotSegmentLocked(nodeSegments, entry)
	}

	replacedNodes := make(map[int64][]SegmentEntry, len(nodeSegments))
	for nodeID, segments := range nodeSegments {
		if len(segments) == 0 {
			delete(d.snapshotNodeSegments, nodeID)
			replacedNodes[nodeID] = nil
			continue
		}
		d.snapshotNodeSegments[nodeID] = segments
		replacedNodes[nodeID] = segments
	}

	dist := make([]SnapshotItem, 0, len(last.dist)+len(replacedNodes))
	seenNodes := make(map[int64]struct{}, len(replacedNodes))
	for _, item := range last.dist {
		segments, ok := replacedNodes[item.NodeID]
		if !ok {
			dist = append(dist, item)
			continue
		}
		seenNodes[item.NodeID] = struct{}{}
		if len(segments) > 0 {
			dist = append(dist, SnapshotItem{NodeID: item.NodeID, Segments: segments})
		}
	}
	for nodeID, segments := range replacedNodes {
		if _, ok := seenNodes[nodeID]; ok || len(segments) == 0 {
			continue
		}
		dist = append(dist, SnapshotItem{NodeID: nodeID, Segments: segments})
	}

	growing := last.growing
	if len(delta.growingDeletes) > 0 || len(delta.growingUpserts) > 0 {
		for segmentID := range delta.growingDeletes {
			delete(d.snapshotGrowings, segmentID)
		}
		for segmentID, entry := range delta.growingUpserts {
			d.snapshotGrowings[segmentID] = d.snapshotEntryForQueryView(entry)
		}
		growing = lo.Values(d.snapshotGrowings)
	}

	d.snapshotVersion++
	newSnapshot := NewSnapshot(dist, growing, last, d.snapshotVersion, d.queryView.GetVersion())
	newSnapshot.partitions = d.queryView.partitions
	d.current.Store(newSnapshot)
	d.snapshots.GetOrInsert(d.snapshotVersion, newSnapshot)
	last.Expire(d.getCleanup(last.version))

	return changedSegments, oldSegments
}

func (d *distribution) SetIDFOracle(idfOracle IDFOracle) {
	d.mut.Lock()
	defer d.mut.Unlock()
	d.idfOracle = idfOracle
}

// return segment distribution in query view
func (d *distribution) PinReadableSegments(requiredLoadRatio float64, partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry, sealedRowCount map[int64]int64, version int64, err error) {
	d.mut.RLock()
	defer d.mut.RUnlock()

	requireFullResult := requiredLoadRatio >= 1.0
	loadRatioSatisfy := d.queryView.GetLoadedRatio() >= requiredLoadRatio
	var isServiceable bool
	if requireFullResult {
		isServiceable = d.queryView.Serviceable()
	} else {
		isServiceable = loadRatioSatisfy
	}

	if !isServiceable {
		mlog.Warn(context.TODO(), "channel distribution is not serviceable",
			mlog.String("channel", d.channelName),
			mlog.Float64("requiredLoadRatio", requiredLoadRatio),
			mlog.Float64("currentLoadRatio", d.queryView.GetLoadedRatio()),
			mlog.Bool("serviceable", d.queryView.Serviceable()),
		)
		return nil, nil, nil, -1, merr.WrapErrChannelNotAvailable(d.channelName, "channel distribution is not serviceable")
	}

	current := d.current.Load()
	// snapshot sanity check
	// if user specified a partition id which is not serviceable, return err
	for _, partition := range partitions {
		if !current.partitions.Contain(partition) {
			return nil, nil, nil, -1, merr.WrapErrPartitionNotLoaded(partition)
		}
	}
	sealed, growing = current.Get(partitions...)
	version = current.version
	sealedRowCount = d.queryView.sealedSegmentRowCount
	if d.queryView.Serviceable() {
		// if query view is serviceable, we can use current target version to filter segments
		targetVersion := current.GetTargetVersion()
		filterReadable := d.readableFilter(targetVersion)
		sealed, growing = d.filterSegments(sealed, growing, filterReadable)
	} else {
		// if query view is not fully loaded, we need to filter segments by query view's segment list to offer partial result
		sealed = lo.Map(sealed, func(item SnapshotItem, _ int) SnapshotItem {
			return SnapshotItem{
				NodeID: item.NodeID,
				Segments: lo.Filter(item.Segments, func(entry SegmentEntry, _ int) bool {
					return d.queryView.sealedSegmentRowCount[entry.SegmentID] > 0
				}),
			}
		})
		growing = lo.Filter(growing, func(entry SegmentEntry, _ int) bool {
			return d.queryView.growingSegments.Contain(entry.SegmentID)
		})
	}

	unloadedSealedSegments := d.queryView.GetUnloadedSealedSegments()
	if len(unloadedSealedSegments) > 0 {
		// append distribution of unloaded segment
		sealed = append(sealed, SnapshotItem{
			NodeID:   -1,
			Segments: unloadedSealedSegments,
		})
	}

	return sealed, growing, sealedRowCount, version, err
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
	return sealed, growing, version
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
		return sealed, growing
	}

	return sealed, growing
}

// IsReadableSealedSegment reuses PeekSegments(readable=true) semantics for Reopen activation.
func (d *distribution) IsReadableSealedSegment(segmentID int64) bool {
	sealed, _ := d.PeekSegments(true)
	for _, item := range sealed {
		for _, entry := range item.Segments {
			if entry.SegmentID == segmentID {
				return true
			}
		}
	}
	return false
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
	unloadedSealedSegments := make(map[UniqueID]SegmentEntry)
	for id, rowCount := range d.queryView.sealedSegmentRowCount {
		if entry, ok := d.snapshotSegments[id]; ok && !entry.Offline {
			loadedSealedSegments += rowCount
		} else {
			unloadedSealedSegments[id] = SegmentEntry{SegmentID: id, NodeID: -1}
		}
		totalSealedRowCount += rowCount
	}

	// unloaded segment entry list for partial result
	d.queryView.unloadedSealedSegments = unloadedSealedSegments
	d.queryView.loadedSealedRowCount = loadedSealedSegments
	d.queryView.totalSealedRowCount = totalSealedRowCount

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
		mlog.Info(context.TODO(), "channel distribution serviceable changed",
			mlog.String("channel", d.channelName),
			mlog.Bool("serviceable", serviceable),
			mlog.Float64("loadedRatio", loadedRatio),
			mlog.Int64("loadedSealedRowCount", loadedSealedSegments),
			mlog.Int64("totalSealedRowCount", totalSealedRowCount),
			mlog.Int("unloadedSealedSegmentNum", len(unloadedSealedSegments)),
			mlog.Int("totalSealedSegmentNum", len(d.queryView.sealedSegmentRowCount)),
			mlog.String("action", triggerAction))
	}

	d.queryView.loadedRatio.Store(loadedRatio)
}

func (d *distribution) updateServiceableByChangedSegments(triggerAction string, changedSegments map[UniqueID]struct{}, oldSegments map[UniqueID]SegmentEntry) {
	if len(changedSegments) == 0 {
		return
	}

	loadedSealedSegments := d.queryView.loadedSealedRowCount
	for segmentID := range changedSegments {
		rowCount, ok := d.queryView.sealedSegmentRowCount[segmentID]
		if !ok {
			continue
		}

		oldEntry, oldExists := oldSegments[segmentID]
		oldLoaded := oldExists && !oldEntry.Offline
		newEntry, newExists := d.snapshotSegments[segmentID]
		newLoaded := newExists && !newEntry.Offline
		if oldLoaded == newLoaded {
			if newLoaded {
				delete(d.queryView.unloadedSealedSegments, segmentID)
			} else {
				d.queryView.unloadedSealedSegments[segmentID] = SegmentEntry{SegmentID: segmentID, NodeID: -1}
			}
			continue
		}

		if newLoaded {
			loadedSealedSegments += rowCount
			delete(d.queryView.unloadedSealedSegments, segmentID)
		} else {
			loadedSealedSegments -= rowCount
			d.queryView.unloadedSealedSegments[segmentID] = SegmentEntry{SegmentID: segmentID, NodeID: -1}
		}
	}

	d.queryView.loadedSealedRowCount = loadedSealedSegments
	loadedRatio := 0.0
	if len(d.queryView.sealedSegmentRowCount) == 0 {
		loadedRatio = 1.0
	} else if loadedSealedSegments > 0 {
		loadedRatio = float64(loadedSealedSegments) / float64(d.queryView.totalSealedRowCount)
	}

	serviceable := loadedRatio >= 1.0
	if serviceable != d.queryView.Serviceable() {
		log.Info("channel distribution serviceable changed",
			zap.String("channel", d.channelName),
			zap.Bool("serviceable", serviceable),
			zap.Float64("loadedRatio", loadedRatio),
			zap.Int64("loadedSealedRowCount", loadedSealedSegments),
			zap.Int64("totalSealedRowCount", d.queryView.totalSealedRowCount),
			zap.Int("unloadedSealedSegmentNum", len(d.queryView.unloadedSealedSegments)),
			zap.Int("totalSealedSegmentNum", len(d.queryView.sealedSegmentRowCount)),
			zap.String("action", triggerAction))
	}

	d.queryView.loadedRatio.Store(loadedRatio)
}

// AddDistributions add multiple segment entries.
func (d *distribution) AddDistributions(entries ...SegmentEntry) {
	var toRefund []pkoracle.Candidate

	if d.closed.Load() {
		for _, entry := range entries {
			if entry.Candidate != nil {
				toRefund = append(toRefund, entry.Candidate)
			}
		}
		refundCandidates(toRefund)
		return
	}

	d.mut.Lock()
	updated := false
	for _, entry := range entries {
		oldEntry, ok := d.sealedSegments[entry.SegmentID]
		if ok && oldEntry.Version >= entry.Version {
			mlog.Warn(context.TODO(), "Invalid segment distribution changed, skip it",
				mlog.FieldSegmentID(entry.SegmentID),
				mlog.Int64("oldVersion", oldEntry.Version),
				mlog.Int64("oldNode", oldEntry.NodeID),
				mlog.Int64("newVersion", entry.Version),
				mlog.Int64("newNode", entry.NodeID),
			)
			if entry.Candidate != nil {
				toRefund = append(toRefund, entry.Candidate)
			}
			continue
		}

		if ok {
			entry.TargetVersion = oldEntry.TargetVersion
			if oldEntry.Candidate != nil {
				toRefund = append(toRefund, oldEntry.Candidate)
			}
		} else {
			entry.TargetVersion = unreadableTargetVersion
		}
		d.sealedSegments[entry.SegmentID] = entry
		d.recordSealedSnapshotUpsertLocked(entry)
		updated = true
	}
	d.mut.Unlock()

	if updated {
		d.notifySnapshotUpdate()
	}
	refundCandidates(toRefund)
}

// refundCandidates refunds resources for removed candidates.
func refundCandidates(candidates []pkoracle.Candidate) {
	for _, c := range candidates {
		c.Refund()
	}
}

// AddGrowing adds growing segment distribution.
// genSnapshot is called synchronously so that the growing segment is
// immediately visible to searches. Growing segments are created
// infrequently (only on the first insert for each segment), so this
// does not regress the lock-contention optimization.
func (d *distribution) AddGrowing(entries ...SegmentEntry) {
	d.mut.Lock()
	for _, entry := range entries {
		d.growingSegments[entry.SegmentID] = entry
	}
	hasPendingSealedDelta := len(d.pendingSnapshotDelta.sealedUpserts) > 0 || len(d.pendingSnapshotDelta.sealedDeletes) > 0
	d.genSnapshot()
	if hasPendingSealedDelta {
		d.updateServiceable("AddGrowing")
	}
	d.mut.Unlock()
}

// AddOffline set segmentIDs to offlines.
func (d *distribution) MarkOfflineSegments(segmentIDs ...int64) {
	d.mut.Lock()
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
		d.recordSealedSnapshotUpsertLocked(entry)
	}
	d.mut.Unlock()

	if updated {
		mlog.Info(context.TODO(), "mark sealed segment offline from distribution",
			mlog.String("channelName", d.channelName),
			mlog.Int64s("segmentIDs", segmentIDs))
		d.notifySnapshotUpdate()
	}
}

// update readable channel view
// 1. update readable channel view to support partial result before distribution is serviceable
// 2. update readable channel view to support full result after new distribution is serviceable
// Notice: if we don't need to be compatible with 2.5.x, we can just update new query view to support query,
// and new query view will become serviceable automatically, a sync action after distribution is serviceable is unnecessary
func (d *distribution) SyncTargetVersion(action *querypb.SyncAction, partitions []int64) {
	d.mut.Lock()
	defer d.mut.Unlock()

	oldValue := d.queryView.version
	d.queryView = &channelQueryView{
		growingSegments:        typeutil.NewUniqueSet(action.GetGrowingInTarget()...),
		sealedSegmentRowCount:  action.GetSealedSegmentRowCount(),
		partitions:             typeutil.NewUniqueSet(partitions...),
		version:                action.GetTargetVersion(),
		loadedRatio:            atomic.NewFloat64(0),
		unloadedSealedSegments: make(map[UniqueID]SegmentEntry),
		syncedByCoord:          true,
	}

	sealedSet := typeutil.NewUniqueSet(action.GetSealedInTarget()...)
	droppedSet := typeutil.NewUniqueSet(action.GetDroppedInTarget()...)
	redundantGrowings := make([]int64, 0)
	for _, s := range d.growingSegments {
		// sealed segment already exists or dropped, make growing segment redundant
		if sealedSet.Contain(s.SegmentID) || droppedSet.Contain(s.SegmentID) {
			s.TargetVersion = redundantTargetVersion
			mlog.Info(context.TODO(), "set growing segment redundant, wait for release",
				mlog.FieldSegmentID(s.SegmentID),
				mlog.Int64("targetVersion", s.TargetVersion),
			)
			d.growingSegments[s.SegmentID] = s
			redundantGrowings = append(redundantGrowings, s.SegmentID)
		}
	}

	d.queryView.growingSegments.Range(func(s UniqueID) bool {
		entry, ok := d.growingSegments[s]
		if !ok {
			mlog.Warn(context.TODO(), "readable growing segment lost, consume from dml seems too slow",
				mlog.FieldSegmentID(s))
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

	// SyncTargetVersion needs synchronous genSnapshot because idfOracle.SetNext
	// depends on the snapshot just generated.
	d.genSnapshot()
	if d.idfOracle != nil {
		d.idfOracle.SetNext(d.current.Load())
		d.idfOracle.LazyRemoveGrowings(action.GetTargetVersion(), redundantGrowings...)
	}
	d.updateServiceable("SyncTargetVersion")

	mlog.Info(context.TODO(), "Update channel query view",
		mlog.String("channel", d.channelName),
		mlog.Int64s("partitions", partitions),
		mlog.Int64("oldVersion", oldValue),
		mlog.Int64("newVersion", action.GetTargetVersion()),
		mlog.Bool("serviceable", d.queryView.Serviceable()),
		mlog.Float64("loadedRatio", d.queryView.GetLoadedRatio()),
		mlog.Int("growingSegmentNum", len(action.GetGrowingInTarget())),
		mlog.Int("sealedSegmentNum", len(action.GetSealedInTarget())),
	)
}

// GetQueryView returns the current query view.
func (d *distribution) GetQueryView() *channelQueryView {
	d.mut.RLock()
	defer d.mut.RUnlock()

	return d.queryView
}

// RemoveDistributions remove segments distributions and returns the clear signal channel.
// The returned channel is closed when the snapshot that still contains the removed segments
// is expired (i.e., all in-flight reads using that snapshot have finished).
func (d *distribution) RemoveDistributions(sealedSegments []SegmentEntry, growingSegments []SegmentEntry) chan struct{} {
	var toRefund []pkoracle.Candidate

	d.mut.Lock()
	updated := false
	for _, sealed := range sealedSegments {
		entry, ok := d.sealedSegments[sealed.SegmentID]
		if !ok {
			continue
		}
		if entry.NodeID == sealed.NodeID || sealed.NodeID == wildcardNodeID {
			if entry.Candidate != nil {
				toRefund = append(toRefund, entry.Candidate)
			}
			delete(d.sealedSegments, sealed.SegmentID)
			d.recordSealedSnapshotDeleteLocked(sealed.SegmentID)
			updated = true
		}
	}

	for _, growing := range growingSegments {
		_, ok := d.growingSegments[growing.SegmentID]
		if !ok {
			continue
		}
		delete(d.growingSegments, growing.SegmentID)
		d.recordGrowingSnapshotDeleteLocked(growing.SegmentID)
		updated = true
	}

	// Capture current snapshot's cleared channel. The next snapshot update will
	// create a new snapshot and expire this one, closing the channel.
	var signal chan struct{}
	if updated {
		if current := d.current.Load(); current != nil {
			signal = current.cleared
		} else {
			signal = getClosedCh()
		}
	} else {
		signal = getClosedCh()
	}
	d.mut.Unlock()

	mlog.Info(context.TODO(), "remove segments from distribution",
		mlog.String("channelName", d.channelName),
		mlog.Int64s("growing", lo.Map(growingSegments, func(s SegmentEntry, _ int) int64 { return s.SegmentID })),
		mlog.Int64s("sealed", lo.Map(sealedSegments, func(s SegmentEntry, _ int) int64 { return s.SegmentID })),
		mlog.Int("sealedCandidatesRefunded", len(toRefund)),
	)

	if updated {
		d.notifySnapshotUpdate()
	}
	refundCandidates(toRefund)

	return signal
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

	snapshotNodeSegments := make(map[int64][]SegmentEntry, len(nodeSegments))
	snapshotSegmentPositions := make(map[UniqueID]snapshotSegmentPosition, len(d.sealedSegments))
	snapshotSegments := make(map[UniqueID]SegmentEntry, len(d.sealedSegments))
	// only store working partition entry in snapshot to reduce calculation
	dist := make([]SnapshotItem, 0, len(nodeSegments))
	for nodeID, items := range nodeSegments {
		segments := lo.Map(items, func(entry SegmentEntry, _ int) SegmentEntry {
			return d.snapshotEntryForQueryView(entry)
		})
		dist = append(dist, SnapshotItem{
			NodeID:   nodeID,
			Segments: segments,
		})
		snapshotNodeSegments[nodeID] = segments
		for idx, entry := range segments {
			snapshotSegmentPositions[entry.SegmentID] = snapshotSegmentPosition{
				nodeID: nodeID,
				index:  idx,
			}
			snapshotSegments[entry.SegmentID] = entry
		}
	}

	snapshotGrowings := make(map[UniqueID]SegmentEntry, len(d.growingSegments))
	growing := make([]SegmentEntry, 0, len(d.growingSegments))
	for _, entry := range d.growingSegments {
		entry = d.snapshotEntryForQueryView(entry)
		growing = append(growing, entry)
		snapshotGrowings[entry.SegmentID] = entry
	}

	// update snapshot version
	d.snapshotVersion++
	newSnapShot := NewSnapshot(dist, growing, last, d.snapshotVersion, d.queryView.GetVersion())
	newSnapShot.partitions = d.queryView.partitions

	d.current.Store(newSnapShot)
	// shall be a new one
	d.snapshots.GetOrInsert(d.snapshotVersion, newSnapShot)
	d.snapshotNodeSegments = snapshotNodeSegments
	d.snapshotSegmentPosition = snapshotSegmentPositions
	d.snapshotSegments = snapshotSegments
	d.snapshotGrowings = snapshotGrowings
	d.resetSnapshotDeltaLocked()

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

// SealedSegmentExists checks if a sealed segment exists in distribution.
func (d *distribution) SealedSegmentExists(segmentID int64) bool {
	d.mut.RLock()
	defer d.mut.RUnlock()
	_, ok := d.sealedSegments[segmentID]
	return ok
}

// SealedSegmentExistsOnNode checks if a sealed segment exists on a specific node.
func (d *distribution) SealedSegmentExistsOnNode(segmentID int64, nodeID int64) bool {
	d.mut.RLock()
	defer d.mut.RUnlock()
	entry, ok := d.sealedSegments[segmentID]
	return ok && entry.NodeID == nodeID
}

// GrowingSegmentExists checks if a growing segment exists in distribution.
func (d *distribution) GrowingSegmentExists(segmentID int64) bool {
	d.mut.RLock()
	defer d.mut.RUnlock()
	_, ok := d.growingSegments[segmentID]
	return ok
}

// BatchGetFromSegments performs batch PK existence check on the provided pinned segments.
// This ensures consistency between BF check and delete application by using the same
// segment snapshot. This function operates on explicitly provided segments rather than
// live distribution data, preventing race conditions where new segments could be added
// between PinOnlineSegments and this call.
//
// Parameters:
//   - pks: Primary keys to check
//   - partitionID: Partition filter (use common.AllPartitionsID for all)
//   - sealed: Pinned sealed segments from PinOnlineSegments()
//   - growing: Pinned growing segments from PinOnlineSegments()
//
// Returns:
//   - map[segmentID][]bool: For each segment, a bool slice indicating PK existence
func BatchGetFromSegments(pks []storage.PrimaryKey, partitionID int64, sealed []SnapshotItem, growing []SegmentEntry) map[int64][]bool {
	result := make(map[int64][]bool)
	lc := storage.NewBatchLocationsCache(pks)

	allTrue := func() []bool {
		hits := make([]bool, lc.Size())
		for i := range hits {
			hits[i] = true
		}
		return hits
	}

	// When bloom filter is disabled, skip BF checks entirely and broadcast all deletes.
	if !paramtable.Get().CommonCfg.BloomFilterEnabled.GetAsBool() {
		for _, item := range sealed {
			for _, entry := range item.Segments {
				if entry.Offline || entry.Candidate == nil {
					continue
				}
				if partitionID != common.AllPartitionsID && entry.Candidate.Partition() != partitionID {
					continue
				}
				result[entry.SegmentID] = allTrue()
			}
		}
		for _, entry := range growing {
			if entry.Offline || entry.Candidate == nil {
				continue
			}
			if partitionID != common.AllPartitionsID && entry.PartitionID != partitionID {
				continue
			}
			result[entry.SegmentID] = allTrue()
		}
		return result
	}

	// Check sealed segments from pinned snapshot
	for _, item := range sealed {
		for _, entry := range item.Segments {
			if entry.Offline || entry.Candidate == nil {
				continue
			}
			if partitionID != common.AllPartitionsID && entry.Candidate.Partition() != partitionID {
				continue
			}
			if !entry.Candidate.PkCandidateExist() {
				result[entry.SegmentID] = allTrue()
				continue
			}
			result[entry.SegmentID] = entry.Candidate.BatchPkExist(lc)
		}
	}

	// Check growing segments from pinned snapshot
	for _, entry := range growing {
		if entry.Offline || entry.Candidate == nil {
			continue
		}
		if partitionID != common.AllPartitionsID && entry.Candidate.Partition() != partitionID {
			continue
		}
		if !entry.Candidate.PkCandidateExist() {
			result[entry.SegmentID] = allTrue()
			continue
		}
		result[entry.SegmentID] = entry.Candidate.BatchPkExist(lc)
	}

	return result
}

// Flush synchronously generates a snapshot so that subsequent reads
// (e.g. PeekSegments) see the latest distribution state.
// This is useful in tests and in scenarios that require immediate consistency.
func (d *distribution) Flush() {
	d.mut.Lock()
	d.genSnapshot()
	d.updateServiceable("Flush")
	d.mut.Unlock()
}

// Close stops the background snapshot loop and waits for it to exit.
func (d *distribution) Close() {
	d.closeOnce.Do(func() {
		d.closed.Store(true)
		close(d.snapshotClose)
	})
	<-d.snapshotDone
}

// RefundAllCandidates refunds resources for all sealed segment candidates.
// Used during shutdown to clean up and refund resources.
// Note: Growing segment candidates (LocalSegment) are managed by segmentManager.
func (d *distribution) RefundAllCandidates() {
	d.mut.Lock()
	var toRefund []pkoracle.Candidate

	// Only refund sealed segment candidates
	// Growing segment candidates (LocalSegment) are managed by segmentManager
	for segmentID, entry := range d.sealedSegments {
		if entry.Candidate != nil {
			toRefund = append(toRefund, entry.Candidate)
			entry.Candidate = nil
			d.sealedSegments[segmentID] = entry
		}
	}
	d.mut.Unlock()

	refundCandidates(toRefund)
}
