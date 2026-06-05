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

package datacoord

import (
	"cmp"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// =====================================================================
// segmentViewMeta — main struct
// =====================================================================

type segmentViewMeta struct {
	ctx      context.Context
	catalog  metastore.DataCoordCatalog
	collLock *lock.KeyLock[int64] // per-collection RWMutex

	// Segments — ConcurrentMap for cross-collection safety.
	// Inner maps of coll2Segments are plain maps protected by collLock.
	segments      *typeutil.ConcurrentMap[UniqueID, *SegmentInfo]
	coll2Segments *typeutil.ConcurrentMap[int64, map[UniqueID]*SegmentInfo]
	compactionTo  *typeutil.ConcurrentMap[UniqueID, []UniqueID]

	// DataViews — outer ConcurrentMap, inner collectionDataViews protected by collLock.
	dataViews *typeutil.ConcurrentMap[int64, *collectionDataViews]
}

func newSegmentViewMeta(ctx context.Context, catalog metastore.DataCoordCatalog) *segmentViewMeta {
	return &segmentViewMeta{
		ctx:           ctx,
		catalog:       catalog,
		collLock:      lock.NewKeyLock[int64](),
		segments:      typeutil.NewConcurrentMap[UniqueID, *SegmentInfo](),
		coll2Segments: typeutil.NewConcurrentMap[int64, map[UniqueID]*SegmentInfo](),
		compactionTo:  typeutil.NewConcurrentMap[UniqueID, []UniqueID](),
		dataViews:     typeutil.NewConcurrentMap[int64, *collectionDataViews](),
	}
}

// =====================================================================
// Segment read methods (lock-free via ConcurrentMap, or RLock for inner maps)
// =====================================================================

// GetSegment returns the SegmentInfo for the given segmentID, or nil if not found.
func (m *segmentViewMeta) GetSegment(segmentID UniqueID) *SegmentInfo {
	seg, _ := m.segments.Get(segmentID)
	return seg
}

// GetSegments returns all segments stored in the meta.
func (m *segmentViewMeta) GetSegments() []*SegmentInfo {
	return m.segments.Values()
}

// GetSegmentsBySelector filters segments using the provided filters.
// Uses secondary indexes to narrow the search space, then applies criterion.Match.
func (m *segmentViewMeta) GetSegmentsBySelector(filters ...SegmentFilter) []*SegmentInfo {
	criterion := &segmentCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	var result []*SegmentInfo
	if criterion.collectionID > 0 {
		m.collLock.RLock(criterion.collectionID)
		collSegments, ok := m.coll2Segments.Get(criterion.collectionID)
		if !ok {
			m.collLock.RUnlock(criterion.collectionID)
			return nil
		}
		for _, seg := range collSegments {
			if criterion.Match(seg) {
				result = append(result, seg)
			}
		}
		m.collLock.RUnlock(criterion.collectionID)
	} else {
		m.segments.Range(func(_ UniqueID, seg *SegmentInfo) bool {
			if criterion.Match(seg) {
				result = append(result, seg)
			}
			return true
		})
	}
	return result
}

// GetCompactionTo returns the segments that the provided segment was compacted to.
func (m *segmentViewMeta) GetCompactionTo(fromSegmentID int64) ([]*SegmentInfo, bool) {
	_, exist := m.segments.Get(fromSegmentID)
	compactTos, ok := m.compactionTo.Get(fromSegmentID)
	if ok {
		result := make([]*SegmentInfo, 0, len(compactTos))
		for _, toID := range compactTos {
			to, found := m.segments.Get(toID)
			if !found {
				mlog.Warn(context.TODO(), "compactionTo relation is broken", mlog.Int64("from", fromSegmentID), mlog.Int64("to", toID))
				return nil, exist
			}
			result = append(result, to)
		}
		return result, exist
	}
	return nil, exist
}

// =====================================================================
// Segment write methods (memory-only, caller must hold collLock)
// =====================================================================

// SetSegment inserts or overwrites a segment in the meta, maintaining all
// secondary indexes and compaction relationships.
func (m *segmentViewMeta) setSegment(segmentID UniqueID, segment *SegmentInfo) {
	if old, ok := m.segments.Get(segmentID); ok {
		m.removeCompactionTo(old)
		m.removeSecondaryIndex(old)
	}
	m.segments.Insert(segmentID, segment)
	m.addSecondaryIndex(segment)
	m.addCompactionTo(segment)
}

// dropSegmentFromMemory removes a segment from the meta and cleans up all
// secondary indexes and compaction relationships.
func (m *segmentViewMeta) dropSegmentFromMemory(segmentID UniqueID) {
	seg, ok := m.segments.Get(segmentID)
	if !ok {
		return
	}
	m.removeCompactionTo(seg)
	m.removeSecondaryIndex(seg)
	m.segments.Remove(segmentID)
}

func (m *segmentViewMeta) addSecondaryIndex(segment *SegmentInfo) {
	collID := segment.GetCollectionID()
	collMap, _ := m.coll2Segments.GetOrInsert(collID, make(map[UniqueID]*SegmentInfo))
	collMap[segment.ID] = segment
}

func (m *segmentViewMeta) removeSecondaryIndex(segment *SegmentInfo) {
	collID := segment.GetCollectionID()
	if collMap, ok := m.coll2Segments.Get(collID); ok {
		delete(collMap, segment.ID)
		if len(collMap) == 0 {
			m.coll2Segments.Remove(collID)
		}
	}
}

func (m *segmentViewMeta) addCompactionTo(segment *SegmentInfo) {
	for _, from := range segment.GetCompactionFrom() {
		existing, _ := m.compactionTo.Get(from)
		m.compactionTo.Insert(from, append(existing, segment.GetID()))
	}
}

func (m *segmentViewMeta) removeCompactionTo(segment *SegmentInfo) {
	for _, from := range segment.GetCompactionFrom() {
		m.compactionTo.Remove(from)
	}
}

// ModifySegments atomically applies in-memory-only options to segments under collLock.
// Use this for transient fields (isCompacting, allocations, lastFlushTime, etc.)
// that do not need catalog persistence.
// All segmentIDs must belong to the given collectionID.
func (m *segmentViewMeta) ModifySegments(collectionID int64, segmentIDs []UniqueID, opts ...SegmentInfoOption) {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)
	for _, id := range segmentIDs {
		if seg, ok := m.segments.Get(id); ok {
			m.segments.Insert(id, seg.ShadowClone(opts...))
		}
	}
}

// =====================================================================
// Segment persistence methods (with collLock)
// =====================================================================

// DropSegment removes a segment from both catalog and memory.
func (m *segmentViewMeta) DropSegment(ctx context.Context, collectionID int64, segmentID UniqueID) error {
	log := logger.Ctx(ctx)
	mlog.Debug(context.TODO(), "meta update: dropping segment", mlog.Int64("segmentID", segmentID))

	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	segment := m.GetSegment(segmentID)
	if segment == nil {
		mlog.Warn(context.TODO(), "meta update: dropping segment failed - segment not found",
			mlog.Int64("segmentID", segmentID))
		return nil
	}

	if err := m.catalog.DropSegment(ctx, segment.SegmentInfo); err != nil {
		mlog.Warn(context.TODO(), "meta update: dropping segment failed",
			mlog.Int64("segmentID", segmentID),
			mlog.Err(err))
		return err
	}

	metrics.DataCoordNumSegments.WithLabelValues(
		segment.GetState().String(), segment.GetLevel().String(),
		getSortStatus(segment.GetIsSorted()), fmt.Sprint(segment.GetStorageVersion()),
	).Dec()
	m.dropSegmentFromMemory(segmentID)
	mlog.Info(context.TODO(), "meta update: dropping segment - complete",
		mlog.Int64("segmentID", segmentID))
	return nil
}

// UpdateSegments applies the given operators to prepare segment updates, then
// persists all changed segments to the catalog and updates memory.
func (m *segmentViewMeta) UpdateSegments(ctx context.Context, collectionID int64, operators ...UpdateOperator) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	updatePack := &updateSegmentPack{
		svm:          m,
		collectionID: collectionID,
		segments:     make(map[int64]*SegmentInfo),
		increments:   make(map[int64]metastore.BinlogsIncrement),
		metricMutation: &segMetricMutation{
			stateChange: make(map[string]map[string]map[string]map[string]int),
		},
	}

	for _, operator := range operators {
		operator(updatePack)
	}
	if len(updatePack.segments) == 0 {
		return nil
	}
	if err := updatePack.Validate(); err != nil {
		return err
	}

	segments := lo.MapToSlice(updatePack.segments, func(_ int64, seg *SegmentInfo) *datapb.SegmentInfo { return seg.SegmentInfo })
	increments := lo.Values(updatePack.increments)

	if err := m.catalog.AlterSegments(ctx, segments, increments...); err != nil {
		mlog.Error(ctx, "meta update: update segments failed", mlog.Err(err))
		return err
	}

	updatePack.metricMutation.commit()
	for id, s := range updatePack.segments {
		m.setSegment(id, s)
	}
	mlog.Info(ctx, "meta update: update segments - complete")
	return nil
}

// =====================================================================
// DataView types
// =====================================================================

// dataViewVersionKey is the composite key for a data view version.
type dataViewVersionKey struct {
	StreamingVersion int64
	CompactVersion   int64
}

func newDataViewVersionKey(v *viewpb.DataVersion) dataViewVersionKey {
	if v == nil {
		return dataViewVersionKey{}
	}
	return dataViewVersionKey{
		StreamingVersion: v.GetStreamingVersion(),
		CompactVersion:   v.GetCompactVersion(),
	}
}

// CompareDataViewVersion compares two DataViewVersion values lexicographically.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// nil is considered less than any non-nil version.
func CompareDataViewVersion(a, b *viewpb.DataVersion) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if c := cmp.Compare(a.GetStreamingVersion(), b.GetStreamingVersion()); c != 0 {
		return c
	}
	return cmp.Compare(a.GetCompactVersion(), b.GetCompactVersion())
}

// CollectionDataView is the in-memory representation of DataViewOfCollection.
// It uses maps instead of repeated proto fields for O(1) segment lookup.
type CollectionDataView struct {
	collectionID int64
	version      *viewpb.DataVersion
	shards       map[string]*ShardDataView // vchannel → shard
}

// ShardDataView is the in-memory representation of DataViewOfShard.
type ShardDataView struct {
	vchannel                      string
	deleteApplyStartAfterTimetick uint64
	partitions                    map[int64]map[int64]struct{} // partitionID → segmentID set
}

// collectionDataViews holds multiple versioned DataViews for a single collection.
type collectionDataViews struct {
	views          map[dataViewVersionKey]*CollectionDataView
	currentVersion *viewpb.DataVersion
	versionList    []*viewpb.DataVersion // sorted ascending

	// segmentRefs tracks how many DataView versions reference each segment.
	// Used by GC: a Dropped segment can only be physically deleted when its ref count is 0.
	segmentRefs map[int64]int32
}

func newCollectionDataViews() *collectionDataViews {
	return &collectionDataViews{
		views:       make(map[dataViewVersionKey]*CollectionDataView),
		segmentRefs: make(map[int64]int32),
	}
}

// collectionDataViewFromProto converts a proto DataViewOfCollection to CollectionDataView.
func collectionDataViewFromProto(pb *viewpb.DataViewOfCollection) *CollectionDataView {
	v := &CollectionDataView{
		collectionID: pb.GetCollectionId(),
		version:      pb.GetDataVersion(),
		shards:       make(map[string]*ShardDataView, len(pb.GetShards())),
	}
	for _, s := range pb.GetShards() {
		sdv := &ShardDataView{
			vchannel:                      s.GetVchannel(),
			deleteApplyStartAfterTimetick: s.GetDeleteApplyStartAfterTimetick(),
			partitions:                    make(map[int64]map[int64]struct{}, len(s.GetPartitions())),
		}
		for _, p := range s.GetPartitions() {
			segSet := make(map[int64]struct{}, len(p.GetSegmentIds()))
			for _, id := range p.GetSegmentIds() {
				segSet[id] = struct{}{}
			}
			sdv.partitions[p.GetPartitionId()] = segSet
		}
		v.shards[s.GetVchannel()] = sdv
	}
	return v
}

// toProto converts the CollectionDataView back to proto for persistence.
func (v *CollectionDataView) toProto() *viewpb.DataViewOfCollection {
	pb := &viewpb.DataViewOfCollection{
		CollectionId: v.collectionID,
		DataVersion:  v.version,
		Shards:       make([]*viewpb.DataViewOfShard, 0, len(v.shards)),
	}
	for _, sdv := range v.shards {
		shard := &viewpb.DataViewOfShard{
			Vchannel:                      sdv.vchannel,
			DeleteApplyStartAfterTimetick: sdv.deleteApplyStartAfterTimetick,
			Partitions:                    make([]*viewpb.DataViewOfPartition, 0, len(sdv.partitions)),
		}
		for partID, segSet := range sdv.partitions {
			segIDs := make([]int64, 0, len(segSet))
			for id := range segSet {
				segIDs = append(segIDs, id)
			}
			shard.Partitions = append(shard.Partitions, &viewpb.DataViewOfPartition{
				PartitionId: partID,
				SegmentIds:  segIDs,
			})
		}
		pb.Shards = append(pb.Shards, shard)
	}
	return pb
}

// clone returns a deep copy of the CollectionDataView.
func (v *CollectionDataView) clone() *CollectionDataView {
	c := &CollectionDataView{
		collectionID: v.collectionID,
		version:      proto.Clone(v.version).(*viewpb.DataVersion),
		shards:       make(map[string]*ShardDataView, len(v.shards)),
	}
	for ch, sdv := range v.shards {
		csdv := &ShardDataView{
			vchannel:                      sdv.vchannel,
			deleteApplyStartAfterTimetick: sdv.deleteApplyStartAfterTimetick,
			partitions:                    make(map[int64]map[int64]struct{}, len(sdv.partitions)),
		}
		for partID, segSet := range sdv.partitions {
			cSet := make(map[int64]struct{}, len(segSet))
			for id := range segSet {
				cSet[id] = struct{}{}
			}
			csdv.partitions[partID] = cSet
		}
		c.shards[ch] = csdv
	}
	return c
}

// addSegment adds a segment to the specified shard and partition.
func (v *CollectionDataView) addSegment(vchannel string, partitionID, segmentID int64) {
	sdv, ok := v.shards[vchannel]
	if !ok {
		sdv = &ShardDataView{
			vchannel:   vchannel,
			partitions: make(map[int64]map[int64]struct{}),
		}
		v.shards[vchannel] = sdv
	}
	segSet, ok := sdv.partitions[partitionID]
	if !ok {
		segSet = make(map[int64]struct{})
		sdv.partitions[partitionID] = segSet
	}
	segSet[segmentID] = struct{}{}
}

// removeSegments removes the given segment IDs from all shards and partitions.
func (v *CollectionDataView) removeSegments(segmentIDs []int64) {
	toRemove := make(map[int64]struct{}, len(segmentIDs))
	for _, id := range segmentIDs {
		toRemove[id] = struct{}{}
	}
	for _, sdv := range v.shards {
		for _, segSet := range sdv.partitions {
			for id := range toRemove {
				delete(segSet, id)
			}
		}
	}
}

// removePartitions removes the given partition IDs from all shards.
func (v *CollectionDataView) removePartitions(partitionIDs []int64) {
	toRemove := make(map[int64]struct{}, len(partitionIDs))
	for _, id := range partitionIDs {
		toRemove[id] = struct{}{}
	}
	for _, sdv := range v.shards {
		for partID := range sdv.partitions {
			if _, ok := toRemove[partID]; ok {
				delete(sdv.partitions, partID)
			}
		}
	}
}

// =====================================================================
// DataView read methods (RLock for inner collectionDataViews)
// =====================================================================

// IsSegmentInDataView returns true if the segment is referenced by any DataView version.
// Used by GC to determine if a Dropped segment can be physically deleted.
func (m *segmentViewMeta) IsSegmentInDataView(collectionID, segmentID int64) bool {
	m.collLock.RLock(collectionID)
	defer m.collLock.RUnlock(collectionID)

	dvc, ok := m.dataViews.Get(collectionID)
	if !ok {
		return false
	}
	return dvc.segmentRefs[segmentID] > 0
}

// GetCurrentVersion returns a deep-cloned current DataVersion for the collection, or nil.
func (m *segmentViewMeta) GetCurrentVersion(collectionID int64) *viewpb.DataVersion {
	m.collLock.RLock(collectionID)
	defer m.collLock.RUnlock(collectionID)

	dvc, ok := m.dataViews.Get(collectionID)
	if !ok || dvc.currentVersion == nil {
		return nil
	}
	return proto.Clone(dvc.currentVersion).(*viewpb.DataVersion)
}

// GetDataView returns a deep-cloned DataViewOfCollection for the specified version, or nil.
func (m *segmentViewMeta) GetDataView(collectionID int64, version *viewpb.DataVersion) *viewpb.DataViewOfCollection {
	m.collLock.RLock(collectionID)
	defer m.collLock.RUnlock(collectionID)

	dvc, ok := m.dataViews.Get(collectionID)
	if !ok {
		return nil
	}
	cdv, ok := dvc.views[newDataViewVersionKey(version)]
	if !ok {
		return nil
	}
	return cdv.toProto()
}

// ListDataViews returns all DataViews for a collection in ascending version order (deep-cloned).
func (m *segmentViewMeta) ListDataViews(collectionID int64) []*viewpb.DataViewOfCollection {
	m.collLock.RLock(collectionID)
	defer m.collLock.RUnlock(collectionID)

	dvc, ok := m.dataViews.Get(collectionID)
	if !ok || len(dvc.versionList) == 0 {
		return nil
	}
	result := make([]*viewpb.DataViewOfCollection, 0, len(dvc.versionList))
	for _, v := range dvc.versionList {
		if cdv, ok := dvc.views[newDataViewVersionKey(v)]; ok {
			result = append(result, cdv.toProto())
		}
	}
	return result
}

// =====================================================================
// DataView write methods (caller must hold collLock) + reload + drop
// =====================================================================

// addDataView adds a CollectionDataView into the in-memory cache.
// Caller must hold collLock.Lock(collectionID).
func (m *segmentViewMeta) addDataView(collectionID int64, cdv *CollectionDataView) {
	if cdv == nil {
		return
	}
	dvc, _ := m.dataViews.GetOrInsert(collectionID, newCollectionDataViews())

	version := cdv.version
	dvc.views[newDataViewVersionKey(version)] = cdv

	// Insert version at correct sorted position.
	newList := make([]*viewpb.DataVersion, 0, len(dvc.versionList)+1)
	inserted := false
	for _, v := range dvc.versionList {
		if !inserted && CompareDataViewVersion(version, v) < 0 {
			newList = append(newList, version)
			inserted = true
		}
		newList = append(newList, v)
	}
	if !inserted {
		newList = append(newList, version)
	}

	dvc.versionList = newList
	dvc.currentVersion = newList[len(newList)-1]

	// Increment segment ref counts for all segments in this DataView.
	for _, sdv := range cdv.shards {
		for _, segSet := range sdv.partitions {
			for segID := range segSet {
				dvc.segmentRefs[segID]++
			}
		}
	}
}

// DropDataView removes a specific DataView version from catalog and memory.
func (m *segmentViewMeta) DropDataView(ctx context.Context, collectionID int64, version *viewpb.DataVersion) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	if err := m.catalog.DropDataView(ctx, collectionID, version); err != nil {
		return err
	}

	dvc, ok := m.dataViews.Get(collectionID)
	if !ok {
		return nil
	}

	// Decrement segment ref counts for all segments in the dropped DataView.
	key := newDataViewVersionKey(version)
	if droppedCDV, ok := dvc.views[key]; ok {
		for _, sdv := range droppedCDV.shards {
			for _, segSet := range sdv.partitions {
				for segID := range segSet {
					dvc.segmentRefs[segID]--
					if dvc.segmentRefs[segID] <= 0 {
						delete(dvc.segmentRefs, segID)
					}
				}
			}
		}
	}
	delete(dvc.views, key)

	// Rebuild versionList without the dropped version.
	newList := make([]*viewpb.DataVersion, 0, len(dvc.versionList))
	for _, v := range dvc.versionList {
		if CompareDataViewVersion(v, version) != 0 {
			newList = append(newList, v)
		}
	}
	dvc.versionList = newList

	if len(newList) > 0 {
		dvc.currentVersion = newList[len(newList)-1]
	} else {
		dvc.currentVersion = nil
	}
	return nil
}

// DropDataViewsByCollection removes all DataViews for a collection from catalog and memory.
func (m *segmentViewMeta) DropDataViewsByCollection(ctx context.Context, collectionID int64) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	if err := m.catalog.DropDataViewsByCollection(ctx, collectionID); err != nil {
		return err
	}
	m.dataViews.Remove(collectionID)
	return nil
}

// reloadDataViews loads all DataViews from catalog and reconstructs in-memory state.
// Called during initialization after segments are already loaded.
func (m *segmentViewMeta) reloadDataViews() error {
	allViews, err := m.catalog.ListDataViews(m.ctx)
	if err != nil {
		return err
	}
	for collectionID, views := range allViews {
		dvc := newCollectionDataViews()
		for _, pbView := range views {
			cdv := collectionDataViewFromProto(pbView)
			dvc.views[newDataViewVersionKey(pbView.GetDataVersion())] = cdv
		}

		// Build sorted versionList.
		versionList := make([]*viewpb.DataVersion, 0, len(views))
		for _, pbView := range views {
			versionList = append(versionList, pbView.GetDataVersion())
		}
		sort.Slice(versionList, func(i, j int) bool {
			return CompareDataViewVersion(versionList[i], versionList[j]) < 0
		})
		dvc.versionList = versionList
		if len(versionList) > 0 {
			dvc.currentVersion = versionList[len(versionList)-1]
		}

		// Build segment ref counts from all views.
		for _, cdv := range dvc.views {
			for _, sdv := range cdv.shards {
				for _, segSet := range sdv.partitions {
					for segID := range segSet {
						dvc.segmentRefs[segID]++
					}
				}
			}
		}

		m.dataViews.Insert(collectionID, dvc)
	}
	mlog.Info(context.TODO(), "segmentViewMeta reloadDataViews done",
		mlog.Int("numCollections", len(allViews)))
	return nil
}

// =====================================================================
// Flush — atomic segment + DataView update
// =====================================================================

// FlushSegments atomically updates segment info and DataView when segments are flushed.
// Supports multi-segment flush (e.g. manual flush) in a single DataView version bump.
// segmentIDs specifies which segments to add to the DataView; operators modify segment metadata.
func (m *segmentViewMeta) FlushSegments(ctx context.Context, collectionID int64, segmentIDs []int64, operators ...UpdateOperator) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	// 1. Prepare segment changes
	updatePack := &updateSegmentPack{
		svm:          m,
		collectionID: collectionID,
		segments:     make(map[int64]*SegmentInfo),
		increments:   make(map[int64]metastore.BinlogsIncrement),
		metricMutation: &segMetricMutation{
			stateChange: make(map[string]map[string]map[string]map[string]int),
		},
	}
	for _, operator := range operators {
		operator(updatePack)
	}
	if len(updatePack.segments) == 0 {
		return nil
	}
	if err := updatePack.Validate(); err != nil {
		return err
	}

	// 2. Compute new DataView
	// streaming_version increments, compact_version resets to 0.
	var newStreamingVer int64 = 1

	dvc, hasDvc := m.dataViews.Get(collectionID)
	var currentCDV *CollectionDataView
	if hasDvc && dvc.currentVersion != nil {
		newStreamingVer = dvc.currentVersion.GetStreamingVersion() + 1
		currentCDV = dvc.views[newDataViewVersionKey(dvc.currentVersion)]
	}

	var newCDV *CollectionDataView
	if currentCDV != nil {
		newCDV = currentCDV.clone()
	} else {
		newCDV = &CollectionDataView{
			collectionID: collectionID,
			shards:       make(map[string]*ShardDataView),
		}
	}

	newVersion := &viewpb.DataVersion{
		StreamingVersion: newStreamingVer,
		CompactVersion:   0,
	}
	newCDV.version = newVersion

	// Add flushed segments to DataView, deriving vchannel and partitionID from each segment.
	for _, segID := range segmentIDs {
		seg, ok := updatePack.segments[segID]
		if !ok {
			seg = m.GetSegment(segID)
		}
		if seg == nil {
			continue
		}
		newCDV.addSegment(seg.GetInsertChannel(), seg.GetPartitionID(), segID)
	}

	// 3. Atomic catalog write
	newViewProto := newCDV.toProto()
	segments := lo.MapToSlice(updatePack.segments, func(_ int64, seg *SegmentInfo) *datapb.SegmentInfo { return seg.SegmentInfo })
	increments := lo.Values(updatePack.increments)
	if err := m.catalog.AlterSegmentsAndSaveDataView(ctx, segments, collectionID, newViewProto, increments...); err != nil {
		mlog.Error(ctx, "meta update: flush segments failed", mlog.Int64s("segmentIDs", segmentIDs), mlog.Err(err))
		return err
	}

	// 4. Update segment memory
	updatePack.metricMutation.commit()
	for id, s := range updatePack.segments {
		m.setSegment(id, s)
	}

	// 5. Update DataView memory
	m.addDataView(collectionID, newCDV)

	mlog.Info(ctx, "meta update: flush segments - complete", mlog.Int64s("segmentIDs", segmentIDs))
	return nil
}

// =====================================================================
// DropPartition — atomic segment drop + DataView update
// =====================================================================

// DropPartition atomically marks all segments of the given partitions as Dropped
// and removes them from the current DataView.
func (m *segmentViewMeta) DropPartition(ctx context.Context, collectionID int64, partitionIDs []int64) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	logger := mlog.With(
		mlog.Int64("collectionID", collectionID),
		mlog.Int64s("partitionIDs", partitionIDs),
	)
	mlog.Info(context.TODO(), "DropPartition - Start")

	// 1. Find and mark segments as Dropped.
	metricMutation := &segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
	var modSegments []*SegmentInfo
	collSegments, ok := m.coll2Segments.Get(collectionID)
	if ok {
		partSet := make(map[int64]struct{}, len(partitionIDs))
		for _, pid := range partitionIDs {
			partSet[pid] = struct{}{}
		}
		for _, seg := range collSegments {
			if _, match := partSet[seg.GetPartitionID()]; match {
				cloned := seg.Clone()
				cloned.DroppedAt = uint64(time.Now().UnixNano())
				updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)
				modSegments = append(modSegments, cloned)
			}
		}
	}

	// 2. Compute new DataView: remove partitions from current view.
	var newCDV *CollectionDataView
	dvc, hasDvc := m.dataViews.Get(collectionID)
	if hasDvc && dvc.currentVersion != nil {
		currentCDV, exists := dvc.views[newDataViewVersionKey(dvc.currentVersion)]
		if exists {
			newCDV = currentCDV.clone()
			newCDV.version = &viewpb.DataVersion{
				StreamingVersion: dvc.currentVersion.GetStreamingVersion() + 1,
				CompactVersion:   0,
			}
			newCDV.removePartitions(partitionIDs)
		}
	}

	// 3. Persist: segments + DataView atomically.
	segInfos := lo.Map(modSegments, func(s *SegmentInfo, _ int) *datapb.SegmentInfo { return s.SegmentInfo })
	var newViewProto *viewpb.DataViewOfCollection
	if newCDV != nil {
		newViewProto = newCDV.toProto()
	}
	if len(segInfos) > 0 || newViewProto != nil {
		if err := m.catalog.AlterSegmentsAndSaveDataView(ctx, segInfos, collectionID, newViewProto); err != nil {
			mlog.Error(context.TODO(), "DropPartition: catalog write failed", mlog.Err(err))
			return err
		}
	}

	// 4. Update memory.
	for _, seg := range modSegments {
		m.setSegment(seg.GetID(), seg)
	}
	metricMutation.commit()
	m.addDataView(collectionID, newCDV)

	mlog.Info(context.TODO(), "DropPartition - complete",
		mlog.Int("numDroppedSegments", len(modSegments)))
	return nil
}

// =====================================================================
// TruncateCollection — atomic segment drop + DataView update for truncate
// =====================================================================

// TruncateCollection atomically marks the given segments as Dropped, removes them
// from the current DataView, and updates deleteApplyStartAfterTimetick per shard.
// Called by DropSegmentsByTime after all channel checkpoints have been reached.
func (m *segmentViewMeta) TruncateCollection(ctx context.Context, collectionID int64, segmentIDs []int64, flushTsList map[string]uint64) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	logger := mlog.With(
		mlog.Int64("collectionID", collectionID),
		mlog.Int("numSegments", len(segmentIDs)),
	)
	mlog.Info(context.TODO(), "TruncateCollection - Start")

	// 1. Find and mark segments as Dropped.
	metricMutation := &segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
	var modSegments []*SegmentInfo
	for _, segID := range segmentIDs {
		seg := m.GetSegment(segID)
		if seg == nil || seg.GetState() == commonpb.SegmentState_Dropped {
			continue
		}
		cloned := seg.Clone()
		cloned.DroppedAt = uint64(time.Now().UnixNano())
		updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)
		modSegments = append(modSegments, cloned)
	}

	// 2. Compute new DataView: remove truncated segments and update deleteApplyStartAfterTimetick.
	var newCDV *CollectionDataView
	dvc, hasDvc := m.dataViews.Get(collectionID)
	if hasDvc && dvc.currentVersion != nil {
		currentCDV, exists := dvc.views[newDataViewVersionKey(dvc.currentVersion)]
		if exists {
			newCDV = currentCDV.clone()
			newCDV.version = &viewpb.DataVersion{
				StreamingVersion: dvc.currentVersion.GetStreamingVersion() + 1,
				CompactVersion:   0,
			}
			newCDV.removeSegments(segmentIDs)
			for ch, flushTs := range flushTsList {
				if sdv, ok := newCDV.shards[ch]; ok {
					if flushTs > sdv.deleteApplyStartAfterTimetick {
						sdv.deleteApplyStartAfterTimetick = flushTs
					}
				}
			}
		}
	}

	// 3. Persist: segments + DataView atomically.
	segInfos := lo.Map(modSegments, func(s *SegmentInfo, _ int) *datapb.SegmentInfo { return s.SegmentInfo })
	var newViewProto *viewpb.DataViewOfCollection
	if newCDV != nil {
		newViewProto = newCDV.toProto()
	}
	if len(segInfos) > 0 || newViewProto != nil {
		if err := m.catalog.AlterSegmentsAndSaveDataView(ctx, segInfos, collectionID, newViewProto); err != nil {
			mlog.Error(context.TODO(), "TruncateCollection: catalog write failed", mlog.Err(err))
			return err
		}
	}

	// 4. Update memory.
	for _, seg := range modSegments {
		m.setSegment(seg.GetID(), seg)
	}
	metricMutation.commit()
	m.addDataView(collectionID, newCDV)

	mlog.Info(context.TODO(), "TruncateCollection - complete",
		mlog.Int("numDroppedSegments", len(modSegments)))
	return nil
}

// =====================================================================
// Compaction — atomic segment + DataView update
// =====================================================================

// CompleteCompactionMutation dispatches to the appropriate compaction mutation
// method based on compaction type, under per-collection locking.
func (m *segmentViewMeta) CompleteCompactionMutation(ctx context.Context, t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
	m.collLock.Lock(t.CollectionID)
	defer m.collLock.Unlock(t.CollectionID)

	switch t.GetType() {
	case datapb.CompactionType_MixCompaction:
		return m.completeMixCompactionMutation(t, result)
	case datapb.CompactionType_ClusteringCompaction:
		return m.completeClusterCompactionMutation(t, result)
	case datapb.CompactionType_SortCompaction:
		return m.completeSortCompactionMutation(t, result)
	default:
		return nil, nil, merr.WrapErrIllegalCompactionPlan("illegal compaction type")
	}
}

// buildCompactionDataView computes the new CollectionDataView for a compaction operation.
// Caller must hold collLock.Lock(collectionID).
// Returns nil if DataView doesn't exist for the collection.
func (m *segmentViewMeta) buildCompactionDataView(collectionID int64, vchannel string, partitionID int64, oldSegmentIDs, newSegmentIDs []int64) *CollectionDataView {
	dvc, ok := m.dataViews.Get(collectionID)
	if !ok || dvc.currentVersion == nil {
		return nil
	}

	currentCDV, ok := dvc.views[newDataViewVersionKey(dvc.currentVersion)]
	if !ok {
		return nil
	}

	newCDV := currentCDV.clone()
	newCDV.version = &viewpb.DataVersion{
		StreamingVersion: dvc.currentVersion.GetStreamingVersion(),
		CompactVersion:   dvc.currentVersion.GetCompactVersion() + 1,
	}

	newCDV.removeSegments(oldSegmentIDs)
	for _, newSegID := range newSegmentIDs {
		newCDV.addSegment(vchannel, partitionID, newSegID)
	}

	return newCDV
}

// completeClusterCompactionMutation handles cluster compaction mutation.
// Caller must hold collLock.Lock(collectionID).
func (m *segmentViewMeta) completeClusterCompactionMutation(t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
	log := logger.Ctx(context.TODO()).With(mlog.Int64("planID", t.GetPlanID()),
		mlog.String("type", t.GetType().String()),
		mlog.Int64("collectionID", t.CollectionID),
		mlog.Int64("partitionID", t.PartitionID),
		mlog.String("channel", t.GetChannel()))

	metricMutation := &segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
	compactFromSegIDs := make([]int64, 0)
	compactToSegIDs := make([]int64, 0)
	compactFromSegInfos := make([]*SegmentInfo, 0)
	compactToSegInfos := make([]*SegmentInfo, 0)

	for _, segmentID := range t.GetInputSegments() {
		segment := m.GetSegment(segmentID)
		if segment == nil {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
		}
		if !isSegmentHealthy(segment) {
			mlog.Warn(context.TODO(), "input segment was dropped during compaction mutation",
				mlog.Int64("segmentID", segmentID),
				mlog.String("state", segment.GetState().String()))
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}
		cloned := segment.Clone()
		compactFromSegInfos = append(compactFromSegInfos, cloned)
		compactFromSegIDs = append(compactFromSegIDs, cloned.GetID())
	}

	for _, seg := range result.GetSegments() {
		segmentInfo := &datapb.SegmentInfo{
			ID:                  seg.GetSegmentID(),
			CollectionID:        compactFromSegInfos[0].CollectionID,
			PartitionID:         compactFromSegInfos[0].PartitionID,
			InsertChannel:       t.GetChannel(),
			NumOfRows:           seg.NumOfRows,
			State:               commonpb.SegmentState_Flushed,
			MaxRowNum:           compactFromSegInfos[0].MaxRowNum,
			Binlogs:             seg.GetInsertLogs(),
			Statslogs:           seg.GetField2StatslogPaths(),
			CreatedByCompaction: true,
			CompactionFrom:      compactFromSegIDs,
			LastExpireTime:      tsoutil.ComposeTSByTime(time.Unix(t.GetStartTime(), 0), 0),
			Level:               datapb.SegmentLevel_L2,
			StartPosition: getMinPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
				return info.GetStartPosition()
			})),
			DmlPosition: getMinPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
				return info.GetDmlPosition()
			})),
			IsInvisible:    true,
			StorageVersion: seg.GetStorageVersion(),
			ManifestPath:   seg.GetManifest(),
			ExpirQuantiles: seg.GetExpirQuantiles(),
		}
		segment := NewSegmentInfo(segmentInfo)
		compactToSegInfos = append(compactToSegInfos, segment)
		compactToSegIDs = append(compactToSegIDs, segment.GetID())
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(), segment.GetStorageVersion(), segment.GetNumOfRows())
	}

	logger = logger.With(mlog.Int64s("compact from", compactFromSegIDs), mlog.Int64s("compact to", compactToSegIDs))
	mlog.Debug(context.TODO(), "meta update: prepare for meta mutation - complete")

	compactToInfos := lo.Map(compactToSegInfos, func(info *SegmentInfo, _ int) *datapb.SegmentInfo {
		return info.SegmentInfo
	})

	binlogs := make([]metastore.BinlogsIncrement, 0)
	for _, seg := range compactToInfos {
		binlogs = append(binlogs, metastore.BinlogsIncrement{Segment: seg})
	}
	newCDV := m.buildCompactionDataView(t.CollectionID, t.GetChannel(), t.PartitionID, compactFromSegIDs, compactToSegIDs)

	// Persist: add new segments (+ DataView)
	var newViewProto *viewpb.DataViewOfCollection
	if newCDV != nil {
		newViewProto = newCDV.toProto()
	}
	if err := m.catalog.AlterSegmentsAndSaveDataView(m.ctx, compactToInfos, t.CollectionID, newViewProto, binlogs...); err != nil {
		mlog.Warn(context.TODO(), "fail to alter compactTo segments", mlog.Err(err))
		return nil, nil, err
	}
	lo.ForEach(compactToSegInfos, func(info *SegmentInfo, _ int) {
		m.setSegment(info.GetID(), info)
	})
	m.addDataView(t.CollectionID, newCDV)

	mlog.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
	return compactToSegInfos, metricMutation, nil
}

// completeMixCompactionMutation handles mix compaction mutation.
// Caller must hold collLock.Lock(collectionID).
func (m *segmentViewMeta) completeMixCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	log := logger.Ctx(context.TODO()).With(mlog.Int64("planID", t.GetPlanID()),
		mlog.String("type", t.GetType().String()),
		mlog.Int64("collectionID", t.CollectionID),
		mlog.Int64("partitionID", t.PartitionID),
		mlog.String("channel", t.GetChannel()),
	)

	metricMutation := &segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
	var compactFromSegIDs []int64
	var compactFromSegInfos []*SegmentInfo
	for _, segmentID := range t.GetInputSegments() {
		segment := m.GetSegment(segmentID)
		if segment == nil {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
		}
		if !isSegmentHealthy(segment) {
			mlog.Warn(context.TODO(), "input segment was dropped during compaction mutation",
				mlog.Int64("segmentID", segmentID),
				mlog.String("state", segment.GetState().String()))
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}

		cloned := segment.Clone()
		cloned.DroppedAt = uint64(time.Now().UnixNano())
		cloned.Compacted = true

		compactFromSegInfos = append(compactFromSegInfos, cloned)
		compactFromSegIDs = append(compactFromSegIDs, cloned.GetID())

		updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)

		mlog.Info(context.TODO(), "compact from segment",
			mlog.Int64("segmentID", cloned.GetID()),
			mlog.Int64("segment size", cloned.getSegmentSize()),
			mlog.Int64("num rows", cloned.GetNumOfRows()),
		)
	}

	logger = logger.With(mlog.Int64s("compactFrom", compactFromSegIDs))

	compactToSegments := make([]*SegmentInfo, 0)
	for _, compactToSegment := range result.GetSegments() {
		compactToSegmentInfo := NewSegmentInfo(
			&datapb.SegmentInfo{
				ID:            compactToSegment.GetSegmentID(),
				CollectionID:  compactFromSegInfos[0].CollectionID,
				PartitionID:   compactFromSegInfos[0].PartitionID,
				InsertChannel: t.GetChannel(),
				NumOfRows:     compactToSegment.NumOfRows,
				State:         commonpb.SegmentState_Flushed,
				MaxRowNum:     compactFromSegInfos[0].MaxRowNum,
				Binlogs:       compactToSegment.GetInsertLogs(),
				Statslogs:     compactToSegment.GetField2StatslogPaths(),
				Deltalogs:     compactToSegment.GetDeltalogs(),
				Bm25Statslogs: compactToSegment.GetBm25Logs(),
				TextStatsLogs: compactToSegment.GetTextStatsLogs(),

				CreatedByCompaction: true,
				CompactionFrom:      compactFromSegIDs,
				LastExpireTime:      tsoutil.ComposeTSByTime(time.Unix(t.GetStartTime(), 0), 0),
				Level:               datapb.SegmentLevel_L1,
				StorageVersion:      compactToSegment.GetStorageVersion(),
				StartPosition: getMinPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
					return info.GetStartPosition()
				})),
				DmlPosition: getMinPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
					return info.GetDmlPosition()
				})),
				IsSorted:       compactToSegment.GetIsSorted(),
				ManifestPath:   compactToSegment.GetManifest(),
				ExpirQuantiles: compactToSegment.GetExpirQuantiles(),
			})

		if compactToSegmentInfo.GetNumOfRows() == 0 {
			compactToSegmentInfo.State = commonpb.SegmentState_Dropped
		}

		metricMutation.addNewSeg(compactToSegmentInfo.GetState(), compactToSegmentInfo.GetLevel(), compactToSegmentInfo.GetIsSorted(), compactToSegmentInfo.GetStorageVersion(), compactToSegmentInfo.GetNumOfRows())

		mlog.Info(context.TODO(), "Add a new compactTo segment",
			mlog.Int64("compactTo", compactToSegmentInfo.GetID()),
			mlog.Int64("compactTo segment numRows", compactToSegmentInfo.GetNumOfRows()),
			mlog.Int("binlog count", len(compactToSegmentInfo.GetBinlogs())),
			mlog.Int("statslog count", len(compactToSegmentInfo.GetStatslogs())),
			mlog.Int("deltalog count", len(compactToSegmentInfo.GetDeltalogs())),
			mlog.Int64("segment size", compactToSegmentInfo.getSegmentSize()),
			mlog.Int64s("expirQuantiles", compactToSegmentInfo.GetExpirQuantiles()),
		)
		compactToSegments = append(compactToSegments, compactToSegmentInfo)
	}

	mlog.Debug(context.TODO(), "meta update: prepare for meta mutation - complete")
	compactFromInfos := lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *datapb.SegmentInfo {
		return info.SegmentInfo
	})

	compactToInfos := lo.Map(compactToSegments, func(info *SegmentInfo, _ int) *datapb.SegmentInfo {
		return info.SegmentInfo
	})

	binlogs := make([]metastore.BinlogsIncrement, 0)
	for _, seg := range compactToInfos {
		binlogs = append(binlogs, metastore.BinlogsIncrement{Segment: seg})
	}

	compactToSegIDs := lo.Map(compactToSegments, func(info *SegmentInfo, _ int) int64 { return info.GetID() })
	newCDV := m.buildCompactionDataView(t.CollectionID, t.GetChannel(), t.PartitionID, compactFromSegIDs, compactToSegIDs)

	// Persist: alter compactTo (+ DataView) before compactFrom to avoid data loss on crash
	var newViewProto *viewpb.DataViewOfCollection
	if newCDV != nil {
		newViewProto = newCDV.toProto()
	}
	if err := m.catalog.AlterSegmentsAndSaveDataView(m.ctx, compactToInfos, t.CollectionID, newViewProto, binlogs...); err != nil {
		mlog.Warn(context.TODO(), "fail to alter compactTo segments", mlog.Err(err))
		return nil, nil, err
	}
	if err := m.catalog.AlterSegments(m.ctx, compactFromInfos); err != nil {
		mlog.Warn(context.TODO(), "fail to alter compactFrom segments", mlog.Err(err))
		return nil, nil, err
	}
	lo.ForEach(compactFromSegInfos, func(info *SegmentInfo, _ int) {
		m.setSegment(info.GetID(), info)
	})
	lo.ForEach(compactToSegments, func(info *SegmentInfo, _ int) {
		m.setSegment(info.GetID(), info)
	})
	m.addDataView(t.CollectionID, newCDV)

	mlog.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
	return compactToSegments, metricMutation, nil
}

// completeSortCompactionMutation handles sort compaction mutation.
// Caller must hold collLock.Lock(collectionID).
func (m *segmentViewMeta) completeSortCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	log := logger.Ctx(context.TODO()).With(mlog.Int64("planID", t.GetPlanID()),
		mlog.String("type", t.GetType().String()),
		mlog.Int64("collectionID", t.CollectionID),
		mlog.Int64("partitionID", t.PartitionID),
		mlog.String("channel", t.GetChannel()))

	metricMutation := &segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
	compactFromSegID := t.GetInputSegments()[0]
	oldSegment := m.GetSegment(compactFromSegID)
	if oldSegment == nil {
		return nil, nil, merr.WrapErrSegmentNotFound(compactFromSegID)
	}

	if !isSegmentHealthy(oldSegment) {
		mlog.Warn(context.TODO(), "input segment was dropped during compaction mutation",
			mlog.Int64("segmentID", compactFromSegID),
			mlog.String("state", oldSegment.GetState().String()))
		return nil, nil, merr.WrapErrSegmentNotFound(compactFromSegID, "input segment was dropped")
	}

	resultInvisible := oldSegment.GetIsInvisible()
	if !oldSegment.GetCreatedByCompaction() {
		resultInvisible = false
	}

	resultSegment := result.GetSegments()[0]

	segmentInfo := &datapb.SegmentInfo{
		CollectionID:              oldSegment.GetCollectionID(),
		PartitionID:               oldSegment.GetPartitionID(),
		InsertChannel:             oldSegment.GetInsertChannel(),
		MaxRowNum:                 oldSegment.GetMaxRowNum(),
		LastExpireTime:            oldSegment.GetLastExpireTime(),
		StartPosition:             oldSegment.GetStartPosition(),
		DmlPosition:               oldSegment.GetDmlPosition(),
		IsImporting:               oldSegment.GetIsImporting(),
		State:                     commonpb.SegmentState_Flushed,
		Level:                     oldSegment.GetLevel(),
		LastLevel:                 oldSegment.GetLastLevel(),
		PartitionStatsVersion:     oldSegment.GetPartitionStatsVersion(),
		LastPartitionStatsVersion: oldSegment.GetLastPartitionStatsVersion(),
		CreatedByCompaction:       oldSegment.GetCreatedByCompaction(),
		IsInvisible:               resultInvisible,
		StorageVersion:            resultSegment.GetStorageVersion(),
		ID:                        resultSegment.GetSegmentID(),
		NumOfRows:                 resultSegment.GetNumOfRows(),
		Binlogs:                   resultSegment.GetInsertLogs(),
		Statslogs:                 resultSegment.GetField2StatslogPaths(),
		TextStatsLogs:             resultSegment.GetTextStatsLogs(),
		Bm25Statslogs:             resultSegment.GetBm25Logs(),
		Deltalogs:                 resultSegment.GetDeltalogs(),
		CompactionFrom:            []int64{compactFromSegID},
		IsSorted:                  true,
		ManifestPath:              resultSegment.GetManifest(),
		ExpirQuantiles:            resultSegment.GetExpirQuantiles(),
	}

	segment := NewSegmentInfo(segmentInfo)
	if segment.GetNumOfRows() > 0 {
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(), segment.GetStorageVersion(), segment.GetNumOfRows())
	} else {
		segment.State = commonpb.SegmentState_Dropped
		segment.DroppedAt = uint64(time.Now().UnixNano())
		mlog.Info(context.TODO(), "drop segment due to 0 rows", mlog.Int64("segmentID", segment.GetID()))
	}

	cloned := oldSegment.Clone()
	cloned.DroppedAt = uint64(time.Now().UnixNano())
	cloned.Compacted = true

	updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)

	logger = logger.With(mlog.Int64s("compactFrom", []int64{oldSegment.GetID()}), mlog.Int64("compactTo", segment.GetID()))

	compactFromSegIDs := []int64{oldSegment.GetID()}
	compactToSegIDs := []int64{segment.GetID()}
	newCDV := m.buildCompactionDataView(t.CollectionID, t.GetChannel(), t.PartitionID, compactFromSegIDs, compactToSegIDs)

	mlog.Info(context.TODO(), "meta update: prepare for complete stats mutation - complete",
		mlog.Int64("num rows", segment.GetNumOfRows()),
		mlog.Int64("segment size", segment.getSegmentSize()),
		mlog.Int64s("expirQuantiles", segment.GetExpirQuantiles()))

	var newViewProto *viewpb.DataViewOfCollection
	if newCDV != nil {
		newViewProto = newCDV.toProto()
	}
	if err := m.catalog.AlterSegmentsAndSaveDataView(m.ctx, []*datapb.SegmentInfo{cloned.SegmentInfo, segment.SegmentInfo}, t.CollectionID, newViewProto, metastore.BinlogsIncrement{Segment: segment.SegmentInfo}); err != nil {
		mlog.Warn(context.TODO(), "fail to alter segments and new segment", mlog.Err(err))
		return nil, nil, err
	}

	m.setSegment(oldSegment.GetID(), cloned)
	m.setSegment(segment.GetID(), segment)
	m.addDataView(t.CollectionID, newCDV)

	mlog.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
	return []*SegmentInfo{segment}, metricMutation, nil
}

// =====================================================================
// Compaction handoff — second step: update DataView after index ready
// =====================================================================

// CompleteCompactionHandoff atomically drops compactFrom segments from the DataView
// and adds compactTo segments, after their indexes are ready.
// This is the second step of the two-phase compaction commit.
func (m *segmentViewMeta) CompleteCompactionHandoff(ctx context.Context, collectionID int64, compactFromIDs, compactToIDs []int64, vchannel string, partitionID int64) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	logger := mlog.With(
		mlog.Int64("collectionID", collectionID),
		mlog.Int64s("compactFrom", compactFromIDs),
		mlog.Int64s("compactTo", compactToIDs),
		mlog.String("channel", vchannel),
	)

	// Build new DataView: remove compactFrom, add compactTo
	newCDV := m.buildCompactionDataView(collectionID, vchannel, partitionID, compactFromIDs, compactToIDs)
	if newCDV == nil {
		mlog.Warn(context.TODO(), "CompleteCompactionHandoff: no DataView for collection, skip")
		return nil
	}

	// Mark compactFrom segments as dropped
	var compactFromInfos []*datapb.SegmentInfo
	metricMutation := &segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
	for _, segID := range compactFromIDs {
		seg := m.GetSegment(segID)
		if seg == nil {
			continue
		}
		cloned := seg.Clone()
		cloned.DroppedAt = uint64(time.Now().UnixNano())
		cloned.Compacted = true
		updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)
		compactFromInfos = append(compactFromInfos, cloned.SegmentInfo)
	}

	// Set data_version on compactTo segments
	var compactToInfos []*datapb.SegmentInfo
	for _, segID := range compactToIDs {
		seg := m.GetSegment(segID)
		if seg == nil {
			mlog.Warn(context.TODO(), "CompleteCompactionHandoff: compactTo segment not found", mlog.Int64("segmentID", segID))
			return merr.WrapErrSegmentNotFound(segID)
		}
		compactToInfos = append(compactToInfos, seg.SegmentInfo)
	}

	// Persist: compactTo segments (+ DataView), then compactFrom segments
	newViewProto := newCDV.toProto()
	if err := m.catalog.AlterSegmentsAndSaveDataView(ctx, compactToInfos, collectionID, newViewProto); err != nil {
		mlog.Warn(context.TODO(), "CompleteCompactionHandoff: fail to save DataView and compactTo", mlog.Err(err))
		return err
	}
	if len(compactFromInfos) > 0 {
		if err := m.catalog.AlterSegments(ctx, compactFromInfos); err != nil {
			mlog.Warn(context.TODO(), "CompleteCompactionHandoff: fail to drop compactFrom", mlog.Err(err))
			return err
		}
	}

	// Update memory
	for _, info := range compactFromInfos {
		m.setSegment(info.GetID(), NewSegmentInfo(info))
	}
	metricMutation.commit()
	m.addDataView(collectionID, newCDV)

	mlog.Info(context.TODO(), "CompleteCompactionHandoff complete")
	return nil
}

// =====================================================================
// Register & Activate — batch segment creation and DataView update
// Used by Import (Bulk Insert) and Snapshot Restore (Copy Segment).
// =====================================================================

// RegisterSegments batch-creates segments (IsImporting=true).
// Only writes segment metadata, does not update DataView.
func (m *segmentViewMeta) RegisterSegments(ctx context.Context, collectionID int64, segments []*SegmentInfo) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	logger := mlog.With(
		mlog.Int64("collectionID", collectionID),
		mlog.Int("numSegments", len(segments)),
	)
	mlog.Info(context.TODO(), "RegisterSegments - Start")

	segInfos := make([]*datapb.SegmentInfo, 0, len(segments))
	for _, seg := range segments {
		segInfos = append(segInfos, seg.SegmentInfo)
	}

	if err := m.catalog.AlterSegments(ctx, segInfos); err != nil {
		mlog.Error(context.TODO(), "RegisterSegments: catalog write failed", mlog.Err(err))
		return err
	}

	for _, seg := range segments {
		m.setSegment(seg.GetID(), seg)
		metrics.DataCoordNumSegments.WithLabelValues(
			seg.GetState().String(), seg.GetLevel().String(),
			getSortStatus(seg.GetIsSorted()), fmt.Sprint(seg.GetStorageVersion()),
		).Inc()
	}

	mlog.Info(context.TODO(), "RegisterSegments - complete")
	return nil
}

// ActivateSegments atomically clears IsImporting flag and adds segments to DataView.
// Called when segments are ready to be queryable (import: after index built; restore: after files copied).
func (m *segmentViewMeta) ActivateSegments(ctx context.Context, collectionID int64, segmentIDs []int64) error {
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)

	logger := mlog.With(
		mlog.Int64("collectionID", collectionID),
		mlog.Int64s("segmentIDs", segmentIDs),
	)
	mlog.Info(context.TODO(), "ActivateSegments - Start")

	// Compute new DataView
	// streaming_version increments, compact_version resets to 0.
	var newStreamingVer int64 = 1

	dvc, hasDvc := m.dataViews.Get(collectionID)
	var currentCDV *CollectionDataView
	if hasDvc && dvc.currentVersion != nil {
		newStreamingVer = dvc.currentVersion.GetStreamingVersion() + 1
		currentCDV = dvc.views[newDataViewVersionKey(dvc.currentVersion)]
	}

	var newCDV *CollectionDataView
	if currentCDV != nil {
		newCDV = currentCDV.clone()
	} else {
		newCDV = &CollectionDataView{
			collectionID: collectionID,
			shards:       make(map[string]*ShardDataView),
		}
	}

	newVersion := &viewpb.DataVersion{
		StreamingVersion: newStreamingVer,
		CompactVersion:   0,
	}
	newCDV.version = newVersion

	// Clear IsImporting and add segments to DataView
	var segInfos []*datapb.SegmentInfo
	for _, segID := range segmentIDs {
		seg := m.GetSegment(segID)
		if seg == nil {
			mlog.Warn(context.TODO(), "ActivateSegments: segment not found, skip", mlog.Int64("segmentID", segID))
			continue
		}
		cloned := seg.Clone()
		cloned.IsImporting = false
		segInfos = append(segInfos, cloned.SegmentInfo)
		newCDV.addSegment(seg.GetInsertChannel(), seg.GetPartitionID(), segID)
	}

	if len(segInfos) == 0 {
		mlog.Info(context.TODO(), "ActivateSegments: no segments to complete")
		return nil
	}

	// Atomic catalog write
	newViewProto := newCDV.toProto()
	if err := m.catalog.AlterSegmentsAndSaveDataView(ctx, segInfos, collectionID, newViewProto); err != nil {
		mlog.Error(context.TODO(), "ActivateSegments: catalog write failed", mlog.Err(err))
		return err
	}

	// Update memory
	for _, info := range segInfos {
		m.setSegment(info.GetID(), NewSegmentInfo(info))
	}
	m.addDataView(collectionID, newCDV)

	mlog.Info(context.TODO(), "ActivateSegments - complete", mlog.Int("numSegments", len(segInfos)))
	return nil
}

// =====================================================================
// Reload — load segments and DataViews from catalog
// =====================================================================

// reloadFromKV loads all segments and DataViews from the catalog into memory.
// Segments and DataViews are loaded in parallel for each collection.
func (m *segmentViewMeta) reloadFromKV(ctx context.Context, collectionIDs []int64) error {
	record := timerecord.NewTimeRecorder("segmentViewMeta.reloadFromKV")

	pool := conc.NewPool[any](paramtable.Get().MetaStoreCfg.ReadConcurrency.GetAsInt())
	defer pool.Release()

	// Load segments per collection in parallel.
	futures := make([]*conc.Future[any], 0, len(collectionIDs))
	collectionSegments := make([][]*datapb.SegmentInfo, len(collectionIDs))
	for i, collectionID := range collectionIDs {
		i := i
		collectionID := collectionID
		futures = append(futures, pool.Submit(func() (any, error) {
			segments, err := m.catalog.ListSegments(m.ctx, collectionID)
			if err != nil {
				return nil, err
			}
			collectionSegments[i] = segments
			return nil, nil
		}))
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return err
	}
	mlog.Info(context.TODO(), "segmentViewMeta reloadFromKV: segments loaded", mlog.Duration("dur", record.RecordSpan()))

	// Populate in-memory structures.
	metrics.DataCoordNumSegments.Reset()
	numStoredRows := int64(0)
	numSegments := 0
	for _, segments := range collectionSegments {
		numSegments += len(segments)
		for _, segment := range segments {
			m.setSegment(segment.ID, NewSegmentInfo(segment))
			metrics.DataCoordNumSegments.WithLabelValues(
				segment.GetState().String(), segment.GetLevel().String(),
				getSortStatus(segment.GetIsSorted()), fmt.Sprint(segment.GetStorageVersion()),
			).Inc()
			if segment.State == commonpb.SegmentState_Flushed {
				numStoredRows += segment.NumOfRows

				insertFileNum := 0
				for _, fieldBinlog := range segment.GetBinlogs() {
					insertFileNum += len(fieldBinlog.GetBinlogs())
				}
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.InsertFileLabel).Observe(float64(insertFileNum))

				statFileNum := 0
				for _, fieldBinlog := range segment.GetStatslogs() {
					statFileNum += len(fieldBinlog.GetBinlogs())
				}
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.StatFileLabel).Observe(float64(statFileNum))

				deleteFileNum := 0
				for _, fieldBinlog := range segment.GetDeltalogs() {
					deleteFileNum += len(fieldBinlog.GetBinlogs())
				}
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(deleteFileNum))
			}
		}
	}

	// Load DataViews.
	if err := m.reloadDataViews(); err != nil {
		return err
	}

	mlog.Info(context.TODO(), "segmentViewMeta reloadFromKV done",
		mlog.Int("numSegments", numSegments),
		mlog.Int64("numStoredRows", numStoredRows),
		mlog.Duration("duration", record.ElapseSpan()))
	return nil
}
