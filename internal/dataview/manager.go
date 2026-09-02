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

package dataview

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type SegmentStore interface {
	GetSegment(ctx context.Context, segID int64) *Segment
	GetSegments(ctx context.Context, segIDs []int64) []*Segment
	SelectSegments(ctx context.Context, collectionID int64) []*Segment
}

type Catalog interface {
	SaveDataView(ctx context.Context, dataView *viewpb.DataViewOfCollection) error
	ListDataViews(ctx context.Context, collectionID int64) ([]*viewpb.DataViewOfCollection, error)
	DropDataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) error
	DropDataViews(ctx context.Context, collectionID int64) error
}

type RecoveryCatalog interface {
	Catalog
	ListAllDataViews(ctx context.Context) ([]*viewpb.DataViewOfCollection, error)
}

type Manager interface {
	OnCreateCollection(ctx context.Context, event CreateCollectionDataViewEvent) (*viewpb.DataVersion, error)
	OnFlush(ctx context.Context, event FlushDataViewEvent) (*viewpb.DataVersion, error)
	OnImport(ctx context.Context, event ImportDataViewEvent) (*viewpb.DataVersion, error)
	OnCopySegmentComplete(ctx context.Context, event CopySegmentCompleteDataViewEvent) (*viewpb.DataVersion, error)
	OnCompact(ctx context.Context, event CompactDataViewEvent) (*viewpb.DataVersion, error)
	OnL0Compact(ctx context.Context, event L0CompactDataViewEvent) (*viewpb.DataVersion, error)
	OnExternalRefresh(ctx context.Context, event ExternalRefreshDataViewEvent) (*viewpb.DataVersion, error)
	OnDropPartition(ctx context.Context, event DropPartitionDataViewEvent) (*viewpb.DataVersion, error)
	OnTruncate(ctx context.Context, event TruncateDataViewEvent) (*viewpb.DataVersion, error)
	OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error)

	RepairCollection(ctx context.Context, collectionID int64) error
	RepairCollections(ctx context.Context, collectionIDs []int64) error
	DataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error)
	LatestVisibleDataView(ctx context.Context, collectionID int64) (*viewpb.DataViewOfCollection, error)
	Snapshot(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewOfCollection, error)
	DataViewSnapshot(ctx context.Context) *balancerapi.DataViewSnapshot
	DataViewSnapshotForCollections(ctx context.Context, collectionIDs map[int64]struct{}) *balancerapi.DataViewSnapshot
	SegmentSnapshot(ctx context.Context, segmentIDs []int64) balancerapi.SegmentSnapshot
	ShardTimeTicks(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewShardTimeTick, error)
	IsSegmentReferenced(ctx context.Context, collectionID int64, segmentID int64) (bool, error)
	GarbageCollect(ctx context.Context, collectionID int64, protected []*viewpb.DataVersion, retainLatest int) error
}

type CreateCollectionDataViewEvent struct {
	CollectionID int64
	VChannels    []string
}

type FlushDataViewEvent struct {
	CollectionID         int64
	SegmentIDs           []int64
	TemporaryUnavailable bool
}

type ImportDataViewEvent struct {
	CollectionID int64
	SegmentIDs   []int64
}

type CopySegmentCompleteDataViewEvent struct {
	CollectionID int64
	SegmentIDs   []int64
}

type CompactDataViewEvent struct {
	CollectionID     int64
	CompactFrom      []int64
	CompactTo        []int64
	AllowInvisibleTo bool
}

type L0CompactDataViewEvent struct {
	CollectionID int64
}

type ExternalRefreshDataViewEvent struct {
	CollectionID int64
	AddSegments  []int64
	DropSegments []int64
}

type DropPartitionDataViewEvent struct {
	CollectionID int64
	PartitionIDs []int64
}

type TruncateDataViewEvent struct {
	CollectionID int64
	VChannel     string
	FlushTs      uint64
}

type collectionDataViewState struct {
	mu           sync.RWMutex
	collectionID int64

	latestResident     *viewpb.DataViewOfCollection
	latestVisible      *viewpb.DataViewOfCollection
	segmentJoinVersion map[int64]*viewpb.DataVersion
	dropped            bool
}

type dataViewManager struct {
	mu             sync.RWMutex
	catalog        Catalog
	segments       SegmentStore
	states         map[int64]*collectionDataViewState
	recoveredAll   bool
	recoveredViews map[int64][]*viewpb.DataViewOfCollection
}

type Segment struct {
	ID                          int64
	CollectionID                int64
	PartitionID                 int64
	InsertChannel               string
	NumOfRows                   int64
	MemSize                     int64
	State                       commonpb.SegmentState
	Level                       datapb.SegmentLevel
	IsImporting                 bool
	IsInvisible                 bool
	StartPosition               *msgpb.MsgPosition
	DmlPosition                 *msgpb.MsgPosition
	CommitTimestamp             uint64
	TransformStartAfterTimetick uint64
	CreatedByCompaction         bool
	CompactionFrom              []int64
}

func (s *Segment) GetID() int64 {
	if s == nil {
		return 0
	}
	return s.ID
}

func (s *Segment) GetCollectionID() int64 {
	if s == nil {
		return 0
	}
	return s.CollectionID
}

func (s *Segment) GetPartitionID() int64 {
	if s == nil {
		return 0
	}
	return s.PartitionID
}

func (s *Segment) GetInsertChannel() string {
	if s == nil {
		return ""
	}
	return s.InsertChannel
}

func (s *Segment) GetNumOfRows() int64 {
	if s == nil {
		return 0
	}
	return s.NumOfRows
}

func (s *Segment) GetMemSize() int64 {
	if s == nil {
		return 0
	}
	return s.MemSize
}

func (s *Segment) GetState() commonpb.SegmentState {
	if s == nil {
		return commonpb.SegmentState_SegmentStateNone
	}
	return s.State
}

func (s *Segment) GetLevel() datapb.SegmentLevel {
	if s == nil {
		return datapb.SegmentLevel_Legacy
	}
	return s.Level
}

func (s *Segment) GetIsImporting() bool {
	return s != nil && s.IsImporting
}

func (s *Segment) GetIsInvisible() bool {
	return s != nil && s.IsInvisible
}

func (s *Segment) GetDmlPosition() *msgpb.MsgPosition {
	if s == nil {
		return nil
	}
	return s.DmlPosition
}

func (s *Segment) GetStartPosition() *msgpb.MsgPosition {
	if s == nil {
		return nil
	}
	return s.StartPosition
}

func (s *Segment) GetCommitTimestamp() uint64 {
	if s == nil {
		return 0
	}
	return s.CommitTimestamp
}

func (s *Segment) GetTransformStartAfterTimetick() uint64 {
	if s == nil {
		return 0
	}
	return s.TransformStartAfterTimetick
}

func (s *Segment) GetCreatedByCompaction() bool {
	return s != nil && s.CreatedByCompaction
}

func (s *Segment) GetCompactionFrom() []int64 {
	if s == nil {
		return nil
	}
	return s.CompactionFrom
}

type dataViewAdvance int

const (
	dataViewAdvanceNone dataViewAdvance = iota
	dataViewAdvanceStreaming
	dataViewAdvanceCompact
)

type dataViewMembershipMutation struct {
	collectionID            int64
	addSegmentIDs           []int64
	dropSegmentIDs          []int64
	advance                 dataViewAdvance
	allowInvisible          bool
	returnSingleJoinVersion bool
	dropPredicate           func(segmentID int64, partitionID int64, vchannel string) bool
	classifyAdvance         func(removed bool, added bool) dataViewAdvance
}

func NewManager(catalog Catalog, segments SegmentStore) Manager {
	return &dataViewManager{
		catalog:  catalog,
		segments: segments,
		states:   make(map[int64]*collectionDataViewState),
	}
}

func RecoverManager(ctx context.Context, catalog RecoveryCatalog, segments SegmentStore) (Manager, error) {
	manager := NewManager(catalog, segments).(*dataViewManager)
	dataViews, err := catalog.ListAllDataViews(ctx)
	if err != nil {
		return nil, err
	}
	manager.recoverFromDataViews(dataViews)
	return manager, nil
}

func (m *dataViewManager) OnCreateCollection(ctx context.Context, event CreateCollectionDataViewEvent) (*viewpb.DataVersion, error) {
	state := m.getOrCreateState(event.CollectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	state.dropped = false

	if state.latestResident != nil {
		if state.latestVisible == nil {
			state.latestVisible = m.withDeleteTimetick(ctx, state.latestResident)
		}
		return dataVersionFromView(state.latestResident), nil
	}

	persistedViews, err := m.catalog.ListDataViews(ctx, event.CollectionID)
	if err != nil {
		return nil, err
	}
	latestPersisted := latestDataView(persistedViews)
	if latestPersisted != nil {
		state.latestResident = canonicalDataViewClone(latestPersisted)
		state.latestVisible = m.latestVisiblePersistedView(ctx, persistedViews)
		state.segmentJoinVersion = segmentJoinVersionsFromDataViews(persistedViews)
		return dataVersionFromView(state.latestResident), nil
	}

	view := buildEmptyDataView(event.CollectionID, event.VChannels)
	view.DataVersion = nextDataVersion(nil, dataViewAdvanceStreaming)
	toPersist := cloneDataViewWithoutDeleteTimetick(view)
	if err := m.catalog.SaveDataView(ctx, toPersist); err != nil {
		return nil, err
	}
	state.latestResident = canonicalDataViewClone(toPersist)
	state.latestVisible = m.withDeleteTimetick(ctx, state.latestResident)
	state.recordSegmentJoinVersions(state.latestResident)
	return dataVersionFromView(state.latestResident), nil
}

func (m *dataViewManager) OnFlush(ctx context.Context, event FlushDataViewEvent) (*viewpb.DataVersion, error) {
	return m.applyMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID:            event.CollectionID,
		addSegmentIDs:           event.SegmentIDs,
		advance:                 dataViewAdvanceStreaming,
		allowInvisible:          event.TemporaryUnavailable,
		returnSingleJoinVersion: len(event.SegmentIDs) == 1,
	})
}

func (m *dataViewManager) OnImport(ctx context.Context, event ImportDataViewEvent) (*viewpb.DataVersion, error) {
	return m.applyMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID:  event.CollectionID,
		addSegmentIDs: event.SegmentIDs,
		advance:       dataViewAdvanceCompact,
	})
}

func (m *dataViewManager) OnCopySegmentComplete(ctx context.Context, event CopySegmentCompleteDataViewEvent) (*viewpb.DataVersion, error) {
	return m.applyMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID:  event.CollectionID,
		addSegmentIDs: event.SegmentIDs,
		advance:       dataViewAdvanceStreaming,
	})
}

func (m *dataViewManager) OnCompact(ctx context.Context, event CompactDataViewEvent) (*viewpb.DataVersion, error) {
	if m.hasPendingCompactOutput(ctx, event.CompactTo, event.AllowInvisibleTo) {
		return nil, nil
	}
	return m.applyMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID:   event.CollectionID,
		addSegmentIDs:  event.CompactTo,
		dropSegmentIDs: event.CompactFrom,
		advance:        dataViewAdvanceCompact,
		allowInvisible: event.AllowInvisibleTo,
	})
}

func (m *dataViewManager) OnL0Compact(ctx context.Context, event L0CompactDataViewEvent) (*viewpb.DataVersion, error) {
	state := m.getState(event.CollectionID)
	if state == nil {
		return nil, nil
	}
	state.mu.Lock()
	if state.dropped {
		state.mu.Unlock()
		return nil, nil
	}

	if state.latestResident != nil {
		state.latestResident = m.withDeleteTimetick(ctx, state.latestResident)
	}
	if state.latestVisible != nil {
		state.latestVisible = m.withDeleteTimetick(ctx, state.latestVisible)
	}
	version := dataVersionFromView(state.latestResident)
	state.mu.Unlock()
	return version, nil
}

func (m *dataViewManager) OnExternalRefresh(ctx context.Context, event ExternalRefreshDataViewEvent) (*viewpb.DataVersion, error) {
	return m.applyMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID:   event.CollectionID,
		addSegmentIDs:  event.AddSegments,
		dropSegmentIDs: event.DropSegments,
		classifyAdvance: func(removed bool, added bool) dataViewAdvance {
			if removed {
				return dataViewAdvanceCompact
			}
			if added {
				return dataViewAdvanceStreaming
			}
			return dataViewAdvanceNone
		},
	})
}

func (m *dataViewManager) OnDropPartition(ctx context.Context, event DropPartitionDataViewEvent) (*viewpb.DataVersion, error) {
	partitions := make(map[int64]struct{}, len(event.PartitionIDs))
	for _, partitionID := range event.PartitionIDs {
		partitions[partitionID] = struct{}{}
	}
	return m.applyMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID: event.CollectionID,
		advance:      dataViewAdvanceCompact,
		dropPredicate: func(segmentID int64, partitionID int64, vchannel string) bool {
			_, ok := partitions[partitionID]
			return ok
		},
	})
}

func (m *dataViewManager) OnTruncate(ctx context.Context, event TruncateDataViewEvent) (*viewpb.DataVersion, error) {
	return m.applyMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID: event.CollectionID,
		advance:      dataViewAdvanceCompact,
		dropPredicate: func(segmentID int64, partitionID int64, vchannel string) bool {
			if vchannel != event.VChannel {
				return false
			}
			segment := m.segments.GetSegment(ctx, segmentID)
			return segment != nil && segmentEffectiveDmlTs(segment) <= event.FlushTs
		},
	})
}

func (m *dataViewManager) OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if err := m.catalog.DropDataViews(ctx, collectionID); err != nil {
		return nil, err
	}
	state.latestResident = nil
	state.latestVisible = nil
	state.segmentJoinVersion = nil
	state.dropped = true
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.states[collectionID] == state {
		delete(m.states, collectionID)
	}
	return nil, nil
}

func (m *dataViewManager) RepairCollection(ctx context.Context, collectionID int64) error {
	persistedViews, ok := m.recoveredDataViews(collectionID)
	if ok {
		return m.repairCollectionWithDataViews(ctx, collectionID, persistedViews)
	}
	persistedViews, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return err
	}
	return m.repairCollectionWithDataViews(ctx, collectionID, persistedViews)
}

func (m *dataViewManager) RepairCollections(ctx context.Context, collectionIDs []int64) error {
	for _, collectionID := range collectionIDs {
		if err := m.RepairCollection(ctx, collectionID); err != nil {
			return err
		}
	}
	return nil
}

func (m *dataViewManager) recoverFromDataViews(dataViews []*viewpb.DataViewOfCollection) {
	viewsByCollection := make(map[int64][]*viewpb.DataViewOfCollection)
	recoveredViews := make(map[int64][]*viewpb.DataViewOfCollection)
	for _, view := range dataViews {
		if view == nil {
			continue
		}
		collectionID := view.GetCollectionId()
		viewsByCollection[collectionID] = append(viewsByCollection[collectionID], view)
		recoveredViews[collectionID] = append(recoveredViews[collectionID], canonicalDataViewClone(view))
	}
	m.mu.Lock()
	m.recoveredAll = true
	m.recoveredViews = recoveredViews
	m.mu.Unlock()

	collectionIDs := make([]int64, 0, len(viewsByCollection))
	for collectionID := range viewsByCollection {
		collectionIDs = append(collectionIDs, collectionID)
	}
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })
	for _, collectionID := range collectionIDs {
		m.recoverCollectionFromDataViews(collectionID, viewsByCollection[collectionID])
	}
}

func (m *dataViewManager) recoveredDataViews(collectionID int64) ([]*viewpb.DataViewOfCollection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.recoveredAll {
		return nil, false
	}
	return cloneDataViews(m.recoveredViews[collectionID]), true
}

func (m *dataViewManager) rememberRecoveredDataView(view *viewpb.DataViewOfCollection) {
	if view == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.recoveredAll {
		return
	}
	collectionID := view.GetCollectionId()
	version := view.GetDataVersion()
	views := m.recoveredViews[collectionID]
	for idx, recovered := range views {
		if compareDataVersion(recovered.GetDataVersion(), version) == 0 {
			views[idx] = canonicalDataViewClone(view)
			m.recoveredViews[collectionID] = views
			return
		}
	}
	m.recoveredViews[collectionID] = append(views, canonicalDataViewClone(view))
}

func (m *dataViewManager) recoverCollectionFromDataViews(collectionID int64, persistedViews []*viewpb.DataViewOfCollection) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.dropped = false
	state.latestResident = canonicalDataViewClone(latestDataView(persistedViews))
	state.latestVisible = canonicalDataViewClone(state.latestResident)
	state.segmentJoinVersion = segmentJoinVersionsFromDataViews(persistedViews)
}

func (m *dataViewManager) repairCollectionWithDataViews(ctx context.Context, collectionID int64, persistedViews []*viewpb.DataViewOfCollection) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	state.dropped = false
	state.mergeSegmentJoinVersions(persistedViews)

	latestPersisted := latestDataView(persistedViews)
	segments := m.segments.SelectSegments(ctx, collectionID)
	pendingRetainedInputs := pendingRetainedCompactionInputs(segments)
	residentExpected := buildDataViewFromSegments(collectionID, segments, true)
	if isDataViewMembershipEqual(latestPersisted, residentExpected) {
		state.latestResident = canonicalDataViewClone(latestPersisted)
		state.latestVisible = m.latestVisiblePersistedView(ctx, persistedViews)
		state.mu.Unlock()
		return nil
	}
	expected := buildRecoverExpectedDataView(collectionID, latestPersisted, segments, pendingRetainedInputs)
	pruneHistoricallyRemovedSegments(latestPersisted, expected, persistedViews)
	if isDataViewMembershipEqual(latestPersisted, expected) {
		state.latestResident = canonicalDataViewClone(latestPersisted)
		state.latestVisible = m.latestVisiblePersistedView(ctx, persistedViews)
		state.mu.Unlock()
		return nil
	}
	if latestPersisted == nil && isDataViewEmpty(expected) {
		state.mu.Unlock()
		return nil
	}

	advance := classifyRecoverAdvance(latestPersisted, expected, m.segments)
	expected.DataVersion = nextDataVersion(latestPersisted, advance)
	toPersist := cloneDataViewWithoutDeleteTimetick(expected)
	if err := m.catalog.SaveDataView(ctx, toPersist); err != nil {
		state.mu.Unlock()
		return err
	}
	m.rememberRecoveredDataView(toPersist)

	state.latestResident = canonicalDataViewClone(toPersist)
	state.recordSegmentJoinVersions(state.latestResident)
	if m.isDataViewVisibleFromBase(ctx, latestPersisted, state.latestResident, pendingRetainedInputs) {
		state.latestVisible = m.withDeleteTimetick(ctx, state.latestResident)
	} else {
		state.latestVisible = m.latestVisiblePersistedView(ctx, persistedViews)
	}
	state.mu.Unlock()
	return nil
}

func (m *dataViewManager) LatestVisibleDataView(ctx context.Context, collectionID int64) (*viewpb.DataViewOfCollection, error) {
	state := m.getState(collectionID)
	if state == nil {
		return nil, nil
	}
	state.mu.RLock()
	defer state.mu.RUnlock()

	if state.dropped || state.latestVisible == nil {
		return nil, nil
	}
	return m.withDeleteTimetick(ctx, state.latestVisible), nil
}

func (m *dataViewManager) DataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
	if dataVersion == nil {
		return nil, nil
	}
	state := m.getOrCreateState(collectionID)
	state.mu.RLock()
	if state.dropped {
		state.mu.RUnlock()
		return nil, nil
	}
	if state.latestVisible != nil && compareDataVersion(state.latestVisible.GetDataVersion(), dataVersion) == 0 {
		view := m.withDeleteTimetick(ctx, state.latestVisible)
		state.mu.RUnlock()
		return view, nil
	}
	state.mu.RUnlock()

	views, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), dataVersion) == 0 {
			return m.withDeleteTimetick(ctx, view), nil
		}
	}
	return nil, nil
}

func (m *dataViewManager) Snapshot(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewOfCollection, error) {
	states := make([]*collectionDataViewState, 0, len(collectionIDs))
	if len(collectionIDs) == 0 {
		states = m.listStates()
	} else {
		for _, collectionID := range collectionIDs {
			state := m.getState(collectionID)
			if state != nil {
				states = append(states, state)
			}
		}
	}
	sort.Slice(states, func(i, j int) bool { return states[i].collectionID < states[j].collectionID })

	views := make([]*viewpb.DataViewOfCollection, 0, len(states))
	for _, state := range states {
		state.mu.RLock()
		if state.dropped || state.latestVisible == nil {
			state.mu.RUnlock()
			continue
		}
		views = append(views, m.withDeleteTimetick(ctx, state.latestVisible))
		state.mu.RUnlock()
	}
	return views, nil
}

func (m *dataViewManager) DataViewSnapshot(ctx context.Context) *balancerapi.DataViewSnapshot {
	return m.DataViewSnapshotForCollections(ctx, nil)
}

type segmentSnapshot map[int64]*balancerapi.SegmentInfo

func (s segmentSnapshot) Get(segmentID int64) (*balancerapi.SegmentInfo, bool) {
	info, ok := s[segmentID]
	return info, ok
}

// DataViewSnapshotForCollections builds an immutable snapshot from the latest
// visible DataViews in the requested collection scope.
func (m *dataViewManager) DataViewSnapshotForCollections(ctx context.Context, collectionIDs map[int64]struct{}) *balancerapi.DataViewSnapshot {
	states := make([]*collectionDataViewState, 0, len(collectionIDs))
	if collectionIDs == nil {
		states = m.listStates()
	} else {
		for collectionID := range collectionIDs {
			if state := m.getState(collectionID); state != nil {
				states = append(states, state)
			}
		}
	}

	views := make([]*viewpb.DataViewOfCollection, 0, len(states))
	segmentIDs := make([]int64, 0)
	seenSegments := make(map[int64]struct{})
	for _, state := range states {
		state.mu.RLock()
		if !state.dropped && state.latestVisible != nil {
			view := canonicalDataViewClone(state.latestVisible)
			views = append(views, view)
			for _, partition := range dataViewPartitions(view) {
				for _, segmentID := range partition.GetSegmentIds() {
					if _, ok := seenSegments[segmentID]; ok {
						continue
					}
					seenSegments[segmentID] = struct{}{}
					segmentIDs = append(segmentIDs, segmentID)
				}
			}
		}
		state.mu.RUnlock()
	}

	segments := m.getSegments(ctx, segmentIDs)
	setDataViewDeleteTimeticks(views, segments)
	return balancerapi.NewDataViewSnapshot(0, views, newSegmentSnapshot(segmentIDs, segments))
}

// SegmentSnapshot looks up arbitrary segment metadata without requiring the
// segments to belong to the latest visible DataViews.
func (m *dataViewManager) SegmentSnapshot(ctx context.Context, segmentIDs []int64) balancerapi.SegmentSnapshot {
	return newSegmentSnapshot(segmentIDs, m.getSegments(ctx, segmentIDs))
}

func (m *dataViewManager) getSegments(ctx context.Context, segmentIDs []int64) map[int64]*Segment {
	segments := make(map[int64]*Segment, len(segmentIDs))
	if len(segmentIDs) == 0 {
		return segments
	}
	for _, segment := range m.segments.GetSegments(ctx, segmentIDs) {
		if segment != nil {
			segments[segment.GetID()] = segment
		}
	}
	return segments
}

func newSegmentSnapshot(segmentIDs []int64, segmentsByID map[int64]*Segment) balancerapi.SegmentSnapshot {
	segments := make(segmentSnapshot, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segment := segmentsByID[segmentID]
		if segment == nil {
			continue
		}
		segments[segmentID] = &balancerapi.SegmentInfo{
			SegmentID:   segment.GetID(),
			PartitionID: segment.GetPartitionID(),
			MemSize:     segment.GetMemSize(),
			RowNum:      segment.GetNumOfRows(),
		}
	}
	return segments
}

// setDataViewDeleteTimeticks derives each shard's minimum transform-start
// timetick from the prefetched segments. Empty shards or any missing member use
// zero so consumers do not advance beyond unknown metadata.
func setDataViewDeleteTimeticks(views []*viewpb.DataViewOfCollection, segments map[int64]*Segment) {
	for _, view := range views {
		for _, shard := range view.GetShards() {
			minTs := uint64(math.MaxUint64)
			hasSegment := false
			missingSegment := false
			for _, partition := range shard.GetPartitions() {
				for _, segmentID := range partition.GetSegmentIds() {
					hasSegment = true
					segment := segments[segmentID]
					if segment == nil {
						missingSegment = true
						continue
					}
					if ts := segmentTransformStartAfterTimetick(segment); ts < minTs {
						minTs = ts
					}
				}
			}
			if !hasSegment || missingSegment {
				shard.TransformStartAfterTimetick = 0
			} else {
				shard.TransformStartAfterTimetick = minTs
			}
		}
	}
}

func (m *dataViewManager) ShardTimeTicks(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewShardTimeTick, error) {
	views, err := m.Snapshot(ctx, collectionIDs)
	if err != nil {
		return nil, err
	}
	timeticks := make([]*viewpb.DataViewShardTimeTick, 0)
	for _, view := range views {
		timeticks = append(timeticks, dataViewTimeTicks(view)...)
	}
	return timeticks, nil
}

func (m *dataViewManager) IsSegmentReferenced(ctx context.Context, collectionID int64, segmentID int64) (bool, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.RLock()
	defer state.mu.RUnlock()

	views, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return true, err
	}
	for _, view := range views {
		if dataViewContainsSegment(view, segmentID) {
			return true, nil
		}
	}
	return false, nil
}

func (m *dataViewManager) GarbageCollect(ctx context.Context, collectionID int64, protected []*viewpb.DataVersion, retainLatest int) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if retainLatest < 1 {
		retainLatest = 1
	}
	views, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return err
	}
	sort.Slice(views, func(i, j int) bool {
		return compareDataVersion(views[i].GetDataVersion(), views[j].GetDataVersion()) > 0
	})
	protectedSet := make(map[string]struct{}, len(protected))
	for _, version := range protected {
		protectedSet[dataVersionKey(version)] = struct{}{}
	}
	for idx, view := range views {
		version := view.GetDataVersion()
		if idx < retainLatest {
			continue
		}
		if _, ok := protectedSet[dataVersionKey(version)]; ok {
			continue
		}
		if err := m.catalog.DropDataView(ctx, collectionID, version); err != nil {
			return err
		}
	}
	return nil
}

func (m *dataViewManager) applyMembershipMutation(ctx context.Context, mutation dataViewMembershipMutation) (*viewpb.DataVersion, error) {
	state := m.getOrCreateState(mutation.collectionID)
	state.mu.Lock()
	if state.dropped {
		state.mu.Unlock()
		return nil, nil
	}

	previousResident := canonicalDataViewClone(state.latestResident)
	next := canonicalDataViewClone(state.latestResident)
	if next == nil {
		next = &viewpb.DataViewOfCollection{
			CollectionId: mutation.collectionID,
			DataVersion:  &viewpb.DataVersion{},
		}
	}

	removed := false
	added := false
	for _, segmentID := range mutation.dropSegmentIDs {
		removed = removeSegmentFromDataView(next, segmentID) || removed
	}
	if mutation.dropPredicate != nil {
		removed = removeSegmentsByPredicate(next, mutation.dropPredicate) || removed
	}
	for _, segmentID := range mutation.addSegmentIDs {
		segment := m.segments.GetSegment(ctx, segmentID)
		if !isDataViewJoinableSegment(segment, mutation.allowInvisible) {
			continue
		}
		added = addSegmentToDataView(next, segment) || added
	}
	canonicalizeDataView(next)
	if isDataViewMembershipEqual(state.latestResident, next) {
		if state.latestResident != nil {
			state.latestResident = m.withDeleteTimetick(ctx, state.latestResident)
		}
		if state.latestVisible != nil {
			state.latestVisible = m.withDeleteTimetick(ctx, state.latestVisible)
		}
		version := dataVersionFromView(state.latestResident)
		if mutation.returnSingleJoinVersion {
			if joined := state.segmentJoinVersion[mutation.addSegmentIDs[0]]; joined != nil {
				version = cloneDataVersion(joined)
			} else {
				version = nil
			}
		}
		state.mu.Unlock()
		return version, nil
	}

	advance := mutation.advance
	if mutation.classifyAdvance != nil {
		advance = mutation.classifyAdvance(removed, added)
	}
	next.DataVersion = nextDataVersion(state.latestResident, advance)
	toPersist := cloneDataViewWithoutDeleteTimetick(next)
	if err := m.catalog.SaveDataView(ctx, toPersist); err != nil {
		state.mu.Unlock()
		return nil, err
	}

	state.latestResident = canonicalDataViewClone(toPersist)
	state.recordSegmentJoinVersions(state.latestResident)
	if m.isDataViewVisibleFromBase(ctx, previousResident, state.latestResident, nil) {
		state.latestVisible = m.withDeleteTimetick(ctx, state.latestResident)
	}
	version := dataVersionFromView(state.latestResident)
	state.mu.Unlock()
	return version, nil
}

func (m *dataViewManager) getState(collectionID int64) *collectionDataViewState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.states[collectionID]
}

func segmentJoinVersionsFromDataViews(views []*viewpb.DataViewOfCollection) map[int64]*viewpb.DataVersion {
	ordered := cloneDataViews(views)
	sort.Slice(ordered, func(i, j int) bool {
		return compareDataVersion(ordered[i].GetDataVersion(), ordered[j].GetDataVersion()) < 0
	})
	versions := make(map[int64]*viewpb.DataVersion)
	for _, view := range ordered {
		for segmentID := range dataViewSegmentIDSet(view) {
			if versions[segmentID] == nil {
				versions[segmentID] = dataVersionFromView(view)
			}
		}
	}
	return versions
}

func (s *collectionDataViewState) recordSegmentJoinVersions(view *viewpb.DataViewOfCollection) {
	if view == nil {
		return
	}
	if s.segmentJoinVersion == nil {
		s.segmentJoinVersion = make(map[int64]*viewpb.DataVersion)
	}
	for segmentID := range dataViewSegmentIDSet(view) {
		if s.segmentJoinVersion[segmentID] == nil {
			s.segmentJoinVersion[segmentID] = dataVersionFromView(view)
		}
	}
}

func (s *collectionDataViewState) mergeSegmentJoinVersions(views []*viewpb.DataViewOfCollection) {
	if s.segmentJoinVersion == nil {
		s.segmentJoinVersion = make(map[int64]*viewpb.DataVersion)
	}
	for segmentID, version := range segmentJoinVersionsFromDataViews(views) {
		if s.segmentJoinVersion[segmentID] == nil {
			s.segmentJoinVersion[segmentID] = version
		}
	}
}

func (m *dataViewManager) getOrCreateState(collectionID int64) *collectionDataViewState {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.states[collectionID]
	if state == nil {
		state = &collectionDataViewState{collectionID: collectionID}
		m.states[collectionID] = state
	}
	return state
}

func (m *dataViewManager) listStates() []*collectionDataViewState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make([]*collectionDataViewState, 0, len(m.states))
	for _, state := range m.states {
		states = append(states, state)
	}
	return states
}

func (m *dataViewManager) isDataViewVisibleFromBase(
	ctx context.Context,
	base *viewpb.DataViewOfCollection,
	view *viewpb.DataViewOfCollection,
	loadableExceptions map[int64]struct{},
) bool {
	if view == nil {
		return false
	}
	baseSegments := dataViewSegmentIDSet(base)
	for _, partition := range dataViewPartitions(view) {
		for _, segmentID := range partition.GetSegmentIds() {
			if _, ok := baseSegments[segmentID]; ok {
				continue
			}
			if !isDataViewLoadableSegment(m.segments.GetSegment(ctx, segmentID)) {
				if _, ok := loadableExceptions[segmentID]; ok {
					continue
				}
				return false
			}
		}
	}
	return true
}

func (m *dataViewManager) latestVisiblePersistedView(ctx context.Context, views []*viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	if len(views) == 0 {
		return nil
	}
	ordered := make([]*viewpb.DataViewOfCollection, 0, len(views))
	for _, view := range views {
		if view != nil {
			ordered = append(ordered, view)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		return compareDataVersion(ordered[i].GetDataVersion(), ordered[j].GetDataVersion()) < 0
	})

	var previous *viewpb.DataViewOfCollection
	var latestVisible *viewpb.DataViewOfCollection
	var loadableExceptions map[int64]struct{}
	for _, view := range ordered {
		if loadableExceptions == nil {
			loadableExceptions = pendingRetainedCompactionInputs(m.segments.SelectSegments(ctx, view.GetCollectionId()))
		}
		if m.isDataViewVisibleFromBase(ctx, previous, view, loadableExceptions) {
			latestVisible = view
		}
		previous = view
	}
	return m.withDeleteTimetick(ctx, latestVisible)
}

func (m *dataViewManager) hasPendingCompactOutput(ctx context.Context, compactTo []int64, allowInvisible bool) bool {
	for _, segmentID := range compactTo {
		segment := m.segments.GetSegment(ctx, segmentID)
		if isDataViewPendingVisibilitySegment(segment, allowInvisible) {
			return true
		}
	}
	return false
}

func (m *dataViewManager) withDeleteTimetick(ctx context.Context, view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	clone := canonicalDataViewClone(view)
	if clone == nil {
		return nil
	}
	for _, shard := range clone.GetShards() {
		minTs := uint64(math.MaxUint64)
		hasSegment := false
		for _, partition := range shard.GetPartitions() {
			for _, segmentID := range partition.GetSegmentIds() {
				hasSegment = true
				segment := m.segments.GetSegment(ctx, segmentID)
				ts := segmentTransformStartAfterTimetick(segment)
				if ts < minTs {
					minTs = ts
				}
			}
		}
		if hasSegment {
			shard.TransformStartAfterTimetick = minTs
		} else {
			shard.TransformStartAfterTimetick = 0
		}
	}
	return clone
}

func dataViewTimeTicks(view *viewpb.DataViewOfCollection) []*viewpb.DataViewShardTimeTick {
	if view == nil {
		return nil
	}
	timeticks := make([]*viewpb.DataViewShardTimeTick, 0, len(view.GetShards()))
	for _, shard := range view.GetShards() {
		timeticks = append(timeticks, &viewpb.DataViewShardTimeTick{
			Vchannel:                    shard.GetVchannel(),
			TransformStartAfterTimetick: shard.GetTransformStartAfterTimetick(),
		})
	}
	return timeticks
}

func isDataViewLoadableSegment(segment *Segment) bool {
	return isDataViewJoinableSegment(segment, false)
}

func isDataViewJoinableSegment(segment *Segment, allowInvisible bool) bool {
	if segment == nil {
		return false
	}
	if segment.GetState() != commonpb.SegmentState_Flushed {
		return false
	}
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if segment.GetIsImporting() {
		return false
	}
	if segment.GetState() == commonpb.SegmentState_Dropped {
		return false
	}
	if !allowInvisible && segment.GetIsInvisible() {
		return false
	}
	return true
}

func isDataViewPendingVisibilitySegment(segment *Segment, allowInvisible bool) bool {
	if segment == nil {
		return false
	}
	if segment.GetState() != commonpb.SegmentState_Flushed {
		return false
	}
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if segment.GetIsImporting() {
		return true
	}
	return segment.GetIsInvisible() && !allowInvisible
}

func segmentEffectiveDmlTs(segment *Segment) uint64 {
	if ts := segment.GetCommitTimestamp(); ts != 0 {
		return ts
	}
	if segment.GetDmlPosition() == nil {
		return 0
	}
	return segment.GetDmlPosition().GetTimestamp()
}

func segmentTransformStartAfterTimetick(segment *Segment) uint64 {
	if segment == nil {
		return 0
	}
	if ts := segment.GetTransformStartAfterTimetick(); ts != 0 {
		return ts
	}
	if ts := segment.GetCommitTimestamp(); ts != 0 {
		return ts
	}
	if segment.GetStartPosition() != nil {
		return segment.GetStartPosition().GetTimestamp()
	}
	return 0
}

func nextDataVersion(base *viewpb.DataViewOfCollection, advance dataViewAdvance) *viewpb.DataVersion {
	if base == nil || base.GetDataVersion() == nil || base.GetDataVersion().GetStreamingVersion() == 0 {
		return &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0}
	}
	current := base.GetDataVersion()
	switch advance {
	case dataViewAdvanceStreaming:
		return &viewpb.DataVersion{StreamingVersion: current.GetStreamingVersion() + 1, CompactVersion: 0}
	case dataViewAdvanceCompact:
		return &viewpb.DataVersion{StreamingVersion: current.GetStreamingVersion(), CompactVersion: current.GetCompactVersion() + 1}
	default:
		return proto.Clone(current).(*viewpb.DataVersion)
	}
}

func latestDataView(views []*viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	var latest *viewpb.DataViewOfCollection
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), latest.GetDataVersion()) > 0 {
			latest = view
		}
	}
	return canonicalDataViewClone(latest)
}

func compareDataVersion(left, right *viewpb.DataVersion) int {
	leftStreaming, leftCompact := int64(0), int64(0)
	if left != nil {
		leftStreaming = left.GetStreamingVersion()
		leftCompact = left.GetCompactVersion()
	}
	rightStreaming, rightCompact := int64(0), int64(0)
	if right != nil {
		rightStreaming = right.GetStreamingVersion()
		rightCompact = right.GetCompactVersion()
	}
	if leftStreaming != rightStreaming {
		if leftStreaming > rightStreaming {
			return 1
		}
		return -1
	}
	if leftCompact != rightCompact {
		if leftCompact > rightCompact {
			return 1
		}
		return -1
	}
	return 0
}

func addSegmentToDataView(view *viewpb.DataViewOfCollection, segment *Segment) bool {
	if segment.GetCollectionID() != view.GetCollectionId() {
		return false
	}
	for _, partition := range dataViewPartitions(view) {
		for _, segmentID := range partition.GetSegmentIds() {
			if segmentID == segment.GetID() {
				return false
			}
		}
	}

	shard := findOrCreateDataViewShard(view, segment.GetInsertChannel())
	partition := findOrCreateDataViewPartition(shard, segment.GetPartitionID())
	partition.SegmentIds = append(partition.SegmentIds, segment.GetID())
	return true
}

func removeSegmentFromDataView(view *viewpb.DataViewOfCollection, segmentID int64) bool {
	return removeSegmentsByPredicate(view, func(id int64, partitionID int64, vchannel string) bool {
		return id == segmentID
	})
}

func addSegmentFromDataView(view *viewpb.DataViewOfCollection, source *viewpb.DataViewOfCollection, target int64) bool {
	if view == nil || source == nil || view.GetCollectionId() != source.GetCollectionId() {
		return false
	}
	if dataViewContainsSegment(view, target) {
		return false
	}
	for _, shard := range source.GetShards() {
		for _, partition := range shard.GetPartitions() {
			for _, segmentID := range partition.GetSegmentIds() {
				if segmentID != target {
					continue
				}
				targetShard := findOrCreateDataViewShard(view, shard.GetVchannel())
				targetPartition := findOrCreateDataViewPartition(targetShard, partition.GetPartitionId())
				targetPartition.SegmentIds = append(targetPartition.SegmentIds, target)
				return true
			}
		}
	}
	return false
}

func removeSegmentsByPredicate(view *viewpb.DataViewOfCollection, predicate func(segmentID int64, partitionID int64, vchannel string) bool) bool {
	changed := false
	for _, shard := range view.GetShards() {
		partitions := shard.Partitions[:0]
		for _, partition := range shard.GetPartitions() {
			segmentIDs := partition.SegmentIds[:0]
			for _, segmentID := range partition.GetSegmentIds() {
				if predicate(segmentID, partition.GetPartitionId(), shard.GetVchannel()) {
					changed = true
					continue
				}
				segmentIDs = append(segmentIDs, segmentID)
			}
			partition.SegmentIds = segmentIDs
			if len(partition.GetSegmentIds()) > 0 {
				partitions = append(partitions, partition)
			}
		}
		shard.Partitions = partitions
	}
	shards := view.Shards[:0]
	for _, shard := range view.GetShards() {
		if len(shard.GetPartitions()) > 0 {
			shards = append(shards, shard)
		}
	}
	view.Shards = shards
	return changed
}

func findOrCreateDataViewShard(view *viewpb.DataViewOfCollection, vchannel string) *viewpb.DataViewOfShard {
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard
		}
	}
	shard := &viewpb.DataViewOfShard{Vchannel: vchannel}
	view.Shards = append(view.Shards, shard)
	return shard
}

func findOrCreateDataViewPartition(shard *viewpb.DataViewOfShard, partitionID int64) *viewpb.DataViewOfPartition {
	for _, partition := range shard.GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return partition
		}
	}
	partition := &viewpb.DataViewOfPartition{PartitionId: partitionID}
	shard.Partitions = append(shard.Partitions, partition)
	return partition
}

func dataViewPartitions(view *viewpb.DataViewOfCollection) []*viewpb.DataViewOfPartition {
	if view == nil {
		return nil
	}
	partitions := make([]*viewpb.DataViewOfPartition, 0)
	for _, shard := range view.GetShards() {
		partitions = append(partitions, shard.GetPartitions()...)
	}
	return partitions
}

func canonicalDataViewClone(view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	if view == nil {
		return nil
	}
	clone := proto.Clone(view).(*viewpb.DataViewOfCollection)
	canonicalizeDataView(clone)
	return clone
}

func cloneDataViews(views []*viewpb.DataViewOfCollection) []*viewpb.DataViewOfCollection {
	if len(views) == 0 {
		return nil
	}
	clones := make([]*viewpb.DataViewOfCollection, 0, len(views))
	for _, view := range views {
		if view == nil {
			continue
		}
		clones = append(clones, canonicalDataViewClone(view))
	}
	return clones
}

func cloneDataVersion(version *viewpb.DataVersion) *viewpb.DataVersion {
	if version == nil {
		return nil
	}
	return proto.Clone(version).(*viewpb.DataVersion)
}

func dataVersionFromView(view *viewpb.DataViewOfCollection) *viewpb.DataVersion {
	if view == nil {
		return nil
	}
	return cloneDataVersion(view.GetDataVersion())
}

func cloneDataViewWithoutDeleteTimetick(view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	clone := canonicalDataViewClone(view)
	if clone == nil {
		return nil
	}
	for _, shard := range clone.GetShards() {
		shard.TransformStartAfterTimetick = 0
	}
	return clone
}

func canonicalizeDataView(view *viewpb.DataViewOfCollection) {
	if view == nil {
		return
	}
	sort.Slice(view.Shards, func(i, j int) bool {
		return view.Shards[i].GetVchannel() < view.Shards[j].GetVchannel()
	})
	for _, shard := range view.GetShards() {
		sort.Slice(shard.Partitions, func(i, j int) bool {
			return shard.Partitions[i].GetPartitionId() < shard.Partitions[j].GetPartitionId()
		})
		for _, partition := range shard.GetPartitions() {
			sort.Slice(partition.SegmentIds, func(i, j int) bool {
				return partition.SegmentIds[i] < partition.SegmentIds[j]
			})
			partition.SegmentIds = dedupSortedInt64s(partition.SegmentIds)
		}
	}
}

func buildEmptyDataView(collectionID int64, vchannels []string) *viewpb.DataViewOfCollection {
	view := &viewpb.DataViewOfCollection{
		CollectionId: collectionID,
		DataVersion:  &viewpb.DataVersion{},
	}
	if len(vchannels) == 0 {
		return view
	}
	seen := make(map[string]struct{}, len(vchannels))
	for _, vchannel := range vchannels {
		if _, ok := seen[vchannel]; ok {
			continue
		}
		seen[vchannel] = struct{}{}
		view.Shards = append(view.Shards, &viewpb.DataViewOfShard{Vchannel: vchannel})
	}
	canonicalizeDataView(view)
	return view
}

func dedupSortedInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return values
	}
	write := 1
	for read := 1; read < len(values); read++ {
		if values[read] == values[write-1] {
			continue
		}
		values[write] = values[read]
		write++
	}
	return values[:write]
}

func buildDataViewFromSegments(collectionID int64, segments []*Segment, allowInvisible ...bool) *viewpb.DataViewOfCollection {
	view := &viewpb.DataViewOfCollection{
		CollectionId: collectionID,
		DataVersion:  &viewpb.DataVersion{},
	}
	allowInvisibleSegment := len(allowInvisible) > 0 && allowInvisible[0]
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetID() < segments[j].GetID()
	})
	for _, segment := range segments {
		if isDataViewJoinableSegment(segment, allowInvisibleSegment) {
			addSegmentToDataView(view, segment)
		}
	}
	canonicalizeDataView(view)
	return view
}

func buildRecoverExpectedDataView(
	collectionID int64,
	latest *viewpb.DataViewOfCollection,
	segments []*Segment,
	pendingRetainedInputs map[int64]struct{},
) *viewpb.DataViewOfCollection {
	expected := buildDataViewFromSegments(collectionID, segments)
	if latest == nil || len(pendingRetainedInputs) == 0 {
		return expected
	}
	for segmentID := range pendingRetainedInputs {
		addSegmentFromDataView(expected, latest, segmentID)
	}
	canonicalizeDataView(expected)
	return expected
}

func pendingRetainedCompactionInputs(segments []*Segment) map[int64]struct{} {
	byID := make(map[int64]*Segment, len(segments))
	for _, segment := range segments {
		if segment != nil {
			byID[segment.GetID()] = segment
		}
	}

	retained := make(map[int64]struct{})
	for _, segment := range segments {
		if !isDataViewPendingVisibilitySegment(segment, false) {
			continue
		}
		for _, inputID := range segment.GetCompactionFrom() {
			input := byID[inputID]
			if input == nil || input.GetIsInvisible() || input.GetIsImporting() || input.GetLevel() == datapb.SegmentLevel_L0 {
				continue
			}
			retained[inputID] = struct{}{}
		}
	}
	return retained
}

func isDataViewMembershipEqual(left, right *viewpb.DataViewOfCollection) bool {
	leftClone := cloneDataViewWithoutDeleteTimetick(left)
	rightClone := cloneDataViewWithoutDeleteTimetick(right)
	if leftClone != nil {
		leftClone.DataVersion = nil
	}
	if rightClone != nil {
		rightClone.DataVersion = nil
	}
	if isDataViewEmpty(leftClone) && isDataViewEmpty(rightClone) {
		return true
	}
	return proto.Equal(leftClone, rightClone)
}

func isDataViewEmpty(view *viewpb.DataViewOfCollection) bool {
	return view == nil || len(view.GetShards()) == 0
}

func classifyRecoverAdvance(latest, expected *viewpb.DataViewOfCollection, segments SegmentStore) dataViewAdvance {
	if latest == nil {
		return dataViewAdvanceStreaming
	}
	latestSegments := dataViewSegmentIDSet(latest)
	added := expectedAddedSegmentIDs(latest, expected)
	for _, segmentID := range added {
		segment := segments.GetSegment(context.TODO(), segmentID)
		if segment == nil {
			continue
		}
		if isRecoverStreamingAddition(segment, latestSegments) {
			return dataViewAdvanceStreaming
		}
	}
	return dataViewAdvanceCompact
}

func isRecoverStreamingAddition(segment *Segment, latestSegments map[int64]struct{}) bool {
	if segment == nil {
		return false
	}
	if !segment.GetCreatedByCompaction() && len(segment.GetCompactionFrom()) == 0 {
		return true
	}
	for _, inputID := range segment.GetCompactionFrom() {
		if _, ok := latestSegments[inputID]; ok {
			return false
		}
	}
	return true
}

func expectedAddedSegmentIDs(latest, expected *viewpb.DataViewOfCollection) []int64 {
	known := make(map[int64]struct{})
	for _, partition := range dataViewPartitions(latest) {
		for _, segmentID := range partition.GetSegmentIds() {
			known[segmentID] = struct{}{}
		}
	}
	added := make([]int64, 0)
	for _, partition := range dataViewPartitions(expected) {
		for _, segmentID := range partition.GetSegmentIds() {
			if _, ok := known[segmentID]; !ok {
				added = append(added, segmentID)
			}
		}
	}
	return added
}

func pruneHistoricallyRemovedSegments(latest, expected *viewpb.DataViewOfCollection, persistedViews []*viewpb.DataViewOfCollection) {
	if latest == nil || expected == nil {
		return
	}
	current := dataViewSegmentIDSet(latest)
	historical := make(map[int64]struct{})
	for _, view := range persistedViews {
		if compareDataVersion(view.GetDataVersion(), latest.GetDataVersion()) > 0 {
			continue
		}
		for segmentID := range dataViewSegmentIDSet(view) {
			historical[segmentID] = struct{}{}
		}
	}
	removeSegmentsByPredicate(expected, func(segmentID int64, partitionID int64, vchannel string) bool {
		if _, ok := current[segmentID]; ok {
			return false
		}
		_, wasRetainedBefore := historical[segmentID]
		return wasRetainedBefore
	})
}

func dataViewSegmentIDSet(view *viewpb.DataViewOfCollection) map[int64]struct{} {
	segments := make(map[int64]struct{})
	for _, partition := range dataViewPartitions(view) {
		for _, segmentID := range partition.GetSegmentIds() {
			segments[segmentID] = struct{}{}
		}
	}
	return segments
}

func dataViewContainsSegment(view *viewpb.DataViewOfCollection, target int64) bool {
	for _, partition := range dataViewPartitions(view) {
		for _, segmentID := range partition.GetSegmentIds() {
			if segmentID == target {
				return true
			}
		}
	}
	return false
}

func dataVersionKey(version *viewpb.DataVersion) string {
	return fmt.Sprintf("%d/%d", version.GetStreamingVersion(), version.GetCompactVersion())
}
