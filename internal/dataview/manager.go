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
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type Catalog interface {
	SaveDataView(ctx context.Context, dataView *viewpb.DataViewOfCollection) error
	ListAllDataViews(ctx context.Context) ([]*viewpb.DataViewOfCollection, error)
	DropDataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) error
	DropDataViews(ctx context.Context, collectionID int64) error
}

type CollectionRecoveryValidator func(ctx context.Context, collectionID int64) (recover bool, err error)

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

	Latest(ctx context.Context, collectionID int64) (DataViewRef, error)
	Get(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) (DataViewRef, error)
	GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error
}

type DataViewRef interface {
	DataView() *viewpb.DataViewOfCollection
	Version() *viewpb.DataVersion
	Deref()
}

type LoadableSegment struct {
	SegmentID       int64
	VChannel        string
	PartitionID     int64
	ManifestVersion int64
}

type CreateCollectionDataViewEvent struct {
	CollectionID int64
	VChannels    []string
}

type FlushDataViewEvent struct {
	CollectionID int64
	Segments     []LoadableSegment
}

type ImportDataViewEvent struct {
	CollectionID int64
	Segments     []LoadableSegment
}

type CopySegmentCompleteDataViewEvent struct {
	CollectionID int64
	Segments     []LoadableSegment
}

type CompactDataViewEvent struct {
	CollectionID int64
	CompactFrom  []int64
	CompactTo    []LoadableSegment
}

type SegmentManifestVersion struct {
	SegmentID       int64
	ManifestVersion int64
}

type L0CompactDataViewEvent struct {
	CollectionID                int64
	VChannel                    string
	SegmentManifestVersions     []SegmentManifestVersion
	TransformStartAfterTimetick uint64
}

type ExternalRefreshDataViewEvent struct {
	CollectionID int64
	AddSegments  []LoadableSegment
	DropSegments []int64
}

type DropPartitionDataViewEvent struct {
	CollectionID int64
	PartitionIDs []int64
}

type TruncateDataViewEvent struct {
	CollectionID int64
	SegmentIDs   []int64
}

type dataViewAdvance int

const (
	dataViewAdvanceStreaming dataViewAdvance = iota
	dataViewAdvanceCompact
)

type membershipMutation struct {
	add              []LoadableSegment
	remove           []int64
	removePartitions map[int64]struct{}
}

type versionEntry struct {
	view *viewpb.DataViewOfCollection
	refs *versionRefCounter
}

type versionRefCounter struct {
	count int
}

type collectionState struct {
	mu       sync.Mutex
	id       int64
	latest   *versionEntry
	versions map[string]*versionEntry
}

type dataViewManager struct {
	mu      sync.RWMutex
	catalog Catalog
	states  map[int64]*collectionState
}

type dataViewRef struct {
	once  sync.Once
	state *collectionState
	entry *versionEntry
}

func NewManager(catalog Catalog) Manager {
	return &dataViewManager{
		catalog: catalog,
		states:  make(map[int64]*collectionState),
	}
}

func RecoverManager(
	ctx context.Context,
	catalog Catalog,
	validator CollectionRecoveryValidator,
) (Manager, error) {
	if validator == nil {
		return nil, merr.WrapErrServiceInternalMsg("DataView recovery requires a Collection recovery validator")
	}
	views, err := catalog.ListAllDataViews(ctx)
	if err != nil {
		return nil, err
	}

	viewsByCollection := make(map[int64][]*viewpb.DataViewOfCollection)
	for _, view := range views {
		if view == nil || view.GetDataVersion() == nil {
			return nil, merr.WrapErrDataIntegrityMsg("persisted DataView has no DataVersion")
		}
		if view.GetCollectionId() == 0 || view.GetDataVersion().GetStreamingVersion() == 0 {
			return nil, merr.WrapErrDataIntegrityMsg(
				"persisted DataView has invalid identity: collection=%d version=%s",
				view.GetCollectionId(), dataVersionKey(view.GetDataVersion()),
			)
		}
		if err := validatePersistedSegmentManifestVersions(view); err != nil {
			return nil, err
		}
		viewsByCollection[view.GetCollectionId()] = append(viewsByCollection[view.GetCollectionId()], view)
	}

	collectionIDs := make([]int64, 0, len(viewsByCollection))
	for collectionID := range viewsByCollection {
		collectionIDs = append(collectionIDs, collectionID)
	}
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })

	recoverCollection := make(map[int64]bool, len(collectionIDs))
	for _, collectionID := range collectionIDs {
		recover, err := validator(ctx, collectionID)
		if err != nil {
			return nil, err
		}
		recoverCollection[collectionID] = recover
	}

	manager := NewManager(catalog).(*dataViewManager)
	for _, collectionID := range collectionIDs {
		if !recoverCollection[collectionID] {
			continue
		}
		for _, view := range viewsByCollection[collectionID] {
			state := manager.getOrCreateState(view.GetCollectionId())
			state.mu.Lock()
			persisted := canonicalDataViewClone(view)
			key := dataVersionKey(view.GetDataVersion())
			if existing := state.versions[key]; existing != nil {
				if !proto.Equal(existing.view, persisted) {
					state.mu.Unlock()
					return nil, merr.WrapErrDataIntegrityMsg(
						"persisted DataView version %s of collection %d has conflicting snapshots",
						key, view.GetCollectionId(),
					)
				}
				state.mu.Unlock()
				continue
			}
			entry := newVersionEntry(persisted)
			state.versions[key] = entry
			if state.latest == nil || compareDataVersion(entry.view.GetDataVersion(), state.latest.view.GetDataVersion()) > 0 {
				state.latest = entry
			}
			state.mu.Unlock()
		}
	}
	for _, collectionID := range collectionIDs {
		if recoverCollection[collectionID] {
			continue
		}
		if err := catalog.DropDataViews(ctx, collectionID); err != nil {
			return nil, err
		}
	}
	return manager, nil
}

func (m *dataViewManager) OnCreateCollection(ctx context.Context, event CreateCollectionDataViewEvent) (*viewpb.DataVersion, error) {
	state, unlock := m.lockStateForMutation(event.CollectionID)
	defer unlock()
	if state.latest != nil {
		return cloneDataVersion(state.latest.view.GetDataVersion()), nil
	}

	view := buildEmptyDataView(event.CollectionID, event.VChannels)
	view.DataVersion = &viewpb.DataVersion{StreamingVersion: 1}
	if err := m.persistLocked(ctx, state, view); err != nil {
		return nil, err
	}
	return cloneDataVersion(view.GetDataVersion()), nil
}

func (m *dataViewManager) OnFlush(ctx context.Context, event FlushDataViewEvent) (*viewpb.DataVersion, error) {
	return m.commit(ctx, event.CollectionID, dataViewAdvanceStreaming, membershipMutation{add: event.Segments})
}

func (m *dataViewManager) OnImport(ctx context.Context, event ImportDataViewEvent) (*viewpb.DataVersion, error) {
	return m.commit(ctx, event.CollectionID, dataViewAdvanceCompact, membershipMutation{add: event.Segments})
}

func (m *dataViewManager) OnCopySegmentComplete(ctx context.Context, event CopySegmentCompleteDataViewEvent) (*viewpb.DataVersion, error) {
	return m.commit(ctx, event.CollectionID, dataViewAdvanceCompact, membershipMutation{add: event.Segments})
}

func (m *dataViewManager) OnCompact(ctx context.Context, event CompactDataViewEvent) (*viewpb.DataVersion, error) {
	state, unlock := m.lockStateForMutation(event.CollectionID)
	defer unlock()

	base := latestView(state)
	if err := validateCompactMutation(base, event); err != nil {
		return nil, err
	}
	return m.commitLocked(ctx, state, dataViewAdvanceCompact, membershipMutation{
		add:    event.CompactTo,
		remove: event.CompactFrom,
	})
}

func (m *dataViewManager) OnL0Compact(ctx context.Context, event L0CompactDataViewEvent) (*viewpb.DataVersion, error) {
	state := m.getState(event.CollectionID)
	if state == nil {
		return nil, nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()

	base := latestView(state)
	if base == nil {
		return nil, nil
	}
	next := canonicalDataViewClone(base)
	changed, err := applyL0Compact(next, event)
	if err != nil {
		return nil, err
	}
	if !changed {
		return dataVersionFromView(base), nil
	}
	if err := m.replaceLatestLocked(ctx, state, next); err != nil {
		return nil, err
	}
	return cloneDataVersion(next.GetDataVersion()), nil
}

func (m *dataViewManager) OnExternalRefresh(ctx context.Context, event ExternalRefreshDataViewEvent) (*viewpb.DataVersion, error) {
	return m.commit(ctx, event.CollectionID, dataViewAdvanceCompact, membershipMutation{
		add:    event.AddSegments,
		remove: event.DropSegments,
	})
}

func (m *dataViewManager) OnDropPartition(ctx context.Context, event DropPartitionDataViewEvent) (*viewpb.DataVersion, error) {
	partitions := make(map[int64]struct{}, len(event.PartitionIDs))
	for _, partitionID := range event.PartitionIDs {
		partitions[partitionID] = struct{}{}
	}
	return m.commit(ctx, event.CollectionID, dataViewAdvanceCompact, membershipMutation{removePartitions: partitions})
}

func (m *dataViewManager) OnTruncate(ctx context.Context, event TruncateDataViewEvent) (*viewpb.DataVersion, error) {
	return m.commit(ctx, event.CollectionID, dataViewAdvanceCompact, membershipMutation{remove: event.SegmentIDs})
}

func (m *dataViewManager) OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[collectionID]
	if state != nil {
		state.mu.Lock()
		defer state.mu.Unlock()
	}
	if err := m.catalog.DropDataViews(ctx, collectionID); err != nil {
		return nil, err
	}
	delete(m.states, collectionID)
	return nil, nil
}

func (m *dataViewManager) Latest(_ context.Context, collectionID int64) (DataViewRef, error) {
	state := m.getState(collectionID)
	if state == nil {
		return nil, nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return acquireRefLocked(state, state.latest), nil
}

func (m *dataViewManager) Get(_ context.Context, collectionID int64, version *viewpb.DataVersion) (DataViewRef, error) {
	if version == nil {
		return nil, nil
	}
	state := m.getState(collectionID)
	if state == nil {
		return nil, nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return acquireRefLocked(state, state.versions[dataVersionKey(version)]), nil
}

func (m *dataViewManager) GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error {
	state := m.getState(collectionID)
	if state == nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if retainLatest < 1 {
		retainLatest = 1
	}

	entries := make([]*versionEntry, 0, len(state.versions))
	for _, entry := range state.versions {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return compareDataVersion(entries[i].view.GetDataVersion(), entries[j].view.GetDataVersion()) > 0
	})
	for idx, entry := range entries {
		if idx < retainLatest || entry == state.latest || entry.refs.count > 0 {
			continue
		}
		version := entry.view.GetDataVersion()
		if err := m.catalog.DropDataView(ctx, collectionID, version); err != nil {
			return err
		}
		delete(state.versions, dataVersionKey(version))
	}
	return nil
}

func (m *dataViewManager) commit(ctx context.Context, collectionID int64, advance dataViewAdvance, mutation membershipMutation) (*viewpb.DataVersion, error) {
	state, unlock := m.lockStateForMutation(collectionID)
	defer unlock()
	return m.commitLocked(ctx, state, advance, mutation)
}

func (m *dataViewManager) commitLocked(
	ctx context.Context,
	state *collectionState,
	advance dataViewAdvance,
	mutation membershipMutation,
) (*viewpb.DataVersion, error) {
	base := latestView(state)
	next := canonicalDataViewClone(base)
	if next == nil {
		next = &viewpb.DataViewOfCollection{CollectionId: state.id}
	}

	removeSegments(next, mutation.remove, mutation.removePartitions)
	if err := addSegments(next, mutation.add); err != nil {
		return nil, err
	}
	canonicalizeDataView(next)
	if dataViewMembershipEqual(base, next) {
		return dataVersionFromView(base), nil
	}

	next.DataVersion = nextDataVersion(base, advance)
	if err := m.persistLocked(ctx, state, next); err != nil {
		return nil, err
	}
	return cloneDataVersion(next.GetDataVersion()), nil
}

func (m *dataViewManager) persistLocked(ctx context.Context, state *collectionState, view *viewpb.DataViewOfCollection) error {
	persisted := canonicalDataViewClone(view)
	key := dataVersionKey(persisted.GetDataVersion())
	if existing := state.versions[key]; existing != nil {
		if !proto.Equal(existing.view, persisted) {
			return merr.WrapErrDataIntegrityMsg(
				"DataView version %s of collection %d has conflicting snapshots",
				key, state.id,
			)
		}
		state.latest = existing
		return nil
	}
	if err := m.catalog.SaveDataView(ctx, persisted); err != nil {
		return err
	}
	entry := newVersionEntry(persisted)
	state.versions[key] = entry
	state.latest = entry
	return nil
}

func (m *dataViewManager) replaceLatestLocked(ctx context.Context, state *collectionState, view *viewpb.DataViewOfCollection) error {
	persisted := canonicalDataViewClone(view)
	if err := m.catalog.SaveDataView(ctx, persisted); err != nil {
		return err
	}
	entry := newVersionEntry(persisted)
	entry.refs = state.latest.refs
	state.versions[dataVersionKey(persisted.GetDataVersion())] = entry
	state.latest = entry
	return nil
}

func (m *dataViewManager) getState(collectionID int64) *collectionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.states[collectionID]
}

func (m *dataViewManager) lockStateForMutation(collectionID int64) (*collectionState, func()) {
	m.mu.Lock()
	state := m.states[collectionID]
	if state == nil {
		state = &collectionState{id: collectionID, versions: make(map[string]*versionEntry)}
		m.states[collectionID] = state
	}
	state.mu.Lock()
	m.mu.Unlock()
	return state, state.mu.Unlock
}

func (m *dataViewManager) getOrCreateState(collectionID int64) *collectionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[collectionID]
	if state == nil {
		state = &collectionState{id: collectionID, versions: make(map[string]*versionEntry)}
		m.states[collectionID] = state
	}
	return state
}

func acquireRefLocked(state *collectionState, entry *versionEntry) DataViewRef {
	if entry == nil {
		return nil
	}
	entry.refs.count++
	return &dataViewRef{state: state, entry: entry}
}

func (r *dataViewRef) DataView() *viewpb.DataViewOfCollection {
	if r == nil || r.entry == nil {
		return nil
	}
	return canonicalDataViewClone(r.entry.view)
}

func (r *dataViewRef) Version() *viewpb.DataVersion {
	if r == nil || r.entry == nil {
		return nil
	}
	return cloneDataVersion(r.entry.view.GetDataVersion())
}

func (r *dataViewRef) Deref() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		r.state.mu.Lock()
		defer r.state.mu.Unlock()
		if r.entry.refs.count > 0 {
			r.entry.refs.count--
		}
	})
}

func newVersionEntry(view *viewpb.DataViewOfCollection) *versionEntry {
	return &versionEntry{view: view, refs: &versionRefCounter{}}
}

func latestView(state *collectionState) *viewpb.DataViewOfCollection {
	if state.latest == nil {
		return nil
	}
	return state.latest.view
}

func nextDataVersion(base *viewpb.DataViewOfCollection, advance dataViewAdvance) *viewpb.DataVersion {
	if base == nil || base.GetDataVersion() == nil || base.GetDataVersion().GetStreamingVersion() == 0 {
		return &viewpb.DataVersion{StreamingVersion: 1}
	}
	current := base.GetDataVersion()
	if advance == dataViewAdvanceStreaming {
		return &viewpb.DataVersion{StreamingVersion: current.GetStreamingVersion() + 1}
	}
	return &viewpb.DataVersion{
		StreamingVersion: current.GetStreamingVersion(),
		CompactVersion:   current.GetCompactVersion() + 1,
	}
}

func addSegments(view *viewpb.DataViewOfCollection, segments []LoadableSegment) error {
	slots := dataViewSegmentSlots(view)
	for _, segment := range segments {
		if segment.SegmentID == 0 || segment.VChannel == "" || segment.ManifestVersion < 0 {
			return merr.WrapErrServiceInternalMsg(
				"invalid loadable Segment descriptor: segment=%d vchannel=%q manifestVersion=%d",
				segment.SegmentID,
				segment.VChannel,
				segment.ManifestVersion,
			)
		}
		location := segmentLocation{vchannel: segment.VChannel, partitionID: segment.PartitionID}
		if known, ok := slots[segment.SegmentID]; ok {
			if known.location != location {
				return merr.WrapErrDataIntegrityMsg("Segment %d has conflicting DataView locations", segment.SegmentID)
			}
			currentVersion := known.partition.SegmentManifestVersions[known.index]
			if segment.ManifestVersion < currentVersion {
				return merr.WrapErrDataIntegrityMsg(
					"Segment %d Manifest version cannot regress from %d to %d",
					segment.SegmentID,
					currentVersion,
					segment.ManifestVersion,
				)
			}
			if segment.ManifestVersion > currentVersion {
				known.partition.SegmentManifestVersions[known.index] = segment.ManifestVersion
			}
			continue
		}
		shard := findOrCreateShard(view, segment.VChannel)
		partition := findOrCreatePartition(shard, segment.PartitionID)
		partition.SegmentIds = append(partition.SegmentIds, segment.SegmentID)
		partition.SegmentManifestVersions = append(partition.SegmentManifestVersions, segment.ManifestVersion)
		slots[segment.SegmentID] = segmentSlot{
			location:  location,
			partition: partition,
			index:     len(partition.SegmentIds) - 1,
		}
	}
	return nil
}

func validateCompactMutation(view *viewpb.DataViewOfCollection, event CompactDataViewEvent) error {
	if len(event.CompactFrom) == 0 {
		return merr.WrapErrServiceInternalMsg("DataView compaction event has no input Segments")
	}
	if view == nil {
		return merr.WrapErrDataIntegrityMsg(
			"DataView compaction references missing collection %d",
			event.CollectionID,
		)
	}

	slots := dataViewSegmentSlots(view)
	inputIDs := typeutil.NewSet(event.CompactFrom...)
	missing := make([]int64, 0)
	for segmentID := range inputIDs {
		if segmentID == 0 {
			return merr.WrapErrServiceInternalMsg("DataView compaction event has invalid input Segment 0")
		}
		if _, ok := slots[segmentID]; !ok {
			missing = append(missing, segmentID)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if len(missing) == len(inputIDs) && compactTargetsPresent(slots, event.CompactTo) {
		return nil
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return merr.WrapErrDataIntegrityMsg(
		"DataView compaction inputs are missing: collection=%d segments=%v",
		event.CollectionID,
		missing,
	)
}

func compactTargetsPresent(slots map[int64]segmentSlot, targets []LoadableSegment) bool {
	for _, target := range targets {
		slot, ok := slots[target.SegmentID]
		if !ok || slot.location != (segmentLocation{vchannel: target.VChannel, partitionID: target.PartitionID}) {
			return false
		}
		if slot.partition.GetSegmentManifestVersions()[slot.index] < target.ManifestVersion {
			return false
		}
	}
	return true
}

func applyL0Compact(view *viewpb.DataViewOfCollection, event L0CompactDataViewEvent) (bool, error) {
	if event.VChannel == "" {
		return false, merr.WrapErrServiceInternalMsg("L0 DataView event has no vchannel")
	}
	var targetShard *viewpb.DataViewOfShard
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == event.VChannel {
			targetShard = shard
			break
		}
	}
	if targetShard == nil {
		return false, merr.WrapErrDataIntegrityMsg("L0 DataView event references unknown vchannel %q", event.VChannel)
	}

	changed := false
	if event.TransformStartAfterTimetick > targetShard.GetTransformStartAfterTimetick() {
		targetShard.TransformStartAfterTimetick = event.TransformStartAfterTimetick
		changed = true
	}

	slots := dataViewSegmentSlots(view)
	for _, update := range event.SegmentManifestVersions {
		if update.SegmentID == 0 || update.ManifestVersion < 0 {
			return false, merr.WrapErrServiceInternalMsg(
				"invalid L0 Segment Manifest version: segment=%d manifestVersion=%d",
				update.SegmentID,
				update.ManifestVersion,
			)
		}
		slot, ok := slots[update.SegmentID]
		if !ok {
			continue
		}
		if slot.location.vchannel != event.VChannel {
			return false, merr.WrapErrDataIntegrityMsg(
				"L0 Segment %d belongs to vchannel %q, not %q",
				update.SegmentID,
				slot.location.vchannel,
				event.VChannel,
			)
		}
		currentVersion := slot.partition.SegmentManifestVersions[slot.index]
		if update.ManifestVersion < currentVersion {
			return false, merr.WrapErrDataIntegrityMsg(
				"Segment %d Manifest version cannot regress from %d to %d",
				update.SegmentID,
				currentVersion,
				update.ManifestVersion,
			)
		}
		if update.ManifestVersion > currentVersion {
			slot.partition.SegmentManifestVersions[slot.index] = update.ManifestVersion
			changed = true
		}
	}
	return changed, nil
}

func validatePersistedSegmentManifestVersions(view *viewpb.DataViewOfCollection) error {
	for _, shard := range view.GetShards() {
		for _, partition := range shard.GetPartitions() {
			versions := partition.GetSegmentManifestVersions()
			if len(versions) != 0 && len(versions) != len(partition.GetSegmentIds()) {
				return merr.WrapErrDataIntegrityMsg(
					"persisted DataView has misaligned Segment arrays: collection=%d vchannel=%q partition=%d segments=%d manifestVersions=%d",
					view.GetCollectionId(),
					shard.GetVchannel(),
					partition.GetPartitionId(),
					len(partition.GetSegmentIds()),
					len(versions),
				)
			}
			for _, version := range versions {
				if version < 0 {
					return merr.WrapErrDataIntegrityMsg(
						"persisted DataView has negative Manifest version: collection=%d segmentManifestVersion=%d",
						view.GetCollectionId(),
						version,
					)
				}
			}
		}
	}
	return nil
}

func removeSegments(view *viewpb.DataViewOfCollection, segmentIDs []int64, partitions map[int64]struct{}) {
	removeIDs := make(map[int64]struct{}, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		removeIDs[segmentID] = struct{}{}
	}
	for _, shard := range view.GetShards() {
		keptPartitions := shard.Partitions[:0]
		for _, partition := range shard.GetPartitions() {
			if _, ok := partitions[partition.GetPartitionId()]; ok {
				continue
			}
			keptSegments := partition.SegmentIds[:0]
			keptVersions := partition.SegmentManifestVersions[:0]
			for idx, segmentID := range partition.GetSegmentIds() {
				if _, ok := removeIDs[segmentID]; ok {
					continue
				}
				keptSegments = append(keptSegments, segmentID)
				keptVersions = append(keptVersions, partition.GetSegmentManifestVersions()[idx])
			}
			partition.SegmentIds = keptSegments
			partition.SegmentManifestVersions = keptVersions
			if len(keptSegments) > 0 {
				keptPartitions = append(keptPartitions, partition)
			}
		}
		shard.Partitions = keptPartitions
	}
}

type segmentLocation struct {
	vchannel    string
	partitionID int64
}

type segmentSlot struct {
	location  segmentLocation
	partition *viewpb.DataViewOfPartition
	index     int
}

func dataViewSegmentSlots(view *viewpb.DataViewOfCollection) map[int64]segmentSlot {
	slots := make(map[int64]segmentSlot)
	for _, shard := range view.GetShards() {
		for _, partition := range shard.GetPartitions() {
			for idx, segmentID := range partition.GetSegmentIds() {
				slots[segmentID] = segmentSlot{
					location:  segmentLocation{vchannel: shard.GetVchannel(), partitionID: partition.GetPartitionId()},
					partition: partition,
					index:     idx,
				}
			}
		}
	}
	return slots
}

func findOrCreateShard(view *viewpb.DataViewOfCollection, vchannel string) *viewpb.DataViewOfShard {
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard
		}
	}
	shard := &viewpb.DataViewOfShard{Vchannel: vchannel}
	view.Shards = append(view.Shards, shard)
	return shard
}

func findOrCreatePartition(shard *viewpb.DataViewOfShard, partitionID int64) *viewpb.DataViewOfPartition {
	for _, partition := range shard.GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return partition
		}
	}
	partition := &viewpb.DataViewOfPartition{PartitionId: partitionID}
	shard.Partitions = append(shard.Partitions, partition)
	return partition
}

func buildEmptyDataView(collectionID int64, vchannels []string) *viewpb.DataViewOfCollection {
	view := &viewpb.DataViewOfCollection{CollectionId: collectionID}
	seen := make(map[string]struct{}, len(vchannels))
	for _, vchannel := range vchannels {
		if vchannel == "" {
			continue
		}
		if _, ok := seen[vchannel]; ok {
			continue
		}
		seen[vchannel] = struct{}{}
		view.Shards = append(view.Shards, &viewpb.DataViewOfShard{Vchannel: vchannel})
	}
	canonicalizeDataView(view)
	return view
}

func canonicalDataViewClone(view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	if view == nil {
		return nil
	}
	clone := proto.Clone(view).(*viewpb.DataViewOfCollection)
	canonicalizeDataView(clone)
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
			canonicalizePartitionSegments(partition)
		}
	}
}

type canonicalSegment struct {
	id              int64
	manifestVersion int64
}

func canonicalizePartitionSegments(partition *viewpb.DataViewOfPartition) {
	segments := make([]canonicalSegment, len(partition.GetSegmentIds()))
	versions := partition.GetSegmentManifestVersions()
	for idx, segmentID := range partition.GetSegmentIds() {
		segments[idx].id = segmentID
		if idx < len(versions) {
			segments[idx].manifestVersion = versions[idx]
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].id < segments[j].id
	})

	partition.SegmentIds = make([]int64, 0, len(segments))
	partition.SegmentManifestVersions = make([]int64, 0, len(segments))
	for _, segment := range segments {
		last := len(partition.SegmentIds) - 1
		if last >= 0 && partition.SegmentIds[last] == segment.id {
			if segment.manifestVersion > partition.SegmentManifestVersions[last] {
				partition.SegmentManifestVersions[last] = segment.manifestVersion
			}
			continue
		}
		partition.SegmentIds = append(partition.SegmentIds, segment.id)
		partition.SegmentManifestVersions = append(partition.SegmentManifestVersions, segment.manifestVersion)
	}
}

func dataViewMembershipEqual(left, right *viewpb.DataViewOfCollection) bool {
	left = canonicalDataViewClone(left)
	right = canonicalDataViewClone(right)
	if left != nil {
		left.DataVersion = nil
	}
	if right != nil {
		right.DataVersion = nil
	}
	return proto.Equal(left, right)
}

func dataVersionFromView(view *viewpb.DataViewOfCollection) *viewpb.DataVersion {
	if view == nil {
		return nil
	}
	return cloneDataVersion(view.GetDataVersion())
}

func cloneDataVersion(version *viewpb.DataVersion) *viewpb.DataVersion {
	if version == nil {
		return nil
	}
	return proto.Clone(version).(*viewpb.DataVersion)
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

func dataVersionKey(version *viewpb.DataVersion) string {
	if version == nil {
		return "0/0"
	}
	return fmt.Sprintf("%d/%d", version.GetStreamingVersion(), version.GetCompactVersion())
}
