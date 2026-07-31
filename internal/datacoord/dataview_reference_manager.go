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
	"context"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type dataViewDropMarkerCatalog interface {
	MarkDataViewCollectionDropped(ctx context.Context, collectionID int64) error
	ListDroppedDataViewCollections(ctx context.Context) ([]int64, error)
	UnmarkDataViewCollectionDropped(ctx context.Context, collectionID int64) error
}

type dataViewReferenceDataViews interface {
	DataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error)
	GarbageCollect(ctx context.Context, collectionID int64, protected []*viewpb.DataVersion, retainLatest int) error
	OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error)
}

type dataViewReferenceState struct {
	mu       sync.Mutex
	terminal bool
	refs     map[qviews.DataVersion]int
}

type dataViewReferenceManager struct {
	mu               sync.Mutex
	states           map[int64]*dataViewReferenceState
	dataViews        dataViewReferenceDataViews
	catalog          dataViewDropMarkerCatalog
	collectionExists func(int64) bool
}

func recoverDataViewReferenceManager(
	ctx context.Context,
	catalog dataViewDropMarkerCatalog,
	dataViews dataViewReferenceDataViews,
	collectionExists func(int64) bool,
) (*dataViewReferenceManager, error) {
	manager := &dataViewReferenceManager{
		states:           make(map[int64]*dataViewReferenceState),
		dataViews:        dataViews,
		catalog:          catalog,
		collectionExists: collectionExists,
	}
	droppedCollections, err := catalog.ListDroppedDataViewCollections(ctx)
	if err != nil {
		return nil, err
	}
	for _, collectionID := range droppedCollections {
		manager.states[collectionID] = &dataViewReferenceState{
			terminal: true,
			refs:     make(map[qviews.DataVersion]int),
		}
	}
	return manager, nil
}

func (m *dataViewReferenceManager) getOrCreateState(collectionID int64) *dataViewReferenceState {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[collectionID]
	if state == nil {
		state = &dataViewReferenceState{refs: make(map[qviews.DataVersion]int)}
		m.states[collectionID] = state
	}
	return state
}

func (m *dataViewReferenceManager) PinDataView(ctx context.Context, collectionID int64, version qviews.DataVersion) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.terminal {
		return unavailableDataViewError(collectionID, version)
	}
	view, err := m.dataViews.DataView(ctx, collectionID, version.IntoProto())
	if err != nil {
		return err
	}
	if view == nil {
		return unavailableDataViewError(collectionID, version)
	}
	state.refs[version]++
	return nil
}

func (m *dataViewReferenceManager) RecoverDataViewReference(
	ctx context.Context,
	collectionID int64,
	version qviews.DataVersion,
) (bool, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.terminal || !m.collectionExists(collectionID) {
		return false, nil
	}
	view, err := m.dataViews.DataView(ctx, collectionID, version.IntoProto())
	if err != nil {
		return false, err
	}
	if view == nil {
		return false, merr.WrapErrDataIntegrityMsg(
			"query view references missing data view %s of collection %d",
			version.String(),
			collectionID,
		)
	}
	state.refs[version]++
	return true, nil
}

func (m *dataViewReferenceManager) UnpinDataView(collectionID int64, version qviews.DataVersion) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.refs[version] <= 1 {
		delete(state.refs, version)
		return
	}
	state.refs[version]--
}

func (m *dataViewReferenceManager) GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.terminal {
		return nil
	}
	protected := make([]qviews.DataVersion, 0, len(state.refs))
	for version := range state.refs {
		protected = append(protected, version)
	}
	sort.Slice(protected, func(i, j int) bool {
		if protected[i].StreamingVersion != protected[j].StreamingVersion {
			return protected[i].StreamingVersion < protected[j].StreamingVersion
		}
		return protected[i].CompactVersion < protected[j].CompactVersion
	})
	protectedProto := make([]*viewpb.DataVersion, 0, len(protected))
	for _, version := range protected {
		protectedProto = append(protectedProto, version.IntoProto())
	}
	return m.dataViews.GarbageCollect(ctx, collectionID, protectedProto, retainLatest)
}

func (m *dataViewReferenceManager) DropCollection(ctx context.Context, collectionID int64) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if err := m.catalog.MarkDataViewCollectionDropped(ctx, collectionID); err != nil {
		return err
	}
	state.terminal = true
	_, err := m.dataViews.OnDropCollection(ctx, collectionID)
	return err
}

func (m *dataViewReferenceManager) FinalizeDropCollection(ctx context.Context, collectionID int64) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	return m.catalog.UnmarkDataViewCollectionDropped(ctx, collectionID)
}

func (m *dataViewReferenceManager) IsTerminal(collectionID int64) bool {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.terminal
}

func unavailableDataViewError(collectionID int64, version qviews.DataVersion) error {
	return merr.WrapErrServiceNotReadyMsg(
		"data view %s of collection %d is no longer available",
		version.String(),
		collectionID,
	)
}
