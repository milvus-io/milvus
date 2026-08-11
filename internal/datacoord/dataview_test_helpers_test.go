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

	balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type fakeGCDataViewManager struct {
	calls              []fakeGCDataViewCall
	createEvents       []CreateCollectionDataViewEvent
	droppedCollections []int64
	flushEvents        []FlushDataViewEvent
	l0CompactEvents    []L0CompactDataViewEvent
	snapshotRequested  []int64
	snapshotViews      []*viewpb.DataViewOfCollection
	segmentReferenced  bool
	segmentRefErr      error
}

type fakeGCDataViewCall struct {
	collectionID int64
	protected    []*viewpb.DataVersion
	retainLatest int
}

func (m *fakeGCDataViewManager) OnCreateCollection(_ context.Context, event CreateCollectionDataViewEvent) (*viewpb.DataVersion, error) {
	m.createEvents = append(m.createEvents, event)
	return nil, nil
}

func (m *fakeGCDataViewManager) OnFlush(_ context.Context, event FlushDataViewEvent) (*viewpb.DataVersion, error) {
	m.flushEvents = append(m.flushEvents, event)
	return nil, nil
}

func (m *fakeGCDataViewManager) OnImport(context.Context, ImportDataViewEvent) (*viewpb.DataVersion, error) {
	return nil, nil
}

func (m *fakeGCDataViewManager) OnCopySegmentComplete(context.Context, CopySegmentCompleteDataViewEvent) (*viewpb.DataVersion, error) {
	return nil, nil
}

func (m *fakeGCDataViewManager) OnCompact(context.Context, CompactDataViewEvent) (*viewpb.DataVersion, error) {
	return nil, nil
}

func (m *fakeGCDataViewManager) OnL0Compact(_ context.Context, event L0CompactDataViewEvent) (*viewpb.DataVersion, error) {
	m.l0CompactEvents = append(m.l0CompactEvents, event)
	return nil, nil
}

func (m *fakeGCDataViewManager) OnExternalRefresh(context.Context, ExternalRefreshDataViewEvent) (*viewpb.DataVersion, error) {
	return nil, nil
}

func (m *fakeGCDataViewManager) OnDropPartition(context.Context, DropPartitionDataViewEvent) (*viewpb.DataVersion, error) {
	return nil, nil
}

func (m *fakeGCDataViewManager) OnTruncate(context.Context, TruncateDataViewEvent) (*viewpb.DataVersion, error) {
	return nil, nil
}

func (m *fakeGCDataViewManager) OnDropCollection(_ context.Context, collectionID int64) (*viewpb.DataVersion, error) {
	m.droppedCollections = append(m.droppedCollections, collectionID)
	return nil, nil
}

func (m *fakeGCDataViewManager) RepairCollection(context.Context, int64) error    { return nil }
func (m *fakeGCDataViewManager) RepairCollections(context.Context, []int64) error { return nil }
func (m *fakeGCDataViewManager) LatestVisibleDataView(context.Context, int64) (*viewpb.DataViewOfCollection, error) {
	return nil, nil
}

func (m *fakeGCDataViewManager) DataView(context.Context, int64, *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
	return nil, nil
}

func (m *fakeGCDataViewManager) Snapshot(_ context.Context, collectionIDs []int64) ([]*viewpb.DataViewOfCollection, error) {
	m.snapshotRequested = append([]int64(nil), collectionIDs...)
	return m.snapshotViews, nil
}

func (m *fakeGCDataViewManager) DataViewSnapshot(context.Context) *balancerapi.DataViewSnapshot {
	return balancerapi.NewDataViewSnapshot(0, m.snapshotViews, nil)
}

func (m *fakeGCDataViewManager) ShardTimeTicks(context.Context, []int64) ([]*viewpb.DataViewShardTimeTick, error) {
	return nil, nil
}

func (m *fakeGCDataViewManager) IsSegmentReferenced(context.Context, int64, int64) (bool, error) {
	return m.segmentReferenced, m.segmentRefErr
}

func (m *fakeGCDataViewManager) GarbageCollect(_ context.Context, collectionID int64, protected []*viewpb.DataVersion, retainLatest int) error {
	m.calls = append(m.calls, fakeGCDataViewCall{
		collectionID: collectionID,
		protected:    protected,
		retainLatest: retainLatest,
	})
	return nil
}
