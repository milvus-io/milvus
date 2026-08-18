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
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type fakeDataViewCatalog struct {
	mu          sync.Mutex
	views       []*viewpb.DataViewOfCollection
	listErr     error
	saveErr     error
	dropErr     error
	dropAllErr  error
	saveCall    int
	dropAllCall []int64
}

func (c *fakeDataViewCatalog) SaveDataView(_ context.Context, view *viewpb.DataViewOfCollection) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.saveErr != nil {
		err := c.saveErr
		c.saveErr = nil
		return err
	}
	c.saveCall++
	key := dataVersionKey(view.GetDataVersion())
	for i, existing := range c.views {
		if existing.GetCollectionId() == view.GetCollectionId() && dataVersionKey(existing.GetDataVersion()) == key {
			c.views[i] = proto.Clone(view).(*viewpb.DataViewOfCollection)
			return nil
		}
	}
	c.views = append(c.views, proto.Clone(view).(*viewpb.DataViewOfCollection))
	return nil
}

func (c *fakeDataViewCatalog) ListAllDataViews(_ context.Context) ([]*viewpb.DataViewOfCollection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listErr != nil {
		return nil, c.listErr
	}
	views := make([]*viewpb.DataViewOfCollection, 0, len(c.views))
	for _, view := range c.views {
		views = append(views, proto.Clone(view).(*viewpb.DataViewOfCollection))
	}
	return views, nil
}

func (c *fakeDataViewCatalog) DropDataView(_ context.Context, collectionID int64, version *viewpb.DataVersion) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropErr != nil {
		return c.dropErr
	}
	kept := c.views[:0]
	for _, view := range c.views {
		if view.GetCollectionId() == collectionID && dataVersionKey(view.GetDataVersion()) == dataVersionKey(version) {
			continue
		}
		kept = append(kept, view)
	}
	c.views = kept
	return nil
}

func (c *fakeDataViewCatalog) DropDataViews(_ context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropAllErr != nil {
		return c.dropAllErr
	}
	c.dropAllCall = append(c.dropAllCall, collectionID)
	kept := c.views[:0]
	for _, view := range c.views {
		if view.GetCollectionId() == collectionID {
			continue
		}
		kept = append(kept, view)
	}
	c.views = kept
	return nil
}

func newTestManager() (*dataViewManager, *fakeDataViewCatalog) {
	catalog := &fakeDataViewCatalog{}
	return NewManager(catalog).(*dataViewManager), catalog
}

func segment(id int64, channel string, partition int64) LoadableSegment {
	return LoadableSegment{SegmentID: id, VChannel: channel, PartitionID: partition}
}

func version(streaming, compact int64) *viewpb.DataVersion {
	return &viewpb.DataVersion{StreamingVersion: streaming, CompactVersion: compact}
}

func recoverAllCollections(context.Context, int64) (bool, error) {
	return true, nil
}

func requireVersion(t *testing.T, actual *viewpb.DataVersion, streaming, compact int64) {
	require.NotNil(t, actual)
	require.Equal(t, streaming, actual.GetStreamingVersion())
	require.Equal(t, compact, actual.GetCompactVersion())
}

func TestManagerLifecycleAndRef(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()

	v, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	requireVersion(t, v, 1, 0)

	v, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})
	require.NoError(t, err)
	requireVersion(t, v, 2, 0)
	before := catalog.saveCall
	v, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})
	require.NoError(t, err)
	requireVersion(t, v, 2, 0)
	require.Equal(t, before, catalog.saveCall)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	requireVersion(t, ref.Version(), 2, 0)
	view := ref.DataView()
	view.Shards[0].Partitions[0].SegmentIds[0] = 999
	ref.Deref()
	ref.Deref()

	ref, err = manager.Get(ctx, 1, version(2, 0))
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	require.Equal(t, int64(10), ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds()[0])
}

func TestManagerMutationsAndVersions(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1", "ch-2"}})
	require.NoError(t, err)
	v, err := manager.OnImport(ctx, ImportDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-1", 10)}})
	require.NoError(t, err)
	requireVersion(t, v, 1, 1)
	v, err = manager.OnCompact(ctx, CompactDataViewEvent{CollectionID: 1, CompactFrom: []int64{1}, CompactTo: []LoadableSegment{segment(2, "ch-1", 10), segment(3, "ch-2", 20)}})
	require.NoError(t, err)
	requireVersion(t, v, 1, 2)
	v, err = manager.OnExternalRefresh(ctx, ExternalRefreshDataViewEvent{CollectionID: 1, AddSegments: []LoadableSegment{segment(4, "ch-1", 10)}})
	require.NoError(t, err)
	requireVersion(t, v, 1, 3)
	v, err = manager.OnDropPartition(ctx, DropPartitionDataViewEvent{CollectionID: 1, PartitionIDs: []int64{20}})
	require.NoError(t, err)
	requireVersion(t, v, 1, 4)
	v, err = manager.OnTruncate(ctx, TruncateDataViewEvent{CollectionID: 1, SegmentIDs: []int64{4}})
	require.NoError(t, err)
	requireVersion(t, v, 1, 5)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	ids := make(map[int64]struct{})
	for _, shard := range ref.DataView().GetShards() {
		for _, partition := range shard.GetPartitions() {
			for _, id := range partition.GetSegmentIds() {
				ids[id] = struct{}{}
			}
		}
	}
	require.Equal(t, map[int64]struct{}{2: {}}, ids)
}

func TestManagerRejectsConflictingSegments(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	_, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-1", 1)}})
	require.NoError(t, err)
	_, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-2", 1)}})
	require.Error(t, err)
}

func TestManagerGCRespectsLatestRetainedAndRefs(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	_, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-1", 1)}})
	require.NoError(t, err)
	oldRef, err := manager.Get(ctx, 1, version(1, 0))
	require.NoError(t, err)
	require.NotNil(t, oldRef)
	_, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(2, "ch-1", 1)}})
	require.NoError(t, err)
	_, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(3, "ch-1", 1)}})
	require.NoError(t, err)
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	require.Len(t, catalog.views, 2)
	oldRef.Deref()
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	require.Len(t, catalog.views, 1)
}

func TestManagerRecoveryAndDrop(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{}
	require.NoError(t, catalog.SaveDataView(ctx, &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: version(1, 0)}))
	require.NoError(t, catalog.SaveDataView(ctx, &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: version(2, 0)}))
	manager, err := RecoverManager(ctx, catalog, recoverAllCollections)
	require.NoError(t, err)
	oldRef, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, oldRef)
	requireVersion(t, oldRef.Version(), 2, 0)

	_, err = manager.OnDropCollection(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, catalog.views)
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, ref)
	requireVersion(t, oldRef.Version(), 2, 0)
	oldRef.Deref()
}

func TestManagerAccessAndMutationEdges(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, ref)
	ref, err = manager.Get(ctx, 1, nil)
	require.NoError(t, err)
	require.Nil(t, ref)
	require.NoError(t, manager.GarbageCollect(ctx, 1, 0))

	_, err = manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1,
		VChannels:    []string{"ch-2", "", "ch-1", "ch-2"},
	})
	require.NoError(t, err)
	v, err := manager.OnCopySegmentComplete(ctx, CopySegmentCompleteDataViewEvent{
		CollectionID: 1,
		Segments: []LoadableSegment{
			segment(2, "ch-2", 20),
			segment(1, "ch-1", 10),
			segment(1, "ch-1", 10),
		},
	})
	require.NoError(t, err)
	requireVersion(t, v, 1, 1)

	ref, err = manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"ch-1", "ch-2"}, []string{
		ref.DataView().GetShards()[0].GetVchannel(),
		ref.DataView().GetShards()[1].GetVchannel(),
	})
	ref.Deref()

	_, err = manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID: 1,
		Segments:     []LoadableSegment{{SegmentID: 3}},
	})
	require.Error(t, err)
	_, err = manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID: 1,
		Segments:     []LoadableSegment{segment(1, "ch-2", 10)},
	})
	require.Error(t, err)

	catalog.dropAllErr = errors.New("drop failed")
	_, err = manager.OnDropCollection(ctx, 1)
	require.Error(t, err)
	catalog.dropAllErr = nil
	_, err = manager.OnDropCollection(ctx, 1)
	require.NoError(t, err)
	_, err = manager.OnDropCollection(ctx, 1)
	require.NoError(t, err)
	ref, err = manager.Get(ctx, 1, version(1, 1))
	require.NoError(t, err)
	require.Nil(t, ref)
}

func TestManagerPersistenceFailuresDoNotAdvance(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	catalog.saveErr = errors.New("save failed")
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.Error(t, err)
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, ref)

	_, err = manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	catalog.saveErr = errors.New("save failed")
	_, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-1", 10)}})
	require.Error(t, err)
	ref, err = manager.Latest(ctx, 1)
	require.NoError(t, err)
	requireVersion(t, ref.Version(), 1, 0)
	ref.Deref()

	_, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-1", 10)}})
	require.NoError(t, err)
	catalog.dropErr = errors.New("drop failed")
	require.Error(t, manager.GarbageCollect(ctx, 1, 1))
	require.Len(t, catalog.views, 2)
}

func TestManagerRecoveryValidation(t *testing.T) {
	ctx := context.Background()

	_, err := RecoverManager(ctx, &fakeDataViewCatalog{listErr: errors.New("list failed")}, recoverAllCollections)
	require.Error(t, err)
	for _, invalid := range []*viewpb.DataViewOfCollection{
		nil,
		{CollectionId: 1},
		{CollectionId: 0, DataVersion: version(1, 0)},
		{CollectionId: 1, DataVersion: version(0, 1)},
	} {
		_, err := RecoverManager(ctx, &fakeDataViewCatalog{views: []*viewpb.DataViewOfCollection{invalid}}, recoverAllCollections)
		require.Error(t, err)
	}

	conflicting := &fakeDataViewCatalog{views: []*viewpb.DataViewOfCollection{
		{CollectionId: 1, DataVersion: version(2, 1)},
		{CollectionId: 1, DataVersion: version(2, 1), Shards: []*viewpb.DataViewOfShard{{Vchannel: "ch-1"}}},
	}}
	_, err = RecoverManager(ctx, conflicting, recoverAllCollections)
	require.Error(t, err)

	duplicate := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: version(2, 1)}
	manager, err := RecoverManager(ctx, &fakeDataViewCatalog{views: []*viewpb.DataViewOfCollection{
		duplicate,
		proto.Clone(duplicate).(*viewpb.DataViewOfCollection),
		{CollectionId: 1, DataVersion: version(1, 9)},
	}}, recoverAllCollections)
	require.NoError(t, err)
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	requireVersion(t, ref.Version(), 2, 1)
	ref.Deref()
}

func TestManagerRecoveryCleansTombstonedCollections(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{views: []*viewpb.DataViewOfCollection{
		{CollectionId: 1, DataVersion: version(2, 0)},
		{CollectionId: 2, DataVersion: version(3, 1)},
		{CollectionId: 2, DataVersion: version(4, 0)},
	}}
	manager, err := RecoverManager(ctx, catalog, func(_ context.Context, collectionID int64) (bool, error) {
		return collectionID == 1, nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{2}, catalog.dropAllCall)
	require.Len(t, catalog.views, 1)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	requireVersion(t, ref.Version(), 2, 0)
	ref.Deref()
	ref, err = manager.Latest(ctx, 2)
	require.NoError(t, err)
	require.Nil(t, ref)
}

func TestManagerRecoveryValidatorFailureDoesNotDelete(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{views: []*viewpb.DataViewOfCollection{
		{CollectionId: 1, DataVersion: version(1, 0)},
		{CollectionId: 2, DataVersion: version(1, 0)},
	}}
	_, err := RecoverManager(ctx, catalog, func(_ context.Context, collectionID int64) (bool, error) {
		if collectionID == 2 {
			return false, errors.New("validation failed")
		}
		return false, nil
	})
	require.Error(t, err)
	require.Empty(t, catalog.dropAllCall)
	require.Len(t, catalog.views, 2)

	_, err = RecoverManager(ctx, catalog, nil)
	require.Error(t, err)
}

func TestManagerRecoveryCleanupFailure(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			{CollectionId: 1, DataVersion: version(1, 0)},
		},
		dropAllErr: errors.New("cleanup failed"),
	}
	_, err := RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) {
		return false, nil
	})
	require.ErrorIs(t, err, catalog.dropAllErr)
	require.Len(t, catalog.views, 1)
}

func TestDataViewRefNilAndVersionHelpers(t *testing.T) {
	var ref *dataViewRef
	require.Nil(t, ref.DataView())
	require.Nil(t, ref.Version())
	ref.Deref()

	require.Equal(t, "0/0", dataVersionKey(nil))
	require.Equal(t, 0, compareDataVersion(nil, nil))
	require.Equal(t, 1, compareDataVersion(version(2, 0), version(1, 9)))
	require.Equal(t, -1, compareDataVersion(version(1, 1), version(1, 2)))
	require.Equal(t, 0, compareDataVersion(version(1, 2), version(1, 2)))
}
