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
	"time"

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
	return NewManager(catalog, nil).(*dataViewManager), catalog
}

func segment(id int64, channel string, partition int64) LoadableSegment {
	return LoadableSegment{SegmentID: id, VChannel: channel, PartitionID: partition}
}

func segmentWithManifestVersion(id int64, channel string, partition, manifestVersion int64) LoadableSegment {
	return LoadableSegment{
		SegmentID:       id,
		VChannel:        channel,
		PartitionID:     partition,
		ManifestVersion: manifestVersion,
	}
}

func version(streaming, compact int64) *viewpb.DataVersion {
	return &viewpb.DataVersion{StreamingVersion: streaming, CompactVersion: compact}
}

func recoverAllCollections(context.Context, int64) (bool, error) {
	return true, nil
}

// projectSegments builds a Projector returning a fixed loadable Segment set,
// mimicking a SegmentMeta loadable projection (as datacoord provides).
func projectSegments(segments ...LoadableSegment) Projector {
	return func(_ context.Context, _ int64) ([]LoadableSegment, error) {
		return segments, nil
	}
}

// flushAndCommit runs the flush atomic-txn flow: PrepareFlush under the
// Collection lock, persists the prepared snapshot through the catalog (as the
// txn does alongside SegmentMeta), then commit() to load it into memory.
func flushAndCommit(t *testing.T, manager Manager, event FlushDataViewEvent) *viewpb.DataVersion {
	view, commit, abort, err := manager.PrepareFlush(context.Background(), event)
	require.NoError(t, err)
	require.NotNil(t, commit)
	require.NotNil(t, abort)
	if mgr, ok := manager.(*dataViewManager); ok {
		require.NoError(t, mgr.catalog.SaveDataView(context.Background(), view))
	}
	commit()
	return view.GetDataVersion()
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

	v = flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})
	requireVersion(t, v, 2, 0)
	beforeViews := len(catalog.views)
	// An idempotent replay of the same flush returns the current snapshot
	// without persisting a new version.
	v = flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})
	requireVersion(t, v, 2, 0)
	require.Equal(t, beforeViews, len(catalog.views))

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
	require.Equal(t, int64(0), ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions()[0])
}

func TestManagerBootstrapCollection(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()

	// a Collection that predates DataView management: no prior snapshot, the
	// bootstrap seeds the initial (1,0,0) with its currently loadable Segments
	v, err := manager.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{
		CollectionID: 1,
		VChannels:    []string{"ch-1"},
		Segments: []LoadableSegment{
			segment(10, "ch-1", 100),
			segment(20, "ch-1", 100),
			segment(30, "ch-2", 200),
		},
	})
	require.NoError(t, err)
	requireVersion(t, v, 1, 0)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	defer ref.Deref()
	view := ref.DataView()
	require.Len(t, view.GetShards(), 2)
	require.Equal(t, []int64{10, 20}, view.GetShards()[0].GetPartitions()[0].GetSegmentIds())
	require.Equal(t, int64(100), view.GetShards()[0].GetPartitions()[0].GetPartitionId())
	require.Equal(t, []int64{30}, view.GetShards()[1].GetPartitions()[0].GetSegmentIds())
	// seeded Segments carry Manifest version zero ("producer does not know")
	require.Equal(t, []int64{0, 0}, view.GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions())

	// idempotent: repeating the bootstrap is a no-op and returns the same version
	before := catalog.saveCall
	v, err = manager.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{
		CollectionID: 1,
		VChannels:    []string{"ch-1"},
		Segments:     []LoadableSegment{segment(10, "ch-1", 100)},
	})
	require.NoError(t, err)
	requireVersion(t, v, 1, 0)
	require.Equal(t, before, catalog.saveCall)

	// a Collection created post-upgrade already has a snapshot: bootstrap leaves it alone
	_, err = manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 2, VChannels: []string{"ch-9"}})
	require.NoError(t, err)
	before = catalog.saveCall
	v, err = manager.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{
		CollectionID: 2,
		VChannels:    []string{"ch-9"},
		Segments:     []LoadableSegment{segment(40, "ch-9", 300)},
	})
	require.NoError(t, err)
	requireVersion(t, v, 1, 0)
	require.Equal(t, before, catalog.saveCall)
	ref2, err := manager.Latest(ctx, 2)
	require.NoError(t, err)
	defer ref2.Deref()
	require.Empty(t, ref2.DataView().GetShards()[0].GetPartitions())
}

func TestManagerSegmentManifestVersions(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)

	v, err := manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 3)))
	require.NoError(t, err)
	requireVersion(t, v, 1, 1)

	before := catalog.saveCall
	v, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 3)))
	require.NoError(t, err)
	requireVersion(t, v, 1, 1)
	require.Equal(t, before, catalog.saveCall)

	v, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 4)))
	require.NoError(t, err)
	requireVersion(t, v, 1, 2)
	require.Equal(t, before+1, catalog.saveCall)

	afterAdvance := catalog.saveCall
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 2)))
	require.Error(t, err)
	require.Equal(t, afterAdvance, catalog.saveCall)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	partition := ref.DataView().GetShards()[0].GetPartitions()[0]
	require.Equal(t, []int64{10}, partition.GetSegmentIds())
	require.Equal(t, []int64{4}, partition.GetSegmentManifestVersions())
}

func TestManagerL0CompactAdvancesCompactVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 3)))
	require.NoError(t, err)

	oldRef, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, oldRef)
	defer oldRef.Deref()

	before := catalog.saveCall
	catalog.saveErr = errors.New("save failed")
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 4)))
	require.Error(t, err)
	require.Equal(t, before, catalog.saveCall)
	failedRef, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []int64{3}, failedRef.DataView().GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions())
	failedRef.Deref()

	// The L0 manifest bump is a hard compact trigger: the snapshot content
	// (member + manifest version) changes, so compact_version advances.
	v, err := manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(10, "ch-1", 100, 4),
		segmentWithManifestVersion(999, "ch-1", 100, 8),
	))
	require.NoError(t, err)
	requireVersion(t, v, 1, 2)
	require.Equal(t, before+1, catalog.saveCall)
	require.Len(t, catalog.views, 3)

	latestRef, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, latestRef)
	defer latestRef.Deref()
	latest := latestRef.DataView()
	requireVersion(t, latest.GetDataVersion(), 1, 2)
	require.Equal(t, []int64{4, 8}, latest.GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions())
	recovered, err := RecoverManager(ctx, catalog, recoverAllCollections, nil, nil, nil)
	require.NoError(t, err)
	recoveredRef, err := recovered.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, recoveredRef)
	recoveredView := recoveredRef.DataView()
	recoveredRef.Deref()
	requireVersion(t, recoveredView.GetDataVersion(), 1, 2)
	require.Equal(t, []int64{4, 8}, recoveredView.GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions())

	old := oldRef.DataView()
	require.Equal(t, []int64{3}, old.GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions())

	before = catalog.saveCall
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(10, "ch-1", 100, 4),
		segmentWithManifestVersion(999, "ch-1", 100, 8),
	))
	require.NoError(t, err)
	require.Equal(t, before, catalog.saveCall)
	latestRefAfterNoop, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	requireVersion(t, latestRefAfterNoop.Version(), 1, 2)
	latestRefAfterNoop.Deref()

	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 2)))
	require.Error(t, err)
	require.Equal(t, before, catalog.saveCall)

	v, err = manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(10, "ch-1", 100, 5),
		segmentWithManifestVersion(999, "ch-1", 100, 8),
	))
	require.NoError(t, err)
	requireVersion(t, v, 1, 3)
	require.Equal(t, before+1, catalog.saveCall)
}

func TestManagerZeroManifestVersionReplayIsNoopAfterL0Bump(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	// Membership producers (flush/import/copy/compact) publish version 0: they
	// do not know the committed manifest version.
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segment(10, "ch-1", 100)))
	require.NoError(t, err)
	// L0 compaction raises the stored manifest version to a positive value.
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 7)))
	require.NoError(t, err)

	before := catalog.saveCall
	// An idempotent replay of the original membership projection (e.g.
	// StreamingNode retrying HandleCommitVchannel) still yields version 0.
	// Zero must be treated as "leave the stored version alone", not as a
	// regression to 0.
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segment(10, "ch-1", 100)))
	require.NoError(t, err)
	require.Equal(t, before, catalog.saveCall)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	partition := ref.DataView().GetShards()[0].GetPartitions()[0]
	require.Equal(t, []int64{10}, partition.GetSegmentIds())
	require.Equal(t, []int64{7}, partition.GetSegmentManifestVersions())
}

func TestManagerL0CompactPreservesRefGCProtection(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 3)))
	require.NoError(t, err)
	oldRef, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, oldRef)

	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(10, "ch-1", 100, 4)))
	require.NoError(t, err)
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(10, "ch-1", 100, 4),
		segment(20, "ch-1", 100),
	))
	require.NoError(t, err)
	latest, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	requireVersion(t, latest.Version(), 1, 3)
	latest.Deref()

	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	protected, err := manager.Get(ctx, 1, version(1, 1))
	require.NoError(t, err)
	require.NotNil(t, protected)
	protected.Deref()

	oldRef.Deref()
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	collected, err := manager.Get(ctx, 1, version(1, 1))
	require.NoError(t, err)
	require.Nil(t, collected)
}

func TestManagerCanonicalizesSegmentManifestVersions(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)

	v, err := manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(30, "ch-1", 100, 7),
		segmentWithManifestVersion(10, "ch-1", 100, 5),
		segmentWithManifestVersion(20, "ch-1", 100, 0),
	))
	require.NoError(t, err)
	requireVersion(t, v, 1, 1)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	partition := ref.DataView().GetShards()[0].GetPartitions()[0]
	require.Equal(t, []int64{10, 20, 30}, partition.GetSegmentIds())
	require.Equal(t, []int64{5, 0, 7}, partition.GetSegmentManifestVersions())
}

func TestDataViewPartitionSegmentFieldsArePacked(t *testing.T) {
	descriptor := (&viewpb.DataViewOfPartition{}).ProtoReflect().Descriptor()
	require.True(t, descriptor.Fields().ByName("segment_ids").IsPacked())
	require.True(t, descriptor.Fields().ByName("segment_manifest_versions").IsPacked())
}

func TestManagerMutationsAndVersions(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1", "ch-2"}})
	require.NoError(t, err)
	// import: membership change -> compact+1
	v, err := manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(1, "ch-1", 10, 1)))
	require.NoError(t, err)
	requireVersion(t, v, 1, 1)
	// compaction: input 1 retired, outputs 2/3 -> compact+1
	v, err = manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(2, "ch-1", 10, 2),
		segmentWithManifestVersion(3, "ch-2", 20, 3),
	))
	require.NoError(t, err)
	requireVersion(t, v, 1, 2)
	// external refresh: add 4 -> compact+1
	v, err = manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(2, "ch-1", 10, 2),
		segmentWithManifestVersion(3, "ch-2", 20, 3),
		segmentWithManifestVersion(4, "ch-1", 10, 4),
	))
	require.NoError(t, err)
	requireVersion(t, v, 1, 3)
	// drop partition 20: segment 3 disappears -> compact+1
	v, err = manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(2, "ch-1", 10, 2),
		segmentWithManifestVersion(4, "ch-1", 10, 4),
	))
	require.NoError(t, err)
	requireVersion(t, v, 1, 4)
	// truncate: segment 4 disappears -> compact+1
	v, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(2, "ch-1", 10, 2)))
	require.NoError(t, err)
	requireVersion(t, v, 1, 5)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	ids := make(map[int64]struct{})
	for _, shard := range ref.DataView().GetShards() {
		for _, partition := range shard.GetPartitions() {
			for idx, id := range partition.GetSegmentIds() {
				ids[id] = struct{}{}
				require.Equal(t, int64(2), partition.GetSegmentManifestVersions()[idx])
			}
		}
	}
	require.Equal(t, map[int64]struct{}{2: {}}, ids)
}

func TestManagerRecomputeRebuildsSnapshot(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(1, "ch-1", 10, 1),
		segmentWithManifestVersion(2, "ch-1", 10, 1),
	))
	require.NoError(t, err)

	// A compaction retires inputs 1/2 and publishes output 3. The recompute
	// rebuilds the snapshot from the projection: inputs absent from the
	// projection disappear, output appears, compact_version advances once.
	v, err := manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(3, "ch-1", 10, 2)))
	require.NoError(t, err)
	requireVersion(t, v, 1, 2)

	// Replaying the same final replacement is idempotent.
	before := catalog.saveCall
	v, err = manager.RecomputeNow(ctx, 1, projectSegments(segmentWithManifestVersion(3, "ch-1", 10, 2)))
	require.NoError(t, err)
	requireVersion(t, v, 1, 2)
	require.Equal(t, before, catalog.saveCall)

	// A projection that is a strict subset (truncate) removes the segment.
	v, err = manager.RecomputeNow(ctx, 1, projectSegments())
	require.NoError(t, err)
	requireVersion(t, v, 1, 3)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	requireVersion(t, ref.Version(), 1, 3)
	require.Empty(t, ref.DataView().GetShards()[0].GetPartitions())
}

func TestManagerRejectsConflictingSegments(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-1", 1)}})
	_, _, _, err = manager.PrepareFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-2", 1)}})
	require.Error(t, err)
}

func TestManagerGCRespectsLatestRetainedAndRefs(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-1", 1)}})
	oldRef, err := manager.Get(ctx, 1, version(1, 0))
	require.NoError(t, err)
	require.NotNil(t, oldRef)
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(2, "ch-1", 1)}})
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(3, "ch-1", 1)}})
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
	require.NoError(t, catalog.SaveDataView(ctx, &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  version(2, 0),
		Shards: []*viewpb.DataViewOfShard{{
			Vchannel: "ch-1",
			Partitions: []*viewpb.DataViewOfPartition{{
				PartitionId: 10,
				SegmentIds:  []int64{100, 200},
			}},
		}},
	}))
	manager, err := RecoverManager(ctx, catalog, recoverAllCollections, nil, nil, nil)
	require.NoError(t, err)
	oldRef, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, oldRef)
	requireVersion(t, oldRef.Version(), 2, 0)
	require.Equal(t, []int64{0, 0}, oldRef.DataView().GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions())

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
	v, err := manager.RecomputeNow(ctx, 1, projectSegments(
		segment(2, "ch-2", 20),
		segment(1, "ch-1", 10),
		segment(1, "ch-1", 10),
	))
	require.NoError(t, err)
	requireVersion(t, v, 1, 1)

	ref, err = manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"ch-1", "ch-2"}, []string{
		ref.DataView().GetShards()[0].GetVchannel(),
		ref.DataView().GetShards()[1].GetVchannel(),
	})
	ref.Deref()

	_, _, _, err = manager.PrepareFlush(ctx, FlushDataViewEvent{
		CollectionID: 1,
		Segments:     []LoadableSegment{{SegmentID: 3}},
	})
	require.Error(t, err)
	_, _, _, err = manager.PrepareFlush(ctx, FlushDataViewEvent{
		CollectionID: 1,
		Segments:     []LoadableSegment{segment(1, "ch-2", 10)},
	})
	require.Error(t, err)
	_, _, _, err = manager.PrepareFlush(ctx, FlushDataViewEvent{
		CollectionID: 1,
		Segments:     []LoadableSegment{segmentWithManifestVersion(3, "ch-1", 10, -1)},
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
	// Recompute persists through the catalog; a save failure must not advance
	// the snapshot.
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(segment(1, "ch-1", 10)))
	require.Error(t, err)
	ref, err = manager.Latest(ctx, 1)
	require.NoError(t, err)
	requireVersion(t, ref.Version(), 1, 0)
	ref.Deref()

	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-1", 10)}})
	catalog.dropErr = errors.New("drop failed")
	require.Error(t, manager.GarbageCollect(ctx, 1, 1))
	require.Len(t, catalog.views, 2)
}

func TestManagerConcurrentCommitRefAndGarbageCollect(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1,
		VChannels:    []string{"ch-1"},
	})
	require.NoError(t, err)

	start := make(chan struct{})
	errCh := make(chan error, 10)
	var wg sync.WaitGroup
	run := func(work func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := work(); err != nil {
				errCh <- err
			}
		}()
	}

	for worker := range 4 {
		worker := worker
		run(func() error {
			for iteration := range 25 {
				segmentID := int64(worker*25 + iteration + 1)
				_, commit, abort, err := manager.PrepareFlush(ctx, FlushDataViewEvent{
					CollectionID: 1,
					Segments:     []LoadableSegment{segment(segmentID, "ch-1", 10)},
				})
				if err != nil {
					return err
				}
				if abort == nil {
					return errors.New("PrepareFlush returned a nil abort")
				}
				commit()
			}
			return nil
		})
	}
	for range 4 {
		run(func() error {
			for range 100 {
				ref, err := manager.Latest(ctx, 1)
				if err != nil {
					return err
				}
				if ref == nil || ref.DataView() == nil || ref.Version() == nil {
					return errors.New("Latest returned an incomplete DataView Ref")
				}
				ref.Deref()
				ref.Deref()
			}
			return nil
		})
	}
	for range 2 {
		run(func() error {
			for range 100 {
				if err := manager.GarbageCollect(ctx, 1, 1); err != nil {
					return err
				}
			}
			return nil
		})
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	require.Len(t, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds(), 100)
}

func TestManagerRecoveryValidation(t *testing.T) {
	ctx := context.Background()

	// A catalog that cannot be listed stays a hard failure: it is not a
	// malformed value but an unreachable store.
	_, err := RecoverManager(ctx, &fakeDataViewCatalog{listErr: errors.New("list failed")}, recoverAllCollections, nil, nil, nil)
	require.Error(t, err)

	// Malformed values are skipped with a warning instead of aborting recovery:
	// a single bad key must not brick the Coordinator.
	for _, invalid := range []*viewpb.DataViewOfCollection{
		nil,
		{CollectionId: 1},
		{CollectionId: 0, DataVersion: version(1, 0)},
		{CollectionId: 1, DataVersion: version(0, 1)},
		{
			CollectionId: 1,
			DataVersion:  version(1, 0),
			Shards: []*viewpb.DataViewOfShard{{Partitions: []*viewpb.DataViewOfPartition{{
				SegmentIds:              []int64{1, 2},
				SegmentManifestVersions: []int64{1},
			}}}},
		},
		{
			CollectionId: 1,
			DataVersion:  version(1, 0),
			Shards: []*viewpb.DataViewOfShard{{Partitions: []*viewpb.DataViewOfPartition{{
				SegmentIds:              []int64{1},
				SegmentManifestVersions: []int64{-1},
			}}}},
		},
	} {
		manager, err := RecoverManager(ctx, &fakeDataViewCatalog{views: []*viewpb.DataViewOfCollection{invalid}}, recoverAllCollections, nil, nil, nil)
		require.NoError(t, err)
		ref, err := manager.Latest(ctx, 1)
		require.NoError(t, err)
		require.Nil(t, ref)
	}

	// A conflicting second snapshot under one version is skipped; the first wins.
	conflicting := &fakeDataViewCatalog{views: []*viewpb.DataViewOfCollection{
		{CollectionId: 1, DataVersion: version(2, 1)},
		{CollectionId: 1, DataVersion: version(2, 1), Shards: []*viewpb.DataViewOfShard{{Vchannel: "ch-1"}}},
	}}
	manager, err := RecoverManager(ctx, conflicting, recoverAllCollections, nil, nil, nil)
	require.NoError(t, err)
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	requireVersion(t, ref.Version(), 2, 1)
	ref.Deref()

	duplicate := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: version(2, 1)}
	manager, err = RecoverManager(ctx, &fakeDataViewCatalog{views: []*viewpb.DataViewOfCollection{
		duplicate,
		proto.Clone(duplicate).(*viewpb.DataViewOfCollection),
		{CollectionId: 1, DataVersion: version(1, 9)},
	}}, recoverAllCollections, nil, nil, nil)
	require.NoError(t, err)
	ref, err = manager.Latest(ctx, 1)
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
	}, nil, nil, nil)
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
	}, nil, nil, nil)
	require.Error(t, err)
	require.Empty(t, catalog.dropAllCall)
	require.Len(t, catalog.views, 2)

	_, err = RecoverManager(ctx, catalog, nil, nil, nil, nil)
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
	}, nil, nil, nil)
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

// TestManagerPrepareFlushCommitAbortContract exercises the flush atomic-txn
// contract: commit() loads the prepared snapshot into memory (idempotently),
// abort() releases the lock without touching memory, and an aborted flush
// leaves the latest snapshot unchanged.
func TestManagerPrepareFlushCommitAbortContract(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	requireVersion(t, mustLatest(t, manager, 1), 1, 0)

	// abort() discards the prepared flush: memory and catalog unchanged.
	_, _, abort, err := manager.PrepareFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(1, "ch-1", 10)}})
	require.NoError(t, err)
	require.NotNil(t, abort)
	abort()
	abort() // idempotent
	requireVersion(t, mustLatest(t, manager, 1), 1, 0)
	require.Len(t, catalog.views, 1)

	// commit() twice is idempotent (sync.Once) and does not corrupt state.
	view, commit, abort2, err := manager.PrepareFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(2, "ch-1", 10)}})
	require.NoError(t, err)
	require.NotNil(t, commit)
	require.NotNil(t, abort2)
	require.NoError(t, catalog.SaveDataView(ctx, view))
	commit()
	commit()
	requireVersion(t, view.GetDataVersion(), 2, 0)
	requireVersion(t, mustLatest(t, manager, 1), 2, 0)

	// An idempotent replay of the already-committed flush returns the same
	// version and stays a no-op on memory.
	view2, commit2, _, err := manager.PrepareFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(2, "ch-1", 10)}})
	require.NoError(t, err)
	require.NoError(t, catalog.SaveDataView(ctx, view2))
	commit2()
	requireVersion(t, mustLatest(t, manager, 1), 2, 0)
}

// mustLatest returns the latest DataView version of a Collection, failing the
// test if no snapshot exists.
func mustLatest(t *testing.T, manager Manager, collectionID int64) *viewpb.DataVersion {
	ref, err := manager.Latest(context.Background(), collectionID)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	return ref.Version()
}

// TestManagerAsyncRecomputeReconciles verifies the async Recompute request
// path: with the projection wired at construction, a non-blocking Recompute
// request converges the snapshot through the manager-internal worker.
func TestManagerAsyncRecomputeReconciles(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(&fakeDataViewCatalog{}, projectSegments(segment(10, "ch-1", 100)))
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	requireVersion(t, mustLatest(t, manager, 1), 1, 0)

	// A burst of requests for the same Collection collapses into one pending
	// entry; the worker drains it against the freshest projection.
	require.NoError(t, manager.Recompute(ctx, 1))
	require.NoError(t, manager.Recompute(ctx, 1))
	require.NoError(t, manager.Recompute(ctx, 1))

	require.Eventually(t, func() bool {
		ref, err := manager.Latest(ctx, 1)
		if err != nil || ref == nil {
			return false
		}
		defer ref.Deref()
		partitions := ref.DataView().GetShards()[0].GetPartitions()
		if len(partitions) == 0 || partitions[0] == nil {
			return false
		}
		ids := partitions[0].GetSegmentIds()
		return ref.Version().GetCompactVersion() == 1 && len(ids) == 1 && ids[0] == 10
	}, 5*time.Second, 10*time.Millisecond, "async recompute did not converge")
}

// TestManagerAsyncRecomputeRequiresProjector verifies that an async Recompute
// request fails when no projection was injected.
func TestManagerAsyncRecomputeRequiresProjector(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	require.Error(t, manager.Recompute(ctx, 1))
}

// TestManagerAsyncRecomputeRetriesFailedProjection verifies the worker
// re-enqueues a Collection whose reconciliation failed, converging after a
// transient projection error.
func TestManagerAsyncRecomputeRetriesFailedProjection(t *testing.T) {
	ctx := context.Background()
	calls := 0
	manager := NewManager(&fakeDataViewCatalog{}, func(_ context.Context, _ int64) ([]LoadableSegment, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("transient projection failure")
		}
		return []LoadableSegment{segment(10, "ch-1", 100)}, nil
	})
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)

	require.NoError(t, manager.Recompute(ctx, 1))
	require.Eventually(t, func() bool {
		ref, err := manager.Latest(ctx, 1)
		if err != nil || ref == nil {
			return false
		}
		defer ref.Deref()
		return ref.Version().GetCompactVersion() == 1
	}, 10*time.Second, 50*time.Millisecond, "worker did not retry and converge after projection failure")
	require.GreaterOrEqual(t, calls, 2)
}

// TestManagerAsyncRecomputeWorkerStopsOnCancel verifies the worker exits when
// the context it was constructed with is cancelled (RecoverManager bounds the
// worker by the coordinator ctx).
func TestManagerAsyncRecomputeWorkerStopsOnCancel(t *testing.T) {
	ctx := context.Background()
	workerCtx, cancel := context.WithCancel(ctx)
	manager, err := RecoverManager(workerCtx, &fakeDataViewCatalog{}, recoverAllCollections, projectSegments(), nil, nil)
	require.NoError(t, err)
	cancel()
	// No deadlock or panic; the worker goroutine exits on cancellation. The
	// queue is still usable for RecomputeNow afterwards.
	_, err = manager.RecomputeNow(ctx, 1, projectSegments())
	require.NoError(t, err)
}

func TestManagerRecoveryBootstrapsVchannelSkeleton(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{}
	// Collection 1 has no persisted snapshot; it declares two vchannels but
	// only ch-1 has a loadable segment. Recovery must seed the first snapshot
	// through the declared skeleton so the empty channel is present.
	manager, err := RecoverManager(ctx, catalog, recoverAllCollections, projectSegments(
		segment(100, "ch-1", 10),
	), []int64{1}, map[int64][]string{1: {"ch-1", "ch-2"}})
	require.NoError(t, err)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	view := ref.DataView()
	require.Len(t, view.GetShards(), 2)
	byChannel := make(map[string]*viewpb.DataViewOfShard)
	for _, shard := range view.GetShards() {
		byChannel[shard.GetVchannel()] = shard
	}
	require.Contains(t, byChannel, "ch-1")
	require.Contains(t, byChannel, "ch-2")
	// The loadable segment landed in its channel; the empty channel has no
	// partition yet but is present in the snapshot.
	found := false
	for _, partition := range byChannel["ch-1"].GetPartitions() {
		if partition.GetPartitionId() == 10 {
			require.Equal(t, []int64{100}, partition.GetSegmentIds())
			found = true
		}
	}
	require.True(t, found)
	require.Empty(t, byChannel["ch-2"].GetPartitions())
	ref.Deref()

	// Collection 2 is entirely empty: the skeleton still produces a snapshot
	// with its declared vchannel.
	manager2, err := RecoverManager(ctx, catalog, recoverAllCollections, projectSegments(),
		[]int64{2}, map[int64][]string{2: {"ch-3"}})
	require.NoError(t, err)
	ref2, err := manager2.Latest(ctx, 2)
	require.NoError(t, err)
	require.NotNil(t, ref2)
	require.Len(t, ref2.DataView().GetShards(), 1)
	require.Equal(t, "ch-3", ref2.DataView().GetShards()[0].GetVchannel())
	ref2.Deref()
}

func TestManagerDropTombstoneMakesLateMutationsNoop(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	_, err = manager.OnDropCollection(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, catalog.views)

	// A late recompute must not resurrect the state or persist an orphan key.
	view, err := manager.RecomputeNow(ctx, 1, projectSegments(segment(100, "ch-1", 10)))
	require.NoError(t, err)
	require.Nil(t, view)
	require.Empty(t, catalog.views)

	// A late flush publish is a no-op: nil snapshot, nothing persisted.
	flushView, commit, abort, err := manager.PrepareFlush(ctx, FlushDataViewEvent{
		CollectionID: 1,
		Segments:     []LoadableSegment{segment(101, "ch-1", 10)},
	})
	require.NoError(t, err)
	require.Nil(t, flushView)
	commit()
	abort()
	require.Empty(t, catalog.views)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, ref)
}

// TestManagerDropCollectionConcurrentWithMutations runs OnDropCollection
// concurrently with in-flight PrepareFlush/RecomputeNow mutations on the same
// Collection. It guards the lock order: lockStateForMutation takes state.mu
// and then only m.mu.RLock (re-validation), while OnDropCollection must never
// hold m.mu while acquiring state.mu - the inversion would deadlock the whole
// coordinator. Run with -race.
func TestManagerDropCollectionConcurrentWithMutations(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				view, commit, abort, err := manager.PrepareFlush(ctx, FlushDataViewEvent{
					CollectionID: 1,
					Segments:     []LoadableSegment{segment(100, "ch-1", 10)},
				})
				if err != nil {
					continue
				}
				if view == nil {
					abort()
					continue
				}
				commit()
				_, _ = manager.RecomputeNow(ctx, 1, projectSegments(segment(100, "ch-1", 10)))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Recreate so the drop always has a registered state to remove.
			_, _ = manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
			_, _ = manager.OnDropCollection(ctx, 1)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	// The drop must win eventually: no state and no persisted views remain.
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, ref)
	require.Empty(t, catalog.views)
}
