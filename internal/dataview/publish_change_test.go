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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// publishAndCommit runs the batch publish atomic-txn flow: PublishChange under
// the Collection lock, persists the prepared snapshot through the catalog (as
// the txn does alongside SegmentMeta + the group record), then commit() to load
// it into memory.
func publishAndCommit(t *testing.T, manager Manager, event ChangeGroupDataViewEvent) *viewpb.DataVersion {
	view, commit, abort, err := manager.PublishChange(context.Background(), event)
	require.NoError(t, err)
	require.NotNil(t, commit)
	require.NotNil(t, abort)
	if mgr, ok := manager.(*dataViewManager); ok {
		require.NoError(t, mgr.catalog.SaveDataView(context.Background(), view))
	}
	commit()
	return view.GetDataVersion()
}

func TestManagerPublishChangeAddsMembersRemovesSuperseded(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	v := flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segmentWithManifestVersion(10, "ch-1", 100, 3)}})
	requireVersion(t, v, 2, 0)

	// A batch publish adds its new member and retires the superseded parent in
	// one snapshot, advancing only compact_version.
	v = publishAndCommit(t, manager, ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segmentWithManifestVersion(20, "ch-1", 100, 5)},
		SupersededSegmentIDs: []int64{10},
	})
	requireVersion(t, v, 2, 1)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	latest := ref.DataView()
	requireVersion(t, latest.GetDataVersion(), 2, 1)
	require.Equal(t, []int64{20}, latest.GetShards()[0].GetPartitions()[0].GetSegmentIds())
	require.Equal(t, []int64{5}, latest.GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions())
	// The superseded parent is gone from the latest snapshot.
	require.Len(t, catalog.views, 3)
}

func TestManagerPublishChangeStreamingVersionUntouched(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})
	// Streaming flush advances streaming_version; a later batch publish must
	// not race it: streaming stays put, compact advances.
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(30, "ch-1", 100)}})
	v := publishAndCommit(t, manager, ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segment(20, "ch-1", 100)},
		SupersededSegmentIDs: []int64{30},
	})
	requireVersion(t, v, 3, 1)
}

func TestManagerPublishChangeIdempotentReplay(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})

	event := ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segment(20, "ch-1", 100)},
		SupersededSegmentIDs: []int64{10},
	}
	v := publishAndCommit(t, manager, event)
	requireVersion(t, v, 2, 1)
	beforeViews := len(catalog.views)

	// An idempotent replay (e.g. the READY-replay probe after recovery) sees an
	// unchanged membership and returns the current snapshot without a new
	// version.
	v = publishAndCommit(t, manager, event)
	requireVersion(t, v, 2, 1)
	require.Equal(t, beforeViews, len(catalog.views))

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	require.Equal(t, []int64{20}, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestManagerPublishChangeCommitLoadsMemory(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})

	view, commit, abort, err := manager.PublishChange(ctx, ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segmentWithManifestVersion(20, "ch-1", 100, 7)},
		SupersededSegmentIDs: []int64{10},
	})
	require.NoError(t, err)
	require.NotNil(t, view)
	commit()

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	requireVersion(t, ref.Version(), 2, 1)
	require.Equal(t, []int64{20}, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds())
	require.Equal(t, []int64{7}, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions())
	require.NotNil(t, abort)
}

func TestManagerPublishChangeAbortDoesNotPublish(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})

	view, commit, abort, err := manager.PublishChange(ctx, ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segment(20, "ch-1", 100)},
		SupersededSegmentIDs: []int64{10},
	})
	require.NoError(t, err)
	require.NotNil(t, view)
	abort()
	// The abort releases the lock without loading anything; Latest is untouched.
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	requireVersion(t, ref.Version(), 2, 0)
	require.Equal(t, []int64{10}, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds())
	require.NotNil(t, commit)
	before := catalog.saveCall

	// A retry after the abort publishes a fresh version.
	_ = publishAndCommit(t, manager, ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segment(20, "ch-1", 100)},
		SupersededSegmentIDs: []int64{10},
	})
	require.Equal(t, before+1, catalog.saveCall)
}

func TestManagerPublishChangeDroppedCollectionNoop(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	_, err = manager.OnDropCollection(ctx, 1)
	require.NoError(t, err)

	view, commit, abort, err := manager.PublishChange(ctx, ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segment(20, "ch-1", 100)},
		SupersededSegmentIDs: []int64{10},
	})
	require.NoError(t, err)
	require.Nil(t, view)
	require.NotNil(t, commit)
	require.NotNil(t, abort)
	commit()
	abort()
}

func TestManagerPublishChangeRemovesSupersededAlignedManifestVersions(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	_, err = manager.RecomputeNow(ctx, 1, projectSegments(
		segmentWithManifestVersion(10, "ch-1", 100, 3),
		segmentWithManifestVersion(11, "ch-1", 100, 4),
		segmentWithManifestVersion(12, "ch-1", 100, 5),
	))
	require.NoError(t, err)

	v := publishAndCommit(t, manager, ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segmentWithManifestVersion(20, "ch-1", 100, 6)},
		SupersededSegmentIDs: []int64{11},
	})
	requireVersion(t, v, 1, 2)
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	partition := ref.DataView().GetShards()[0].GetPartitions()[0]
	require.Equal(t, []int64{10, 12, 20}, partition.GetSegmentIds())
	require.Equal(t, []int64{3, 5, 6}, partition.GetSegmentManifestVersions())
}

func TestManagerPublishChangeRejectsConflictingLocation(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})

	_, commit, abort, err := manager.PublishChange(ctx, ChangeGroupDataViewEvent{
		CollectionID: 1,
		// Same Segment ID under a conflicting channel/location.
		NewSegments: []LoadableSegment{segment(10, "ch-2", 100)},
	})
	require.Error(t, err)
	require.Nil(t, commit)
	require.Nil(t, abort)
}

func TestManagerPublishChangeRejectsInvalidRemovalID(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)

	_, commit, abort, err := manager.PublishChange(ctx, ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segment(20, "ch-1", 100)},
		SupersededSegmentIDs: []int64{-1},
	})
	require.Error(t, err)
	require.Nil(t, commit)
	require.Nil(t, abort)
}

func TestManagerPublishChangePublishErrorKeepsState(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment(10, "ch-1", 100)}})

	before := catalog.saveCall
	catalog.saveErr = errors.New("save failed")
	// The catalog failure surfaces from SaveDataView (simulated here as the
	// caller's composite txn failed); abort() releases the lock, Latest keeps
	// the old snapshot, and the version is NOT burned.
	view, commit, abort, err := manager.PublishChange(ctx, ChangeGroupDataViewEvent{
		CollectionID:         1,
		NewSegments:          []LoadableSegment{segment(20, "ch-1", 100)},
		SupersededSegmentIDs: []int64{10},
	})
	require.NoError(t, err)
	require.NotNil(t, view)
	abort()

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	requireVersion(t, ref.Version(), 2, 0)
	require.Equal(t, before, catalog.saveCall)
	require.NotNil(t, commit)
}
