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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/dataview"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestExternalRefreshTransformOrigin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const collectionID int64 = 100
	const channel = "by-dev-rootcoord-dml_0_v1"
	const origin uint64 = 123
	var persisted *viewpb.DataViewOfCollection
	save := mockey.Mock((*catalogkv.Catalog).SaveDataView).To(func(_ *catalogkv.Catalog, _ context.Context, view *viewpb.DataViewOfCollection) error {
		persisted = proto.Clone(view).(*viewpb.DataViewOfCollection)
		return nil
	}).Build()
	defer save.UnPatch()
	list := mockey.Mock((*catalogkv.Catalog).ListAllDataViews).To(func(_ *catalogkv.Catalog, _ context.Context) ([]*viewpb.DataViewOfCollection, error) {
		if persisted == nil {
			return nil, nil
		}
		return []*viewpb.DataViewOfCollection{proto.Clone(persisted).(*viewpb.DataViewOfCollection)}, nil
	}).Build()
	defer list.UnPatch()
	recoverManager := func() dataview.Manager {
		manager, err := dataview.RecoverManager(ctx, &catalogkv.Catalog{}, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
		require.NoError(t, err)
		return manager
	}
	mt := &meta{collections: newTestCollections(collectionID), segments: NewSegmentsInfo(), catalog: &stubCatalog{}, dataViewManager: recoverManager()}
	_, err := externalRefreshTransformStart(ctx, mt, collectionID, channel)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	_, err = mt.dataViewManager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: collectionID, VChannels: []string{channel}, TransformStarts: map[string]uint64{channel: origin},
	})
	require.NoError(t, err)
	_, err = externalRefreshTransformStart(ctx, mt, collectionID, "another-channel")
	require.ErrorIs(t, err, merr.ErrServiceNotReady)

	first := newTestExternalRefreshSegment(10, collectionID, 3000)
	first.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/1/10", 1)
	first.TransformStartAfterTimetick = 999 // A worker cannot substitute its own cursor.
	require.NoError(t, applyExternalCollectionSegmentUpdateForBaseline(ctx, mt, collectionID, nil, nil, []*datapb.SegmentInfo{first}))
	require.Equal(t, origin, mt.segments.GetSegment(10).GetTransformStartAfterTimetick())
	require.Equal(t, uint64(999), first.GetTransformStartAfterTimetick(), "worker result remains immutable")
	require.Equal(t, origin, mt.catalog.(*stubCatalog).alteredSegments[0].GetTransformStartAfterTimetick())
	version, err := mt.dataViewManager.RecomputeNow(ctx, collectionID, mt.loadableProjection)
	require.NoError(t, err)
	require.Equal(t, int64(1), version.GetCompactVersion())

	assertFrontier := func(ids []int64, versions []int64) {
		ref, err := mt.dataViewManager.Latest(ctx, collectionID)
		require.NoError(t, err)
		require.NotNil(t, ref)
		defer ref.Deref()
		shard := ref.DataView().GetShards()[0]
		require.Equal(t, origin, shard.GetGrowingTransformStartAfterTimetick())
		require.Equal(t, origin, shard.GetCompactedTransformStartAfterTimetick())
		partition := shard.GetPartitions()[0]
		require.Equal(t, ids, partition.GetSegmentIds())
		require.Equal(t, versions, partition.GetSegmentManifestVersions())
		for _, cursor := range partition.GetSegmentTransformStartAfterTimeticks() {
			require.Equal(t, origin, cursor)
		}
	}
	assertFrontier([]int64{10}, []int64{1})

	// Same-ID manifest patch and new segment publication retain the origin.
	patched := proto.Clone(mt.segments.GetSegment(10).SegmentInfo).(*datapb.SegmentInfo)
	patched.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/1/10", 2)
	patched.TransformStartAfterTimetick = 0
	added := newTestExternalRefreshSegment(20, collectionID, 1000)
	added.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/1/20", 1)
	require.NoError(t, applyExternalCollectionSegmentUpdateForBaseline(ctx, mt, collectionID, []int64{10}, nil, []*datapb.SegmentInfo{patched, added}))
	require.Equal(t, origin, mt.segments.GetSegment(10).GetTransformStartAfterTimetick())
	require.Equal(t, origin, mt.segments.GetSegment(20).GetTransformStartAfterTimetick())
	_, err = mt.dataViewManager.RecomputeNow(ctx, collectionID, mt.loadableProjection)
	require.NoError(t, err)
	assertFrontier([]int64{10, 20}, []int64{2, 1})

	// Recover from a later persisted DataVersion, without the initial snapshot.
	mt.dataViewManager = recoverManager()
	assertFrontier([]int64{10, 20}, []int64{2, 1})
	require.NoError(t, applyExternalCollectionSegmentUpdateForBaseline(ctx, mt, collectionID, []int64{10}, nil, []*datapb.SegmentInfo{patched, added}))
	_, err = mt.dataViewManager.RecomputeNow(ctx, collectionID, mt.loadableProjection)
	require.NoError(t, err)
	assertFrontier([]int64{10, 20}, []int64{2, 1})

	// A subsequent refresh can keep one ID and replace the other.
	replacement := newTestExternalRefreshSegment(30, collectionID, 2000)
	replacement.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/1/30", 1)
	require.NoError(t, applyExternalCollectionSegmentUpdateForBaseline(ctx, mt, collectionID, []int64{10, 20}, []int64{10}, []*datapb.SegmentInfo{replacement}))
	_, err = mt.dataViewManager.RecomputeNow(ctx, collectionID, mt.loadableProjection)
	require.NoError(t, err)
	assertFrontier([]int64{10, 30}, []int64{2, 1})
	// A new broadcast supplies a target-shard fence; W remains the creation
	// origin, while A follows the minimum visible segment cursor.
	patched = proto.Clone(mt.segments.GetSegment(10).SegmentInfo).(*datapb.SegmentInfo)
	patched.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/1/10", 3)
	replacement = proto.Clone(mt.segments.GetSegment(30).SegmentInfo).(*datapb.SegmentInfo)
	replacement.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/1/30", 2)
	starts := map[string]uint64{channel: 500}
	require.NoError(t, applyExternalCollectionSegmentUpdateAtFence(ctx, mt, collectionID, []int64{10, 30}, nil, []*datapb.SegmentInfo{patched, replacement}, starts))
	_, err = mt.dataViewManager.RecomputeNow(ctx, collectionID, mt.loadableProjection)
	require.NoError(t, err)
	mt.dataViewManager = recoverManager()
	ref, err := mt.dataViewManager.Latest(ctx, collectionID)
	require.NoError(t, err)
	shard := ref.DataView().GetShards()[0]
	require.Equal(t, origin, shard.GetGrowingTransformStartAfterTimetick())
	require.Equal(t, uint64(500), shard.GetCompactedTransformStartAfterTimetick())
	require.Equal(t, []uint64{500, 500}, shard.GetPartitions()[0].GetSegmentTransformStartAfterTimeticks())
	ref.Deref()
	require.NoError(t, applyExternalCollectionSegmentUpdateAtFence(ctx, mt, collectionID, []int64{10, 30}, nil, []*datapb.SegmentInfo{patched, replacement}, starts))
	require.Equal(t, uint64(500), mt.segments.GetSegment(10).GetTransformStartAfterTimetick())
	require.Error(t, applyExternalCollectionSegmentUpdateAtFence(ctx, mt, collectionID, nil, nil, nil, map[string]uint64{"missing": 500}))
	// Missing creation metadata must stay retriable rather than borrowing a checkpoint.
	_, err = mt.dataViewManager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 200, VChannels: []string{channel}})
	require.NoError(t, err)
	_, err = externalRefreshTransformStart(ctx, mt, 200, channel)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	latestError := mockey.Mock(mockey.GetMethod(mt.dataViewManager, "Latest")).Return(nil, merr.ErrServiceNotReady).Build()
	defer latestError.UnPatch()
	_, err = externalRefreshTransformStart(ctx, mt, collectionID, channel)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
}

func applyExternalCollectionSegmentUpdateForBaseline(
	ctx context.Context,
	mt *meta,
	collectionID int64,
	baselineSegmentIDs []int64,
	keptSegmentIDs []int64,
	updatedSegments []*datapb.SegmentInfo,
	logFields ...mlog.Field,
) error {
	return applyExternalCollectionSegmentUpdateAtFence(ctx, mt, collectionID, baselineSegmentIDs, keptSegmentIDs, updatedSegments, nil, logFields...)
}
