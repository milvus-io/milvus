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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestPublicationNotifiesQueryViews(t *testing.T) {
	oldManifest := packed.MarshalManifestPath("segment", 1)
	newManifest := packed.MarshalManifestPath("segment", 2)
	commit := SegmentManifestCommit{
		SegmentID: 10,
		Mutation:  ManifestMutation{Type: ManifestMutationNoop, ManifestPath: newManifest},
	}
	for _, tc := range []struct {
		name  string
		apply func(context.Context, *meta) error
	}{
		{"single", func(ctx context.Context, m *meta) error { return m.CommitSegmentManifest(ctx, commit) }},
		{"batch", func(ctx context.Context, m *meta) error {
			return m.CommitSegmentManifests(ctx, []SegmentManifestCommit{commit})
		}},
		{"backfill", func(ctx context.Context, m *meta) error {
			return m.UpdateSegmentsInfo(ctx, UpdateManifestVersion(10, 2))
		}},
		{"flush", func(ctx context.Context, m *meta) error {
			_, err := m.UpdateSegmentsInfoAndDataView(ctx, nil, UpdateManifestVersion(10, 2))
			return err
		}},
		{"schema-bump", func(ctx context.Context, m *meta) error {
			_, err := m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
				CollectionID: 1, InputSegments: []int64{10}, Type: datapb.CompactionType_BumpSchemaVersionCompaction,
				Schema: &schemapb.CollectionSchema{Version: 2},
			}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
				SegmentID: 10, NumOfRows: 5, Manifest: newManifest, BaseManifest: oldManifest, StorageVersion: storage.StorageV3,
			}}})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := metacache.NewMetaStore(nil)
			m := &meta{metaStore: store, ctx: context.Background(), catalog: catalogkv.NewCatalog(NewMetaMemoryKV(), "", ""), segments: NewSegmentsInfo(store)}
			segment := NewSegmentInfo(&datapb.SegmentInfo{
				ID: 10, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1, StorageVersion: storage.StorageV3, ManifestPath: oldManifest,
				NumOfRows: 5, SchemaVersion: 1,
			})
			m.segments.SetSegment(10, segment)
			m.queryViewLoadInfoNotifier = noopQueryViewLoadInfoNotifier{}
			notifications := 0
			recomputes := 0
			recompute := mockey.Mock((*meta).recomputeDataView).To(func(got *meta, ctx context.Context, collectionID int64) {
				require.Same(t, m, got)
				require.EqualValues(t, 1, collectionID)
				require.True(t, m.segMu.TryLock(), "recompute must run after publication outside segMu")
				m.segMu.Unlock()
				require.Equal(t, newManifest, m.GetSegment(ctx, 10).GetManifestPath())
				recomputes++
			}).Build()
			defer recompute.UnPatch()
			notifier := mockey.Mock(noopQueryViewLoadInfoNotifier.NotifySegments).To(func(_ noopQueryViewLoadInfoNotifier, collectionID int64, ids ...int64) {
				notifications++
				require.EqualValues(t, 1, collectionID)
				require.Equal(t, []int64{10}, ids)
				require.True(t, m.segMu.TryLock(), "notification must run outside segMu")
				m.segMu.Unlock()
				require.True(t, m.getSegmentManifestLocks().TryLock(10), "notification must run outside manifest lock")
				m.getSegmentManifestLocks().Unlock(10)
				require.Equal(t, newManifest, m.GetSegment(context.Background(), 10).GetManifestPath())
			}).Build()
			defer notifier.UnPatch()

			// Failed persistence must leave both the in-memory pointer and subscribers
			// unchanged. A canceled context also stops the atomic-flush retry loop.
			update := mockey.Mock((*catalogkv.Catalog).Update).Return(merr.ErrServiceUnavailable).Build()
			defer update.UnPatch()
			alter := mockey.Mock((*catalogkv.Catalog).AlterSegments).Return(merr.ErrServiceUnavailable).Build()
			defer alter.UnPatch()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.Error(t, tc.apply(ctx, m))
			require.Zero(t, notifications)
			require.Zero(t, recomputes)
			require.Equal(t, oldManifest, m.GetSegment(context.Background(), 10).GetManifestPath())
			update.UnPatch()
			alter.UnPatch()

			require.NoError(t, tc.apply(context.Background(), m))
			require.Equal(t, 1, notifications)
			require.Positive(t, recomputes, "committed Manifest must also update DataView requirements")
			// Retrying a committed result may notify again, but cannot lose the update
			// or move the Manifest backwards.
			require.NoError(t, tc.apply(context.Background(), m))
			require.Equal(t, newManifest, m.GetSegment(context.Background(), 10).GetManifestPath())
		})
	}
}

func TestManifestPublicationGroupsNotifications(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	m := &meta{metaStore: store, ctx: context.Background(), catalog: catalogkv.NewCatalog(NewMetaMemoryKV(), "", ""), segments: NewSegmentsInfo(store)}
	for _, id := range []int64{10, 11, 20} {
		m.segments.SetSegment(id, NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: id / 10, State: commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath("segment", 1),
		}))
	}
	m.queryViewLoadInfoNotifier = noopQueryViewLoadInfoNotifier{}
	observed := make(map[int64][]int64)
	notifier := mockey.Mock(noopQueryViewLoadInfoNotifier.NotifySegments).To(func(_ noopQueryViewLoadInfoNotifier, collectionID int64, ids ...int64) {
		observed[collectionID] = append(observed[collectionID], ids...)
	}).Build()
	defer notifier.UnPatch()
	require.NoError(t, m.UpdateSegmentsInfo(context.Background(), UpdateManifestVersion(10, 2), UpdateManifestVersion(11, 2), UpdateManifestVersion(20, 2)))
	require.Len(t, observed, 2)
	require.ElementsMatch(t, []int64{10, 11}, observed[1])
	require.Equal(t, []int64{20}, observed[2])
	clear(observed)
	require.NoError(t, m.UpdateSegmentsInfo(context.Background(), UpdateManifestVersion(10, 2), UpdateManifestVersion(20, 1)))
	require.Empty(t, observed, "unchanged or stale Manifest must not refresh subscribers")
}
