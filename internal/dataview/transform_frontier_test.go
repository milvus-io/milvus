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

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func transformTestView(version int64, starts ...uint64) *viewpb.DataViewOfCollection {
	shard := &viewpb.DataViewOfShard{Vchannel: "v1"}
	for i, start := range starts {
		shard.Partitions = append(shard.Partitions, &viewpb.DataViewOfPartition{
			PartitionId: int64(i + 1), SegmentIds: []int64{int64(i + 10)},
			SegmentManifestVersions: []int64{1}, SegmentTransformStartAfterTimeticks: []uint64{start},
		})
	}
	return &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: version}, Shards: []*viewpb.DataViewOfShard{shard}}
}

func TestTransformFrontierIncludesEveryPartition(t *testing.T) {
	view := transformTestView(1, 90, 70)
	require.NoError(t, advanceTransformFrontiers(view, map[string]uint64{"v1": 100}))
	require.Equal(t, uint64(70), view.Shards[0].TransformStartAfterTimetick)

	// Another partition's unflushed data constrains the entire shard buffer.
	view = transformTestView(1, 90, 70)
	require.NoError(t, advanceTransformFrontiers(view, map[string]uint64{"v1": 50}))
	require.Equal(t, uint64(50), view.Shards[0].TransformStartAfterTimetick)

	// Publishing that partition transfers its constraint from G to S.
	view = transformTestView(2, 90, 70, 50)
	require.NoError(t, advanceTransformFrontiers(view, map[string]uint64{"v1": 100}))
	require.Equal(t, uint64(50), view.Shards[0].TransformStartAfterTimetick)
}

func TestTransformFrontierDoesNotFabricateCoverage(t *testing.T) {
	view := transformTestView(1, 0)
	require.ErrorIs(t, advanceTransformFrontiers(view, map[string]uint64{"v1": 100}), merr.ErrServiceNotReady)
	view = transformTestView(1, 50)
	view.Shards[0].TransformStartAfterTimetick = 60
	require.ErrorIs(t, advanceTransformFrontiers(view, map[string]uint64{"v1": 100}), merr.ErrDataIntegrity)
	require.Equal(t, uint64(60), view.Shards[0].TransformStartAfterTimetick)

	view = transformTestView(1, 100)
	view.Shards[0].TransformStartAfterTimetick = 40
	require.NoError(t, advanceTransformFrontiers(view, map[string]uint64{}))
	require.Equal(t, uint64(40), view.Shards[0].TransformStartAfterTimetick)
}

func TestTransformFrontierRecoveryIsDynamicAndVersionOrdered(t *testing.T) {
	bounds := map[string]uint64{"v1": 200}
	manager := newManager(context.Background(), nil, nil, WithFrontierProjector(func(context.Context, int64) (map[string]uint64, error) { return bounds, nil }))
	state := newCollectionState(1)
	// Old empty and compacted Views retain their own base coverage. There is
	// deliberately no persisted F, and no lookup of current Segment revisions.
	views := []*viewpb.DataViewOfCollection{transformTestView(1), transformTestView(2, 50), transformTestView(3, 80)}
	original := proto.Clone(views[1])
	for _, view := range views {
		entry := newVersionEntry(view)
		state.versions[protoVersionToStruct(view.DataVersion)] = entry
		state.latest = entry
	}
	require.NoError(t, manager.restoreTransformFrontiers(context.Background(), state))
	for i, expected := range []uint64{50, 50, 80} {
		entry := state.versions[protoVersionToStruct(views[i].DataVersion)]
		require.Equal(t, expected, entry.view.Shards[0].TransformStartAfterTimetick)
	}
	require.True(t, proto.Equal(original, views[1]), "reconstruction must not mutate catalog snapshots")
	require.Equal(t, uint64(200), bounds["v1"], "projection input remains owned by the producer")
}

func TestTransformCursorStaysWithResolvedManifest(t *testing.T) {
	view := transformTestView(1, 50)
	require.NoError(t, rebuildSegments(view, []LoadableSegment{{SegmentID: 10, VChannel: "v1", PartitionID: 1, ManifestVersion: 0, TransformStartAfterTimetick: 90}}))
	partition := view.Shards[0].Partitions[0]
	require.Equal(t, []int64{1}, partition.SegmentManifestVersions)
	require.Equal(t, []uint64{50}, partition.SegmentTransformStartAfterTimeticks)
}

func TestTransformFrontierRetriesRecoveryWithoutMutatingPublishedViews(t *testing.T) {
	ctx := context.Background()
	ready := false
	manager := newManager(ctx, nil, nil, WithFrontierProjector(func(context.Context, int64) (map[string]uint64, error) {
		if !ready {
			return nil, merr.WrapErrServiceNotReadyMsg("checkpoint is recovering")
		}
		return map[string]uint64{"v1": 200}, nil
	}))
	state := newCollectionState(1)
	old := newVersionEntry(transformTestView(1, 50))
	latest := newVersionEntry(transformTestView(2, 80))
	state.versions[protoVersionToStruct(old.view.DataVersion)] = old
	state.versions[protoVersionToStruct(latest.view.DataVersion)] = latest
	state.latest = latest
	manager.states[1] = state
	_, err := manager.Latest(ctx, 1)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	ready = true
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	defer ref.Deref()
	published := ref.DataView()
	require.Equal(t, uint64(80), published.Shards[0].TransformStartAfterTimetick)
	older, err := manager.Get(ctx, 1, old.view.DataVersion)
	require.NoError(t, err)
	defer older.Deref()
	require.Equal(t, uint64(50), older.DataView().Shards[0].TransformStartAfterTimetick)
	require.True(t, proto.Equal(published, ref.DataView()))
}
