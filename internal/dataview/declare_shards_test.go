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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func shardOf(view *viewpb.DataViewOfCollection, vchannel string) *viewpb.DataViewOfShard {
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard
		}
	}
	return nil
}

func declareTargets(start uint64) DeclareShardsDataViewEvent {
	return DeclareShardsDataViewEvent{
		CollectionID:    1,
		VChannels:       []string{"t-1", "t-2"},
		TransformStarts: map[string]uint64{"t-1": start, "t-2": start},
	}
}

func TestDeclareShardsAddsEmptyShardsAndKeepsTheRest(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1, VChannels: []string{"s"}, TransformStarts: map[string]uint64{"s": 50},
	})
	require.NoError(t, err)
	flushed := segmentWithManifestVersion(10, "s", 100, 3)
	flushed.TransformStartAfterTimetick = 60
	flushed.RowNum = 7
	v := flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{flushed}})
	requireVersion(t, v, 2, 0)
	saves := catalog.saveCall

	v, err = manager.DeclareShards(ctx, declareTargets(100))
	require.NoError(t, err)
	requireVersion(t, v, 2, 1)
	require.Equal(t, saves+1, catalog.saveCall)

	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	defer ref.Deref()
	view := ref.DataView()
	require.Len(t, view.GetShards(), 3)
	for _, target := range []string{"t-1", "t-2"} {
		shard := shardOf(view, target)
		require.NotNil(t, shard)
		require.Empty(t, shard.GetPartitions())
		require.EqualValues(t, 100, shard.GetGrowingTransformStartAfterTimetick())
		require.EqualValues(t, 100, shard.GetCompactedTransformStartAfterTimetick())
	}
	// The source stays a shard, with its segment and the statistics of it.
	source := shardOf(view, "s")
	require.NotNil(t, source)
	require.Equal(t, []int64{10}, source.GetPartitions()[0].GetSegmentIds())
	stats, ok := ref.Stats(10)
	require.True(t, ok)
	require.EqualValues(t, 7, stats.RowNum)
}

func TestDeclareShardsIsIdempotent(t *testing.T) {
	ctx := context.Background()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1, VChannels: []string{"s"}, TransformStarts: map[string]uint64{"s": 50},
	})
	require.NoError(t, err)
	first, err := manager.DeclareShards(ctx, declareTargets(100))
	require.NoError(t, err)
	saves := catalog.saveCall

	// Declared again, even with another frontier: nothing is written and the
	// shards keep the frontier they were declared with.
	second, err := manager.DeclareShards(ctx, declareTargets(200))
	require.NoError(t, err)
	require.Equal(t, 0, compareDataVersion(first, second))
	require.Equal(t, saves, catalog.saveCall)
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	defer ref.Deref()
	require.EqualValues(t, 100, shardOf(ref.DataView(), "t-1").GetGrowingTransformStartAfterTimetick())

	// One shard known, one new: only the new one is added.
	third, err := manager.DeclareShards(ctx, DeclareShardsDataViewEvent{
		CollectionID:    1,
		VChannels:       []string{"t-1", "t-3", "t-3"},
		TransformStarts: map[string]uint64{"t-1": 300, "t-3": 300},
	})
	require.NoError(t, err)
	require.Positive(t, compareDataVersion(third, second))
	latest, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	defer latest.Deref()
	require.Len(t, latest.DataView().GetShards(), 4)
	require.EqualValues(t, 100, shardOf(latest.DataView(), "t-1").GetGrowingTransformStartAfterTimetick())
	require.EqualValues(t, 300, shardOf(latest.DataView(), "t-3").GetGrowingTransformStartAfterTimetick())
}

func TestDeclareShardsRefusals(t *testing.T) {
	ctx := context.Background()

	t.Run("a shard without a frontier", func(t *testing.T) {
		manager, _ := newTestManager()
		_, err := manager.DeclareShards(ctx, DeclareShardsDataViewEvent{CollectionID: 1, VChannels: []string{"t-1"}})
		require.Error(t, err)
		_, err = manager.DeclareShards(ctx, DeclareShardsDataViewEvent{CollectionID: 1, VChannels: []string{""}, TransformStarts: map[string]uint64{"": 1}})
		require.Error(t, err)
	})

	t.Run("a collection without a DataView is not ready", func(t *testing.T) {
		manager, catalog := newTestManager()
		_, err := manager.DeclareShards(ctx, declareTargets(100))
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		require.Zero(t, catalog.saveCall)
	})

	t.Run("a dropped collection is a no-op", func(t *testing.T) {
		manager, catalog := newTestManager()
		_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"s"}, TransformStarts: map[string]uint64{"s": 50}})
		require.NoError(t, err)
		_, err = manager.OnDropCollection(ctx, 1)
		require.NoError(t, err)
		saves := catalog.saveCall
		v, err := manager.DeclareShards(ctx, declareTargets(100))
		require.NoError(t, err)
		require.Nil(t, v)
		require.Equal(t, saves, catalog.saveCall)
	})

	t.Run("a failed catalog write leaves the DataView unchanged", func(t *testing.T) {
		manager, catalog := newTestManager()
		before, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"s"}, TransformStarts: map[string]uint64{"s": 50}})
		require.NoError(t, err)
		catalog.saveErr = errors.New("mock")
		_, err = manager.DeclareShards(ctx, declareTargets(100))
		require.Error(t, err)
		ref, err := manager.Latest(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, 0, compareDataVersion(before, ref.Version()))
		require.Len(t, ref.DataView().GetShards(), 1)
		ref.Deref()

		// The retry succeeds.
		catalog.saveErr = nil
		_, err = manager.DeclareShards(ctx, declareTargets(100))
		require.NoError(t, err)
	})
}

// A flush on a declared shard finds it, and a pending Transform constraint on
// the shard keeps its frontier where it was declared.
func TestDeclareShardsThenFlushKeepsThePinnedFrontier(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{}
	pinned := true
	manager := NewManager(catalog, nil, WithPendingTransformProjector(
		func(context.Context, *viewpb.DataViewOfCollection) (map[string]uint64, error) {
			if !pinned {
				return nil, nil
			}
			return map[string]uint64{"t-1": 100, "t-2": 100}, nil
		})).(*dataViewManager)
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1, VChannels: []string{"s"}, TransformStarts: map[string]uint64{"s": 50},
	})
	require.NoError(t, err)
	_, err = manager.DeclareShards(ctx, declareTargets(100))
	require.NoError(t, err)

	flushed := segmentWithManifestVersion(20, "t-1", 100, 1)
	flushed.TransformStartAfterTimetick = 120
	flushAndCommit(t, manager, FlushDataViewEvent{
		CollectionID:           1,
		Segments:               []LoadableSegment{flushed},
		GrowingTransformStarts: map[string]uint64{"t-1": 150},
	})
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	target := shardOf(ref.DataView(), "t-1")
	ref.Deref()
	require.Equal(t, []int64{20}, target.GetPartitions()[0].GetSegmentIds())
	require.EqualValues(t, 100, target.GetGrowingTransformStartAfterTimetick())
	require.EqualValues(t, 120, target.GetCompactedTransformStartAfterTimetick())
	require.EqualValues(t, 100, transformStart(target))

	// Once the constraint is gone the next flush certificate advances W.
	pinned = false
	next := segmentWithManifestVersion(21, "t-1", 100, 1)
	next.TransformStartAfterTimetick = 160
	flushAndCommit(t, manager, FlushDataViewEvent{
		CollectionID:           1,
		Segments:               []LoadableSegment{next},
		GrowingTransformStarts: map[string]uint64{"t-1": 200},
	})
	ref, err = manager.Latest(ctx, 1)
	require.NoError(t, err)
	target = shardOf(ref.DataView(), "t-1")
	ref.Deref()
	require.EqualValues(t, 200, target.GetGrowingTransformStartAfterTimetick())
	require.EqualValues(t, 120, transformStart(target))
}

// A coordinator that restarts after the declaration recovers the declared
// shards from the persisted DataView, although no segment names them.
func TestDeclareShardsSurvivesRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, catalog := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1, VChannels: []string{"s"}, TransformStarts: map[string]uint64{"s": 50},
	})
	require.NoError(t, err)
	declared, err := manager.DeclareShards(ctx, declareTargets(100))
	require.NoError(t, err)

	recovered, err := RecoverManager(ctx, catalog,
		func(context.Context, int64) (bool, error) { return true, nil },
		func(context.Context, int64, *viewpb.DataViewOfCollection) ([]LoadableSegment, error) { return nil, nil },
		[]int64{1}, map[int64][]string{1: {"t-1", "t-2"}})
	require.NoError(t, err)
	ref, err := recovered.Latest(ctx, 1)
	require.NoError(t, err)
	defer ref.Deref()
	require.Equal(t, 0, compareDataVersion(declared, ref.Version()))
	require.Len(t, ref.DataView().GetShards(), 3)
	require.NotNil(t, shardOf(ref.DataView(), "s"))
	require.EqualValues(t, 100, shardOf(ref.DataView(), "t-2").GetGrowingTransformStartAfterTimetick())

	// The callback is entered again after the restart: still a no-op.
	again, err := recovered.DeclareShards(ctx, declareTargets(100))
	require.NoError(t, err)
	require.Equal(t, 0, compareDataVersion(declared, again))
}
