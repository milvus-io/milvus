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
)

func TestAdoptSplitMovesAllSegmentsAndRemovesSourceInOneVersion(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"s"}, TransformStarts: map[string]uint64{"s": 50}})
	require.NoError(t, err)
	segment := segmentWithManifestVersion(10, "s", 100, 3)
	segment.TransformStartAfterTimetick = 50
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment}})
	_, err = manager.DeclareShards(ctx, declareTargets(100))
	require.NoError(t, err)
	event := AdoptSplitDataViewEvent{CollectionID: 1, Source: "s", Moves: []SplitAdoptionMove{{SegmentID: 10, VChannel: "t-1", TransformStartAfterTimetick: 100}}}
	next, _, abort, err := manager.PrepareAdoptSplit(ctx, event)
	require.NoError(t, err)
	require.Nil(t, shardOf(next, "s"))
	abort()
	// Abort leaves the old generation intact.
	ref, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	before := ref.DataView().GetDataVersion().GetCompactVersion()
	beforeEpoch := ref.DataView().GetShardSetEpoch()
	require.NotNil(t, shardOf(ref.DataView(), "s"))
	ref.Deref()
	next, commit, _, err := manager.PrepareAdoptSplit(ctx, event)
	require.NoError(t, err)
	require.Equal(t, before+1, next.GetDataVersion().GetCompactVersion())
	require.Equal(t, beforeEpoch+1, next.GetShardSetEpoch())
	commit()
	ref, err = manager.Latest(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, shardOf(ref.DataView(), "s"))
	require.Equal(t, []int64{10}, shardOf(ref.DataView(), "t-1").GetPartitions()[0].GetSegmentIds())
	ref.Deref()
	replay, commit, _, err := manager.PrepareAdoptSplit(ctx, event)
	require.NoError(t, err)
	require.Equal(t, next.GetDataVersion(), replay.GetDataVersion())
	require.Equal(t, next.GetShardSetEpoch(), replay.GetShardSetEpoch(), "replay must keep the published membership identity")
	commit()
}

func TestAdoptSplitRefusesOmittedOrMisplacedSegments(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"s"}, TransformStarts: map[string]uint64{"s": 50}})
	require.NoError(t, err)
	segment := segmentWithManifestVersion(10, "s", 100, 3)
	segment.TransformStartAfterTimetick = 50
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment}})
	_, err = manager.DeclareShards(ctx, declareTargets(100))
	require.NoError(t, err)
	for _, event := range []AdoptSplitDataViewEvent{{CollectionID: 1, Source: "s"}, {CollectionID: 1, Source: "s", Moves: []SplitAdoptionMove{{SegmentID: 10, VChannel: "missing", TransformStartAfterTimetick: 100}}}, {CollectionID: 1, Source: "s", Moves: []SplitAdoptionMove{{SegmentID: 99, VChannel: "t-1", TransformStartAfterTimetick: 100}}}} {
		_, _, _, err = manager.PrepareAdoptSplit(ctx, event)
		require.Error(t, err)
	}
}

func TestAdoptionCommitMergesTargetFlushDuringInvisiblePreparation(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"s"}, TransformStarts: map[string]uint64{"s": 50}})
	require.NoError(t, err)
	source := segmentWithManifestVersion(10, "s", 100, 3)
	source.TransformStartAfterTimetick = 50
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{source}})
	_, err = manager.DeclareShards(ctx, declareTargets(100))
	require.NoError(t, err)
	// Persisting the invisible moves does not reserve the collection DataView
	// lock. A concurrent target publication must survive the final merge.
	event := AdoptSplitDataViewEvent{CollectionID: 1, Source: "s", Moves: []SplitAdoptionMove{{SegmentID: 10, VChannel: "t-1", TransformStartAfterTimetick: 100}}}
	target := segmentWithManifestVersion(11, "t-1", 100, 9)
	target.TransformStartAfterTimetick = 100
	flushAndCommit(t, manager, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{target}})
	next, commit, abort, err := manager.PrepareAdoptSplit(ctx, event)
	require.NoError(t, err)
	defer abort()
	partition := shardOf(next, "t-1").GetPartitions()[0]
	require.ElementsMatch(t, []int64{10, 11}, partition.GetSegmentIds())
	for i, id := range partition.GetSegmentIds() {
		if id == 11 {
			require.EqualValues(t, 9, partition.GetSegmentManifestVersions()[i])
		}
	}
	require.Nil(t, shardOf(next, "s"))
	commit()
	replay, commit, abort, err := manager.PrepareAdoptSplit(ctx, event)
	require.NoError(t, err)
	defer abort()
	require.Equal(t, next.GetShardSetEpoch(), replay.GetShardSetEpoch())
	commit()
}
