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
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	kvstreamingnode "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestShardSplitAdoptionCommitFailureAndReplay(t *testing.T) {
	previousWAL := streaming.WAL()
	streaming.SetupNoopWALForTest()
	t.Cleanup(func() { streaming.SetWALForTest(previousWAL) })
	ctx := context.Background()
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	store, _ := newSplitTestStore(t)
	record := splitTestRecord(7)
	_, err = store.Record(ctx, record)
	require.NoError(t, err)
	require.NoError(t, store.MarkSourceAcked(ctx, 7, 100, splitTestSource, nil))
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskRewriteDone))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: splitTestSource, SplitTarget: splitTestTarget1, Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, NumOfRows: 1, TransformStartAfterTimetick: 50})))
	catalog := mt.Catalog.(dataview.Catalog)
	dv, err := dataview.RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	require.NoError(t, err)
	_, err = dv.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 100, VChannels: []string{splitTestSource}, TransformStarts: map[string]uint64{splitTestSource: 50}})
	require.NoError(t, err)
	next, commit, _, err := dv.PrepareFlush(ctx, dataview.FlushDataViewEvent{CollectionID: 100, Segments: []dataview.LoadableSegment{{SegmentID: 10, VChannel: splitTestSource, PartitionID: 1, TransformStartAfterTimetick: 50, RowNum: 1}}})
	require.NoError(t, err)
	require.NoError(t, catalog.SaveDataView(ctx, next))
	commit()
	_, err = dv.DeclareShards(ctx, dataview.DeclareShardsDataViewEvent{CollectionID: 100, VChannels: []string{splitTestTarget1, splitTestTarget2}, TransformStarts: map[string]uint64{splitTestTarget1: 100, splitTestTarget2: 100}})
	require.NoError(t, err)
	server := &Server{meta: mt, splitStore: store, dataViewManager: dv}
	failure := merr.WrapErrServiceUnavailableMsg("injected adoption commit failure")
	patch := mockey.Mock(mockey.GetMethod(mt.Catalog, "CommitSplitAdoption")).Return(failure).Build()
	defer patch.UnPatch()
	err = server.AdoptShardSplit(ctx, 7)
	require.ErrorIs(t, err, failure)
	patch.UnPatch()
	require.Equal(t, splitTestSource, mt.GetSegment(ctx, 10).GetInsertChannel())
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRewriteDone, store.Get(7).GetState())
	require.NoError(t, server.AdoptShardSplit(ctx, 7))
	segment := mt.GetSegment(ctx, 10)
	require.Equal(t, splitTestTarget1, segment.GetInsertChannel())
	require.Empty(t, segment.GetSplitTarget())
	require.EqualValues(t, 100, segment.GetTransformStartAfterTimetick())
	require.True(t, store.IsL0CompactionSuspended(splitTestTarget1))
	require.NotEmpty(t, store.TransformPins(100))
	require.NoError(t, server.AdoptShardSplit(ctx, 7), "already adopted is idempotent")
	streaming.SetWALForTest(&pinFlushWAL{appendFn: func(context.Context, message.MutableMessage) (*types.AppendResult, error) {
		return &types.AppendResult{TimeTick: 150}, nil
	}})
	require.NoError(t, server.ResumeShardSplitL0(ctx, 7))
	require.False(t, store.IsL0CompactionSuspended(splitTestTarget1))
	done, err := server.ReleaseShardSplitPins(ctx, 7)
	require.NoError(t, err)
	require.False(t, done, "history cursor has not advanced beyond pin")
	mt.StreamingNodeCatalog = kvstreamingnode.NewCataLog(nil)
	proof := mockey.Mock(mockey.GetMethod(mt.StreamingNodeCatalog, "GetVChannelBase")).Return(&streamingpb.VChannelMeta{TransformMaterializedTimeTick: 150}, nil).Build()
	defer proof.UnPatch()
	done, err = server.ReleaseShardSplitPins(ctx, 7)
	require.NoError(t, err)
	require.True(t, done, "a no-delete target can release its pin using a materialized prefix")
	require.EqualValues(t, 150, mt.GetSegment(ctx, 10).GetTransformStartAfterTimetick())
	require.Empty(t, store.TransformPins(100))
}

func TestSplitL0ProtectedBeforeCallbackAndUntilG2(t *testing.T) {
	ctx := context.Background()
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: 100, VChannelNames: []string{splitTestSource}})
	store, _ := newSplitTestStore(t)
	require.True(t, splitL0CompactionSuspended(mt, store, "p_999v0"), "collection callback may also be lagging")
	require.True(t, splitL0CompactionSuspended(mt, store, splitTestTarget1), "first L0 flush precedes callback on a secondary")
	require.False(t, splitL0CompactionSuspended(mt, store, splitTestSource))
	_, err = store.Record(ctx, splitTestRecord(7))
	require.NoError(t, err)
	require.True(t, splitL0CompactionSuspended(mt, store, splitTestTarget1))
	require.NoError(t, store.MarkSourceAcked(ctx, 7, 100, splitTestSource, nil))
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
	require.True(t, splitL0CompactionSuspended(mt, store, splitTestTarget1))
	require.NoError(t, store.ResumeL0(ctx, 7))
	require.False(t, splitL0CompactionSuspended(mt, store, splitTestTarget1))
	require.False(t, splitL0CompactionSuspended(mt, store, splitTestSource), "source never stalls its own finite L0 work")
}

func TestSplitStagedCapacityAcceptsLargeSourceBeforeMessage(t *testing.T) {
	ctx := context.Background()
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: splitTestSource, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, Stats: &datapb.Statistics{InsertBinlogSize: 1 << 40}})))
	server := &Server{meta: mt}
	require.NoError(t, server.checkSplitRewriteCapacity(ctx, 100, splitTestSource))
}
