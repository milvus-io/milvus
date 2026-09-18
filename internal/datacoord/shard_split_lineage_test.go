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
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	lineageCollectionID = splitMgrCollection
	lineageIndexID      = int64(1)
)

// lineageFixture is a datacoord server over real meta with one vector-indexed
// collection and a Redistributing split task src -> {t1, t2}.
type lineageFixture struct {
	svr *Server
	mgr *shardSplitManager
}

func newLineageFixture(t *testing.T) *lineageFixture {
	svr := newTestServer(t)
	t.Cleanup(func() { closeTestServer(t, svr) })
	svr.meta.AddCollection(&collectionInfo{
		ID:     lineageCollectionID,
		Schema: newTestSchema(),
	})
	require.NoError(t, svr.meta.indexMeta.CreateIndex(context.TODO(), &model.Index{
		CollectionID: lineageCollectionID,
		FieldID:      2,
		IndexID:      lineageIndexID,
	}))
	require.NotNil(t, svr.shardSplitManager)
	require.NoError(t, svr.shardSplitManager.store.create(context.TODO(), svr.meta.catalog, newHashTask(nil)))
	return &lineageFixture{svr: svr, mgr: svr.shardSplitManager}
}

type lineageSegment struct {
	id             int64
	channel        string
	state          commonpb.SegmentState
	level          datapb.SegmentLevel
	compactionFrom []int64
	indexed        bool
	importing      bool
	invisible      bool
	startTs        uint64
	// compacted marks a compaction output (CreatedByCompaction), sorted a
	// segment sorted by pk.
	compacted bool
	sorted    bool
}

func (f *lineageFixture) add(t *testing.T, s lineageSegment) {
	level := s.level
	if level == datapb.SegmentLevel_Legacy {
		level = datapb.SegmentLevel_L1
	}
	info := &datapb.SegmentInfo{
		ID:             s.id,
		CollectionID:   lineageCollectionID,
		InsertChannel:  s.channel,
		State:          s.state,
		Level:          level,
		NumOfRows:      2048,
		CompactionFrom: s.compactionFrom,
		IsImporting:    s.importing,
		IsInvisible:    s.invisible,
		IsSorted:       s.sorted,
		StartPosition:  &msgpb.MsgPosition{ChannelName: s.channel, MsgID: []byte{1}, Timestamp: s.startTs},
		DmlPosition:    &msgpb.MsgPosition{ChannelName: s.channel, MsgID: []byte{1}, Timestamp: s.startTs + 1},
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{{EntriesNum: 2048, LogID: s.id}},
		}},
	}
	info.CreatedByCompaction = s.compacted
	require.NoError(t, f.svr.meta.AddSegment(context.TODO(), NewSegmentInfo(info)))
	if s.indexed {
		require.NoError(t, f.svr.meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			CollectionID: lineageCollectionID,
			SegmentID:    s.id,
			BuildID:      s.id,
			IndexID:      lineageIndexID,
		}))
		require.NoError(t, f.svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: s.id,
			State:   commonpb.IndexState_Finished,
		}))
	}
}

// setState moves the split task to state.
func (f *lineageFixture) setState(t *testing.T, state datapb.SplitShardTaskState) {
	_, err := f.mgr.store.modify(context.TODO(), f.mgr.catalog, hashTaskID, func(task *datapb.SplitShardTask) bool {
		task.State = state
		return true
	})
	require.NoError(t, err)
}

func (f *lineageFixture) view(channel string) *datapb.VchannelInfo {
	return f.svr.handler.GetQueryVChanPositions(&channelMeta{Name: channel, CollectionID: lineageCollectionID})
}

// describing makes DescribeCollection list exactly the given vchannels.
func (f *lineageFixture) describing(t *testing.T, channels ...string) {
	mixCoord := mocks2.NewMixCoord(t)
	mixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:              merr.Success(),
		CollectionID:        lineageCollectionID,
		VirtualChannelNames: channels,
	}, nil).Maybe()
	f.svr.mixCoord = mixCoord
}

func (f *lineageFixture) recovery(t *testing.T) (map[int64]*datapb.SegmentInfo, map[string]*datapb.VchannelInfo) {
	resp, err := f.svr.GetRecoveryInfoV2(context.TODO(), &datapb.GetRecoveryInfoRequestV2{CollectionID: lineageCollectionID})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	segments := make(map[int64]*datapb.SegmentInfo)
	for _, segment := range resp.GetSegments() {
		_, dup := segments[segment.GetID()]
		require.False(t, dup, "a recovery response lists each segment once")
		segments[segment.GetID()] = segment
	}
	channels := make(map[string]*datapb.VchannelInfo)
	for _, channel := range resp.GetChannels() {
		channels[channel.GetChannelName()] = channel
	}
	return segments, channels
}

// DC1: a split source's recovery view inherits its targets' flushed segments,
// so a rewrite's input and its outputs share one compaction frontier.
func TestSplitSourceViewInheritsFlushedTargetSegments(t *testing.T) {
	const src, t1, t2 = hashSrcVChannel, hashTgtA, hashTgtB

	t.Run("source segment, no outputs yet", func(t *testing.T) {
		f := newLineageFixture(t)
		f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Flushed, indexed: true})

		assert.ElementsMatch(t, []int64{100}, f.view(src).GetFlushedSegmentIds())
	})

	t.Run("outputs published, input still live", func(t *testing.T) {
		f := newLineageFixture(t)
		f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Flushed, indexed: true})
		f.add(t, lineageSegment{id: 901, channel: t1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})
		f.add(t, lineageSegment{id: 902, channel: t2, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})

		// Every parent is present: the output set may be incomplete, keep S.
		assert.ElementsMatch(t, []int64{100}, f.view(src).GetFlushedSegmentIds())
	})

	t.Run("outputs published, input dropped", func(t *testing.T) {
		f := newLineageFixture(t)
		f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Dropped, indexed: true})
		f.add(t, lineageSegment{id: 901, channel: t1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})
		f.add(t, lineageSegment{id: 902, channel: t2, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})

		view := f.view(src)
		assert.ElementsMatch(t, []int64{901, 902}, view.GetFlushedSegmentIds())
		assert.Contains(t, view.GetDroppedSegmentIds(), int64(100))
	})

	// R3. The outputs carry the deletes of the source's L0s, folded in while the
	// rows were routed; the input's own deltalogs never received them, and the
	// L0s are retired once nothing on the source is left to fold them. So the
	// index fallback must NOT serve an unindexed output through its Dropped
	// input: that read would resurrect every row deleted through a source L0.
	t.Run("an unindexed output is served directly, never through its rewrite input", func(t *testing.T) {
		f := newLineageFixture(t)
		f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Dropped, indexed: true})
		f.add(t, lineageSegment{id: 901, channel: t1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})
		f.add(t, lineageSegment{id: 902, channel: t2, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}})

		view := f.view(src)
		assert.ElementsMatch(t, []int64{901, 902}, view.GetFlushedSegmentIds(),
			"both outputs serve; 902 is read unindexed until its index builds")
		assert.Contains(t, view.GetDroppedSegmentIds(), int64(100))

		// The recovery info QueryCoord builds its target from says the same,
		// and attributes both outputs to the source while it is listed.
		f.describing(t, src, t1, t2)
		segments, _ := f.recovery(t)
		assert.NotContains(t, segments, int64(100))
		for _, id := range []int64{901, 902} {
			require.Contains(t, segments, id)
			assert.Equal(t, src, segments[id].GetInsertChannel())
		}
	})

	// No blanket refusal: an ordinary compaction on the source writes its
	// outputs back to the source, and its parent answers the same rows.
	t.Run("a same-channel compaction still falls back to its parent", func(t *testing.T) {
		f := newLineageFixture(t)
		f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Dropped, indexed: true})
		f.add(t, lineageSegment{id: 101, channel: src, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}})

		assert.ElementsMatch(t, []int64{100}, f.view(src).GetFlushedSegmentIds(),
			"the indexed parent serves until 101 is indexed")
	})

	t.Run("target growing, L0, importing and invisible never reach the source", func(t *testing.T) {
		f := newLineageFixture(t)
		f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Flushed, indexed: true})
		f.add(t, lineageSegment{id: 910, channel: t1, state: commonpb.SegmentState_Growing})
		f.add(t, lineageSegment{id: 911, channel: t1, state: commonpb.SegmentState_Flushed, level: datapb.SegmentLevel_L0, startTs: 50})
		f.add(t, lineageSegment{id: 912, channel: t1, state: commonpb.SegmentState_Flushed, importing: true, indexed: true})
		f.add(t, lineageSegment{id: 913, channel: t2, state: commonpb.SegmentState_Flushed, invisible: true, indexed: true})
		f.add(t, lineageSegment{id: 914, channel: t2, state: commonpb.SegmentState_Dropped, indexed: true})
		f.add(t, lineageSegment{id: 915, channel: t2, state: commonpb.SegmentState_Flushed, indexed: true})

		view := f.view(src)
		assert.ElementsMatch(t, []int64{100, 915}, view.GetFlushedSegmentIds())
		assert.Empty(t, view.GetUnflushedSegmentIds())
		assert.Empty(t, view.GetLevelZeroSegmentIds())
		assert.Empty(t, view.GetDroppedSegmentIds())

		// The target's own view is unchanged.
		own := f.view(t1)
		assert.ElementsMatch(t, []int64{910}, own.GetUnflushedSegmentIds())
		assert.ElementsMatch(t, []int64{911}, own.GetLevelZeroSegmentIds())
	})

	t.Run("a finished split takes nothing in", func(t *testing.T) {
		f := newLineageFixture(t)
		f.add(t, lineageSegment{id: 915, channel: t2, state: commonpb.SegmentState_Flushed, indexed: true})
		f.setState(t, datapb.SplitShardTaskState_SplitShardTaskDone)
		assert.Empty(t, f.view(src).GetFlushedSegmentIds())
	})
}

func TestSplitTargetsOfSource(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, nil), newHashTask(nil))
	mgr := c.manager

	assert.Equal(t, []string{hashTgtA, hashTgtB}, mgr.SplitTargetsOfSource(hashSrcVChannel))
	assert.Empty(t, mgr.SplitTargetsOfSource(hashTgtA), "a target is not a source")

	// A task whose targets are not allocated yet has nothing to take in.
	preparing := preparingTask()
	preparing.TaskId = 8
	preparing.Sources[0].Vchannel = splitMgrV3
	require.NoError(t, mgr.store.create(context.TODO(), mgr.catalog, preparing))
	assert.Empty(t, mgr.SplitTargetsOfSource(splitMgrV3))

	mgr.finishTask(c.task(), "")
	assert.Empty(t, mgr.SplitTargetsOfSource(hashSrcVChannel), "a finished task has no window")
}

// DC2: a split source's L0 list and delete checkpoint come from its own L0.
func TestSplitSourceDeleteCheckpointIsOwnChannel(t *testing.T) {
	const src, t1 = hashSrcVChannel, hashTgtA

	t.Run("a target L0 never replaces the source seek position", func(t *testing.T) {
		f := newLineageFixture(t)
		require.NoError(t, f.svr.meta.UpdateChannelCheckpoint(context.TODO(), src,
			&msgpb.MsgPosition{ChannelName: src, MsgID: []byte{1}, Timestamp: 10}))
		f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Flushed, indexed: true})
		f.add(t, lineageSegment{id: 911, channel: t1, state: commonpb.SegmentState_Flushed, level: datapb.SegmentLevel_L0, startTs: 500})

		view := f.view(src)
		assert.Empty(t, view.GetLevelZeroSegmentIds())
		assert.Equal(t, view.GetSeekPosition().GetTimestamp(), view.GetDeleteCheckpoint().GetTimestamp())
		assert.EqualValues(t, 10, view.GetDeleteCheckpoint().GetTimestamp())
	})

	t.Run("a foreign L0 in the family is ignored", func(t *testing.T) {
		f := newLineageFixture(t)
		own := NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1, CollectionID: lineageCollectionID, InsertChannel: src, State: commonpb.SegmentState_Flushed,
			Level: datapb.SegmentLevel_L0, StartPosition: &msgpb.MsgPosition{ChannelName: src, Timestamp: 80},
		})
		foreign := NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2, CollectionID: lineageCollectionID, InsertChannel: t1, State: commonpb.SegmentState_Flushed,
			Level: datapb.SegmentLevel_L0, StartPosition: &msgpb.MsgPosition{ChannelName: t1, Timestamp: 20},
		})
		mocker := mockey.Mock((*ServerHandler).getRealSegmentsForSplitFamily).Return([]*SegmentInfo{own, foreign}).Build()
		defer mocker.UnPatch()

		// The checkpoint is the source's OWN L0 start, not the earlier foreign one.
		view := f.view(src)
		assert.ElementsMatch(t, []int64{1}, view.GetLevelZeroSegmentIds())
		assert.EqualValues(t, 80, view.GetDeleteCheckpoint().GetTimestamp())
	})
}

// DC3: while the source is listed, a target's non-L0 flushed segments are
// reported under the source.
func TestRecoveryInfoAttributesSplitTargetsToSource(t *testing.T) {
	const src, t1, t2 = hashSrcVChannel, hashTgtA, hashTgtB

	populate := func(t *testing.T, f *lineageFixture, inputState commonpb.SegmentState) {
		f.add(t, lineageSegment{id: 100, channel: src, state: inputState, indexed: true})
		f.add(t, lineageSegment{id: 901, channel: t1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})
		f.add(t, lineageSegment{id: 902, channel: t2, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})
		// Flushed from the child's WAL during the window.
		f.add(t, lineageSegment{id: 903, channel: t1, state: commonpb.SegmentState_Flushed, indexed: true})
		f.add(t, lineageSegment{id: 904, channel: t1, state: commonpb.SegmentState_Flushed, level: datapb.SegmentLevel_L0, startTs: 70})
		f.add(t, lineageSegment{id: 905, channel: t1, state: commonpb.SegmentState_Growing, startTs: 80})
	}

	t.Run("outputs are attributed to the listed source", func(t *testing.T) {
		f := newLineageFixture(t)
		populate(t, f, commonpb.SegmentState_Dropped)
		f.describing(t, src, t1, t2)

		segments, channels := f.recovery(t)
		assert.ElementsMatch(t, []int64{901, 902, 903}, lo.Keys(segments))
		for _, id := range []int64{901, 902, 903} {
			assert.Equal(t, src, segments[id].GetInsertChannel(), "segment %d", id)
		}

		// The targets' own channel info is untouched: a child respawn needs it.
		assert.ElementsMatch(t, []int64{901, 903}, channels[t1].GetFlushedSegmentIds())
		assert.ElementsMatch(t, []int64{905}, channels[t1].GetUnflushedSegmentIds())
		assert.ElementsMatch(t, []int64{904}, channels[t1].GetLevelZeroSegmentIds())
		assert.Empty(t, channels[src].GetLevelZeroSegmentIds())
		assert.Empty(t, channels[src].GetUnflushedSegmentIds())
	})

	t.Run("a live input keeps its outputs out of the response", func(t *testing.T) {
		// The targets' own views still list the outputs; the source's merged
		// view is what decides, so S and O1/O2 are never reported together.
		f := newLineageFixture(t)
		populate(t, f, commonpb.SegmentState_Flushed)
		f.describing(t, src, t1, t2)

		segments, channels := f.recovery(t)
		assert.ElementsMatch(t, []int64{100, 903}, lo.Keys(segments))
		assert.Equal(t, src, segments[100].GetInsertChannel())
		assert.Equal(t, src, segments[903].GetInsertChannel())
		assert.ElementsMatch(t, []int64{901, 903}, channels[t1].GetFlushedSegmentIds())
	})

	t.Run("a delisted source ends the window", func(t *testing.T) {
		f := newLineageFixture(t)
		populate(t, f, commonpb.SegmentState_Dropped)
		f.describing(t, t1, t2)

		segments, _ := f.recovery(t)
		assert.ElementsMatch(t, []int64{901, 902, 903}, lo.Keys(segments))
		assert.Equal(t, t1, segments[901].GetInsertChannel())
		assert.Equal(t, t2, segments[902].GetInsertChannel())
		assert.Equal(t, t1, segments[903].GetInsertChannel())
	})
}

func TestSplitAttributedInsertChannel(t *testing.T) {
	lineage := map[string]string{hashTgtA: hashSrcVChannel}
	l1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{InsertChannel: hashTgtA, Level: datapb.SegmentLevel_L1}}
	l0 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{InsertChannel: hashTgtA, Level: datapb.SegmentLevel_L0}}
	other := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{InsertChannel: hashTgtB, Level: datapb.SegmentLevel_L1}}

	assert.Equal(t, hashSrcVChannel, splitAttributedInsertChannel(l1, lineage))
	assert.Equal(t, hashTgtA, splitAttributedInsertChannel(l0, lineage), "an L0 on a target stays on the target")
	assert.Equal(t, hashTgtB, splitAttributedInsertChannel(other, lineage))
	assert.Equal(t, hashTgtA, splitAttributedInsertChannel(l1, nil))
}

func TestSplitLineageOfListedSources(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, nil), newHashTask(nil))
	mgr := c.manager

	assert.Equal(t, map[string]string{hashTgtA: hashSrcVChannel, hashTgtB: hashSrcVChannel},
		mgr.SplitLineageOfListedSources(typeutil.NewSet(hashSrcVChannel, hashTgtA, hashTgtB)))
	assert.Empty(t, mgr.SplitLineageOfListedSources(typeutil.NewSet(hashTgtA, hashTgtB)),
		"the window ends when the source is delisted")

	assert.Equal(t, map[string][]string{hashSrcVChannel: {hashTgtA, hashTgtB}},
		splitFamiliesOf(map[string]string{hashTgtB: hashSrcVChannel, hashTgtA: hashSrcVChannel}))

	mgr.finishTask(c.task(), "")
	assert.Empty(t, mgr.SplitLineageOfListedSources(typeutil.NewSet(hashSrcVChannel)))
}

// GetRecoveryInfoV2 resolves the split family once. A task that turns Done
// between the attribution and the source's view must not drop the targets'
// segments from both lists, nor report them twice.
func TestRecoveryInfoResolvesSplitFamilyOnce(t *testing.T) {
	const src, t1, t2 = hashSrcVChannel, hashTgtA, hashTgtB
	f := newLineageFixture(t)
	f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Dropped, indexed: true})
	f.add(t, lineageSegment{id: 901, channel: t1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})
	f.add(t, lineageSegment{id: 902, channel: t2, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})
	f.add(t, lineageSegment{id: 903, channel: t1, state: commonpb.SegmentState_Flushed, indexed: true})
	f.describing(t, src, t1, t2)

	var origin func(m *shardSplitManager, listed typeutil.Set[string]) map[string]string
	mocker := mockey.Mock((*shardSplitManager).SplitLineageOfListedSources).Origin(&origin).
		To(func(m *shardSplitManager, listed typeutil.Set[string]) map[string]string {
			lineage := origin(m, listed)
			// The task finishes right after the lineage is read.
			f.setState(t, datapb.SplitShardTaskState_SplitShardTaskDone)
			return lineage
		}).Build()
	defer mocker.UnPatch()

	segments, _ := f.recovery(t)
	assert.ElementsMatch(t, []int64{901, 902, 903}, lo.Keys(segments))
	for _, id := range []int64{901, 902, 903} {
		assert.Equal(t, src, segments[id].GetInsertChannel(), "segment %d", id)
	}
}

// Without a split manager nothing is attributed and nothing is skipped.
func TestRecoveryInfoWithoutSplitManager(t *testing.T) {
	const src, t1 = hashSrcVChannel, hashTgtA
	f := newLineageFixture(t)
	f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Flushed, indexed: true})
	f.add(t, lineageSegment{id: 903, channel: t1, state: commonpb.SegmentState_Flushed, indexed: true})
	f.describing(t, src, t1)
	manager := f.svr.shardSplitManager
	f.svr.shardSplitManager = nil
	defer func() { f.svr.shardSplitManager = manager }()

	segments, channels := f.recovery(t)
	assert.ElementsMatch(t, []int64{100, 903}, lo.Keys(segments))
	assert.Equal(t, src, segments[100].GetInsertChannel())
	assert.Equal(t, t1, segments[903].GetInsertChannel())
	assert.ElementsMatch(t, []int64{100}, channels[src].GetFlushedSegmentIds())
}

// A handler that cannot build a view over a resolved family opens no window:
// nothing is attributed and no target's flushed list is skipped.
func TestRecoveryInfoWithoutFamilyViewer(t *testing.T) {
	const src, t1 = hashSrcVChannel, hashTgtA
	f := newLineageFixture(t)
	f.add(t, lineageSegment{id: 903, channel: t1, state: commonpb.SegmentState_Flushed, indexed: true})
	f.describing(t, src, t1)
	handler := NewNMockHandler(t)
	handler.EXPECT().GetQueryVChanPositions(mock.Anything).RunAndReturn(func(ch RWChannel, _ ...int64) *datapb.VchannelInfo {
		info := &datapb.VchannelInfo{ChannelName: ch.GetName()}
		if ch.GetName() == t1 {
			info.FlushedSegmentIds = []int64{903}
		}
		return info
	}).Times(2)
	original := f.svr.handler
	f.svr.handler = handler
	defer func() { f.svr.handler = original }()

	segments, _ := f.recovery(t)
	assert.ElementsMatch(t, []int64{903}, lo.Keys(segments))
	assert.Equal(t, t1, segments[903].GetInsertChannel())
}

// crossChannelParents is the mark the index fallback refuses on: only a shard
// split rewrite writes an output to a vchannel other than its input's.
func TestCrossChannelParents(t *testing.T) {
	seg := func(id int64, channel string, from ...int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, InsertChannel: channel, CompactionFrom: from}}
	}
	view := func(segments ...*SegmentInfo) map[int64]*SegmentInfo {
		out := make(map[int64]*SegmentInfo, len(segments))
		for _, s := range segments {
			out[s.GetID()] = s
		}
		return out
	}

	assert.ElementsMatch(t, []int64{100}, crossChannelParents(view(
		seg(100, "src"), seg(901, "t1", 100), seg(902, "t2", 100))).Collect(), "a rewrite input, once")
	assert.Empty(t, crossChannelParents(view(
		seg(100, "src"), seg(101, "src"), seg(102, "src", 100, 101))).Collect(), "a same-channel parent")
	assert.Empty(t, crossChannelParents(view(seg(901, "t1", 100))).Collect(), "a parent outside the view")
	assert.Empty(t, crossChannelParents(view(seg(100, "src"))).Collect(), "no lineage")
}

// Retiring the source L0s takes away what pinned the source's delete
// checkpoint below T_switch. Left alone it would jump to the channel's own,
// advancing seek position, and the source delegator would discard the deletes
// its children forward from the target WALs. So it is held at T_switch for as
// long as the split is active.
func TestSplitSourceDeleteCheckpointStaysPinnedAtTheFence(t *testing.T) {
	const src = hashSrcVChannel

	// A source past its fence with every L0 retired: nothing else pins it.
	newRetiredSource := func(t *testing.T) *lineageFixture {
		f := newLineageFixture(t)
		require.NoError(t, f.svr.meta.UpdateChannelCheckpoint(context.TODO(), src,
			&msgpb.MsgPosition{ChannelName: src, MsgID: []byte{1}, Timestamp: 500}))
		f.add(t, lineageSegment{
			id: 200, channel: src, state: commonpb.SegmentState_Dropped,
			level: datapb.SegmentLevel_L0, startTs: 50,
		})
		return f
	}

	t.Run("held at T_switch while the split is active", func(t *testing.T) {
		f := newRetiredSource(t)
		view := f.view(src)
		require.Empty(t, view.GetLevelZeroSegmentIds(), "the source L0 is retired")
		assert.EqualValues(t, 500, view.GetSeekPosition().GetTimestamp())
		assert.EqualValues(t, hashFenceTick, view.GetDeleteCheckpoint().GetTimestamp(),
			"without the clamp this would be the seek position, and forwarded target deletes would be dropped")
		assert.EqualValues(t, 500, view.GetSeekPosition().GetTimestamp(), "only a copy is lowered")
	})

	t.Run("held through Adopting, released at Done", func(t *testing.T) {
		f := newRetiredSource(t)
		f.setState(t, datapb.SplitShardTaskState_SplitShardTaskAdopting)
		assert.EqualValues(t, hashFenceTick, f.view(src).GetDeleteCheckpoint().GetTimestamp())

		f.setState(t, datapb.SplitShardTaskState_SplitShardTaskDone)
		assert.EqualValues(t, 500, f.view(src).GetDeleteCheckpoint().GetTimestamp())
	})

	t.Run("no clamp before the fence is recorded", func(t *testing.T) {
		f := newRetiredSource(t)
		_, err := f.mgr.store.modify(context.TODO(), f.mgr.catalog, hashTaskID, func(task *datapb.SplitShardTask) bool {
			task.Sources[0].SwitchTimeTick = 0
			return true
		})
		require.NoError(t, err)
		assert.EqualValues(t, 500, f.view(src).GetDeleteCheckpoint().GetTimestamp(),
			"a source still taking writes has its own L0s and no tick to clamp to")
	})

	t.Run("a live source L0 already below the fence is left alone", func(t *testing.T) {
		f := newLineageFixture(t)
		require.NoError(t, f.svr.meta.UpdateChannelCheckpoint(context.TODO(), src,
			&msgpb.MsgPosition{ChannelName: src, MsgID: []byte{1}, Timestamp: 500}))
		f.add(t, lineageSegment{id: 201, channel: src, state: commonpb.SegmentState_Flushed, level: datapb.SegmentLevel_L0, startTs: 50})

		view := f.view(src)
		assert.ElementsMatch(t, []int64{201}, view.GetLevelZeroSegmentIds())
		assert.EqualValues(t, 50, view.GetDeleteCheckpoint().GetTimestamp(), "the clamp is a ceiling, never a floor")
	})

	t.Run("the ceiling applies to an L0-derived checkpoint too", func(t *testing.T) {
		// Only reachable were an L0 registered on the source after its fence,
		// which the fence forbids; the ceiling does not rely on it.
		f := newLineageFixture(t)
		require.NoError(t, f.svr.meta.UpdateChannelCheckpoint(context.TODO(), src,
			&msgpb.MsgPosition{ChannelName: src, MsgID: []byte{1}, Timestamp: 500}))
		f.add(t, lineageSegment{id: 202, channel: src, state: commonpb.SegmentState_Flushed, level: datapb.SegmentLevel_L0, startTs: 300})

		view := f.view(src)
		assert.ElementsMatch(t, []int64{202}, view.GetLevelZeroSegmentIds())
		assert.EqualValues(t, hashFenceTick, view.GetDeleteCheckpoint().GetTimestamp())
		assert.EqualValues(t, 300, f.svr.meta.GetSegment(context.TODO(), 202).GetStartPosition().GetTimestamp(),
			"the L0's own start position in meta is not touched")
	})

	t.Run("a channel that is no split source is untouched", func(t *testing.T) {
		f := newLineageFixture(t)
		require.NoError(t, f.svr.meta.UpdateChannelCheckpoint(context.TODO(), hashTgtA,
			&msgpb.MsgPosition{ChannelName: hashTgtA, MsgID: []byte{1}, Timestamp: 500}))
		assert.EqualValues(t, 500, f.view(hashTgtA).GetDeleteCheckpoint().GetTimestamp())
	})

	t.Run("the recovery info carries the clamp", func(t *testing.T) {
		f := newRetiredSource(t)
		f.describing(t, src, hashTgtA, hashTgtB)
		_, channels := f.recovery(t)
		assert.EqualValues(t, hashFenceTick, channels[src].GetDeleteCheckpoint().GetTimestamp())
	})
}

func TestActiveSplitSourceFenceTick(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, nil), newHashTask(nil))
	mgr := c.manager
	assert.EqualValues(t, hashFenceTick, mgr.activeSplitSourceFenceTick(hashSrcVChannel))
	assert.Zero(t, mgr.activeSplitSourceFenceTick(hashTgtA), "a target is not a source")
	assert.Zero(t, mgr.activeSplitSourceFenceTick(splitMgrV3), "no task at all")

	mgr.finishTask(c.task(), "")
	assert.Zero(t, mgr.activeSplitSourceFenceTick(hashSrcVChannel), "a finished task has no window")

	// Without a manager nothing is clamped.
	handler := &ServerHandler{s: &Server{}}
	position := &msgpb.MsgPosition{Timestamp: 500}
	assert.Same(t, position, handler.clampSplitSourceDeleteCheckpoint(hashSrcVChannel, []string{hashTgtA}, position))
}

// F1: an unsorted rewrite output is sorted during the window, and the sorted
// output -- a new segment id on the same target -- is indexed, is a stats
// candidate, replaces the output in the source's view, and leaves the rewrite's
// bookkeeping of its input intact.
func TestAnUnsortedRewriteOutputIsSortedAndIndexedInTheWindow(t *testing.T) {
	const src, t1 = hashSrcVChannel, hashTgtA
	const input, output, flushed, sorted = int64(100), int64(901), int64(920), int64(931)
	ctx := context.TODO()
	f := newLineageFixture(t)
	f.add(t, lineageSegment{id: input, channel: src, state: commonpb.SegmentState_Dropped, indexed: true})
	f.add(t, lineageSegment{id: output, channel: t1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{input}, compacted: true})
	f.add(t, lineageSegment{id: flushed, channel: t1, state: commonpb.SegmentState_Flushed, invisible: true})

	inspector, ok := f.svr.compactionInspector.(*compactionInspector)
	require.True(t, ok)
	sortOf := func(segmentID int64) *datapb.CompactionTask {
		return &datapb.CompactionTask{
			PlanID: 5000 + segmentID, Channel: t1, Type: datapb.CompactionType_SortCompaction,
			CollectionID: lineageCollectionID, InputSegments: []int64{segmentID}, Schema: newTestSchema(),
		}
	}
	assert.False(t, inspector.frozenBySplit(sortOf(output)), "the rewrite output is sortable in the window")
	assert.True(t, inspector.frozenBySplit(sortOf(flushed)), "a target-flushed segment is not")

	// Unsorted, the output is skipped by the index and the stats inspectors.
	require.NoError(t, f.svr.indexInspector.createIndexesForSegment(ctx, f.svr.meta.GetSegment(ctx, output)))
	assert.Empty(t, f.svr.meta.indexMeta.GetSegmentIndexes(lineageCollectionID, output))
	assert.False(t, needDoTextIndex(f.svr.meta.GetSegment(ctx, output), []int64{1}, false))
	assert.False(t, needDoJSONKeyIndex(f.svr.meta.GetSegment(ctx, output), []int64{1}, false))

	_, _, err := f.svr.meta.CompleteCompactionMutation(ctx, sortOf(output), &datapb.CompactionPlanResult{
		PlanID: 5000 + output,
		Segments: []*datapb.CompactionSegment{{
			SegmentID: sorted, NumOfRows: 2048, IsSorted: true, Channel: t1,
			InsertLogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{EntriesNum: 2048, LogID: sorted}}}},
		}},
	})
	require.NoError(t, err)
	got := f.svr.meta.GetSegment(ctx, sorted)
	require.NotNil(t, got)
	assert.True(t, got.GetIsSorted())
	assert.False(t, got.GetIsInvisible())
	assert.True(t, got.GetCreatedByCompaction())
	assert.Equal(t, []int64{output}, got.GetCompactionFrom())

	// Sorted, both inspectors pick it up.
	require.NoError(t, f.svr.indexInspector.createIndexesForSegment(ctx, got))
	assert.Contains(t, f.svr.meta.indexMeta.GetSegmentIndexes(lineageCollectionID, sorted), lineageIndexID)
	assert.True(t, needDoTextIndex(got, []int64{1}, false))
	assert.True(t, needDoJSONKeyIndex(got, []int64{1}, false))

	// The source's view swaps the output for its sorted copy and serves the
	// copy directly while it is unindexed: a target's Dropped segments never
	// enter the source's view (inheritedBySplitSource), so the output is no
	// fallback parent, and the rewrite input above it never is one either.
	view := f.view(src)
	assert.ElementsMatch(t, []int64{sorted}, view.GetFlushedSegmentIds())
	assert.Contains(t, view.GetDroppedSegmentIds(), input)

	// The sorted copy names the output, not the input, so the input drops out
	// of the rewritten set; the rewrite still counts it done because it is no
	// longer a rewrite input, and never queues it again.
	task := mustTask(t, f.mgr, hashTaskID)
	rewritten := f.mgr.rewrittenSourceSegments(task)
	assert.False(t, rewritten.Contain(input))
	assert.True(t, f.mgr.planAlreadyCommitted(ctx, []int64{input}, rewritten))
	assert.NotContains(t, f.mgr.rewriteInputIDs(src), input)
	pending := map[string]typeutil.Set[int64]{src: typeutil.NewSet(input)}
	assert.Equal(t, 1, forgetNonInputSegments(pending, func(id int64) *SegmentInfo { return f.svr.meta.GetSegment(ctx, id) }))
}
