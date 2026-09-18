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
		StartPosition:  &msgpb.MsgPosition{ChannelName: s.channel, MsgID: []byte{1}, Timestamp: s.startTs},
		DmlPosition:    &msgpb.MsgPosition{ChannelName: s.channel, MsgID: []byte{1}, Timestamp: s.startTs + 1},
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{{EntriesNum: 2048, LogID: s.id}},
		}},
	}
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

	t.Run("an unindexed output falls back to its indexed input", func(t *testing.T) {
		f := newLineageFixture(t)
		f.add(t, lineageSegment{id: 100, channel: src, state: commonpb.SegmentState_Dropped, indexed: true})
		f.add(t, lineageSegment{id: 901, channel: t1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}, indexed: true})
		f.add(t, lineageSegment{id: 902, channel: t2, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{100}})

		assert.ElementsMatch(t, []int64{100}, f.view(src).GetFlushedSegmentIds())
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
