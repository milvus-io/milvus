package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func TestGetQueryVChanPositionsRetrieveM2N(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()

	channel := "ch1"
	svr.meta.AddCollection(&collectionInfo{
		ID:         1,
		Partitions: []int64{0},
		Schema:     schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  channel,
				Data: []byte{8, 9, 10},
			},
		},
	})
	indexReq := &indexpb.CreateIndexRequest{
		CollectionID: 1,
		FieldID:      2,
	}
	_, err := svr.meta.indexMeta.CreateIndex(context.TODO(), indexReq, 1, false)
	require.NoError(t, err)

	segArgs := []struct {
		segID   int64
		state   commonpb.SegmentState
		level   datapb.SegmentLevel
		indexed bool
	}{
		{100, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1, false},
		{200, commonpb.SegmentState_Flushing, datapb.SegmentLevel_L1, false},
		{300, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1, false},
		{400, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1, true},
		{401, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1, true},
		{402, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1, true},
		{403, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1, true},
		{404, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1, false},
		// (400[indexed], 401[indexed], 402(indexed) -> 403(indexed), 404(no index))
		{500, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L2, true},
		{600, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L2, false},
		{700, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0, false},
		{800, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1, false},
	}

	compactFroms := []int64{400, 401, 402}
	compactTos := []int64{403, 404}

	for _, arg := range segArgs {
		seg := NewSegmentInfo(&datapb.SegmentInfo{
			ID:            arg.segID,
			CollectionID:  1,
			InsertChannel: channel,
			State:         arg.state,
			Level:         arg.level,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: channel,
				MsgID:       []byte{1, 2, 3},
			},
			NumOfRows: 2048,
		})

		if lo.Contains(compactTos, arg.segID) {
			seg.CompactionFrom = compactFroms
		}
		err := svr.meta.AddSegment(context.TODO(), seg)
		require.NoError(t, err)

		if arg.indexed {
			err = svr.meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
				SegmentID: arg.segID,
				BuildID:   arg.segID,
				IndexID:   1,
			})
			assert.NoError(t, err)
			err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
				BuildID: arg.segID,
				State:   commonpb.IndexState_Finished,
			})
			assert.NoError(t, err)
		}
	}

	info := svr.handler.GetQueryVChanPositions(&channelMeta{Name: channel, CollectionID: 1}, -1)

	totalSegs := len(info.GetLevelZeroSegmentIds()) +
		len(info.GetUnflushedSegmentIds()) +
		len(info.GetFlushedSegmentIds()) +
		len(info.GetDroppedSegmentIds())
	assert.EqualValues(t, 1, info.CollectionID)
	assert.EqualValues(t, len(segArgs)-2, totalSegs)
	assert.ElementsMatch(t, []int64{700}, info.GetLevelZeroSegmentIds())
	assert.ElementsMatch(t, []int64{100}, info.GetUnflushedSegmentIds())
	assert.ElementsMatch(t, []int64{200, 300, 400, 401, 402, 500, 600}, info.GetFlushedSegmentIds())
	assert.ElementsMatch(t, []int64{800}, info.GetDroppedSegmentIds())

	assert.Empty(t, info.GetUnflushedSegments())
	assert.Empty(t, info.GetFlushedSegments())
	assert.Empty(t, info.GetDroppedSegments())
	assert.Empty(t, info.GetIndexedSegments())
	assert.Empty(t, info.GetIndexedSegmentIds())
}

func TestGetQueryVChanPositions(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&collectionInfo{
		ID:         0,
		Partitions: []int64{0, 1},
		Schema:     schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch1",
				Data: []byte{8, 9, 10},
			},
		},
	})
	svr.meta.AddCollection(&collectionInfo{
		ID:     1,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch0",
				Data: []byte{8, 9, 10},
			},
		},
	})

	indexReq := &indexpb.CreateIndexRequest{
		CollectionID: 0,
		FieldID:      2,
	}

	_, err := svr.meta.indexMeta.CreateIndex(context.TODO(), indexReq, 1, false)
	assert.NoError(t, err)

	s1 := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   0,
		},
		NumOfRows: 2048,
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s1))
	assert.NoError(t, err)
	err = svr.meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
		SegmentID: 1,
		BuildID:   1,
		IndexID:   1,
	})
	assert.NoError(t, err)
	err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
		BuildID: 1,
		State:   commonpb.IndexState_Finished,
	})
	assert.NoError(t, err)
	s2 := &datapb.SegmentInfo{
		ID:            2,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
			MsgGroup:    "",
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s2))
	assert.NoError(t, err)
	s3 := &datapb.SegmentInfo{
		ID:            3,
		CollectionID:  0,
		PartitionID:   1,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
			MsgGroup:    "",
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{11, 12, 13},
			MsgGroup:    "",
			Timestamp:   2,
		},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s3))
	assert.NoError(t, err)

	s4 := &datapb.SegmentInfo{
		ID:            4,
		CollectionID:  0,
		PartitionID:   common.AllPartitionsID,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
			MsgGroup:    "",
			Timestamp:   1,
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{11, 12, 13},
			MsgGroup:    "",
			Timestamp:   2,
		},
		Level: datapb.SegmentLevel_L0,
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s4))
	assert.NoError(t, err)

	t.Run("get unexisted channel", func(t *testing.T) {
		vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "chx1", CollectionID: 0})
		assert.Empty(t, vchan.UnflushedSegmentIds)
		assert.Empty(t, vchan.FlushedSegmentIds)
	})

	t.Run("empty collection", func(t *testing.T) {
		infos := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch0_suffix", CollectionID: 1})
		assert.EqualValues(t, 1, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 0, len(infos.UnflushedSegmentIds))
		assert.EqualValues(t, 0, len(infos.GetLevelZeroSegmentIds()))
	})

	t.Run("filter partition", func(t *testing.T) {
		infos := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0}, 1)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, 1, len(infos.GetLevelZeroSegmentIds()))
		assert.EqualValues(t, uint64(1), infos.GetDeleteCheckpoint().GetTimestamp())
	})

	t.Run("empty collection with passed positions", func(t *testing.T) {
		vchannel := "ch_no_segment_1"
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		infos := svr.handler.GetQueryVChanPositions(&channelMeta{
			Name:           vchannel,
			CollectionID:   0,
			StartPositions: []*commonpb.KeyDataPair{{Key: pchannel, Data: []byte{14, 15, 16}}},
		})
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, vchannel, infos.ChannelName)
		assert.EqualValues(t, 0, len(infos.GetLevelZeroSegmentIds()))
	})
}

func TestGetQueryVChanPositions_PartitionStats(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	collectionID := int64(0)
	partitionID := int64(1)
	vchannel := "test_vchannel"
	version := int64(100)
	svr.meta.AddCollection(&collectionInfo{
		ID:     collectionID,
		Schema: schema,
	})
	svr.meta.partitionStatsMeta.partitionStatsInfos = map[string]map[int64]*partitionStatsInfo{
		vchannel: {
			partitionID: {
				currentVersion: version,
				infos: map[int64]*datapb.PartitionStatsInfo{
					version: {Version: version},
				},
			},
			partitionID + 1: {
				currentVersion: version + 1,
				infos: map[int64]*datapb.PartitionStatsInfo{
					version + 1: {Version: version + 1},
				},
			},
		},
	}
	vChannelInfo := svr.handler.GetQueryVChanPositions(&channelMeta{Name: vchannel, CollectionID: collectionID}, partitionID)
	statsVersions := vChannelInfo.GetPartitionStatsVersions()
	assert.Equal(t, 1, len(statsVersions))
	assert.Equal(t, int64(100), statsVersions[partitionID])

	vChannelInfo2 := svr.handler.GetQueryVChanPositions(&channelMeta{Name: vchannel, CollectionID: collectionID})
	statsVersions2 := vChannelInfo2.GetPartitionStatsVersions()
	assert.Equal(t, 2, len(statsVersions2))
}

func TestGetQueryVChanPositions_Retrieve_unIndexed(t *testing.T) {
	t.Run("ab GC-ed, cde unIndexed", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: schema,
		})
		indexReq := &indexpb.CreateIndexRequest{
			CollectionID: 0,
			FieldID:      2,
		}
		_, err := svr.meta.indexMeta.CreateIndex(context.TODO(), indexReq, 1, false)
		assert.NoError(t, err)
		c := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{99, 100}, // a, b which have been GC-ed
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(c))
		assert.NoError(t, err)
		d := &datapb.SegmentInfo{
			ID:            2,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(d))
		assert.NoError(t, err)
		e := &datapb.SegmentInfo{
			ID:            3,
			CollectionID:  0,
			PartitionID:   1,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{1, 2}, // c, d
			NumOfRows:      2048,
		}

		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(e))
		assert.NoError(t, err)
		// vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0})
		// assert.EqualValues(t, 2, len(vchan.FlushedSegmentIds))
		// assert.EqualValues(t, 0, len(vchan.UnflushedSegmentIds))
		// assert.ElementsMatch(t, []int64{c.GetID(), d.GetID()}, vchan.FlushedSegmentIds) // expected c, d
	})

	t.Run("a GC-ed, bcde unIndexed", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: schema,
		})
		indexReq := &indexpb.CreateIndexRequest{
			CollectionID: 0,
			FieldID:      2,
		}
		_, err := svr.meta.indexMeta.CreateIndex(context.TODO(), indexReq, 1, false)
		assert.NoError(t, err)
		a := &datapb.SegmentInfo{
			ID:            99,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(a))
		assert.NoError(t, err)

		c := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{99, 100}, // a, b which have been GC-ed
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(c))
		assert.NoError(t, err)
		d := &datapb.SegmentInfo{
			ID:            2,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(d))
		assert.NoError(t, err)
		e := &datapb.SegmentInfo{
			ID:            3,
			CollectionID:  0,
			PartitionID:   1,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{1, 2}, // c, d
			NumOfRows:      2048,
		}

		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(e))
		assert.NoError(t, err)
		// vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0})
		// assert.EqualValues(t, 2, len(vchan.FlushedSegmentIds))
		// assert.EqualValues(t, 0, len(vchan.UnflushedSegmentIds))
		// assert.ElementsMatch(t, []int64{c.GetID(), d.GetID()}, vchan.FlushedSegmentIds) // expected c, d
	})

	t.Run("ab GC-ed, c unIndexed, de indexed", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: schema,
		})
		indexReq := &indexpb.CreateIndexRequest{
			CollectionID: 0,
			FieldID:      2,
		}
		_, err := svr.meta.indexMeta.CreateIndex(context.TODO(), indexReq, 1, false)
		assert.NoError(t, err)
		c := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{99, 100}, // a, b which have been GC-ed
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(c))
		assert.NoError(t, err)
		d := &datapb.SegmentInfo{
			ID:            2,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(d))
		assert.NoError(t, err)
		err = svr.meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			SegmentID: 2,
			BuildID:   1,
			IndexID:   1,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: 1,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)
		e := &datapb.SegmentInfo{
			ID:            3,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			CompactionFrom: []int64{1, 2}, // c, d
			NumOfRows:      2048,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(e))
		assert.NoError(t, err)
		err = svr.meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
			SegmentID: 3,
			BuildID:   2,
			IndexID:   1,
		})
		assert.NoError(t, err)
		err = svr.meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: 2,
			State:   commonpb.IndexState_Finished,
		})
		assert.NoError(t, err)

		// vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0})
		// assert.EqualValues(t, 1, len(vchan.FlushedSegmentIds))
		// assert.EqualValues(t, 0, len(vchan.UnflushedSegmentIds))
		// assert.ElementsMatch(t, []int64{e.GetID()}, vchan.FlushedSegmentIds) // expected e
	})

	t.Run("complex derivation", func(t *testing.T) {
		// numbers indicate segmentID, letters indicate segment information
		// i: indexed,  u: unindexed,  g: gced
		//        1i,  2i,  3g     4i,  5i,  6i
		//        |    |    |      |    |    |
		//        \    |    /      \    |    /
		//         \   |   /        \   |   /
		//    7u,  [8i,9i,10i]      [11u, 12i]
		//    |     |  |  |          |     |
		//    \     |  /  \          /     |
		//     \    | /    \        /      |
		//       [13u]      [14i, 15u]    12i                       [19u](unsorted)
		//        |         |     |        |                              |
		//        \         /     \        /                              |
		//         \       /       \      /                               |
		//           [16u]          [17u]       [18u](unsorted)     [20u](sorted)      [21i](unsorted)
		// all leaf nodes are [1,2,3,4,5,6,7], but because segment3 has been gced, the leaf node becomes [7,8,9,10,4,5,6]
		// should be returned: flushed: [7, 8, 9, 10, 4, 5, 6, 20, 21], growing: [18]
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:         0,
			Partitions: []int64{0},
			Schema:     schema,
		})
		indexReq := &indexpb.CreateIndexRequest{
			CollectionID: 0,
			FieldID:      2,
		}
		_, err := svr.meta.indexMeta.CreateIndex(context.TODO(), indexReq, 1, false)
		assert.NoError(t, err)
		seg1 := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows: 100,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg1))
		assert.NoError(t, err)
		seg2 := &datapb.SegmentInfo{
			ID:            2,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows: 100,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg2))
		assert.NoError(t, err)
		// seg3 was GCed
		seg4 := &datapb.SegmentInfo{
			ID:            4,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows: 100,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg4))
		assert.NoError(t, err)
		seg5 := &datapb.SegmentInfo{
			ID:            5,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows: 100,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg5))
		assert.NoError(t, err)
		seg6 := &datapb.SegmentInfo{
			ID:            6,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows: 100,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg6))
		assert.NoError(t, err)
		seg7 := &datapb.SegmentInfo{
			ID:            7,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows: 2048,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg7))
		assert.NoError(t, err)
		seg8 := &datapb.SegmentInfo{
			ID:            8,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{1, 2, 3},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg8))
		assert.NoError(t, err)
		seg9 := &datapb.SegmentInfo{
			ID:            9,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{1, 2, 3},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg9))
		assert.NoError(t, err)
		seg10 := &datapb.SegmentInfo{
			ID:            10,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{1, 2, 3},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg10))
		assert.NoError(t, err)
		seg11 := &datapb.SegmentInfo{
			ID:            11,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           2048,
			CompactionFrom:      []int64{4, 5, 6},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg11))
		assert.NoError(t, err)
		seg12 := &datapb.SegmentInfo{
			ID:            12,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{4, 5, 6},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg12))
		assert.NoError(t, err)
		seg13 := &datapb.SegmentInfo{
			ID:            13,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           2047,
			CompactionFrom:      []int64{7, 8, 9},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg13))
		assert.NoError(t, err)
		seg14 := &datapb.SegmentInfo{
			ID:            14,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{10, 11},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg14))
		assert.NoError(t, err)
		seg15 := &datapb.SegmentInfo{
			ID:            15,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           2048,
			CompactionFrom:      []int64{10, 11},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg15))
		assert.NoError(t, err)
		seg16 := &datapb.SegmentInfo{
			ID:            16,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           2048,
			CompactionFrom:      []int64{13, 14},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg16))
		assert.NoError(t, err)
		seg17 := &datapb.SegmentInfo{
			ID:            17,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           2048,
			CompactionFrom:      []int64{12, 15},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg17))
		assert.NoError(t, err)
		seg18 := &datapb.SegmentInfo{
			ID:            18,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L1,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:      2048,
			CompactionFrom: []int64{},
			IsInvisible:    true,
			IsSorted:       false,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg18))
		assert.NoError(t, err)
		seg19 := &datapb.SegmentInfo{
			ID:            19,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			Level:         datapb.SegmentLevel_L1,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:      2048,
			CompactionFrom: []int64{},
			IsInvisible:    true,
			IsSorted:       false,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg19))
		assert.NoError(t, err)
		seg20 := &datapb.SegmentInfo{
			ID:            20,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L1,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           2048,
			CompactionFrom:      []int64{19},
			CreatedByCompaction: true,
			IsInvisible:         false,
			IsSorted:            true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg20))
		assert.NoError(t, err)
		seg21 := &datapb.SegmentInfo{
			ID:            21,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L1,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:      100,
			CompactionFrom: []int64{},
			IsInvisible:    false,
			IsSorted:       false,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg21))
		assert.NoError(t, err)

		vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0})
		assert.ElementsMatch(t, []int64{7, 8, 9, 10, 4, 5, 6, 20, 21}, vchan.FlushedSegmentIds)
		assert.ElementsMatch(t, []int64{18}, vchan.UnflushedSegmentIds)
		assert.ElementsMatch(t, []int64{1, 2, 19}, vchan.DroppedSegmentIds)
	})

	t.Run("compaction iterate", func(t *testing.T) {
		svr := newTestServer(t)
		defer closeTestServer(t, svr)
		schema := newTestSchema()
		svr.meta.AddCollection(&collectionInfo{
			ID:         0,
			Partitions: []int64{0},
			Schema:     schema,
		})
		indexReq := &indexpb.CreateIndexRequest{
			CollectionID: 0,
			FieldID:      2,
		}
		_, err := svr.meta.indexMeta.CreateIndex(context.TODO(), indexReq, 1, false)
		assert.NoError(t, err)
		seg1 := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows: 100,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg1))
		assert.NoError(t, err)
		seg2 := &datapb.SegmentInfo{
			ID:            2,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows: 100,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg2))
		assert.NoError(t, err)
		seg3 := &datapb.SegmentInfo{
			ID:            3,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{1, 2},
			IsInvisible:         true,
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg3))
		assert.NoError(t, err)
		seg4 := &datapb.SegmentInfo{
			ID:            4,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{1, 2},
			IsInvisible:         true,
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg4))
		assert.NoError(t, err)
		seg5 := &datapb.SegmentInfo{
			ID:            5,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{3},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg5))
		assert.NoError(t, err)
		seg6 := &datapb.SegmentInfo{
			ID:            6,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{4},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg6))
		assert.NoError(t, err)
		seg7 := &datapb.SegmentInfo{
			ID:            7,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{5, 6},
			IsInvisible:         true,
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg7))
		assert.NoError(t, err)
		seg8 := &datapb.SegmentInfo{
			ID:            8,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Dropped,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{5, 6},
			IsInvisible:         true,
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg8))
		assert.NoError(t, err)
		seg9 := &datapb.SegmentInfo{
			ID:            9,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{7},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg9))
		assert.NoError(t, err)
		seg10 := &datapb.SegmentInfo{
			ID:            10,
			CollectionID:  0,
			PartitionID:   0,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Flushed,
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: "ch1",
				MsgID:       []byte{1, 2, 3},
				MsgGroup:    "",
				Timestamp:   1,
			},
			NumOfRows:           100,
			CompactionFrom:      []int64{8},
			CreatedByCompaction: true,
		}
		err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg10))
		assert.NoError(t, err)

		vchan := svr.handler.GetQueryVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0})
		assert.ElementsMatch(t, []int64{5, 6}, vchan.FlushedSegmentIds)
		assert.ElementsMatch(t, []int64{}, vchan.UnflushedSegmentIds)
		assert.ElementsMatch(t, []int64{1, 2}, vchan.DroppedSegmentIds)
	})
}

func TestGetCurrentSegmentsView(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&collectionInfo{
		ID:         0,
		Partitions: []int64{0},
		Schema:     schema,
	})

	indexReq := &indexpb.CreateIndexRequest{
		CollectionID: 0,
		FieldID:      2,
	}
	_, err := svr.meta.indexMeta.CreateIndex(context.TODO(), indexReq, 1, false)
	assert.NoError(t, err)
	seg1 := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Dropped,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
		NumOfRows: 100,
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg1))
	assert.NoError(t, err)
	seg2 := &datapb.SegmentInfo{
		ID:            2,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
		NumOfRows:      100,
		CompactionFrom: []int64{1},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg2))
	assert.NoError(t, err)
	seg3 := &datapb.SegmentInfo{
		ID:            3,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
		NumOfRows: 100,
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg3))
	assert.NoError(t, err)
	seg4 := &datapb.SegmentInfo{
		ID:            4,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
		NumOfRows: 100,
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg4))
	assert.NoError(t, err)
	seg5 := &datapb.SegmentInfo{
		ID:            5,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
		NumOfRows:      100,
		CompactionFrom: []int64{3, 4},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg5))
	assert.NoError(t, err)
	seg6 := &datapb.SegmentInfo{
		ID:            6,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
		NumOfRows:      100,
		CompactionFrom: []int64{3, 4},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg6))
	assert.NoError(t, err)
	seg7 := &datapb.SegmentInfo{
		ID:            7,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L0,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg7))
	assert.NoError(t, err)
	seg8 := &datapb.SegmentInfo{
		ID:            8,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
		NumOfRows: 100,
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg8))
	assert.NoError(t, err)
	seg9 := &datapb.SegmentInfo{
		ID:            9,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Importing,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			MsgGroup:    "",
			Timestamp:   1,
		},
		NumOfRows: 100,
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(seg9))
	assert.NoError(t, err)

	view := svr.handler.GetCurrentSegmentsView(context.Background(), &channelMeta{Name: "ch1", CollectionID: 0})
	assert.ElementsMatch(t, []int64{2, 3, 4}, view.FlushedSegmentIDs)
	assert.ElementsMatch(t, []int64{8}, view.GrowingSegmentIDs)
	assert.ElementsMatch(t, []int64{1}, view.DroppedSegmentIDs)
	assert.ElementsMatch(t, []int64{7}, view.L0SegmentIDs)
	assert.ElementsMatch(t, []int64{9}, view.ImportingSegmentIDs)
}

func TestShouldDropChannel(t *testing.T) {
	type myRootCoord struct {
		mocks2.MockRootCoordClient
	}
	myRoot := &myRootCoord{}
	myRoot.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocTimestampResponse{
		Status:    merr.Success(),
		Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
		Count:     1,
	}, nil)

	myRoot.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     int64(tsoutil.ComposeTSByTime(time.Now(), 0)),
		Count:  1,
	}, nil)

	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&collectionInfo{
		ID:     0,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch1",
				Data: []byte{8, 9, 10},
			},
		},
	})
	svr.meta.AddCollection(&collectionInfo{
		ID:     1,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch0",
				Data: []byte{8, 9, 10},
			},
		},
	})

	t.Run("channel name not in kv ", func(t *testing.T) {
		assert.False(t, svr.handler.CheckShouldDropChannel("ch99"))
	})

	t.Run("channel in remove flag", func(t *testing.T) {
		err := svr.meta.catalog.MarkChannelDeleted(context.TODO(), "ch1")
		require.NoError(t, err)
		assert.True(t, svr.handler.CheckShouldDropChannel("ch1"))
	})
}

func TestGetDataVChanPositions(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()
	svr.meta.AddCollection(&collectionInfo{
		ID:     0,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch1",
				Data: []byte{8, 9, 10},
			},
		},
	})
	svr.meta.AddCollection(&collectionInfo{
		ID:     1,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "ch0",
				Data: []byte{8, 9, 10},
			},
		},
	})

	s1 := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Flushed,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
		},
	}
	err := svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s1))
	require.Nil(t, err)
	s2 := &datapb.SegmentInfo{
		ID:            2,
		CollectionID:  0,
		PartitionID:   0,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{1, 2, 3},
			Timestamp:   1,
		},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s2))
	require.Nil(t, err)
	s3 := &datapb.SegmentInfo{
		ID:            3,
		CollectionID:  0,
		PartitionID:   1,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Growing,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{8, 9, 10},
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: "ch1",
			MsgID:       []byte{11, 12, 13},
			Timestamp:   2,
		},
	}
	err = svr.meta.AddSegment(context.TODO(), NewSegmentInfo(s3))
	require.Nil(t, err)

	t.Run("get unexisted channel", func(t *testing.T) {
		vchan := svr.handler.GetDataVChanPositions(&channelMeta{Name: "chx1", CollectionID: 0}, allPartitionID)
		assert.Empty(t, vchan.UnflushedSegmentIds)
		assert.Empty(t, vchan.FlushedSegmentIds)
	})

	t.Run("get existed channel", func(t *testing.T) {
		vchan := svr.handler.GetDataVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0}, allPartitionID)
		assert.EqualValues(t, 1, len(vchan.FlushedSegmentIds))
		assert.EqualValues(t, 1, vchan.FlushedSegmentIds[0])
		assert.EqualValues(t, 2, len(vchan.UnflushedSegmentIds))
		assert.ElementsMatch(t, []int64{s2.ID, s3.ID}, vchan.UnflushedSegmentIds)
	})

	t.Run("empty collection", func(t *testing.T) {
		infos := svr.handler.GetDataVChanPositions(&channelMeta{Name: "ch0_suffix", CollectionID: 1}, allPartitionID)
		assert.EqualValues(t, 1, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 0, len(infos.UnflushedSegmentIds))
	})

	t.Run("filter partition", func(t *testing.T) {
		infos := svr.handler.GetDataVChanPositions(&channelMeta{Name: "ch1", CollectionID: 0}, 1)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, 0, len(infos.FlushedSegmentIds))
		assert.EqualValues(t, 1, len(infos.UnflushedSegmentIds))
	})

	t.Run("empty collection with passed positions", func(t *testing.T) {
		vchannel := "ch_no_segment_1"
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		infos := svr.handler.GetDataVChanPositions(&channelMeta{
			Name:           vchannel,
			CollectionID:   0,
			StartPositions: []*commonpb.KeyDataPair{{Key: pchannel, Data: []byte{14, 15, 16}}},
		}, allPartitionID)
		assert.EqualValues(t, 0, infos.CollectionID)
		assert.EqualValues(t, vchannel, infos.ChannelName)
	})
}
