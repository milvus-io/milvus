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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func TestGetQueryVChanPositionsRetrieveM2N(t *testing.T) {
	svr := newTestServer(t)
	defer closeTestServer(t, svr)
	schema := newTestSchema()

	channel := "ch1"
	svr.meta.AddCollection(&collectionInfo{
		ID:     1,
		Schema: schema,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  channel,
				Data: []byte{8, 9, 10},
			},
		},
	})
	err := svr.meta.indexMeta.CreateIndex(&model.Index{
		CollectionID: 1,
		FieldID:      2,
		IndexID:      1,
	})
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
			err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
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
	assert.EqualValues(t, 1, info.CollectionID)
	assert.ElementsMatch(t, []int64{700}, info.GetLevelZeroSegmentIds())
	assert.ElementsMatch(t, []int64{100}, info.GetUnflushedSegmentIds())
	assert.ElementsMatch(t, []int64{300, 400, 401, 402, 500, 600}, info.GetFlushedSegmentIds())
	assert.Empty(t, info.GetUnflushedSegments())
	assert.Empty(t, info.GetFlushedSegments())
	assert.Empty(t, info.GetDroppedSegments())
	assert.Empty(t, info.GetDroppedSegmentIds())
	assert.Empty(t, info.GetIndexedSegments())
	assert.Empty(t, info.GetIndexedSegmentIds())
}

func TestGetQueryVChanPositions(t *testing.T) {
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

	err := svr.meta.indexMeta.CreateIndex(&model.Index{
		TenantID:     "",
		CollectionID: 0,
		FieldID:      2,
		IndexID:      1,
	})
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
	err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
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
		},
	}
	partitionIDs := make([]UniqueID, 0)
	partitionIDs = append(partitionIDs, partitionID)
	vChannelInfo := svr.handler.GetQueryVChanPositions(&channelMeta{Name: vchannel, CollectionID: collectionID}, partitionIDs...)
	statsVersions := vChannelInfo.GetPartitionStatsVersions()
	assert.Equal(t, 1, len(statsVersions))
	assert.Equal(t, int64(100), statsVersions[partitionID])
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
		err := svr.meta.indexMeta.CreateIndex(&model.Index{
			TenantID:     "",
			CollectionID: 0,
			FieldID:      2,
			IndexID:      1,
		})
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
		err := svr.meta.indexMeta.CreateIndex(&model.Index{
			TenantID:     "",
			CollectionID: 0,
			FieldID:      2,
			IndexID:      1,
		})
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
		err := svr.meta.indexMeta.CreateIndex(&model.Index{
			TenantID:     "",
			CollectionID: 0,
			FieldID:      2,
			IndexID:      1,
		})
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
		err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
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
		err = svr.meta.indexMeta.AddSegmentIndex(&model.SegmentIndex{
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
