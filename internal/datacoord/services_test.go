package datacoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metautil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestBroadcastAlteredCollection(t *testing.T) {
	t.Run("test server is closed", func(t *testing.T) {
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Initializing)
		ctx := context.Background()
		resp, err := s.BroadcastAlteredCollection(ctx, nil)
		assert.NotNil(t, resp.Reason)
		assert.Nil(t, err)
	})

	t.Run("test meta non exist", func(t *testing.T) {
		s := &Server{meta: &meta{collections: make(map[UniqueID]*collectionInfo, 1)}}
		s.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		req := &datapb.AlterCollectionRequest{
			CollectionID: 1,
			PartitionIDs: []int64{1},
			Properties:   []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
		}
		resp, err := s.BroadcastAlteredCollection(ctx, req)
		assert.NotNil(t, resp)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(s.meta.collections))
	})

	t.Run("test update meta", func(t *testing.T) {
		s := &Server{meta: &meta{collections: map[UniqueID]*collectionInfo{
			1: {ID: 1},
		}}}
		s.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		req := &datapb.AlterCollectionRequest{
			CollectionID: 1,
			PartitionIDs: []int64{1},
			Properties:   []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
		}

		assert.Nil(t, s.meta.collections[1].Properties)
		resp, err := s.BroadcastAlteredCollection(ctx, req)
		assert.NotNil(t, resp)
		assert.NoError(t, err)
		assert.NotNil(t, s.meta.collections[1].Properties)
	})
}

func TestServer_GcConfirm(t *testing.T) {
	t.Run("closed server", func(t *testing.T) {
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.GcConfirm(context.TODO(), &datapb.GcConfirmRequest{CollectionId: 100, PartitionId: 10000})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		s := &Server{}
		s.stateCode.Store(commonpb.StateCode_Healthy)

		m := &meta{}
		catalog := mocks.NewDataCoordCatalog(t)
		m.catalog = catalog

		catalog.On("GcConfirm",
			mock.Anything,
			mock.AnythingOfType("int64"),
			mock.AnythingOfType("int64")).
			Return(false)

		s.meta = m

		resp, err := s.GcConfirm(context.TODO(), &datapb.GcConfirmRequest{CollectionId: 100, PartitionId: 10000})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.False(t, resp.GetGcFinished())
	})
}

func TestGetRecoveryInfoV2(t *testing.T) {

	t.Run("test get recovery info with no segments", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
			PartitionIDs: []int64{0},
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 0, len(resp.GetSegments()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.Nil(t, resp.GetChannels()[0].SeekPosition)
	})

	createSegment := func(id, collectionID, partitionID, numOfRows int64, posTs uint64,
		channel string, state commonpb.SegmentState) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:            id,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
			NumOfRows:     numOfRows,
			State:         state,
			DmlPosition: &internalpb.MsgPosition{
				ChannelName: channel,
				MsgID:       []byte{},
				Timestamp:   posTs,
			},
			StartPosition: &internalpb.MsgPosition{
				ChannelName: "",
				MsgID:       []byte{},
				MsgGroup:    "",
				Timestamp:   0,
			},
		}
	}

	t.Run("test get earliest position of flushed segments as seek position", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &internalpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   10,
		})
		assert.NoError(t, err)

		seg1 := createSegment(0, 0, 0, 100, 10, "vchan1", commonpb.SegmentState_Flushed)
		seg1.Binlogs = []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 0, 1, 901),
					},
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 0, 1, 902),
					},
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 0, 1, 903),
					},
				},
			},
		}
		seg2 := createSegment(1, 0, 0, 100, 20, "vchan1", commonpb.SegmentState_Flushed)
		seg2.Binlogs = []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 30,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 1, 1, 801),
					},
					{
						EntriesNum: 70,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 1, 1, 802),
					},
				},
			},
		}
		err = svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.Nil(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
		assert.Nil(t, err)
		mockResp := &indexpb.GetIndexInfoResponse{
			Status: &commonpb.Status{},
			SegmentInfo: map[int64]*indexpb.SegmentInfo{
				seg1.ID: {
					CollectionID: seg1.CollectionID,
					SegmentID:    seg1.ID,
					EnableIndex:  true,
					IndexInfos: []*indexpb.IndexFilePathInfo{
						{
							SegmentID: seg1.ID,
							FieldID:   2,
						},
					},
				},
				seg2.ID: {
					CollectionID: seg2.CollectionID,
					SegmentID:    seg2.ID,
					EnableIndex:  true,
					IndexInfos: []*indexpb.IndexFilePathInfo{
						{
							SegmentID: seg2.ID,
							FieldID:   2,
						},
					},
				},
			},
		}
		svr.indexCoord = mocks.NewIndexCoord(t)
		svr.indexCoord.(*mocks.IndexCoord).EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(mockResp, nil)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
			PartitionIDs: []int64{0},
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.EqualValues(t, 0, len(resp.GetChannels()[0].GetUnflushedSegmentIds()))
		assert.ElementsMatch(t, []int64{0, 1}, resp.GetChannels()[0].GetFlushedSegmentIds())
		assert.EqualValues(t, 10, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		assert.EqualValues(t, 2, len(resp.GetSegments()))
		// Row count corrected from 100 + 100 -> 100 + 60.
		assert.EqualValues(t, 160, resp.GetSegments()[0].GetNumOfRows()+resp.GetSegments()[1].GetNumOfRows())
	})

	t.Run("test get recovery of unflushed segments ", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &internalpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
		})
		assert.NoError(t, err)

		seg1 := createSegment(3, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg1.Binlogs = []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 3, 1, 901),
					},
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 3, 1, 902),
					},
					{
						EntriesNum: 20,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 3, 1, 903),
					},
				},
			},
		}
		seg2 := createSegment(4, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Growing)
		seg2.Binlogs = []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 30,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 4, 1, 801),
					},
					{
						EntriesNum: 70,
						LogPath:    metautil.BuildInsertLogPath("a", 0, 0, 4, 1, 802),
					},
				},
			},
		}
		err = svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.Nil(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
		assert.Nil(t, err)
		svr.indexCoord.(*mocks.IndexCoord).EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(nil, nil)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
			PartitionIDs: []int64{0},
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 0, len(resp.GetSegments()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
	})

	t.Run("test get binlogs", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.meta.AddCollection(&collectionInfo{
			Schema: newTestSchema(),
		})

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		binlogReq := &datapb.SaveBinlogPathsRequest{
			SegmentID:    0,
			CollectionID: 0,
			Field2BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogPath: metautil.BuildInsertLogPath("a", 0, 100, 0, 1, 801),
						},
						{
							LogPath: metautil.BuildInsertLogPath("a", 0, 100, 0, 1, 801),
						},
					},
				},
			},
			Field2StatslogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogPath: metautil.BuildStatsLogPath("a", 0, 100, 0, 1000, 10000),
						},
						{
							LogPath: metautil.BuildStatsLogPath("a", 0, 100, 0, 1000, 10000),
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							TimestampFrom: 0,
							TimestampTo:   1,
							LogPath:       metautil.BuildDeltaLogPath("a", 0, 100, 0, 100000),
							LogSize:       1,
						},
					},
				},
			},
		}
		segment := createSegment(0, 0, 1, 100, 10, "vchan1", commonpb.SegmentState_Flushed)
		err := svr.meta.AddSegment(NewSegmentInfo(segment))
		assert.Nil(t, err)

		mockResp := &indexpb.GetIndexInfoResponse{
			Status: &commonpb.Status{},
			SegmentInfo: map[int64]*indexpb.SegmentInfo{
				segment.ID: {
					CollectionID: segment.CollectionID,
					SegmentID:    segment.ID,
					EnableIndex:  true,
					IndexInfos: []*indexpb.IndexFilePathInfo{
						{
							SegmentID: segment.ID,
							FieldID:   2,
						},
					},
				},
			},
		}
		svr.indexCoord = mocks.NewIndexCoord(t)
		svr.indexCoord.(*mocks.IndexCoord).EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(mockResp, nil)
		err = svr.channelManager.AddNode(0)
		assert.Nil(t, err)
		err = svr.channelManager.Watch(&channel{Name: "vchan1", CollectionID: 0})
		assert.Nil(t, err)

		sResp, err := svr.SaveBinlogPaths(context.TODO(), binlogReq)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, sResp.ErrorCode)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
			PartitionIDs: []int64{0},
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 1, len(resp.GetSegments()))
		assert.EqualValues(t, 0, resp.GetSegments()[0].GetID())
		assert.EqualValues(t, 1, len(resp.GetSegments()[0].GetBinlogs()))
		assert.EqualValues(t, 1, resp.GetSegments()[0].GetBinlogs()[0].GetFieldID())
		for _, binlog := range resp.GetSegments()[0].GetBinlogs()[0].GetBinlogs() {
			assert.Equal(t, "", binlog.GetLogPath())
			assert.Equal(t, int64(801), binlog.GetLogID())
		}
		for _, binlog := range resp.GetSegments()[0].GetStatslogs()[0].GetBinlogs() {
			assert.Equal(t, "", binlog.GetLogPath())
			assert.Equal(t, int64(10000), binlog.GetLogID())
		}
		for _, binlog := range resp.GetSegments()[0].GetDeltalogs()[0].GetBinlogs() {
			assert.Equal(t, "", binlog.GetLogPath())
			assert.Equal(t, int64(100000), binlog.GetLogID())
		}
	})
	t.Run("with dropped segments", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &internalpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
		})
		assert.NoError(t, err)

		seg1 := createSegment(7, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Growing)
		seg2 := createSegment(8, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
		err = svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.Nil(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
		assert.Nil(t, err)
		svr.indexCoord.(*mocks.IndexCoord).EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(nil, nil)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
			PartitionIDs: []int64{0},
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, 0, len(resp.GetSegments()))
		assert.EqualValues(t, 1, len(resp.GetChannels()))
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		assert.Len(t, resp.GetChannels()[0].GetDroppedSegmentIds(), 1)
		assert.Equal(t, UniqueID(8), resp.GetChannels()[0].GetDroppedSegmentIds()[0])
	})

	t.Run("with continuous compaction", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)

		svr.rootCoordClientCreator = func(ctx context.Context, metaRootPath string, etcdCli *clientv3.Client) (types.RootCoord, error) {
			return newMockRootCoordService(), nil
		}

		svr.meta.AddCollection(&collectionInfo{
			ID:     0,
			Schema: newTestSchema(),
		})

		err := svr.meta.UpdateChannelCheckpoint("vchan1", &internalpb.MsgPosition{
			ChannelName: "vchan1",
			Timestamp:   0,
		})
		assert.NoError(t, err)

		seg1 := createSegment(9, 0, 0, 100, 30, "vchan1", commonpb.SegmentState_Dropped)
		seg2 := createSegment(10, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
		seg3 := createSegment(11, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
		seg3.CompactionFrom = []int64{9, 10}
		seg4 := createSegment(12, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Dropped)
		seg5 := createSegment(13, 0, 0, 100, 40, "vchan1", commonpb.SegmentState_Flushed)
		seg5.CompactionFrom = []int64{11, 12}
		err = svr.meta.AddSegment(NewSegmentInfo(seg1))
		assert.Nil(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg2))
		assert.Nil(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg3))
		assert.Nil(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg4))
		assert.Nil(t, err)
		err = svr.meta.AddSegment(NewSegmentInfo(seg5))
		assert.Nil(t, err)
		mockResp := &indexpb.GetIndexInfoResponse{
			Status: &commonpb.Status{},
			SegmentInfo: map[int64]*indexpb.SegmentInfo{
				seg4.ID: {
					CollectionID: seg4.CollectionID,
					SegmentID:    seg4.ID,
					EnableIndex:  true,
					IndexInfos: []*indexpb.IndexFilePathInfo{
						{
							SegmentID: seg4.ID,
							FieldID:   2,
						},
					},
				},
			},
		}
		svr.indexCoord = mocks.NewIndexCoord(t)
		svr.indexCoord.(*mocks.IndexCoord).EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(mockResp, nil)

		req := &datapb.GetRecoveryInfoRequestV2{
			CollectionID: 0,
			PartitionIDs: []int64{0},
		}
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), req)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.NotNil(t, resp.GetChannels()[0].SeekPosition)
		assert.NotEqual(t, 0, resp.GetChannels()[0].GetSeekPosition().GetTimestamp())
		assert.Len(t, resp.GetChannels()[0].GetDroppedSegmentIds(), 0)
		assert.ElementsMatch(t, []UniqueID{9, 10}, resp.GetChannels()[0].GetUnflushedSegmentIds())
		assert.ElementsMatch(t, []UniqueID{12}, resp.GetChannels()[0].GetFlushedSegmentIds())
	})

	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GetRecoveryInfoV2(context.TODO(), &datapb.GetRecoveryInfoRequestV2{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
	})
}

func TestReportDataNodeTtMsgs(t *testing.T) {
	t.Run("with closed server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.ReportDataNodeTtMsgs(context.TODO(), &datapb.ReportDataNodeTtMsgsRequest{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})
}

func TestGcControl(t *testing.T) {
	t.Run("with_closed_server", func(t *testing.T) {
		svr := newTestServer(t, nil)
		closeTestServer(t, svr)
		resp, err := svr.GcControl(context.TODO(), &datapb.GcControlRequest{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})

	t.Run("unknown_cmd", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		resp, err := svr.GcControl(context.TODO(), &datapb.GcControlRequest{
			Command: 0,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("pause", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		resp, err := svr.GcControl(context.TODO(), &datapb.GcControlRequest{
			Command: datapb.GcCommand_Pause,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())

		resp, err = svr.GcControl(context.TODO(), &datapb.GcControlRequest{
			Command: datapb.GcCommand_Pause,
			Params: []*commonpb.KeyValuePair{
				{Key: "duration", Value: "not_int"},
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())

		resp, err = svr.GcControl(context.TODO(), &datapb.GcControlRequest{
			Command: datapb.GcCommand_Pause,
			Params: []*commonpb.KeyValuePair{
				{Key: "duration", Value: "60"},
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("resume", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		resp, err := svr.GcControl(context.TODO(), &datapb.GcControlRequest{
			Command: datapb.GcCommand_Resume,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("timeout", func(t *testing.T) {
		svr := newTestServer(t, nil)
		defer closeTestServer(t, svr)
		svr.garbageCollector.close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp, err := svr.GcControl(ctx, &datapb.GcControlRequest{
			Command: datapb.GcCommand_Resume,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())

		resp, err = svr.GcControl(ctx, &datapb.GcControlRequest{
			Command: datapb.GcCommand_Pause,
			Params: []*commonpb.KeyValuePair{
				{Key: "duration", Value: "60"},
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})
}
