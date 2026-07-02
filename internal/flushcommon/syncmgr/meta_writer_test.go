package syncmgr

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type MetaWriterSuite struct {
	suite.Suite

	broker    *broker.MockBroker
	metacache *metacache.MockMetaCache

	writer MetaWriter
}

func (s *MetaWriterSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *MetaWriterSuite) SetupTest() {
	s.broker = broker.NewMockBroker(s.T())
	s.metacache = metacache.NewMockMetaCache(s.T())
	s.writer = BrokerMetaWriter(s.broker, 1, retry.Attempts(1))
}

func (s *MetaWriterSuite) TestNormalSave() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1,
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		},
		Statslogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		},
		Deltalogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		},
		Bm25Statslogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		},
	}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
	task := NewSyncTask().WithMetaCache(s.metacache).WithSyncPack(new(SyncPack))
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {
			s.Equal(1, len(req.Field2BinlogPaths))
			s.Equal(1, len(req.Field2Bm25LogPaths))
			s.Equal(1, len(req.Field2StatslogPaths))
			s.Equal(1, len(req.Deltalogs))
			return nil
		})

	err := s.writer.UpdateSync(ctx, task)
	s.NoError(err)
}

func (s *MetaWriterSuite) TestReturnError() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(errors.New("mocked"))

	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	task := NewSyncTask().WithMetaCache(s.metacache).WithSyncPack(new(SyncPack))
	err := s.writer.UpdateSync(ctx, task)
	s.Error(err)
}

func (s *MetaWriterSuite) TestGrowingSourceSyncPersistsColumnGroupBinlogs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             1,
		CollectionID:   3,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
		Statslogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogID:   31,
				LogPath: "stats/100/31",
			}},
		}},
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 0,
			Binlogs: []*datapb.Binlog{{
				LogID:   41,
				LogPath: "delta/41",
			}},
		}},
		Bm25Statslogs: []*datapb.FieldBinlog{{
			FieldID: 102,
			Binlogs: []*datapb.Binlog{{
				LogID:   51,
				LogPath: "bm25/102/51",
			}},
		}},
	}, pkoracle.NewBloomFilterSet(), nil)
	metacache.UpdateNumOfRows(5)(seg)
	s.metacache.EXPECT().GetSegmentByID(int64(1)).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return()

	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Fields: []int64{100}, Format: "parquet"},
		{GroupID: 101, Fields: []int64{101}, Format: "parquet"},
	}
	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(1).
		WithChannelName("ch").
		WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
		WithBatchRows(10).
		WithTargetOffset(15).
		WithMetaCache(s.metacache)
	task.manifestPath = "manifest"
	task.insertBinlogs = buildV3ColumnGroupFieldBinlogs(columnGroups, 0, 0, 0, nil, nil, nil, nil, nil)

	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {
			s.True(req.GetWithFullBinlogs())
			s.EqualValues(storage.StorageV3, req.GetStorageVersion())
			s.Equal("manifest", req.GetManifestPath())
			s.Len(req.GetField2BinlogPaths(), 2)
			s.EqualValues(0, req.GetField2BinlogPaths()[0].GetFieldID())
			s.EqualValues([]int64{100}, req.GetField2BinlogPaths()[0].GetChildFields())
			s.Equal("parquet", req.GetField2BinlogPaths()[0].GetFormat())
			s.Empty(req.GetField2BinlogPaths()[0].GetBinlogs())
			s.EqualValues(101, req.GetField2BinlogPaths()[1].GetFieldID())
			s.EqualValues([]int64{101}, req.GetField2BinlogPaths()[1].GetChildFields())
			s.Len(req.GetField2StatslogPaths(), 1)
			s.EqualValues(100, req.GetField2StatslogPaths()[0].GetFieldID())
			s.Equal("stats/100/31", req.GetField2StatslogPaths()[0].GetBinlogs()[0].GetLogPath())
			s.Len(req.GetDeltalogs(), 1)
			s.Equal("delta/41", req.GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
			s.Len(req.GetField2Bm25LogPaths(), 1)
			s.EqualValues(102, req.GetField2Bm25LogPaths()[0].GetFieldID())
			s.Equal("bm25/102/51", req.GetField2Bm25LogPaths()[0].GetBinlogs()[0].GetLogPath())
			return nil
		})

	err := s.writer.UpdateGrowingSourceSync(ctx, task)
	s.NoError(err)
	s.Len(seg.Binlogs(), 2)
	s.EqualValues([]int64{100}, seg.Binlogs()[0].GetChildFields())
	s.Len(seg.Statslogs(), 1)
	s.Len(seg.Deltalogs(), 1)
	s.Len(seg.Bm25logs(), 1)
}

func (s *MetaWriterSuite) TestGrowingSourceSyncDoesNotAccumulateColumnGroupSkeletons() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Fields: []int64{100}, Format: "parquet"},
		{GroupID: 101, Fields: []int64{101}, Format: "vortex"},
	}
	existingSkeleton := storage.SortFieldBinlogs(buildV3ColumnGroupFieldBinlogs(columnGroups, 0, 0, 0, nil, nil, nil, nil, nil))
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             1,
		CollectionID:   3,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
		Binlogs:        existingSkeleton,
	}, pkoracle.NewBloomFilterSet(), nil)
	metacache.UpdateNumOfRows(5)(seg)

	s.metacache.EXPECT().GetSegmentByID(int64(1)).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return()

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(1).
		WithChannelName("ch").
		WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
		WithBatchRows(10).
		WithTargetOffset(15).
		WithMetaCache(s.metacache)
	task.manifestPath = "manifest"
	task.insertBinlogs = buildV3ColumnGroupFieldBinlogs(columnGroups, 0, 0, 0, nil, nil, nil, nil, nil)

	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {
			s.True(req.GetWithFullBinlogs())
			s.Equal("manifest", req.GetManifestPath())
			s.Len(req.GetField2BinlogPaths(), 2)
			s.EqualValues(0, req.GetField2BinlogPaths()[0].GetFieldID())
			s.EqualValues([]int64{100}, req.GetField2BinlogPaths()[0].GetChildFields())
			s.Equal("parquet", req.GetField2BinlogPaths()[0].GetFormat())
			s.EqualValues(101, req.GetField2BinlogPaths()[1].GetFieldID())
			s.EqualValues([]int64{101}, req.GetField2BinlogPaths()[1].GetChildFields())
			s.Equal("vortex", req.GetField2BinlogPaths()[1].GetFormat())
			return nil
		})

	err := s.writer.UpdateGrowingSourceSync(ctx, task)
	s.NoError(err)
	s.Len(seg.Binlogs(), 2)
	s.EqualValues(0, seg.Binlogs()[0].GetFieldID())
	s.EqualValues(101, seg.Binlogs()[1].GetFieldID())
}

func (s *MetaWriterSuite) TestGrowingSourceSyncMetaErrorsReturnError() {
	testCases := []struct {
		name      string
		err       error
		targetErr error
	}{
		{
			name:      "segment_not_found",
			err:       merr.WrapErrSegmentNotFound(1),
			targetErr: merr.ErrSegmentNotFound,
		},
		{
			name:      "channel_not_found",
			err:       merr.WrapErrChannelNotFound("ch"),
			targetErr: merr.ErrChannelNotFound,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			bfs := pkoracle.NewBloomFilterSet()
			seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
				ID:          1,
				PartitionID: 2,
			}, bfs, nil)
			s.metacache.EXPECT().GetSegmentByID(int64(1)).Return(seg, true).Once()
			s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(tc.err).Once()

			task := NewGrowingSourceSyncTask().
				WithCollectionID(3).
				WithPartitionID(2).
				WithSegmentID(1).
				WithChannelName("ch").
				WithCheckpoint(&msgpb.MsgPosition{Timestamp: 100}).
				WithBatchRows(10).
				WithTargetOffset(10).
				WithMetaCache(s.metacache)
			task.manifestPath = "manifest"

			err := s.writer.UpdateGrowingSourceSync(ctx, task)
			s.ErrorIs(err, tc.targetErr)
		})
	}
}

func TestMetaWriter(t *testing.T) {
	suite.Run(t, new(MetaWriterSuite))
}
