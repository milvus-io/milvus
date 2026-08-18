package syncmgr

import (
	"context"
	"path"
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
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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
	}, bfs, nil, metacache.NewEmptySegmentStats())
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
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil, metacache.NewEmptySegmentStats())
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
	}, pkoracle.NewBloomFilterSet(), nil, nil)
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
	task.insertBinlogs = buildV3ColumnGroupFieldBinlogs(
		columnGroups,
		10,
		101,
		200,
		func(columnGroupID int64) int64 { return 0 },
		func(columnGroupID int64) int64 { return columnGroupID + 1000 },
		func(columnGroupID int64) int64 { return columnGroupID + 2000 },
		nil,
		func(columnGroup storagecommon.ColumnGroup) map[int64]int64 {
			return map[int64]int64{columnGroup.Fields[0]: columnGroup.GroupID}
		},
	)

	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {
			s.True(req.GetWithFullBinlogs())
			s.EqualValues(storage.StorageV3, req.GetStorageVersion())
			s.Equal("manifest", req.GetManifestPath())
			s.Len(req.GetField2BinlogPaths(), 2)
			s.EqualValues(0, req.GetField2BinlogPaths()[0].GetFieldID())
			s.EqualValues([]int64{100}, req.GetField2BinlogPaths()[0].GetChildFields())
			s.Equal("parquet", req.GetField2BinlogPaths()[0].GetFormat())
			s.Len(req.GetField2BinlogPaths()[0].GetBinlogs(), 1)
			s.EqualValues(10, req.GetField2BinlogPaths()[0].GetBinlogs()[0].GetEntriesNum())
			s.EqualValues(101, req.GetField2BinlogPaths()[0].GetBinlogs()[0].GetTimestampFrom())
			s.EqualValues(200, req.GetField2BinlogPaths()[0].GetBinlogs()[0].GetTimestampTo())
			s.EqualValues(1000, req.GetField2BinlogPaths()[0].GetBinlogs()[0].GetMemorySize())
			s.EqualValues(2000, req.GetField2BinlogPaths()[0].GetBinlogs()[0].GetLogID())
			s.EqualValues(0, req.GetField2BinlogPaths()[0].GetBinlogs()[0].GetFieldNullCounts()[100])
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

func (s *MetaWriterSuite) TestGrowingSourceSyncAppendsColumnGroupBinlogs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Fields: []int64{100}, Format: "parquet"},
		{GroupID: 101, Fields: []int64{101}, Format: "vortex"},
	}
	existingBinlogs := storage.SortFieldBinlogs(buildV3ColumnGroupFieldBinlogs(
		columnGroups,
		5,
		10,
		20,
		func(columnGroupID int64) int64 { return 0 },
		func(columnGroupID int64) int64 { return columnGroupID + 100 },
		func(columnGroupID int64) int64 { return columnGroupID + 1000 },
		nil,
		nil,
	))
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             1,
		CollectionID:   3,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
		Binlogs:        existingBinlogs,
	}, pkoracle.NewBloomFilterSet(), nil, nil)
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
	task.insertBinlogs = buildV3ColumnGroupFieldBinlogs(
		columnGroups,
		10,
		30,
		40,
		func(columnGroupID int64) int64 { return 0 },
		func(columnGroupID int64) int64 { return columnGroupID + 200 },
		func(columnGroupID int64) int64 { return columnGroupID + 2000 },
		nil,
		nil,
	)

	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {
			s.True(req.GetWithFullBinlogs())
			s.Equal("manifest", req.GetManifestPath())
			s.Len(req.GetField2BinlogPaths(), 4)
			s.EqualValues(0, req.GetField2BinlogPaths()[0].GetFieldID())
			s.EqualValues([]int64{100}, req.GetField2BinlogPaths()[0].GetChildFields())
			s.Equal("parquet", req.GetField2BinlogPaths()[0].GetFormat())
			s.EqualValues(5, req.GetField2BinlogPaths()[0].GetBinlogs()[0].GetEntriesNum())
			s.EqualValues(101, req.GetField2BinlogPaths()[1].GetFieldID())
			s.EqualValues(10, req.GetField2BinlogPaths()[2].GetBinlogs()[0].GetEntriesNum())
			s.EqualValues(30, req.GetField2BinlogPaths()[2].GetBinlogs()[0].GetTimestampFrom())
			return nil
		})

	err := s.writer.UpdateGrowingSourceSync(ctx, task)
	s.NoError(err)
	s.Len(seg.Binlogs(), 4)
	s.EqualValues(0, seg.Binlogs()[0].GetFieldID())
	s.EqualValues(101, seg.Binlogs()[1].GetFieldID())
	s.EqualValues(0, seg.Binlogs()[2].GetFieldID())
	s.EqualValues(101, seg.Binlogs()[3].GetFieldID())
}

// TestGrowingSourceSyncShipsStats verifies the V3 growing-source flush ships a
// Statistics whose StatsBinlogSize is sourced from the committed manifest
// (bloom-filter + BM25 footprint), not left at 0 from empty V3 statslog arrays.
func (s *MetaWriterSuite) TestGrowingSourceSyncShipsStats() {
	ctx := context.Background()

	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: s.T().TempDir()}
	basePath := "files/growing_stats/seg1"
	bloomPath := path.Join(cfg.RootPath, basePath, "_stats/bloom_filter.100/1")
	bm25Path := path.Join(cfg.RootPath, basePath, "_stats/bm25.102/1")
	s.Require().NoError(packed.WriteFile(cfg, bloomPath, []byte("bloom")))
	s.Require().NoError(packed.WriteFile(cfg, bm25Path, []byte("bm25")))
	manifestPath, err := packed.CommitManifestUpdates(basePath, packed.ManifestEarliest, cfg, &packed.ManifestUpdates{
		Stats: []packed.StatEntry{
			{Key: "bloom_filter.100", Files: []string{bloomPath}, Metadata: map[string]string{"memory_size": "10"}},
			{Key: "bm25.102", Files: []string{bm25Path}, Metadata: map[string]string{"memory_size": "20"}},
		},
	})
	s.Require().NoError(err)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             1,
		CollectionID:   3,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
	}, pkoracle.NewBloomFilterSet(), nil, nil)
	metacache.UpdateNumOfRows(10)(seg)

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
		WithTargetOffset(10).
		WithMetaCache(s.metacache).
		WithStorageConfig(cfg)
	task.manifestPath = manifestPath
	task.insertBinlogs = map[int64]*datapb.FieldBinlog{
		0: {FieldID: 0, ChildFields: []int64{100}, Binlogs: []*datapb.Binlog{
			{LogID: 1, EntriesNum: 10, MemorySize: 1000, TimestampFrom: 101, TimestampTo: 200},
		}},
	}

	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {
			s.Require().NotNil(req.GetStats())
			s.EqualValues(30, req.GetStats().GetStatsBinlogSize()) // bloom(10)+bm25(20)
			s.EqualValues(1000, req.GetStats().GetInsertBinlogSize())
			s.EqualValues(1, req.GetStats().GetInsertBinlogCount())
			return nil
		})

	err = s.writer.UpdateGrowingSourceSync(ctx, task)
	s.NoError(err)
}

// TestGrowingSourceSyncShipsCumulativeStatsAfterRestart verifies that a
// post-recovery V3 growing segment (empty in-memory binlog arrays, cumulative
// state restored onto the collector) ships a Statistics that reflects the
// restored baseline plus this batch — not this batch alone. Rebuilding from the
// empty arrays would undercount InsertBinlogSize/count and drop delta aggregates
// until a later compaction.
func (s *MetaWriterSuite) TestGrowingSourceSyncShipsCumulativeStatsAfterRestart() {
	ctx := context.Background()

	// Cumulative state as persisted before restart; the binlog arrays are empty
	// because V3 skips their per-field KVs.
	restored := metacache.NewSegmentStatsFromStats(&datapb.Statistics{
		InsertBinlogSize:  100000,
		InsertBinlogCount: 50,
		DeltaBinlogSize:   500,
		DeltaBinlogCount:  2,
		DeleteNumRows:     7,
	}, 1000)
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             1,
		CollectionID:   3,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
	}, pkoracle.NewBloomFilterSet(), nil, restored)
	metacache.UpdateNumOfRows(1000)(seg)

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
		WithTargetOffset(10).
		WithMetaCache(s.metacache)
	task.manifestPath = "manifest"
	// This batch's insert binlog (arrays on the segment are empty post-restart).
	task.insertBinlogs = map[int64]*datapb.FieldBinlog{
		0: {FieldID: 0, ChildFields: []int64{100}, Binlogs: []*datapb.Binlog{
			{LogID: 2000, EntriesNum: 10, MemorySize: 1000, TimestampFrom: 101, TimestampTo: 200, FieldNullCounts: map[int64]int64{100: 0}},
		}},
	}

	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {
			s.Require().NotNil(req.GetStats())
			// restored baseline + this batch, not this batch alone.
			s.EqualValues(101000, req.GetStats().GetInsertBinlogSize())
			s.EqualValues(51, req.GetStats().GetInsertBinlogCount())
			// delta aggregates preserved from the restored collector.
			s.EqualValues(7, req.GetStats().GetDeleteNumRows())
			s.EqualValues(2, req.GetStats().GetDeltaBinlogCount())
			s.EqualValues(500, req.GetStats().GetDeltaBinlogSize())
			return nil
		})

	err := s.writer.UpdateGrowingSourceSync(ctx, task)
	s.NoError(err)
	// The digested cumulative collector is installed back so the next batch keeps
	// accumulating instead of resetting to the restored baseline.
	s.Require().NotNil(seg.Statistics())
	s.EqualValues(101000, seg.Statistics().Publish().GetInsertBinlogSize())
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
				ID:             1,
				PartitionID:    2,
				StorageVersion: storage.StorageV3,
			}, bfs, nil, nil)
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
