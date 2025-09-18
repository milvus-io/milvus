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

package compactor

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestLevelZeroCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(LevelZeroCompactionTaskSuite))
}

type LevelZeroCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *mock_util.MockBinlogIO
	task         *LevelZeroCompactionTask

	dData *storage.DeleteData
	dBlob []byte
}

func (s *LevelZeroCompactionTaskSuite) SetupTest() {
	paramtable.Init()
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())
	// plan of the task is unset
	plan := &datapb.CompactionPlan{
		PreAllocatedLogIDs: &datapb.IDRange{
			Begin: 200,
			End:   2000,
		},
	}
	s.task = NewLevelZeroCompactionTask(context.Background(), s.mockBinlogIO, nil, plan, compaction.GenParams())
	var err error
	s.task.compactionParams, err = compaction.ParseParamsFromJSON("")
	s.Require().NoError(err)

	pk2ts := map[int64]uint64{
		1: 20000,
		2: 20001,
		3: 20002,
	}

	s.dData = storage.NewDeleteData([]storage.PrimaryKey{}, []typeutil.Timestamp{})
	for pk, ts := range pk2ts {
		s.dData.Append(storage.NewInt64PrimaryKey(pk), ts)
	}

	dataCodec := storage.NewDeleteCodec()
	blob, err := dataCodec.Serialize(0, 0, 0, s.dData)
	s.Require().NoError(err)
	s.dBlob = blob.GetValue()
}

func (s *LevelZeroCompactionTaskSuite) TestGetMaxBatchSize() {
	tests := []struct {
		baseMem        float64
		memLimit       float64
		batchSizeLimit string

		expected    int
		description string
	}{
		{10, 100, "-1", 10, "no limitation on maxBatchSize"},
		{10, 100, "0", 10, "no limitation on maxBatchSize v2"},
		{10, 100, "11", 10, "maxBatchSize == 11"},
		{10, 100, "1", 1, "maxBatchSize == 1"},
		{10, 12, "-1", 1, "no limitation on maxBatchSize"},
		{10, 12, "100", 1, "maxBatchSize == 100"},
	}

	maxSizeK := paramtable.Get().DataNodeCfg.L0CompactionMaxBatchSize.Key
	defer paramtable.Get().Reset(maxSizeK)
	for _, test := range tests {
		s.Run(test.description, func() {
			paramtable.Get().Save(maxSizeK, test.batchSizeLimit)
			defer paramtable.Get().Reset(maxSizeK)

			actual := getMaxBatchSize(test.baseMem, test.memLimit)
			s.Equal(test.expected, actual)
		})
	}
}

func (s *LevelZeroCompactionTaskSuite) TestProcessLoadDeltaFail() {
	plan := &datapb.CompactionPlan{
		PlanID: 19530,
		Type:   datapb.CompactionType_Level0DeleteCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID: 100, Level: datapb.SegmentLevel_L0, Deltalogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{LogPath: "a/b/c1", LogSize: 100},
							{LogPath: "a/b/c2", LogSize: 100},
							{LogPath: "a/b/c3", LogSize: 100},
							{LogPath: "a/b/c4", LogSize: 100},
						},
					},
				},
			},
			{SegmentID: 200, Level: datapb.SegmentLevel_L1},
		},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					IsPrimaryKey: true,
				},
			},
		},
	}

	s.task.plan = plan
	s.task.tr = timerecord.NewTimeRecorder("test")
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("mock download fail")).Once()

	targetSegments := lo.Filter(plan.SegmentBinlogs, func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L1
	})
	deltaLogs := map[int64][]string{100: {"a/b/c1"}}

	segments, err := s.task.process(context.Background(), 1, targetSegments, lo.Values(deltaLogs)...)
	s.Error(err)
	s.Empty(segments)
}

func (s *LevelZeroCompactionTaskSuite) TestProcessUploadByCheckFail() {
	plan := &datapb.CompactionPlan{
		PlanID: 19530,
		Type:   datapb.CompactionType_Level0DeleteCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID: 100, Level: datapb.SegmentLevel_L0, Deltalogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{LogPath: "a/b/c1", LogSize: 100},
							{LogPath: "a/b/c2", LogSize: 100},
							{LogPath: "a/b/c3", LogSize: 100},
							{LogPath: "a/b/c4", LogSize: 100},
						},
					},
				},
			},
			{SegmentID: 200, Level: datapb.SegmentLevel_L1, Field2StatslogPaths: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{LogID: 9999, LogSize: 100},
					},
				},
			}},
		},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					IsPrimaryKey: true,
				},
			},
		},
	}

	s.task.plan = plan
	s.task.tr = timerecord.NewTimeRecorder("test")

	data := &storage.Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &storage.StatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	s.NoError(err)
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).Return([][]byte{sw.GetBuffer()}, nil)
	s.task.cm = cm

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return([][]byte{s.dBlob}, nil).Once()
	mockAlloc := allocator.NewMockAllocator(s.T())
	mockAlloc.EXPECT().AllocOne().Return(0, errors.New("mock alloc err"))
	s.task.allocator = mockAlloc

	targetSegments := lo.Filter(plan.SegmentBinlogs, func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L1
	})
	deltaLogs := map[int64][]string{100: {"a/b/c1"}}

	segments, err := s.task.process(context.Background(), 2, targetSegments, lo.Values(deltaLogs)...)
	s.Error(err)
	s.Empty(segments)
}

func (s *LevelZeroCompactionTaskSuite) TestCompactLinear() {
	plan := &datapb.CompactionPlan{
		PlanID: 19530,
		Type:   datapb.CompactionType_Level0DeleteCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				CollectionID: 1,
				SegmentID:    100, Level: datapb.SegmentLevel_L0, Deltalogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{LogPath: "a/b/c1", LogSize: 100},
							{LogPath: "a/b/c2", LogSize: 100},
							{LogPath: "a/b/c3", LogSize: 100},
							{LogPath: "a/b/c4", LogSize: 100},
						},
					},
				},
			},
			{
				CollectionID: 1,
				SegmentID:    101, Level: datapb.SegmentLevel_L0, Deltalogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{LogPath: "a/d/c1", LogSize: 100},
							{LogPath: "a/d/c2", LogSize: 100},
							{LogPath: "a/d/c3", LogSize: 100},
							{LogPath: "a/d/c4", LogSize: 100},
						},
					},
				},
			},
			{
				CollectionID: 1,
				SegmentID:    200, Level: datapb.SegmentLevel_L1, Field2StatslogPaths: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{LogID: 9999, LogSize: 100},
						},
					},
				},
			},
			{
				CollectionID: 1,
				SegmentID:    201, Level: datapb.SegmentLevel_L1, Field2StatslogPaths: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{LogID: 9999, LogSize: 100},
						},
					},
				},
			},
		},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					IsPrimaryKey: true,
				},
			},
		},
		PreAllocatedLogIDs: &datapb.IDRange{Begin: 11111, End: 21111},
	}

	s.task.plan = plan
	s.task.tr = timerecord.NewTimeRecorder("test")

	data := &storage.Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &storage.StatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	s.NoError(err)
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).Return([][]byte{sw.GetBuffer()}, nil)
	s.task.cm = cm

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return([][]byte{s.dBlob}, nil).Times(1)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Once()

	s.Require().Equal(plan.GetPlanID(), s.task.GetPlanID())
	s.Require().Equal(plan.GetChannel(), s.task.GetChannelName())
	s.Require().EqualValues(1, s.task.GetCollection())

	l0Segments := lo.Filter(s.task.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L0
	})

	targetSegments := lo.Filter(s.task.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L1
	})
	totalDeltalogs := make(map[int64][]string)

	for _, s := range l0Segments {
		paths := []string{}
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				paths = append(paths, l.GetLogPath())
			}
		}
		if len(paths) > 0 {
			totalDeltalogs[s.GetSegmentID()] = paths
		}
	}
	segments, err := s.task.process(context.Background(), 1, targetSegments, lo.Values(totalDeltalogs)...)
	s.NoError(err)
	s.NotEmpty(segments)
	s.Equal(2, len(segments))
	s.ElementsMatch([]int64{200, 201},
		lo.Map(segments, func(seg *datapb.CompactionSegment, _ int) int64 {
			return seg.GetSegmentID()
		}))
	for _, segment := range segments {
		s.NotNil(segment.GetDeltalogs())
	}

	log.Info("test segment results", zap.Any("result", segments))
}

func (s *LevelZeroCompactionTaskSuite) TestCompactBatch() {
	plan := &datapb.CompactionPlan{
		PlanID: 19530,
		Type:   datapb.CompactionType_Level0DeleteCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID: 100, Level: datapb.SegmentLevel_L0, Deltalogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{LogPath: "a/b/c1", LogSize: 100},
							{LogPath: "a/b/c2", LogSize: 100},
							{LogPath: "a/b/c3", LogSize: 100},
							{LogPath: "a/b/c4", LogSize: 100},
						},
					},
				},
			},
			{
				SegmentID: 101, Level: datapb.SegmentLevel_L0, Deltalogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{LogPath: "a/d/c1", LogSize: 100},
							{LogPath: "a/d/c2", LogSize: 100},
							{LogPath: "a/d/c3", LogSize: 100},
							{LogPath: "a/d/c4", LogSize: 100},
						},
					},
				},
			},
			{SegmentID: 200, Level: datapb.SegmentLevel_L1, Field2StatslogPaths: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{LogID: 9999, LogSize: 100},
					},
				},
			}},
			{SegmentID: 201, Level: datapb.SegmentLevel_L1, Field2StatslogPaths: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{LogID: 9999, LogSize: 100},
					},
				},
			}},
		},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					IsPrimaryKey: true,
				},
			},
		},
		PreAllocatedLogIDs: &datapb.IDRange{Begin: 11111, End: 21111},
	}

	s.task.plan = plan
	s.task.tr = timerecord.NewTimeRecorder("test")

	data := &storage.Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &storage.StatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	s.NoError(err)
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).Return([][]byte{sw.GetBuffer()}, nil)
	s.task.cm = cm

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return([][]byte{s.dBlob}, nil).Once()
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Once()

	l0Segments := lo.Filter(s.task.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L0
	})

	targetSegments := lo.Filter(s.task.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L1
	})
	totalDeltalogs := make(map[int64][]string)

	for _, s := range l0Segments {
		paths := []string{}
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				paths = append(paths, l.GetLogPath())
			}
		}
		if len(paths) > 0 {
			totalDeltalogs[s.GetSegmentID()] = paths
		}
	}
	segments, err := s.task.process(context.TODO(), 2, targetSegments, lo.Values(totalDeltalogs)...)
	s.NoError(err)
	s.NotEmpty(segments)
	s.Equal(2, len(segments))
	s.ElementsMatch([]int64{200, 201},
		lo.Map(segments, func(seg *datapb.CompactionSegment, _ int) int64 {
			return seg.GetSegmentID()
		}))
	for _, segment := range segments {
		s.NotNil(segment.GetDeltalogs())
	}

	log.Info("test segment results", zap.Any("result", segments))
}

func (s *LevelZeroCompactionTaskSuite) TestSerializeUpload() {
	ctx := context.Background()
	plan := &datapb.CompactionPlan{
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID: 100,
			},
		},
		PreAllocatedLogIDs: &datapb.IDRange{Begin: 11111, End: 21111},
	}

	s.Run("serializeUpload allocator Alloc failed", func() {
		s.SetupTest()
		s.task.plan = plan
		mockAlloc := allocator.NewMockAllocator(s.T())
		mockAlloc.EXPECT().AllocOne().Return(0, errors.New("mock alloc err"))
		s.task.allocator = mockAlloc

		writer := NewSegmentDeltaWriter(100, 10, 1)
		writer.WriteBatch(s.dData.Pks, s.dData.Tss)
		writers := map[int64]*SegmentDeltaWriter{100: writer}

		result, err := s.task.serializeUpload(ctx, writers)
		s.Error(err)
		s.Equal(0, len(result))
	})

	s.Run("serializeUpload Upload failed", func() {
		s.SetupTest()
		s.task.plan = plan
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(errors.New("mock upload failed"))
		writer := NewSegmentDeltaWriter(100, 10, 1)
		writer.WriteBatch(s.dData.Pks, s.dData.Tss)
		writers := map[int64]*SegmentDeltaWriter{100: writer}

		results, err := s.task.serializeUpload(ctx, writers)
		s.Error(err)
		s.Equal(0, len(results))
	})

	s.Run("upload success", func() {
		s.SetupTest()
		s.task.plan = plan
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
		writer := NewSegmentDeltaWriter(100, 10, 1)
		writer.WriteBatch(s.dData.Pks, s.dData.Tss)
		writers := map[int64]*SegmentDeltaWriter{100: writer}

		results, err := s.task.serializeUpload(ctx, writers)
		s.NoError(err)
		s.Equal(1, len(results))

		seg1 := results[0]
		s.EqualValues(100, seg1.GetSegmentID())
		s.Equal(1, len(seg1.GetDeltalogs()))
		s.Equal(1, len(seg1.GetDeltalogs()[0].GetBinlogs()))
	})
}

func (s *LevelZeroCompactionTaskSuite) TestSplitDelta() {
	bfs1 := pkoracle.NewBloomFilterSetWithBatchSize(100)
	bfs1.UpdatePKRange(&storage.Int64FieldData{Data: []int64{1, 3}})
	bfs2 := pkoracle.NewBloomFilterSetWithBatchSize(100)
	bfs2.UpdatePKRange(&storage.Int64FieldData{Data: []int64{3}})
	bfs3 := pkoracle.NewBloomFilterSetWithBatchSize(100)
	bfs3.UpdatePKRange(&storage.Int64FieldData{Data: []int64{3}})

	predicted := []int64{100, 101, 102}
	segmentBFs := map[int64]*pkoracle.BloomFilterSet{
		100: bfs1,
		101: bfs2,
		102: bfs3,
	}
	deltaWriters := s.task.splitDelta(context.TODO(), s.dData, segmentBFs)

	s.NotEmpty(deltaWriters)
	s.ElementsMatch(predicted, lo.Keys(deltaWriters))
	s.EqualValues(2, deltaWriters[100].GetRowNum())
	s.EqualValues(1, deltaWriters[101].GetRowNum())
	s.EqualValues(1, deltaWriters[102].GetRowNum())

	s.ElementsMatch([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(3)}, deltaWriters[100].deleteData.Pks)
	s.Equal(storage.NewInt64PrimaryKey(3), deltaWriters[101].deleteData.Pks[0])
	s.Equal(storage.NewInt64PrimaryKey(3), deltaWriters[102].deleteData.Pks[0])
}

func (s *LevelZeroCompactionTaskSuite) TestLoadDelta() {
	ctx := context.TODO()

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(
		func(paths []string) bool {
			return len(paths) > 0 && paths[0] == "correct"
		})).Return([][]byte{s.dBlob}, nil).Once()

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(
		func(paths []string) bool {
			return len(paths) > 0 && paths[0] == "error"
		})).Return(nil, errors.New("mock err")).Once()

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(
		func(paths []string) bool {
			return len(paths) > 0 && paths[0] == "invalid-blobs"
		})).Return([][]byte{{1}}, nil).Once()

	tests := []struct {
		description string
		paths       []string

		expectError bool
	}{
		{"no error", []string{"correct"}, false},
		{"download error", []string{"error"}, true},
		{"deserialize error", []string{"invalid-blobs"}, true},
	}

	for _, test := range tests {
		dData, err := s.task.loadDelta(ctx, test.paths)

		if test.expectError {
			s.Error(err)
		} else {
			s.NoError(err)
			s.NotEmpty(dData)
			s.NotNil(dData)
			s.ElementsMatch(s.dData.Pks, dData.Pks)
			s.Equal(s.dData.RowCount, dData.RowCount)
		}
	}
}

func (s *LevelZeroCompactionTaskSuite) TestLoadBF() {
	plan := &datapb.CompactionPlan{
		PlanID: 19530,
		Type:   datapb.CompactionType_Level0DeleteCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{SegmentID: 201, Level: datapb.SegmentLevel_L1, Field2StatslogPaths: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{LogID: 9999, LogSize: 100},
					},
				},
			}},
		},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					IsPrimaryKey: true,
				},
			},
		},
	}

	s.task.plan = plan

	data := &storage.Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &storage.StatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	s.NoError(err)
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).Return([][]byte{sw.GetBuffer()}, nil)
	s.task.cm = cm

	bfs, err := s.task.loadBF(context.Background(), plan.SegmentBinlogs)
	s.NoError(err)

	s.Len(bfs, 1)
	for _, pk := range s.dData.Pks {
		lc := storage.NewLocationsCache(pk)
		s.True(bfs[201].PkExists(lc))
	}
}

func (s *LevelZeroCompactionTaskSuite) TestBuildBinlogWithRootPath() {
	ctx := context.Background()

	// Test cases for different binlog types and parameters
	testCases := []struct {
		name         string
		rootPath     string
		binlogType   storage.BinlogType
		collectionID int64
		partitionID  int64
		segmentID    int64
		fieldID      int64
		logID        int64
		expectError  bool
		expectPrefix string
	}{
		{
			name:         "DeleteBinlog with valid params",
			rootPath:     "/test/root",
			binlogType:   storage.DeleteBinlog,
			collectionID: 1001,
			partitionID:  2001,
			segmentID:    3001,
			fieldID:      -1,
			logID:        4001,
			expectError:  false,
			expectPrefix: "/test/root/delta_log/",
		},
		{
			name:         "InsertBinlog with valid params",
			rootPath:     "/test/root",
			binlogType:   storage.InsertBinlog,
			collectionID: 1002,
			partitionID:  2002,
			segmentID:    3002,
			fieldID:      100,
			logID:        4002,
			expectError:  false,
			expectPrefix: "/test/root/insert_log/",
		},
		{
			name:         "StatsBinlog with valid params",
			rootPath:     "/test/root",
			binlogType:   storage.StatsBinlog,
			collectionID: 1003,
			partitionID:  2003,
			segmentID:    3003,
			fieldID:      101,
			logID:        4003,
			expectError:  false,
			expectPrefix: "/test/root/stats_log/",
		},
		{
			name:         "BM25Binlog with valid params",
			rootPath:     "/test/root",
			binlogType:   storage.BM25Binlog,
			collectionID: 1004,
			partitionID:  2004,
			segmentID:    3004,
			fieldID:      102,
			logID:        4004,
			expectError:  false,
			expectPrefix: "/test/root/bm25_stats/",
		},
		{
			name:         "Empty root path",
			rootPath:     "",
			binlogType:   storage.DeleteBinlog,
			collectionID: 1005,
			partitionID:  2005,
			segmentID:    3005,
			fieldID:      -1,
			logID:        4005,
			expectError:  false,
			expectPrefix: "delta_log/",
		},
		{
			name:         "Invalid binlog type",
			rootPath:     "/test/root",
			binlogType:   storage.BinlogType(999),
			collectionID: 1006,
			partitionID:  2006,
			segmentID:    3006,
			fieldID:      103,
			logID:        4006,
			expectError:  true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			path, err := binlog.BuildLogPathWithRootPath(
				tc.rootPath,
				tc.binlogType,
				tc.collectionID,
				tc.partitionID,
				tc.segmentID,
				tc.fieldID,
				tc.logID,
			)

			if tc.expectError {
				s.Error(err)
				s.Empty(path)
			} else {
				s.NoError(err)
				s.NotEmpty(path)
				s.Contains(path, tc.expectPrefix)
				s.Contains(path, strconv.FormatInt(tc.collectionID, 10))
				s.Contains(path, strconv.FormatInt(tc.partitionID, 10))
				s.Contains(path, strconv.FormatInt(tc.segmentID, 10))
				s.Contains(path, strconv.FormatInt(tc.logID, 10))

				// For non-delete binlogs, check field ID is included
				if tc.binlogType != storage.DeleteBinlog {
					s.Contains(path, strconv.FormatInt(tc.fieldID, 10))
				}
			}
		})
	}

	// Test the actual usage in serializeUpload method context
	s.Run("serializeUpload context usage", func() {
		plan := &datapb.CompactionPlan{
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					CollectionID: 1234,
					PartitionID:  5678,
					SegmentID:    9012,
				},
			},
			PreAllocatedLogIDs: &datapb.IDRange{Begin: 1000, End: 2000},
		}
		s.task.plan = plan
		s.task.compactionParams.StorageConfig.RootPath = "/compaction/root"

		// Create a segment delta writer for testing
		writer := NewSegmentDeltaWriter(9012, 5678, 1234)
		writer.WriteBatch(s.dData.Pks, s.dData.Tss)
		segmentWriters := map[int64]*SegmentDeltaWriter{9012: writer}

		// Mock the upload
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, blobs map[string][]byte) error {
				s.Len(blobs, 1)
				for blobKey := range blobs {
					// Verify the blob key format
					s.Contains(blobKey, "/compaction/root/delta_log/")
					s.Contains(blobKey, "1234") // collection ID
					s.Contains(blobKey, "5678") // partition ID
					s.Contains(blobKey, "9012") // segment ID
				}
				return nil
			},
		).Once()

		results, err := s.task.serializeUpload(ctx, segmentWriters)
		s.NoError(err)
		s.Len(results, 1)
		s.Equal(int64(9012), results[0].GetSegmentID())
		s.NotNil(results[0].GetDeltalogs())
		s.Len(results[0].GetDeltalogs(), 1)
		s.Len(results[0].GetDeltalogs()[0].GetBinlogs(), 1)

		// Verify the log path in result
		logPath := results[0].GetDeltalogs()[0].GetBinlogs()[0].GetLogPath()
		s.Contains(logPath, "/compaction/root/delta_log/")
	})
}

func (s *LevelZeroCompactionTaskSuite) TestFailed() {
	s.Run("no primary key", func() {
		plan := &datapb.CompactionPlan{
			PlanID: 19530,
			Type:   datapb.CompactionType_Level0DeleteCompaction,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 201, Level: datapb.SegmentLevel_L1, Field2StatslogPaths: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{LogID: 9999, LogSize: 100},
						},
					},
				}},
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						IsPrimaryKey: false,
					},
				},
			},
		}

		s.task.plan = plan

		_, err := s.task.loadBF(context.Background(), plan.SegmentBinlogs)
		s.Error(err)
	})

	s.Run("no l1 segments", func() {
		plan := &datapb.CompactionPlan{
			PlanID: 19530,
			Type:   datapb.CompactionType_Level0DeleteCompaction,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 201, Level: datapb.SegmentLevel_L0},
			},
		}

		s.task.plan = plan

		_, err := s.task.Compact()
		s.Error(err)
	})
}

func (s *LevelZeroCompactionTaskSuite) TestLoadBFWithDecompression() {
	s.Run("successful decompression with root path", func() {
		// Test that DecompressBinLogWithRootPath is called with correct parameters
		plan := &datapb.CompactionPlan{
			PlanID: 19530,
			Type:   datapb.CompactionType_Level0DeleteCompaction,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					CollectionID: 100,
					PartitionID:  200,
					SegmentID:    300,
					Level:        datapb.SegmentLevel_L1,
					Field2StatslogPaths: []*datapb.FieldBinlog{
						{
							FieldID: 1,
							Binlogs: []*datapb.Binlog{
								{LogID: 9999, LogSize: 100},
							},
						},
					},
				},
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      1,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
		}

		s.task.plan = plan

		data := &storage.Int64FieldData{
			Data: []int64{1, 2, 3, 4, 5},
		}
		sw := &storage.StatsWriter{}
		err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
		s.NoError(err)

		cm := mocks.NewChunkManager(s.T())
		cm.EXPECT().MultiRead(mock.Anything, mock.Anything).Return([][]byte{sw.GetBuffer()}, nil)
		s.task.cm = cm

		bfs, err := s.task.loadBF(context.Background(), plan.SegmentBinlogs)
		s.NoError(err)
		s.Len(bfs, 1)
		s.Contains(bfs, int64(300))
	})

	s.Run("multiple segments with decompression", func() {
		plan := &datapb.CompactionPlan{
			PlanID: 19531,
			Type:   datapb.CompactionType_Level0DeleteCompaction,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					CollectionID: 100,
					PartitionID:  200,
					SegmentID:    301,
					Level:        datapb.SegmentLevel_L1,
					Field2StatslogPaths: []*datapb.FieldBinlog{
						{
							FieldID: 1,
							Binlogs: []*datapb.Binlog{
								{LogID: 10001, LogSize: 100},
							},
						},
					},
				},
				{
					CollectionID: 100,
					PartitionID:  200,
					SegmentID:    302,
					Level:        datapb.SegmentLevel_L1,
					Field2StatslogPaths: []*datapb.FieldBinlog{
						{
							FieldID: 1,
							Binlogs: []*datapb.Binlog{
								{LogID: 10002, LogSize: 100},
							},
						},
					},
				},
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      1,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
		}

		s.task.plan = plan

		data1 := &storage.Int64FieldData{
			Data: []int64{1, 2, 3},
		}
		data2 := &storage.Int64FieldData{
			Data: []int64{4, 5, 6},
		}
		sw1 := &storage.StatsWriter{}
		err := sw1.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data1)
		s.NoError(err)
		sw2 := &storage.StatsWriter{}
		err = sw2.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data2)
		s.NoError(err)

		cm := mocks.NewChunkManager(s.T())
		cm.EXPECT().MultiRead(mock.Anything, mock.Anything).Return([][]byte{sw1.GetBuffer()}, nil).Once()
		cm.EXPECT().MultiRead(mock.Anything, mock.Anything).Return([][]byte{sw2.GetBuffer()}, nil).Once()
		s.task.cm = cm

		bfs, err := s.task.loadBF(context.Background(), plan.SegmentBinlogs)
		s.NoError(err)
		s.Len(bfs, 2)
		s.Contains(bfs, int64(301))
		s.Contains(bfs, int64(302))
	})

	s.Run("decompression with empty field2statslogpaths", func() {
		plan := &datapb.CompactionPlan{
			PlanID: 19532,
			Type:   datapb.CompactionType_Level0DeleteCompaction,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					CollectionID:        100,
					PartitionID:         200,
					SegmentID:           303,
					Level:               datapb.SegmentLevel_L1,
					Field2StatslogPaths: []*datapb.FieldBinlog{},
				},
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      1,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
		}

		s.task.plan = plan

		cm := mocks.NewChunkManager(s.T())
		// Expect no calls since there are no stats log paths
		s.task.cm = cm

		bfs, err := s.task.loadBF(context.Background(), plan.SegmentBinlogs)
		s.NoError(err)
		s.Len(bfs, 1)
		s.Contains(bfs, int64(303))
		// Bloom filter should be empty since no stats were loaded
	})

	s.Run("verify root path parameter usage", func() {
		// Test that the root path from compactionParams.StorageConfig is used correctly
		plan := &datapb.CompactionPlan{
			PlanID: 19533,
			Type:   datapb.CompactionType_Level0DeleteCompaction,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					CollectionID: 100,
					PartitionID:  200,
					SegmentID:    304,
					Level:        datapb.SegmentLevel_L1,
					Field2StatslogPaths: []*datapb.FieldBinlog{
						{
							FieldID: 1,
							Binlogs: []*datapb.Binlog{
								{LogID: 10003, LogSize: 100, LogPath: ""},
							},
						},
					},
				},
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      1,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
		}

		s.task.plan = plan
		// Verify that compactionParams.StorageConfig.GetRootPath() is used
		s.NotEmpty(s.task.compactionParams.StorageConfig.GetRootPath())

		data := &storage.Int64FieldData{
			Data: []int64{1, 2, 3},
		}
		sw := &storage.StatsWriter{}
		err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
		s.NoError(err)

		cm := mocks.NewChunkManager(s.T())
		cm.EXPECT().MultiRead(mock.Anything, mock.MatchedBy(func(paths []string) bool {
			// Verify the path includes the root path
			return len(paths) > 0
		})).Return([][]byte{sw.GetBuffer()}, nil)
		s.task.cm = cm

		bfs, err := s.task.loadBF(context.Background(), plan.SegmentBinlogs)
		s.NoError(err)
		s.Len(bfs, 1)
		s.Contains(bfs, int64(304))
	})
}
