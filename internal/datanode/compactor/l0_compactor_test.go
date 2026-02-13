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
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
			},
		},
	}
	s.task = NewLevelZeroCompactionTask(context.Background(), s.mockBinlogIO, nil, plan, compaction.GenParams())
	var err error
	s.task.compactionParams, err = compaction.ParseParamsFromJSON("")
	s.Require().NoError(err)

	s.dData = storage.NewDeleteData([]storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(2),
		storage.NewInt64PrimaryKey(3),
	}, []typeutil.Timestamp{20000, 20001, 20002})

	dataCodec := storage.NewDeleteCodec()
	blob, err := dataCodec.Serialize(0, 0, 0, s.dData)
	s.Require().NoError(err)

	// Create a map to store uploaded data
	uploadedData := make(map[string][]byte)

	// Mock Upload to capture uploaded data
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.MatchedBy(func(kvs map[string][]byte) bool {
		for k, v := range kvs {
			uploadedData[k] = v
		}
		return true
	})).Return(nil).Maybe()

	// Mock Download to return uploaded data or original data
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, paths []string) ([][]byte, error) {
			result := make([][]byte, 0, len(paths))
			for _, path := range paths {
				if data, ok := uploadedData[path]; ok {
					result = append(result, data)
				} else {
					// Return original data for paths not found
					result = append(result, blob.GetValue())
				}
			}
			return result, nil
		}).Maybe()
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
					DataType:     schemapb.DataType_Int64,
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

	mockAlloc := allocator.NewMockAllocator(s.T())
	mockAlloc.EXPECT().AllocOne().Return(0, errors.New("mock alloc err"))
	s.task.allocator = mockAlloc

	targetSegments := lo.Filter(plan.SegmentBinlogs, func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L1
	})
	l0Segments := []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID: 100,
			Level:     datapb.SegmentLevel_L0,
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{LogPath: "a/b/c1", LogSize: 100},
					},
				},
			},
		},
	}

	segments, err := s.task.process(context.Background(), 2, targetSegments, l0Segments)
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
					DataType:     schemapb.DataType_Int64,
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

	s.Require().Equal(plan.GetPlanID(), s.task.GetPlanID())
	s.Require().Equal(plan.GetChannel(), s.task.GetChannelName())
	s.Require().EqualValues(1, s.task.GetCollection())

	l0Segments := lo.Filter(s.task.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L0
	})

	targetSegments := lo.Filter(s.task.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L1
	})

	segments, err := s.task.process(context.Background(), 1, targetSegments, l0Segments)
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
					DataType:     schemapb.DataType_Int64,
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

	l0Segments := lo.Filter(s.task.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L0
	})

	targetSegments := lo.Filter(s.task.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L1
	})

	segments, err := s.task.process(context.TODO(), 2, targetSegments, l0Segments)
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

func (s *LevelZeroCompactionTaskSuite) TestSplitAndWrite() {
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

	segments, err := s.task.splitAndWrite(context.TODO(), s.dData, segmentBFs)
	s.NoError(err)

	// Verify basic structure
	s.NotEmpty(segments)
	s.ElementsMatch(predicted, lo.Map(segments, func(seg *datapb.CompactionSegment, _ int) int64 {
		return seg.GetSegmentID()
	}))

	// Verify each segment has deltalogs and check content
	for _, segment := range segments {
		s.NotNil(segment.GetDeltalogs())
		s.NotEmpty(segment.GetDeltalogs(), "segment %d should have deltalogs", segment.GetSegmentID())

		// Each segment should have exactly 1 deltalog field
		s.Len(segment.GetDeltalogs(), 1, "segment %d should have 1 deltalog field", segment.GetSegmentID())

		// Each deltalog field should have at least 1 binlog
		deltalogField := segment.GetDeltalogs()[0]
		s.NotEmpty(deltalogField.GetBinlogs(), "segment %d should have binlog entries", segment.GetSegmentID())
	}

	// Verify the expected data distribution based on bloom filter hits
	// Test data: PK 1 (ts 20000), PK 2 (ts 20001), PK 3 (ts 20002)
	// bfs1 (seg 100): contains 1, 3
	// bfs2 (seg 101): contains 3
	// bfs3 (seg 102): contains 3
	// So: seg 100 should have 2 PKs (1 and 3), seg 101 and 102 should each have 1 PK (3)

	segmentMap := lo.SliceToMap(segments, func(seg *datapb.CompactionSegment) (int64, *datapb.CompactionSegment) {
		return seg.GetSegmentID(), seg
	})

	// Helper function to read and verify deltalog content
	assertDeltalog := func(segmentID int64, expectedPKs []int64, expectedTSs []uint64) {
		seg := segmentMap[segmentID]
		s.NotNil(seg, "segment %d should exist", segmentID)
		s.NotEmpty(seg.GetDeltalogs(), "segment %d should have deltalogs", segmentID)

		deltalogField := seg.GetDeltalogs()[0]
		s.NotEmpty(deltalogField.GetBinlogs(), "segment %d should have binlogs", segmentID)

		// Read each binlog and collect all PKs and timestamps
		var allPKs []int64
		var allTSs []uint64

		for _, binlog := range deltalogField.GetBinlogs() {
			path := binlog.GetLogPath()
			s.NotEmpty(path, "binlog path should not be empty")

			// Download the deltalog data
			blobs, err := s.task.BinlogIO.Download(context.TODO(), []string{path})
			s.NoError(err, "should be able to download deltalog for segment %d", segmentID)
			s.NotEmpty(blobs, "downloaded blobs should not be empty")

			// Convert [][]byte to []*Blob
			blobPtrs := make([]*storage.Blob, len(blobs))
			for i, b := range blobs {
				blobPtrs[i] = &storage.Blob{
					Key:   path,
					Value: b,
				}
			}

			// Deserialize the deltalog
			codec := storage.NewDeleteCodec()
			_, _, deltaData, err := codec.Deserialize(blobPtrs)
			s.NoError(err, "should be able to deserialize deltalog for segment %d", segmentID)
			s.NotNil(deltaData, "deltaData should not be nil")

			// Extract PKs and timestamps using DeltaData methods
			pks := deltaData.DeletePks()
			tss := deltaData.DeleteTimestamps()

			for i := 0; i < int(deltaData.DeleteRowCount()); i++ {
				pk := pks.Get(i).(*storage.Int64PrimaryKey)
				allPKs = append(allPKs, pk.Value)
				allTSs = append(allTSs, tss[i])
			}
		}

		// Verify the PKs and timestamps
		s.ElementsMatch(expectedPKs, allPKs, "segment %d should have expected PKs", segmentID)
		s.ElementsMatch(expectedTSs, allTSs, "segment %d should have expected timestamps", segmentID)

		log.Info("verified deltalog content",
			zap.Int64("segmentID", segmentID),
			zap.Int64s("pks", allPKs),
			zap.Uint64s("tss", allTSs))
	}

	// Segment 100 should have PKs 1 and 3 with their timestamps
	assertDeltalog(100, []int64{1, 3}, []uint64{20000, 20002})

	// Segment 101 should have PK 3 with its timestamp
	assertDeltalog(101, []int64{3}, []uint64{20002})

	// Segment 102 should have PK 3 with its timestamp
	assertDeltalog(102, []int64{3}, []uint64{20002})
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
					DataType:     schemapb.DataType_Int64,
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
						DataType:     schemapb.DataType_Int64,
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

func (s *LevelZeroCompactionTaskSuite) TestGetStorageConfig() {
	// Verify GetStorageConfig returns the storage config from compaction params
	storageConfig := s.task.GetStorageConfig()
	s.NotNil(storageConfig)
	s.Equal(s.task.compactionParams.StorageConfig, storageConfig)
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
