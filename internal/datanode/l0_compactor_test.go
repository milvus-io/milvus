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

package datanode

import (
	"context"
	"path"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	iter "github.com/milvus-io/milvus/internal/datanode/iterators"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

func TestLevelZeroCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(LevelZeroCompactionTaskSuite))
}

type LevelZeroCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *io.MockBinlogIO
	mockAlloc    *allocator.MockAllocator
	mockMeta     *metacache.MockMetaCache
	task         *levelZeroCompactionTask

	dData *storage.DeleteData
	dBlob []byte
}

func (s *LevelZeroCompactionTaskSuite) SetupTest() {
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.mockBinlogIO = io.NewMockBinlogIO(s.T())
	s.mockMeta = metacache.NewMockMetaCache(s.T())
	// plan of the task is unset
	s.task = newLevelZeroCompactionTask(context.Background(), s.mockBinlogIO, s.mockAlloc, s.mockMeta, nil, nil)

	pk2ts := map[int64]uint64{
		1: 20000,
		2: 20001,
		3: 20002,
	}

	s.dData = storage.NewDeleteData([]storage.PrimaryKey{}, []Timestamp{})
	for pk, ts := range pk2ts {
		s.dData.Append(storage.NewInt64PrimaryKey(pk), ts)
	}

	dataCodec := storage.NewDeleteCodec()
	blob, err := dataCodec.Serialize(0, 0, 0, s.dData)
	s.Require().NoError(err)
	s.dBlob = blob.GetValue()
}

func (s *LevelZeroCompactionTaskSuite) TestCompactLinear() {
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
			{SegmentID: 200, Level: datapb.SegmentLevel_L1},
			{SegmentID: 201, Level: datapb.SegmentLevel_L1},
		},
	}

	s.task.plan = plan
	s.task.tr = timerecord.NewTimeRecorder("test")

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return([][]byte{s.dBlob}, nil).Times(2)
	s.mockMeta.EXPECT().PredictSegments(mock.Anything, mock.Anything).Return([]int64{200, 201}, true)
	s.mockMeta.EXPECT().Collection().Return(1)
	s.mockMeta.EXPECT().GetSegmentByID(mock.Anything, mock.Anything).
		RunAndReturn(func(id int64, filters ...metacache.SegmentFilter) (*metacache.SegmentInfo, bool) {
			return metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: id, PartitionID: 10}, nil), true
		})

	s.mockAlloc.EXPECT().AllocOne().Return(19530, nil).Times(2)
	s.mockBinlogIO.EXPECT().JoinFullPath(mock.Anything, mock.Anything).
		RunAndReturn(func(paths ...string) string {
			return path.Join(paths...)
		}).Times(2)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Times(2)

	s.Require().Equal(plan.GetPlanID(), s.task.getPlanID())
	s.Require().Equal(plan.GetChannel(), s.task.getChannelName())
	s.Require().EqualValues(1, s.task.getCollection())

	result, err := s.task.compact()
	s.NoError(err)
	s.NotNil(result)
	s.Equal(commonpb.CompactionState_Completed, result.GetState())
	s.Equal(plan.GetChannel(), result.GetChannel())
	s.Equal(2, len(result.GetSegments()))
	s.ElementsMatch([]int64{200, 201},
		lo.Map(result.GetSegments(), func(seg *datapb.CompactionSegment, _ int) int64 {
			return seg.GetSegmentID()
		}))

	s.EqualValues(plan.GetPlanID(), result.GetPlanID())
	log.Info("test segment results", zap.Any("result", result))

	s.task.complete()
	s.task.stop()
}

func (s *LevelZeroCompactionTaskSuite) TestCompactBatch() {
	s.T().Skip()
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
			{SegmentID: 200, Level: datapb.SegmentLevel_L1},
			{SegmentID: 201, Level: datapb.SegmentLevel_L1},
		},
	}

	s.task.plan = plan
	s.task.tr = timerecord.NewTimeRecorder("test")

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return([][]byte{s.dBlob}, nil).Times(2)
	s.mockMeta.EXPECT().PredictSegments(mock.Anything, mock.Anything).Return([]int64{200, 201}, true)
	s.mockMeta.EXPECT().Collection().Return(1)
	s.mockMeta.EXPECT().GetSegmentByID(mock.Anything, mock.Anything).
		RunAndReturn(func(id int64, filters ...metacache.SegmentFilter) (*metacache.SegmentInfo, bool) {
			return metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: id, PartitionID: 10}, nil), true
		})

	s.mockAlloc.EXPECT().AllocOne().Return(19530, nil).Times(2)
	s.mockBinlogIO.EXPECT().JoinFullPath(mock.Anything, mock.Anything).
		RunAndReturn(func(paths ...string) string {
			return path.Join(paths...)
		}).Times(2)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Times(2)

	result, err := s.task.compact()
	s.NoError(err)
	s.NotNil(result)
	s.Equal(commonpb.CompactionState_Completed, result.GetState())
	s.Equal(plan.GetChannel(), result.GetChannel())
	s.Equal(2, len(result.GetSegments()))
	s.ElementsMatch([]int64{200, 201},
		lo.Map(result.GetSegments(), func(seg *datapb.CompactionSegment, _ int) int64 {
			return seg.GetSegmentID()
		}))

	s.EqualValues(plan.GetPlanID(), result.GetPlanID())
	log.Info("test segment results", zap.Any("result", result))
}

func (s *LevelZeroCompactionTaskSuite) TestUploadByCheck() {
	ctx := context.Background()

	s.Run("upload directly", func() {
		s.SetupTest()
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
		s.mockMeta.EXPECT().Collection().Return(1)
		s.mockMeta.EXPECT().GetSegmentByID(
			mock.MatchedBy(func(ID int64) bool {
				return ID == 100
			}), mock.Anything).
			Return(metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 100, PartitionID: 10}, nil), true)

		s.mockAlloc.EXPECT().AllocOne().Return(19530, nil)
		blobKey := metautil.JoinIDPath(1, 10, 100, 19530)
		blobPath := path.Join(common.SegmentDeltaLogPath, blobKey)
		s.mockBinlogIO.EXPECT().JoinFullPath(mock.Anything, mock.Anything).Return(blobPath)
		segments := map[int64]*storage.DeleteData{100: s.dData}
		results := make(map[int64]*datapb.CompactionSegment)
		err := s.task.uploadByCheck(ctx, false, segments, results)
		s.NoError(err)
		s.Equal(1, len(results))

		seg1, ok := results[100]
		s.True(ok)
		s.EqualValues(100, seg1.GetSegmentID())
		s.Equal(1, len(seg1.GetDeltalogs()))
		s.Equal(1, len(seg1.GetDeltalogs()[0].GetBinlogs()))
	})

	s.Run("check without upload", func() {
		s.SetupTest()
		segments := map[int64]*storage.DeleteData{100: s.dData}
		results := make(map[int64]*datapb.CompactionSegment)
		s.Require().Empty(results)

		err := s.task.uploadByCheck(ctx, true, segments, results)
		s.NoError(err)
		s.Empty(results)
	})

	s.Run("check with upload", func() {
		blobKey := metautil.JoinIDPath(1, 10, 100, 19530)
		blobPath := path.Join(common.SegmentDeltaLogPath, blobKey)

		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
		s.mockMeta.EXPECT().Collection().Return(1)
		s.mockMeta.EXPECT().GetSegmentByID(
			mock.MatchedBy(func(ID int64) bool {
				return ID == 100
			}), mock.Anything).
			Return(metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 100, PartitionID: 10}, nil), true)

		s.mockAlloc.EXPECT().AllocOne().Return(19530, nil)
		s.mockBinlogIO.EXPECT().JoinFullPath(mock.Anything, mock.Anything).Return(blobPath)

		segments := map[int64]*storage.DeleteData{100: s.dData}
		results := map[int64]*datapb.CompactionSegment{
			100: {SegmentID: 100, Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogID: 1}}}}},
		}
		s.Require().Equal(1, len(results))

		paramtable.Get().Save(paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.Key, "1")
		defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.Key)
		err := s.task.uploadByCheck(ctx, true, segments, results)
		s.NoError(err)
		s.NotEmpty(results)
		s.Equal(1, len(results))

		seg1, ok := results[100]
		s.True(ok)
		s.EqualValues(100, seg1.GetSegmentID())
		s.Equal(1, len(seg1.GetDeltalogs()))
		s.Equal(2, len(seg1.GetDeltalogs()[0].GetBinlogs()))
	})
}

func (s *LevelZeroCompactionTaskSuite) TestComposeDeltalog() {
	s.mockMeta.EXPECT().Collection().Return(1)
	s.mockMeta.EXPECT().
		GetSegmentByID(
			mock.MatchedBy(func(ID int64) bool {
				return ID == 100
			}), mock.Anything).
		Return(metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 100, PartitionID: 10}, nil), true)

	s.mockMeta.EXPECT().
		GetSegmentByID(
			mock.MatchedBy(func(ID int64) bool {
				return ID == 101
			}), mock.Anything).
		Return(nil, false)

	s.mockAlloc.EXPECT().AllocOne().Return(19530, nil)

	blobKey := metautil.JoinIDPath(1, 10, 100, 19530)
	blobPath := path.Join(common.SegmentDeltaLogPath, blobKey)
	s.mockBinlogIO.EXPECT().JoinFullPath(mock.Anything, mock.Anything).Return(blobPath)

	kvs, binlog, err := s.task.composeDeltalog(100, s.dData)
	s.NoError(err)
	s.Equal(1, len(kvs))
	v, ok := kvs[blobPath]
	s.True(ok)
	s.NotNil(v)
	s.Equal(blobPath, binlog.LogPath)

	_, _, err = s.task.composeDeltalog(101, s.dData)
	s.Error(err)
}

func (s *LevelZeroCompactionTaskSuite) TestSplitDelta() {
	predicted := []int64{100, 101, 102}
	s.mockMeta.EXPECT().PredictSegments(mock.MatchedBy(func(pk storage.PrimaryKey) bool {
		return pk.GetValue().(int64) == 1
	}), mock.Anything).Return([]int64{100}, true)
	s.mockMeta.EXPECT().PredictSegments(mock.MatchedBy(func(pk storage.PrimaryKey) bool {
		return pk.GetValue().(int64) == 2
	}), mock.Anything).Return(nil, false)
	s.mockMeta.EXPECT().PredictSegments(mock.MatchedBy(func(pk storage.PrimaryKey) bool {
		return pk.GetValue().(int64) == 3
	}), mock.Anything).Return([]int64{100, 101, 102}, true)

	diter, err := iter.NewDeltalogIterator([][]byte{s.dBlob}, nil)
	s.Require().NoError(err)
	s.Require().NotNil(diter)

	targetSegBuffer := make(map[int64]*storage.DeleteData)
	targetSegIDs := predicted
	err = s.task.splitDelta([]*iter.DeltalogIterator{diter}, targetSegBuffer, targetSegIDs)
	s.NoError(err)

	s.NotEmpty(targetSegBuffer)
	s.ElementsMatch(predicted, lo.Keys(targetSegBuffer))
	s.EqualValues(2, targetSegBuffer[100].RowCount)
	s.EqualValues(1, targetSegBuffer[101].RowCount)
	s.EqualValues(1, targetSegBuffer[102].RowCount)

	s.ElementsMatch([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(3)}, targetSegBuffer[100].Pks)
	s.Equal(storage.NewInt64PrimaryKey(3), targetSegBuffer[101].Pks[0])
	s.Equal(storage.NewInt64PrimaryKey(3), targetSegBuffer[102].Pks[0])
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

		expectNilIter bool
		expectError   bool
	}{
		{"no error", []string{"correct"}, false, false},
		{"download error", []string{"error"}, true, true},
		{"new iter error", []string{"invalid-blobs"}, true, true},
	}

	for _, test := range tests {
		iters, err := s.task.loadDelta(ctx, test.paths)
		if test.expectNilIter {
			s.Nil(iters)
		} else {
			s.NotNil(iters)
			s.Equal(1, len(iters))
			s.True(iters[0].HasNext())

			iter := iters[0]
			var pks []storage.PrimaryKey
			var tss []storage.Timestamp
			for iter.HasNext() {
				labeled, err := iter.Next()
				s.NoError(err)
				pks = append(pks, labeled.GetPk())
				tss = append(tss, labeled.GetTimestamp())
			}

			s.ElementsMatch(pks, s.dData.Pks)
			s.ElementsMatch(tss, s.dData.Tss)
		}

		if test.expectError {
			s.Error(err)
		} else {
			s.NoError(err)
		}
	}
}
