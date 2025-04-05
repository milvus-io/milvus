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
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestMixCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(MixCompactionTaskSuite))
}

type MixCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *mock_util.MockBinlogIO

	meta      *etcdpb.CollectionMeta
	segWriter *SegmentWriter

	task *mixCompactionTask
}

func (s *MixCompactionTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *MixCompactionTaskSuite) setupTest() {
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())

	s.meta = genTestCollectionMeta()

	paramtable.Get().Save(paramtable.Get().CommonCfg.EntityExpirationTTL.Key, "0")

	plan := &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
		}},
		TimeoutInSeconds:       10,
		Type:                   datapb.CompactionType_MixCompaction,
		Schema:                 s.meta.GetSchema(),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
	}

	s.task = NewMixCompactionTask(context.Background(), s.mockBinlogIO, plan)
}

func (s *MixCompactionTaskSuite) SetupTest() {
	s.setupTest()
}

func (s *MixCompactionTaskSuite) SetupBM25() {
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())
	s.meta = genTestCollectionMetaWithBM25()

	plan := &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
		}},
		TimeoutInSeconds:       10,
		Type:                   datapb.CompactionType_MixCompaction,
		Schema:                 s.meta.GetSchema(),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
	}

	s.task = NewMixCompactionTask(context.Background(), s.mockBinlogIO, plan)
}

func (s *MixCompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *MixCompactionTaskSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
}

func getMilvusBirthday() time.Time {
	return time.Date(2019, time.Month(5), 30, 0, 0, 0, 0, time.UTC)
}

func (s *MixCompactionTaskSuite) prepareCompactDupPKSegments() {
	segments := []int64{7, 8, 9}
	dblobs, err := getInt64DeltaBlobs(
		1,
		[]int64{100},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second), 0)},
	)
	s.Require().NoError(err)

	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"1"}).
		Return([][]byte{dblobs.GetValue()}, nil).Times(len(segments))
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)

	// Clear original segments
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	for _, segID := range segments {
		s.initSegBuffer(1, segID)
		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()

		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "1"}}},
			},
		})
	}
}

func (s *MixCompactionTaskSuite) TestCompactDupPK() {
	s.prepareCompactDupPKSegments()
	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(3, segment.GetNumOfRows())
	s.EqualValues(len(s.task.plan.Schema.Fields), len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskSuite) prepareCompactTwoToOneSegments() {
	segments := []int64{5, 6, 7}
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	for _, segID := range segments {
		s.initSegBuffer(1, segID)
		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()

		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
		})
	}

	// append an empty segment
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		CollectionID: CollectionID,
		PartitionID:  PartitionID,
		ID:           99999,
		NumOfRows:    0,
	}, pkoracle.NewBloomFilterSet(), nil)

	s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
		SegmentID: seg.SegmentID(),
	})
}

func (s *MixCompactionTaskSuite) TestCompactTwoToOne() {
	s.prepareCompactTwoToOneSegments()
	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(3, segment.GetNumOfRows())
	s.EqualValues(len(s.task.plan.Schema.Fields), len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskSuite) prepareCompactTwoToOneWithBM25Segments() {
	s.SetupBM25()
	segments := []int64{5, 6, 7}
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	for _, segID := range segments {
		s.initSegBufferWithBM25(segID)
		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()

		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
		})
	}

	// append an empty segment
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		CollectionID: CollectionID,
		PartitionID:  PartitionID,
		ID:           99999,
		NumOfRows:    0,
	}, pkoracle.NewBloomFilterSet(), nil)

	s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
		SegmentID: seg.SegmentID(),
	})
}

func (s *MixCompactionTaskSuite) TestCompactTwoToOneWithBM25() {
	s.prepareCompactTwoToOneWithBM25Segments()
	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(3, segment.GetNumOfRows())
	s.EqualValues(len(s.task.plan.Schema.Fields), len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.EqualValues(1, len(segment.Bm25Logs))
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskSuite) prepareCompactSortedSegment() {
	segments := []int64{1001, 1002, 1003}
	alloc := allocator.NewLocalAllocator(100, math.MaxInt64)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	deleteTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(10*time.Second), 0)
	for _, segID := range segments {
		s.initMultiRowsSegBuffer(segID, 100, 3)
		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()

		blob, err := getInt64DeltaBlobs(
			segID,
			[]int64{segID, segID + 3, segID + 6},
			[]uint64{deleteTs, deleteTs, deleteTs},
		)
		s.Require().NoError(err)
		deltaPath := fmt.Sprintf("deltalog/%d", segID)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{deltaPath}).
			Return([][]byte{blob.GetValue()}, nil).Once()

		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
			IsSorted:     true,
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{LogPath: deltaPath}}},
			},
		})

	}
}

func (s *MixCompactionTaskSuite) TestCompactSortedSegment() {
	s.prepareCompactSortedSegment()
	paramtable.Get().Save("dataNode.compaction.useMergeSort", "true")
	defer paramtable.Get().Reset("dataNode.compaction.useMergeSort")

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.True(result.GetSegments()[0].GetIsSorted())

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(291, segment.GetNumOfRows())
	s.EqualValues(len(s.task.plan.Schema.Fields), len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskSuite) prepareCompactSortedSegmentLackBinlog() {
	segments := []int64{1001, 1002, 1003}
	alloc := allocator.NewLocalAllocator(100, math.MaxInt64)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	deleteTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(10*time.Second), 0)
	addedFieldSet := typeutil.NewSet[int64]()
	for _, f := range s.meta.GetSchema().GetFields() {
		if !f.Nullable {
			continue
		}
		addedFieldSet.Insert(f.FieldID)
	}

	for _, segID := range segments {
		s.initMultiRowsSegBuffer(segID, 100, 3)
		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)

		for fid, binlog := range fBinlogs {
			if addedFieldSet.Contain(fid) {
				if rand.Intn(2) == 0 {
					continue
				}
				for _, k := range binlog.GetBinlogs() {
					delete(kvs, k.LogPath)
				}
				delete(fBinlogs, fid)
			}
		}

		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()

		blob, err := getInt64DeltaBlobs(
			segID,
			[]int64{segID, segID + 3, segID + 6},
			[]uint64{deleteTs, deleteTs, deleteTs},
		)
		s.Require().NoError(err)
		deltaPath := fmt.Sprintf("deltalog/%d", segID)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{deltaPath}).
			Return([][]byte{blob.GetValue()}, nil).Once()

		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
			IsSorted:     true,
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{LogPath: deltaPath}}},
			},
		})

	}
}

func (s *MixCompactionTaskSuite) TestCompactSortedSegmentLackBinlog() {
	s.prepareCompactSortedSegmentLackBinlog()
	paramtable.Get().Save("dataNode.compaction.useMergeSort", "true")
	defer paramtable.Get().Reset("dataNode.compaction.useMergeSort")

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.True(result.GetSegments()[0].GetIsSorted())

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(291, segment.GetNumOfRows())
	s.EqualValues(len(s.task.plan.Schema.Fields), len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskSuite) prepareSplitMergeEntityExpired() {
	s.initSegBuffer(1, 3)
	collTTL := 864000 // 10 days
	s.task.currentTime = getMilvusBirthday().Add(time.Second * (time.Duration(collTTL) + 1))
	s.task.plan.CollectionTtl = int64(collTTL)
	alloc := allocator.NewLocalAllocator(888888, math.MaxInt64)

	kvs, _, err := serializeWrite(context.TODO(), alloc, s.segWriter)
	s.Require().NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, paths []string) ([][]byte, error) {
			s.Require().Equal(len(paths), len(kvs))
			return lo.Values(kvs), nil
		})
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	s.task.collectionID = CollectionID
	s.task.partitionID = PartitionID
	s.task.maxRows = 1000

	fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(kvs))
	for k := range kvs {
		fieldBinlogs = append(fieldBinlogs, &datapb.FieldBinlog{
			Binlogs: []*datapb.Binlog{
				{
					LogPath: k,
				},
			},
		})
	}
	s.task.plan.SegmentBinlogs[0].FieldBinlogs = fieldBinlogs
}

func (s *MixCompactionTaskSuite) TestSplitMergeEntityExpired() {
	s.prepareSplitMergeEntityExpired()
	compactionSegments, err := s.task.mergeSplit(s.task.ctx)
	s.NoError(err)
	s.Equal(1, len(compactionSegments))
	s.EqualValues(0, compactionSegments[0].GetNumOfRows())
	s.EqualValues(19531, compactionSegments[0].GetSegmentID())
	s.Empty(compactionSegments[0].GetDeltalogs())
	s.Empty(compactionSegments[0].GetInsertLogs())
	s.Empty(compactionSegments[0].GetField2StatslogPaths())
}

func (s *MixCompactionTaskSuite) TestMergeNoExpirationLackBinlog() {
	s.initSegBuffer(1, 4)
	deleteTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(10*time.Second), 0)
	tests := []struct {
		description string
		deletions   map[int64]uint64
		expectedRes int
		leftNumRows int
	}{
		{"no deletion", nil, 1, 1},
		{"mismatch deletion", map[int64]uint64{int64(1): deleteTs}, 1, 1},
		{"deleted pk=4", map[int64]uint64{int64(4): deleteTs}, 1, 0},
	}

	alloc := allocator.NewLocalAllocator(888888, math.MaxInt64)
	addedFieldSet := typeutil.NewSet[int64]()
	for _, f := range s.meta.GetSchema().GetFields() {
		if !f.Nullable {
			continue
		}
		addedFieldSet.Insert(f.FieldID)
	}
	assert.NotEmpty(s.T(), addedFieldSet)

	kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
	for fid, binlog := range fBinlogs {
		if addedFieldSet.Contain(fid) {
			for _, k := range binlog.GetBinlogs() {
				delete(kvs, k.LogPath)
			}
			delete(fBinlogs, fid)
		}
	}
	s.Require().NoError(err)

	for _, test := range tests {
		s.Run(test.description, func() {
			if len(test.deletions) > 0 {
				blob, err := getInt64DeltaBlobs(
					s.segWriter.segmentID,
					lo.Keys(test.deletions),
					lo.Values(test.deletions),
				)
				s.Require().NoError(err)
				s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"foo"}).
					Return([][]byte{blob.GetValue()}, nil).Once()
				s.task.plan.SegmentBinlogs[0].Deltalogs = []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "foo",
							},
						},
					},
				}
			}

			insertPaths := lo.Keys(kvs)
			insertBytes := func() [][]byte {
				res := make([][]byte, 0, len(insertPaths))
				for _, path := range insertPaths {
					res = append(res, kvs[path])
				}
				return res
			}()
			s.mockBinlogIO.EXPECT().Download(mock.Anything, insertPaths).RunAndReturn(
				func(ctx context.Context, paths []string) ([][]byte, error) {
					s.Require().Equal(len(paths), len(kvs))
					return insertBytes, nil
				})
			fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(insertPaths))
			for _, k := range insertPaths {
				fieldBinlogs = append(fieldBinlogs, &datapb.FieldBinlog{
					Binlogs: []*datapb.Binlog{
						{
							LogPath: k,
						},
					},
				})
			}
			s.task.plan.SegmentBinlogs[0].FieldBinlogs = fieldBinlogs

			s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

			s.task.collectionID = CollectionID
			s.task.partitionID = PartitionID
			s.task.maxRows = 1000

			res, err := s.task.mergeSplit(s.task.ctx)
			s.NoError(err)
			s.EqualValues(test.expectedRes, len(res))
			s.EqualValues(test.leftNumRows, res[0].GetNumOfRows())
		})
	}
}

func (s *MixCompactionTaskSuite) TestMergeNoExpiration() {
	s.initSegBuffer(1, 4)
	deleteTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(10*time.Second), 0)
	tests := []struct {
		description string
		deletions   map[int64]uint64
		expectedRes int
		leftNumRows int
	}{
		{"no deletion", nil, 1, 1},
		{"mismatch deletion", map[int64]uint64{int64(1): deleteTs}, 1, 1},
		{"deleted pk=4", map[int64]uint64{int64(4): deleteTs}, 1, 0},
	}

	alloc := allocator.NewLocalAllocator(888888, math.MaxInt64)
	kvs, _, err := serializeWrite(context.TODO(), alloc, s.segWriter)
	s.Require().NoError(err)

	for _, test := range tests {
		s.Run(test.description, func() {
			if len(test.deletions) > 0 {
				blob, err := getInt64DeltaBlobs(
					s.segWriter.segmentID,
					lo.Keys(test.deletions),
					lo.Values(test.deletions),
				)
				s.Require().NoError(err)
				s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"foo"}).
					Return([][]byte{blob.GetValue()}, nil).Once()
				s.task.plan.SegmentBinlogs[0].Deltalogs = []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{
								LogPath: "foo",
							},
						},
					},
				}
			}

			insertPaths := lo.Keys(kvs)
			insertBytes := func() [][]byte {
				res := make([][]byte, 0, len(insertPaths))
				for _, path := range insertPaths {
					res = append(res, kvs[path])
				}
				return res
			}()
			s.mockBinlogIO.EXPECT().Download(mock.Anything, insertPaths).RunAndReturn(
				func(ctx context.Context, paths []string) ([][]byte, error) {
					s.Require().Equal(len(paths), len(kvs))
					return insertBytes, nil
				})
			fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(insertPaths))
			for _, k := range insertPaths {
				fieldBinlogs = append(fieldBinlogs, &datapb.FieldBinlog{
					Binlogs: []*datapb.Binlog{
						{
							LogPath: k,
						},
					},
				})
			}
			s.task.plan.SegmentBinlogs[0].FieldBinlogs = fieldBinlogs

			s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

			s.task.collectionID = CollectionID
			s.task.partitionID = PartitionID
			s.task.maxRows = 1000

			res, err := s.task.mergeSplit(s.task.ctx)
			s.NoError(err)
			s.EqualValues(test.expectedRes, len(res))
			s.EqualValues(test.leftNumRows, res[0].GetNumOfRows())
		})
	}
}

func (s *MixCompactionTaskSuite) TestGetBM25FieldIDs() {
	fieldIDs := GetBM25FieldIDs(&schemapb.CollectionSchema{
		Functions: []*schemapb.FunctionSchema{{}},
	})
	s.Equal(0, len(fieldIDs))

	fieldIDs = GetBM25FieldIDs(genCollectionSchemaWithBM25())
	s.Equal(1, len(fieldIDs))
}

func (s *MixCompactionTaskSuite) TestMergeDeltalogsMultiSegment() {
	tests := []struct {
		segIDA  int64
		dataApk []int64
		dataAts []uint64

		segIDB  int64
		dataBpk []int64
		dataBts []uint64

		segIDC  int64
		dataCpk []int64
		dataCts []uint64

		expectedpk2ts map[int64]uint64
		description   string
	}{
		{
			0, nil, nil,
			100,
			[]int64{1, 2, 3},
			[]uint64{20000, 30000, 20005},
			200,
			[]int64{4, 5, 6},
			[]uint64{50000, 50001, 50002},
			map[int64]uint64{
				1: 20000,
				2: 30000,
				3: 20005,
				4: 50000,
				5: 50001,
				6: 50002,
			},
			"2 segments",
		},
		{
			300,
			[]int64{10, 20},
			[]uint64{20001, 40001},
			100,
			[]int64{1, 2, 3},
			[]uint64{20000, 30000, 20005},
			200,
			[]int64{4, 5, 6},
			[]uint64{50000, 50001, 50002},
			map[int64]uint64{
				10: 20001,
				20: 40001,
				1:  20000,
				2:  30000,
				3:  20005,
				4:  50000,
				5:  50001,
				6:  50002,
			},
			"3 segments",
		},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			dValues := make([][]byte, 0)
			if test.dataApk != nil {
				d, err := getInt64DeltaBlobs(test.segIDA, test.dataApk, test.dataAts)
				s.Require().NoError(err)
				dValues = append(dValues, d.GetValue())
			}
			if test.dataBpk != nil {
				d, err := getInt64DeltaBlobs(test.segIDB, test.dataBpk, test.dataBts)
				s.Require().NoError(err)
				dValues = append(dValues, d.GetValue())
			}
			if test.dataCpk != nil {
				d, err := getInt64DeltaBlobs(test.segIDC, test.dataCpk, test.dataCts)
				s.Require().NoError(err)
				dValues = append(dValues, d.GetValue())
			}

			s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).
				Return(dValues, nil)

			got, err := compaction.ComposeDeleteFromDeltalogs(s.task.ctx, s.task.binlogIO, []string{"random"})
			s.NoError(err)
			s.Equal(len(got), len(test.expectedpk2ts))

			for gotKT, gotV := range got {
				gotK, ok := gotKT.(int64)
				s.Require().True(ok)

				s.EqualValues(test.expectedpk2ts[gotK], gotV)
			}
		})
	}
}

func (s *MixCompactionTaskSuite) TestMergeDeltalogsOneSegment() {
	blob, err := getInt64DeltaBlobs(
		100,
		[]int64{1, 2, 3, 4, 5, 1, 2},
		[]uint64{20000, 20001, 20002, 30000, 50000, 50000, 10000},
	)
	s.Require().NoError(err)

	expectedMap := map[int64]uint64{1: 50000, 2: 20001, 3: 20002, 4: 30000, 5: 50000}

	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"a"}).
		Return([][]byte{blob.GetValue()}, nil).Once()
	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"mock_error"}).
		Return(nil, errors.New("mock_error")).Once()

	invalidPaths := []string{"mock_error"}
	got, err := compaction.ComposeDeleteFromDeltalogs(s.task.ctx, s.task.binlogIO, invalidPaths)
	s.Error(err)
	s.Nil(got)

	dpaths := []string{"a"}
	got, err = compaction.ComposeDeleteFromDeltalogs(s.task.ctx, s.task.binlogIO, dpaths)
	s.NoError(err)
	s.NotNil(got)
	s.Equal(len(expectedMap), len(got))

	for gotKT, gotV := range got {
		gotK, ok := gotKT.(int64)
		s.Require().True(ok)

		s.EqualValues(expectedMap[gotK], gotV)
	}
}

func (s *MixCompactionTaskSuite) TestCompactFail() {
	s.Run("mock ctx done", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		s.task.ctx = ctx
		s.task.cancel = cancel
		_, err := s.task.Compact()
		s.Error(err)
		s.ErrorIs(err, context.Canceled)
	})

	s.Run("Test compact invalid empty segment binlogs", func() {
		s.task.plan.SegmentBinlogs = nil

		_, err := s.task.Compact()
		s.Error(err)
	})

	s.Run("Test compact failed maxSize zero", func() {
		s.task.plan.MaxSize = 0
		_, err := s.task.Compact()
		s.Error(err)
	})
}

func getRow(magic int64) map[int64]interface{} {
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)
	return map[int64]interface{}{
		common.RowIDField:      magic,
		common.TimeStampField:  int64(ts), // should be int64 here
		BoolField:              true,
		Int8Field:              int8(magic),
		Int16Field:             int16(magic),
		Int32Field:             int32(magic),
		Int64Field:             magic,
		FloatField:             float32(magic),
		DoubleField:            float64(magic),
		StringField:            "str",
		VarCharField:           "varchar",
		BinaryVectorField:      []byte{0},
		FloatVectorField:       []float32{4, 5, 6, 7},
		Float16VectorField:     []byte{0, 0, 0, 0, 255, 255, 255, 255},
		BFloat16VectorField:    []byte{0, 0, 0, 0, 255, 255, 255, 255},
		SparseFloatVectorField: typeutil.CreateSparseFloatRow([]uint32{0, 1, 2}, []float32{4, 5, 6}),
		ArrayField: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
			},
		},
		JSONField:                    []byte(`{"batch":ok}`),
		BoolFieldWithDefaultValue:    nil,
		Int8FieldWithDefaultValue:    nil,
		Int16FieldWithDefaultValue:   nil,
		Int32FieldWithDefaultValue:   nil,
		Int64FieldWithDefaultValue:   nil,
		FloatFieldWithDefaultValue:   nil,
		DoubleFieldWithDefaultValue:  nil,
		StringFieldWithDefaultValue:  nil,
		VarCharFieldWithDefaultValue: nil,
	}
}

func (s *MixCompactionTaskSuite) initMultiRowsSegBuffer(magic, numRows, step int64) {
	segWriter, err := NewSegmentWriter(s.meta.GetSchema(), 65535, compactionBatchSize, magic, PartitionID, CollectionID, []int64{})
	s.Require().NoError(err)

	for i := int64(0); i < numRows; i++ {
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(magic + i*step),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
			Value:     getRow(magic + i*step),
		}
		err = segWriter.Write(&v)
		s.Require().NoError(err)
	}

	segWriter.FlushAndIsFull()

	s.segWriter = segWriter
}

func (s *MixCompactionTaskSuite) initSegBufferWithBM25(magic int64) {
	segWriter, err := NewSegmentWriter(s.meta.GetSchema(), 100, compactionBatchSize, magic, PartitionID, CollectionID, []int64{102})
	s.Require().NoError(err)

	v := storage.Value{
		PK:        storage.NewInt64PrimaryKey(magic),
		Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
		Value:     genRowWithBM25(magic),
	}
	err = segWriter.Write(&v)
	s.Require().NoError(err)
	segWriter.FlushAndIsFull()

	s.segWriter = segWriter
}

func (s *MixCompactionTaskSuite) initSegBuffer(size int, seed int64) {
	segWriter, err := NewSegmentWriter(s.meta.GetSchema(), 100, compactionBatchSize, seed, PartitionID, CollectionID, []int64{})
	s.Require().NoError(err)

	for i := 0; i < size; i++ {
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(seed),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
			Value:     getRow(seed),
		}
		err = segWriter.Write(&v)
		s.Require().NoError(err)
	}
	segWriter.FlushAndIsFull()

	s.segWriter = segWriter
}

const (
	CollectionID                 = 1
	PartitionID                  = 1
	SegmentID                    = 1
	BoolField                    = 100
	Int8Field                    = 101
	Int16Field                   = 102
	Int32Field                   = 103
	Int64Field                   = 104
	FloatField                   = 105
	DoubleField                  = 106
	StringField                  = 107
	BinaryVectorField            = 108
	FloatVectorField             = 109
	ArrayField                   = 110
	JSONField                    = 111
	Float16VectorField           = 112
	BFloat16VectorField          = 113
	SparseFloatVectorField       = 114
	VarCharField                 = 115
	BoolFieldWithDefaultValue    = 116
	Int8FieldWithDefaultValue    = 117
	Int16FieldWithDefaultValue   = 118
	Int32FieldWithDefaultValue   = 119
	Int64FieldWithDefaultValue   = 120
	FloatFieldWithDefaultValue   = 121
	DoubleFieldWithDefaultValue  = 122
	StringFieldWithDefaultValue  = 123
	VarCharFieldWithDefaultValue = 124
)

func getInt64DeltaBlobs(segID int64, pks []int64, tss []uint64) (*storage.Blob, error) {
	primaryKeys := make([]storage.PrimaryKey, len(pks))
	for index, v := range pks {
		primaryKeys[index] = storage.NewInt64PrimaryKey(v)
	}
	deltaData := storage.NewDeleteData(primaryKeys, tss)

	dCodec := storage.NewDeleteCodec()
	blob, err := dCodec.Serialize(1, 10, segID, deltaData)
	return blob, err
}

func genTestCollectionMetaWithBM25() *etcdpb.CollectionMeta {
	return &etcdpb.CollectionMeta{
		ID:            CollectionID,
		PartitionTags: []string{"partition_0", "partition_1"},
		Schema:        genCollectionSchemaWithBM25(),
	}
}

func genTestCollectionMeta() *etcdpb.CollectionMeta {
	return &etcdpb.CollectionMeta{
		ID:            CollectionID,
		PartitionTags: []string{"partition_0", "partition_1"},
		Schema: &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  common.RowIDField,
					Name:     "row_id",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  common.TimeStampField,
					Name:     "Timestamp",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  BoolField,
					Name:     "field_bool",
					DataType: schemapb.DataType_Bool,
				},
				{
					FieldID:  Int8Field,
					Name:     "field_int8",
					DataType: schemapb.DataType_Int8,
				},
				{
					FieldID:  Int16Field,
					Name:     "field_int16",
					DataType: schemapb.DataType_Int16,
				},
				{
					FieldID:  Int32Field,
					Name:     "field_int32",
					DataType: schemapb.DataType_Int32,
				},
				{
					FieldID:      Int64Field,
					Name:         "field_int64",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  FloatField,
					Name:     "field_float",
					DataType: schemapb.DataType_Float,
				},
				{
					FieldID:  DoubleField,
					Name:     "field_double",
					DataType: schemapb.DataType_Double,
				},
				{
					FieldID:  StringField,
					Name:     "field_string",
					DataType: schemapb.DataType_String,
					Nullable: true,
				},
				{
					FieldID:  VarCharField,
					Name:     "field_varchar",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "128",
						},
					},
				},
				{
					FieldID:     ArrayField,
					Name:        "field_int32_array",
					Description: "int32 array",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
				},
				{
					FieldID:     JSONField,
					Name:        "field_json",
					Description: "json",
					DataType:    schemapb.DataType_JSON,
				},
				{
					FieldID:  BoolFieldWithDefaultValue,
					Name:     "field_bool_with_default_value",
					DataType: schemapb.DataType_Bool,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: true,
						},
					},
				},
				{
					FieldID:  Int8FieldWithDefaultValue,
					Name:     "field_int8_with_default_value",
					DataType: schemapb.DataType_Int8,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 10,
						},
					},
				},
				{
					FieldID:  Int16FieldWithDefaultValue,
					Name:     "field_int16_with_default_value",
					DataType: schemapb.DataType_Int16,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 10,
						},
					},
				},
				{
					FieldID:  Int32FieldWithDefaultValue,
					Name:     "field_int32_with_default_value",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 10,
						},
					},
				},
				{
					FieldID:      Int64FieldWithDefaultValue,
					Name:         "field_int64_with_default_value",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 10,
						},
					},
				},
				{
					FieldID:  FloatFieldWithDefaultValue,
					Name:     "field_float_with_default_value",
					DataType: schemapb.DataType_Float,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_FloatData{
							FloatData: 10,
						},
					},
				},
				{
					FieldID:  DoubleFieldWithDefaultValue,
					Name:     "field_double_with_default_value",
					DataType: schemapb.DataType_Double,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_DoubleData{
							DoubleData: 10,
						},
					},
				},
				{
					FieldID:  StringFieldWithDefaultValue,
					Name:     "field_string_with_default_value",
					DataType: schemapb.DataType_String,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "a",
						},
					},
				},
				{
					FieldID:  VarCharFieldWithDefaultValue,
					Name:     "field_varchar_with_default_value",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "128",
						},
					},
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "a",
						},
					},
				},
				{
					FieldID:     BinaryVectorField,
					Name:        "field_binary_vector",
					Description: "binary_vector",
					DataType:    schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					FieldID:     FloatVectorField,
					Name:        "field_float_vector",
					Description: "float_vector",
					DataType:    schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
				{
					FieldID:     Float16VectorField,
					Name:        "field_float16_vector",
					Description: "float16_vector",
					DataType:    schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
				{
					FieldID:     BFloat16VectorField,
					Name:        "field_bfloat16_vector",
					Description: "bfloat16_vector",
					DataType:    schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
				{
					FieldID:     SparseFloatVectorField,
					Name:        "field_sparse_float_vector",
					Description: "sparse_float_vector",
					DataType:    schemapb.DataType_SparseFloatVector,
					TypeParams:  []*commonpb.KeyValuePair{},
				},
			},
		},
	}
}

func BenchmarkMixCompactor(b *testing.B) {
	// Setup
	s := new(MixCompactionTaskSuite)

	s.SetT(&testing.T{})
	s.SetupSuite()
	s.SetupTest()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		seq := int64(i * 100000)
		segments := []int64{seq, seq + 1, seq + 2}
		alloc := allocator.NewLocalAllocator(seq+3, math.MaxInt64)
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
		s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
		for _, segID := range segments {
			s.initSegBuffer(100000, segID)
			kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
			s.Require().NoError(err)
			s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
				left, right := lo.Difference(keys, lo.Keys(kvs))
				return len(left) == 0 && len(right) == 0
			})).Return(lo.Values(kvs), nil).Once()

			s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
				SegmentID:    segID,
				FieldBinlogs: lo.Values(fBinlogs),
			})
		}

		b.StartTimer()

		result, err := s.task.Compact()
		s.NoError(err)
		s.NotNil(result)
		s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
		s.Equal(1, len(result.GetSegments()))
		segment := result.GetSegments()[0]
		s.EqualValues(19531, segment.GetSegmentID())
		s.EqualValues(3, segment.GetNumOfRows())
		s.NotEmpty(segment.InsertLogs)
		s.NotEmpty(segment.Field2StatslogPaths)
		s.Empty(segment.Deltalogs)

	}

	s.TearDownTest()
}
