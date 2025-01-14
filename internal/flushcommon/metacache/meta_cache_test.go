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

package metacache

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type MetaCacheSuite struct {
	suite.Suite

	collectionID    int64
	collSchema      *schemapb.CollectionSchema
	vchannel        string
	invaliedSeg     int64
	partitionIDs    []int64
	flushedSegments []int64
	growingSegments []int64
	newSegments     []int64
	cache           MetaCache

	bfsFactory PkStatsFactory
}

func (s *MetaCacheSuite) SetupSuite() {
	paramtable.Init()

	s.collectionID = 1
	s.vchannel = "test"
	s.partitionIDs = []int64{1, 2, 3, 4}
	s.flushedSegments = []int64{1, 2, 3, 4}
	s.growingSegments = []int64{5, 6, 7, 8}
	s.newSegments = []int64{9, 10, 11, 12}
	s.invaliedSeg = 111
	s.bfsFactory = func(*datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}
	s.collSchema = &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			}},
		},
	}
}

func (s *MetaCacheSuite) SetupTest() {
	flushSegmentInfos := lo.RepeatBy(len(s.flushedSegments), func(i int) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:          s.flushedSegments[i],
			PartitionID: s.partitionIDs[i],
			State:       commonpb.SegmentState_Flushed,
		}
	})

	growingSegmentInfos := lo.RepeatBy(len(s.growingSegments), func(i int) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:          s.growingSegments[i],
			PartitionID: s.partitionIDs[i],
			State:       commonpb.SegmentState_Growing,
		}
	})

	s.cache = NewMetaCache(&datapb.ChannelWatchInfo{
		Schema: s.collSchema,
		Vchan: &datapb.VchannelInfo{
			CollectionID:      s.collectionID,
			ChannelName:       s.vchannel,
			FlushedSegments:   flushSegmentInfos,
			UnflushedSegments: growingSegmentInfos,
		},
	}, s.bfsFactory, NoneBm25StatsFactory)
}

func (s *MetaCacheSuite) TestMetaInfo() {
	s.Equal(s.collectionID, s.cache.Collection())
	s.Equal(s.collSchema, s.cache.Schema())
}

func (s *MetaCacheSuite) TestAddSegment() {
	testSegs := []int64{100, 101, 102}
	for _, segID := range testSegs {
		info := &datapb.SegmentInfo{
			ID:          segID,
			PartitionID: 10,
		}
		s.cache.AddSegment(info, func(info *datapb.SegmentInfo) pkoracle.PkStat {
			return pkoracle.NewBloomFilterSet()
		}, NoneBm25StatsFactory, UpdateState(commonpb.SegmentState_Flushed))
	}

	segments := s.cache.GetSegmentsBy(WithSegmentIDs(testSegs...))
	s.Require().Equal(3, len(segments))
	for _, seg := range segments {
		s.Equal(commonpb.SegmentState_Flushed, seg.State())
		s.EqualValues(10, seg.partitionID)

		seg, ok := s.cache.GetSegmentByID(seg.segmentID, WithSegmentState(commonpb.SegmentState_Flushed))
		s.NotNil(seg)
		s.True(ok)
		seg, ok = s.cache.GetSegmentByID(seg.segmentID, WithSegmentState(commonpb.SegmentState_Growing))
		s.Nil(seg)
		s.False(ok)
	}

	gotSegIDs := lo.Map(segments, func(info *SegmentInfo, _ int) int64 {
		return info.segmentID
	})

	s.ElementsMatch(testSegs, gotSegIDs)
}

func (s *MetaCacheSuite) TestUpdateSegments() {
	s.cache.UpdateSegments(UpdateState(commonpb.SegmentState_Flushed), WithSegmentIDs(5))
	segments := s.cache.GetSegmentsBy(WithSegmentIDs(5))
	s.Require().Equal(1, len(segments))
	segment := segments[0]
	s.Equal(commonpb.SegmentState_Flushed, segment.State())
}

func (s *MetaCacheSuite) TestRemoveSegments() {
	ids := s.cache.RemoveSegments()
	s.Empty(ids, "remove without filter shall not succeed")

	ids = s.cache.RemoveSegments(WithSegmentIDs(s.flushedSegments...))
	s.ElementsMatch(s.flushedSegments, ids)

	for _, segID := range s.flushedSegments {
		_, ok := s.cache.GetSegmentByID(segID)
		s.False(ok)
	}
}

func (s *MetaCacheSuite) TestPredictSegments() {
	pk := storage.NewInt64PrimaryKey(100)
	predict, ok := s.cache.PredictSegments(pk)
	s.False(ok)
	s.Empty(predict)

	pkFieldData := &storage.Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7},
	}
	info, got := s.cache.GetSegmentByID(1)
	s.Require().True(got)
	s.Require().NotNil(info)
	err := info.GetBloomFilterSet().UpdatePKRange(pkFieldData)
	s.Require().NoError(err)

	predict, ok = s.cache.PredictSegments(pk, SegmentFilterFunc(func(s *SegmentInfo) bool {
		return s.segmentID == 1
	}))
	s.False(ok)
	s.Empty(predict)

	predict, ok = s.cache.PredictSegments(
		storage.NewInt64PrimaryKey(5),
		SegmentFilterFunc(func(s *SegmentInfo) bool {
			return s.segmentID == 1
		}))
	s.True(ok)
	s.NotEmpty(predict)
	s.Equal(1, len(predict))
	s.EqualValues(1, predict[0])
}

func (s *MetaCacheSuite) Test_DetectMissingSegments() {
	segments := map[int64]struct{}{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {}, 10: {},
	}

	missingSegments := s.cache.DetectMissingSegments(segments)
	s.ElementsMatch(missingSegments, []int64{9, 10})
}

func (s *MetaCacheSuite) Test_UpdateSegmentView() {
	addSegments := []*datapb.SyncSegmentInfo{
		{
			SegmentId:  100,
			PkStatsLog: nil,
			State:      commonpb.SegmentState_Flushed,
			Level:      datapb.SegmentLevel_L1,
			NumOfRows:  10240,
		},
	}
	addSegmentsBF := []*pkoracle.BloomFilterSet{
		pkoracle.NewBloomFilterSet(),
	}
	segments := map[int64]struct{}{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 100: {},
	}

	s.cache.UpdateSegmentView(1, addSegments, addSegmentsBF, segments)

	addSegments = []*datapb.SyncSegmentInfo{
		{
			SegmentId:  101,
			PkStatsLog: nil,
			State:      commonpb.SegmentState_Flushed,
			Level:      datapb.SegmentLevel_L1,
			NumOfRows:  10240,
		},
	}

	segments = map[int64]struct{}{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 101: {},
	}
	s.cache.UpdateSegmentView(1, addSegments, addSegmentsBF, segments)
}

func TestMetaCacheSuite(t *testing.T) {
	suite.Run(t, new(MetaCacheSuite))
}

func BenchmarkGetSegmentsBy(b *testing.B) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			}},
		},
	}
	flushSegmentInfos := lo.RepeatBy(10000, func(i int) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:    int64(i),
			State: commonpb.SegmentState_Flushed,
		}
	})
	cache := NewMetaCache(&datapb.ChannelWatchInfo{
		Schema: schema,
		Vchan: &datapb.VchannelInfo{
			FlushedSegments: flushSegmentInfos,
		},
	}, func(*datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, NoneBm25StatsFactory)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter := WithSegmentIDs(0)
		cache.GetSegmentsBy(filter)
	}
}

func BenchmarkGetSegmentsByWithoutIDs(b *testing.B) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			}},
		},
	}
	flushSegmentInfos := lo.RepeatBy(10000, func(i int) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:    int64(i),
			State: commonpb.SegmentState_Flushed,
		}
	})
	cache := NewMetaCache(&datapb.ChannelWatchInfo{
		Schema: schema,
		Vchan: &datapb.VchannelInfo{
			FlushedSegments: flushSegmentInfos,
		},
	}, func(*datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, NoneBm25StatsFactory)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// use old func filter
		filter := SegmentFilterFunc(func(info *SegmentInfo) bool {
			return info.segmentID == 0
		})
		cache.GetSegmentsBy(filter)
	}
}
