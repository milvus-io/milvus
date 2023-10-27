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
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type MetaCacheSuite struct {
	suite.Suite

	collectionID    int64
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
	s.collectionID = 1
	s.vchannel = "test"
	s.partitionIDs = []int64{1, 2, 3, 4}
	s.flushedSegments = []int64{1, 2, 3, 4}
	s.growingSegments = []int64{5, 6, 7, 8}
	s.newSegments = []int64{9, 10, 11, 12}
	s.invaliedSeg = 111
	s.bfsFactory = func(*datapb.SegmentInfo) *BloomFilterSet {
		return newBloomFilterSet()
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

	s.cache = NewMetaCache(&datapb.VchannelInfo{
		CollectionID:      s.collectionID,
		ChannelName:       s.vchannel,
		FlushedSegments:   flushSegmentInfos,
		UnflushedSegments: growingSegmentInfos,
	}, s.bfsFactory)
}

func (s *MetaCacheSuite) TestNewSegment() {
	for i, seg := range s.newSegments {
		s.cache.NewSegment(seg, s.partitionIDs[i])
	}

	for id, partitionID := range s.partitionIDs {
		segs := s.cache.GetSegmentIDsBy(WithPartitionID(partitionID))
		targets := []int64{s.flushedSegments[id], s.growingSegments[id], s.newSegments[id]}
		s.Equal(len(targets), len(segs))
		for _, seg := range segs {
			s.True(lo.Contains(targets, seg))
		}
	}
}

func (s *MetaCacheSuite) TestCompactSegments() {
	for i, seg := range s.newSegments {
		// compaction from flushed[i], unflushed[i] and invalidSeg to new[i]
		s.cache.CompactSegments(seg, s.partitionIDs[i], s.flushedSegments[i], s.growingSegments[i], s.invaliedSeg)
	}

	for i, partitionID := range s.partitionIDs {
		segs := s.cache.GetSegmentIDsBy(WithPartitionID(partitionID))
		s.Equal(1, len(segs))
		for _, seg := range segs {
			s.Equal(seg, s.newSegments[i])
		}
	}
}

func (s *MetaCacheSuite) TestUpdateSegments() {
	s.cache.UpdateSegments(UpdateState(commonpb.SegmentState_Flushed), WithSegmentID(5))
	segments := s.cache.GetSegmentsBy(WithSegmentID(5))
	s.Require().Equal(1, len(segments))
	segment := segments[0]
	s.Equal(commonpb.SegmentState_Flushed, segment.State())
}

func TestMetaCacheSuite(t *testing.T) {
	suite.Run(t, new(MetaCacheSuite))
}
