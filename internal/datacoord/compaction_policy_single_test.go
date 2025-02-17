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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestSingleCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(SingleCompactionPolicySuite))
}

type SingleCompactionPolicySuite struct {
	suite.Suite

	mockAlloc          *allocator.MockAllocator
	mockTriggerManager *MockTriggerManager
	testLabel          *CompactionGroupLabel
	handler            *NMockHandler
	mockPlanContext    *MockCompactionPlanContext

	singlePolicy *singleCompactionPolicy
}

func (s *SingleCompactionPolicySuite) SetupTest() {
	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := genSegmentsForMeta(s.testLabel)
	meta := &meta{segments: NewSegmentsInfo()}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}

	s.mockAlloc = newMockAllocator(s.T())
	mockHandler := NewNMockHandler(s.T())
	s.handler = mockHandler
	s.singlePolicy = newSingleCompactionPolicy(meta, s.mockAlloc, mockHandler)
}

func (s *SingleCompactionPolicySuite) TestTrigger() {
	events, err := s.singlePolicy.Trigger(context.Background())
	s.NoError(err)
	gotViews, ok := events[TriggerTypeSingle]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(0, len(gotViews))
}

func buildTestSegment(id int64, collId int64, level datapb.SegmentLevel, deleteRows int64, totalRows int64, deltaLogNum int) *SegmentInfo {
	deltaBinlogs := make([]*datapb.Binlog, 0)
	for i := 0; i < deltaLogNum; i++ {
		deltaBinlogs = append(deltaBinlogs, &datapb.Binlog{
			EntriesNum: deleteRows,
		})
	}

	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           id,
			CollectionID: collId,
			Level:        level,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    totalRows,
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: deltaBinlogs,
				},
			},
		},
	}
}

func (s *SingleCompactionPolicySuite) TestIsDeltalogTooManySegment() {
	segment := buildTestSegment(101, collID, datapb.SegmentLevel_L2, 0, 10000, 201)
	s.Equal(true, isDeltalogTooManySegment(segment))
}

func (s *SingleCompactionPolicySuite) TestIsDeleteRowsTooManySegment() {
	segment := buildTestSegment(101, collID, datapb.SegmentLevel_L2, 3000, 10000, 1)
	s.Equal(true, isDeleteRowsTooManySegment(segment))

	segment2 := buildTestSegment(101, collID, datapb.SegmentLevel_L2, 300, 10000, 10)
	s.Equal(true, isDeleteRowsTooManySegment(segment2))
}

func (s *SingleCompactionPolicySuite) TestL2SingleCompaction() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	segments := make(map[UniqueID]*SegmentInfo, 0)
	segments[101] = buildTestSegment(101, collID, datapb.SegmentLevel_L2, 0, 10000, 201)
	segments[102] = buildTestSegment(101, collID, datapb.SegmentLevel_L2, 500, 10000, 10)
	segments[103] = buildTestSegment(101, collID, datapb.SegmentLevel_L2, 100, 10000, 1)
	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: {
					101: segments[101],
					102: segments[102],
					103: segments[103],
				},
			},
		},
	}

	compactionTaskMeta := newTestCompactionTaskMeta(s.T())
	s.singlePolicy.meta = &meta{
		compactionTaskMeta: compactionTaskMeta,
		segments:           segmentsInfo,
	}
	compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: collID,
		State:        datapb.CompactionTaskState_executing,
	})

	views, _, err := s.singlePolicy.triggerOneCollection(context.TODO(), collID, false)
	s.NoError(err)
	s.Equal(2, len(views))
}
