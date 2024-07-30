// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestClusteringCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionPolicySuite))
}

type ClusteringCompactionPolicySuite struct {
	suite.Suite

	mockAlloc                  *NMockAllocator
	mockTriggerManager         *MockTriggerManager
	testLabel                  *CompactionGroupLabel
	handler                    *NMockHandler
	mockPlanContext            *MockCompactionPlanContext
	catalog                    *mocks.DataCoordCatalog
	clusteringCompactionPolicy *clusteringCompactionPolicy
}

func (s *ClusteringCompactionPolicySuite) SetupTest() {
	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().SavePartitionStatsInfo(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil).Maybe()
	s.catalog = catalog

	segments := genSegmentsForMeta(s.testLabel)
	meta := &meta{segments: NewSegmentsInfo()}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	mockAllocator := newMockAllocator()
	mockHandler := NewNMockHandler(s.T())
	s.handler = mockHandler
	s.clusteringCompactionPolicy = newClusteringCompactionPolicy(meta, mockAllocator, mockHandler)
}

func (s *ClusteringCompactionPolicySuite) TestTrigger() {
	events, err := s.clusteringCompactionPolicy.Trigger()
	s.NoError(err)
	gotViews, ok := events[TriggerTypeClustering]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(0, len(gotViews))
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionAbnormal() {
	// mock error in handler.GetCollection
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{}, errors.New("mock Error")).Once()
	views, triggerID, err := s.clusteringCompactionPolicy.triggerOneCollection(context.TODO(), 1, false)
	s.Error(err)
	s.Nil(views)
	s.Equal(int64(0), triggerID)

	// mock "collection not exist" in handler.GetCollection
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(nil, nil).Once()
	views2, triggerID2, err2 := s.clusteringCompactionPolicy.triggerOneCollection(context.TODO(), 1, false)
	s.NoError(err2)
	s.Nil(views2)
	s.Equal(int64(0), triggerID2)
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionNoClusteringKeySchema() {
	coll := &collectionInfo{
		ID:     100,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	compactionTaskMeta := newTestCompactionTaskMeta(s.T())
	s.clusteringCompactionPolicy.meta = &meta{
		compactionTaskMeta: compactionTaskMeta,
	}
	compactionTaskMeta.SaveCompactionTask(&datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
		State:        datapb.CompactionTaskState_executing,
	})

	views, triggerID, err := s.clusteringCompactionPolicy.triggerOneCollection(context.TODO(), 100, false)
	s.NoError(err)
	s.Nil(views)
	s.Equal(int64(0), triggerID)
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionCompacting() {
	coll := &collectionInfo{
		ID:     100,
		Schema: newTestScalarClusteringKeySchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	compactionTaskMeta := newTestCompactionTaskMeta(s.T())
	s.clusteringCompactionPolicy.meta = &meta{
		compactionTaskMeta: compactionTaskMeta,
	}
	compactionTaskMeta.SaveCompactionTask(&datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
		State:        datapb.CompactionTaskState_executing,
	})

	views3, triggerID3, err3 := s.clusteringCompactionPolicy.triggerOneCollection(context.TODO(), 100, false)
	s.NoError(err3)
	s.Nil(views3)
	s.Equal(int64(1), triggerID3)
}

func (s *ClusteringCompactionPolicySuite) TestCollectionIsClusteringCompacting() {
	s.Run("no collection is compacting", func() {
		compactionTaskMeta := newTestCompactionTaskMeta(s.T())
		s.clusteringCompactionPolicy.meta = &meta{
			compactionTaskMeta: compactionTaskMeta,
		}
		compacting, triggerID := s.clusteringCompactionPolicy.collectionIsClusteringCompacting(collID)
		s.False(compacting)
		s.Equal(int64(0), triggerID)
	})

	s.Run("collection is compacting, different state", func() {
		tests := []struct {
			state        datapb.CompactionTaskState
			isCompacting bool
			triggerID    int64
		}{
			{datapb.CompactionTaskState_pipelining, true, 1},
			{datapb.CompactionTaskState_executing, true, 1},
			{datapb.CompactionTaskState_completed, false, 1},
			{datapb.CompactionTaskState_failed, false, 1},
			{datapb.CompactionTaskState_timeout, false, 1},
			{datapb.CompactionTaskState_analyzing, true, 1},
			{datapb.CompactionTaskState_indexing, true, 1},
			{datapb.CompactionTaskState_cleaned, false, 1},
			{datapb.CompactionTaskState_meta_saved, true, 1},
		}

		for _, test := range tests {
			s.Run(test.state.String(), func() {
				collID := int64(19530)
				compactionTaskMeta := newTestCompactionTaskMeta(s.T())
				s.clusteringCompactionPolicy.meta = &meta{
					compactionTaskMeta: compactionTaskMeta,
				}
				compactionTaskMeta.SaveCompactionTask(&datapb.CompactionTask{
					TriggerID:    1,
					PlanID:       10,
					CollectionID: collID,
					State:        test.state,
				})

				compacting, triggerID := s.clusteringCompactionPolicy.collectionIsClusteringCompacting(collID)
				s.Equal(test.isCompacting, compacting)
				s.Equal(test.triggerID, triggerID)
			})
		}
	})
}

func (s *ClusteringCompactionPolicySuite) TestTimeIntervalLogic() {
	ctx := context.TODO()
	collectionID := int64(100)
	partitionID := int64(101)
	channel := "ch1"

	tests := []struct {
		description    string
		partitionStats []*datapb.PartitionStatsInfo
		currentVersion int64
		segments       []*SegmentInfo
		succeed        bool
	}{
		{"no partition stats and not enough new data", []*datapb.PartitionStatsInfo{}, emptyPartitionStatsVersion, []*SegmentInfo{}, false},
		{"no partition stats and enough new data", []*datapb.PartitionStatsInfo{}, emptyPartitionStatsVersion, []*SegmentInfo{
			{
				size: *atomic.NewInt64(1024 * 1024 * 1024 * 10),
			},
		}, true},
		{"very recent partition stats and enough new data",
			[]*datapb.PartitionStatsInfo{
				{
					CollectionID: collectionID,
					PartitionID:  partitionID,
					VChannel:     channel,
					CommitTime:   time.Now().Unix(),
					Version:      100,
				},
			},
			100,
			[]*SegmentInfo{
				{
					size: *atomic.NewInt64(1024 * 1024 * 1024 * 10),
				},
			}, false},
		{"very old partition stats and not enough new data",
			[]*datapb.PartitionStatsInfo{
				{
					CollectionID: collectionID,
					PartitionID:  partitionID,
					VChannel:     channel,
					CommitTime:   time.Unix(1704038400, 0).Unix(),
					Version:      100,
				},
			},
			100,
			[]*SegmentInfo{
				{
					size: *atomic.NewInt64(1024),
				},
			}, true},
		{"partition stats and enough new data",
			[]*datapb.PartitionStatsInfo{
				{
					CollectionID: collectionID,
					PartitionID:  partitionID,
					VChannel:     channel,
					CommitTime:   time.Now().Add(-3 * time.Hour).Unix(),
					SegmentIDs:   []int64{100000},
					Version:      100,
				},
			},
			100,
			[]*SegmentInfo{
				{
					SegmentInfo: &datapb.SegmentInfo{ID: 9999},
					size:        *atomic.NewInt64(1024 * 1024 * 1024 * 10),
				},
			}, true},
		{"partition stats and not enough new data",
			[]*datapb.PartitionStatsInfo{
				{
					CollectionID: collectionID,
					PartitionID:  partitionID,
					VChannel:     channel,
					CommitTime:   time.Now().Add(-3 * time.Hour).Unix(),
					SegmentIDs:   []int64{100000},
					Version:      100,
				},
			},
			100,
			[]*SegmentInfo{
				{
					SegmentInfo: &datapb.SegmentInfo{ID: 9999},
					size:        *atomic.NewInt64(1024),
				},
			}, false},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			partitionStatsMeta, err := newPartitionStatsMeta(ctx, s.catalog)
			s.NoError(err)
			for _, partitionStats := range test.partitionStats {
				partitionStatsMeta.SavePartitionStatsInfo(partitionStats)
			}
			if test.currentVersion != 0 {
				partitionStatsMeta.partitionStatsInfos[channel][partitionID].currentVersion = test.currentVersion
			}

			meta := &meta{
				partitionStatsMeta: partitionStatsMeta,
			}

			succeed, err := triggerClusteringCompactionPolicy(ctx, meta, collectionID, partitionID, channel, test.segments)
			s.NoError(err)
			s.Equal(test.succeed, succeed)
		})
	}
}
