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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestClusteringCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionPolicySuite))
}

type ClusteringCompactionPolicySuite struct {
	suite.Suite

	mockAlloc          *allocator.MockAllocator
	mockTriggerManager *MockTriggerManager
	handler            *NMockHandler
	inspector          *MockCompactionInspector
	catalog            *mocks.DataCoordCatalog
	meta               *meta

	clusteringCompactionPolicy *clusteringCompactionPolicy
}

func (s *ClusteringCompactionPolicySuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	s.catalog = catalog

	compactionTaskMeta, _ := newCompactionTaskMeta(context.TODO(), s.catalog)
	partitionStatsMeta, _ := newPartitionStatsMeta(context.TODO(), s.catalog)
	indexMeta, _ := newIndexMeta(context.TODO(), s.catalog, nil)

	store := metacache.NewMetaStore(nil)
	meta := &meta{
		segments:           NewSegmentsInfo(store),
		metaStore:          store,
		compactionTaskMeta: compactionTaskMeta,
		partitionStatsMeta: partitionStatsMeta,
		indexMeta:          indexMeta,
	}
	s.meta = meta

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocID(mock.Anything).Return(19530, nil).Maybe()
	mockHandler := NewNMockHandler(s.T())
	s.handler = mockHandler
	s.clusteringCompactionPolicy = newClusteringCompactionPolicy(s.meta, mockAllocator, mockHandler)
}

func (s *ClusteringCompactionPolicySuite) TestEnable() {
	// by default
	s.False(s.clusteringCompactionPolicy.Enable())
	// enable
	enableAutoCompactionKey := paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key
	clusteringCompactionEnableKey := paramtable.Get().DataCoordCfg.ClusteringCompactionEnable.Key
	clusteringCompactionAutoEnableKey := paramtable.Get().DataCoordCfg.ClusteringCompactionAutoEnable.Key
	paramtable.Get().Save(enableAutoCompactionKey, "true")
	paramtable.Get().Save(clusteringCompactionEnableKey, "true")
	paramtable.Get().Save(clusteringCompactionAutoEnableKey, "true")
	defer paramtable.Get().Reset(enableAutoCompactionKey)
	defer paramtable.Get().Reset(clusteringCompactionEnableKey)
	defer paramtable.Get().Reset(clusteringCompactionAutoEnableKey)
	s.True(s.clusteringCompactionPolicy.Enable())
}

func (s *ClusteringCompactionPolicySuite) TestTriggerWithNoCollecitons() {
	// trigger with no collections
	events, err := s.clusteringCompactionPolicy.Trigger(context.Background())
	s.NoError(err)
	gotViews, ok := events[TriggerTypeClustering]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(0, len(gotViews))
}

func (s *ClusteringCompactionPolicySuite) TestTriggerWithCollections() {
	// valid collection
	s.meta.metaStore.PutCollection(&collectionInfo{
		ID:     1,
		Schema: newTestScalarClusteringKeySchema(),
	})
	// deleted collection
	s.meta.metaStore.PutCollection(&collectionInfo{
		ID:     2,
		Schema: newTestScalarClusteringKeySchema(),
	})
	s.clusteringCompactionPolicy.meta = s.meta

	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collectionID int64) (*collectionInfo, error) {
		if collectionID == 2 {
			return nil, errors.New("mock get collection fail error")
		}
		coll, exist := s.meta.metaStore.GetCollection(collectionID)
		if exist {
			return coll, nil
		}
		return nil, nil
	})

	// trigger
	events, err := s.clusteringCompactionPolicy.Trigger(context.Background())
	s.NoError(err)
	gotViews, ok := events[TriggerTypeClustering]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(0, len(gotViews))
}

func (s *ClusteringCompactionPolicySuite) TestCalculateClusteringCompactionConfig() {
	testCases := []struct {
		description       string
		coll              *collectionInfo
		view              CompactionView
		totalRows         int64
		maxSegmentRows    int64
		preferSegmentRows int64
		err               error
	}{
		{
			description: "",
			coll: &collectionInfo{
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{
							DataType: schemapb.DataType_Int64,
						},
						{
							DataType: schemapb.DataType_FloatVector,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: common.DimKey, Value: "128"},
							},
						},
					},
				},
			},
			view: &ClusteringSegmentsView{
				segments: []*SegmentView{
					{
						NumOfRows: 1000,
					},
				},
			},
			totalRows:         int64(1000),
			maxSegmentRows:    int64(2064888),
			preferSegmentRows: int64(1651910),
			err:               nil,
		},
	}

	for _, test := range testCases {
		s.Run(test.description, func() {
			expectedSegmentSize := getExpectedSegmentSize(s.meta, test.coll.ID, test.coll.Schema)
			if view, ok := test.view.(*ClusteringSegmentsView); ok {
				for _, segment := range view.segments {
					if segment == nil || segment.NumOfRows <= 0 || test.maxSegmentRows == 0 {
						continue
					}
					segment.Size = float64(expectedSegmentSize) * float64(segment.NumOfRows) / float64(test.maxSegmentRows)
				}
			}
			totalRows, maxSegmentRows, preferSegmentRows, err := calculateClusteringCompactionConfig(test.coll, test.view, expectedSegmentSize)
			s.Equal(test.totalRows, totalRows)
			s.Equal(test.maxSegmentRows, maxSegmentRows)
			s.Equal(test.preferSegmentRows, preferSegmentRows)
			s.Equal(test.err, err)
		})
	}
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

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionSkipExternal() {
	collID := int64(100)
	s.handler.EXPECT().GetCollection(mock.Anything, collID).Return(&collectionInfo{
		ID: collID,
		Schema: func() *schemapb.CollectionSchema {
			schema := newTestScalarClusteringKeySchema()
			schema.ExternalSource = "s3://external"
			// External collections are identified by having ExternalField set on fields.
			schema.Fields[0].ExternalField = "field1_col"
			return schema
		}(),
	}, nil)

	views, triggerID, err := s.clusteringCompactionPolicy.triggerOneCollection(context.Background(), collID, false)
	s.NoError(err)
	s.Nil(views)
	s.EqualValues(0, triggerID)
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionNoClusteringKeySchema() {
	ctx := context.Background()
	coll := &collectionInfo{
		ID:     100,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	s.meta.compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
		Type:         datapb.CompactionType_ClusteringCompaction,
		State:        datapb.CompactionTaskState_executing,
	})

	views, triggerID, err := s.clusteringCompactionPolicy.triggerOneCollection(context.TODO(), 100, false)
	s.NoError(err)
	s.Nil(views)
	s.Equal(int64(0), triggerID)
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionCompacting() {
	ctx := context.Background()
	coll := &collectionInfo{
		ID:     100,
		Schema: newTestScalarClusteringKeySchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	s.meta.compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
		Type:         datapb.CompactionType_ClusteringCompaction,
		State:        datapb.CompactionTaskState_executing,
	})

	views3, triggerID3, err3 := s.clusteringCompactionPolicy.triggerOneCollection(context.TODO(), 100, false)
	s.NoError(err3)
	s.Nil(views3)
	s.Equal(int64(1), triggerID3)
}

func (s *ClusteringCompactionPolicySuite) TestCollectionIsClusteringCompacting() {
	ctx := context.Background()
	s.Run("no collection is compacting", func() {
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
			{datapb.CompactionTaskState_statistic, true, 1},
		}

		for _, test := range tests {
			s.Run(test.state.String(), func() {
				collID := int64(19530)
				compactionTaskMeta := newTestCompactionTaskMeta(s.T())
				s.clusteringCompactionPolicy.meta = &meta{
					compactionTaskMeta: compactionTaskMeta,
				}
				compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
					TriggerID:    1,
					PlanID:       10,
					CollectionID: collID,
					Type:         datapb.CompactionType_ClusteringCompaction,
					State:        test.state,
				})

				compacting, triggerID := s.clusteringCompactionPolicy.collectionIsClusteringCompacting(collID)
				s.Equal(test.isCompacting, compacting)
				s.Equal(test.triggerID, triggerID)
			})
		}
	})
}

func (s *ClusteringCompactionPolicySuite) TestCollectionIsClusteringCompactingOtherTriggers() {
	tests := []struct {
		name         string
		tasks        []*datapb.CompactionTask
		isCompacting bool
		triggerID    int64
	}{
		{
			name: "only mix compaction is running",
			tasks: []*datapb.CompactionTask{
				{TriggerID: 1, Type: datapb.CompactionType_MixCompaction, State: datapb.CompactionTaskState_executing},
			},
		},
		{
			name: "newer mix compaction does not hide active clustering",
			tasks: []*datapb.CompactionTask{
				{TriggerID: 1, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_indexing},
				{TriggerID: 2, Type: datapb.CompactionType_MixCompaction, State: datapb.CompactionTaskState_executing},
			},
			isCompacting: true,
			triggerID:    1,
		},
		{
			name: "newer completed clustering does not hide active clustering",
			tasks: []*datapb.CompactionTask{
				{TriggerID: 1, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_analyzing},
				{TriggerID: 2, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_completed},
			},
			isCompacting: true,
			triggerID:    1,
		},
		{
			name: "all clustering tasks are terminal despite running mix compaction",
			tasks: []*datapb.CompactionTask{
				{TriggerID: 1, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_failed},
				{TriggerID: 2, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_completed},
				{TriggerID: 3, Type: datapb.CompactionType_MixCompaction, State: datapb.CompactionTaskState_executing},
			},
			triggerID: 2,
		},
		{
			name: "unfinished partition keeps the clustering trigger active",
			tasks: []*datapb.CompactionTask{
				{TriggerID: 1, PartitionID: 10, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_completed},
				{TriggerID: 1, PartitionID: 11, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_meta_saved},
			},
			isCompacting: true,
			triggerID:    1,
		},
		{
			name: "latest active clustering trigger is reused",
			tasks: []*datapb.CompactionTask{
				{TriggerID: 1, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_executing},
				{TriggerID: 2, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_pipelining},
			},
			isCompacting: true,
			triggerID:    2,
		},
		{
			name: "mix task in the same trigger is not a clustering task",
			tasks: []*datapb.CompactionTask{
				{TriggerID: 1, Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_completed},
				{TriggerID: 1, Type: datapb.CompactionType_MixCompaction, State: datapb.CompactionTaskState_executing},
			},
			triggerID: 1,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			compactionTaskMeta := newTestCompactionTaskMeta(s.T())
			policy := newClusteringCompactionPolicy(&meta{compactionTaskMeta: compactionTaskMeta}, nil, nil)
			for i, task := range test.tasks {
				task.PlanID = int64(i + 10)
				task.CollectionID = collID
				s.Require().NoError(compactionTaskMeta.SaveCompactionTask(context.Background(), task))
			}

			compacting, triggerID := policy.collectionIsClusteringCompacting(collID)
			s.Equal(test.isCompacting, compacting)
			s.Equal(test.triggerID, triggerID)
		})
	}
}

func (s *ClusteringCompactionPolicySuite) TestManualClusteringCompactionRetriesAfterMixCompaction() {
	ctx := context.Background()
	label := &CompactionGroupLabel{CollectionID: 100, PartitionID: 10, Channel: "ch-1"}
	collection := &collectionInfo{ID: label.CollectionID, Schema: newTestScalarClusteringKeySchema()}
	s.handler.EXPECT().GetCollection(mock.Anything, collection.ID).Return(collection, nil)
	for _, segmentID := range []int64{300, 301} {
		segment := genTestSegmentInfo(label, segmentID, datapb.SegmentLevel_L2, commonpb.SegmentState_Flushed)
		segment.NumOfRows = 100
		segment.Binlogs = genTestBinlogs(1, MB)
		s.meta.segments.SetSegment(segmentID, segment)
	}
	mixTask := &datapb.CompactionTask{
		TriggerID:     1,
		PlanID:        10,
		CollectionID:  collection.ID,
		PartitionID:   label.PartitionID,
		Channel:       label.Channel,
		Type:          datapb.CompactionType_MixCompaction,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{300},
	}
	s.Require().NoError(s.meta.compactionTaskMeta.SaveCompactionTask(ctx, mixTask))
	s.meta.SetSegmentsCompacting(ctx, mixTask.InputSegments, true)

	// A busy L2 segment prevents partial clustering, but its Mix trigger must
	// never be returned as though the manual clustering request was accepted.
	views, triggerID, err := s.clusteringCompactionPolicy.triggerOneCollection(ctx, collection.ID, true)
	s.Require().NoError(err)
	s.Empty(views)
	s.NotEqual(mixTask.TriggerID, triggerID)

	mixTask.State = datapb.CompactionTaskState_completed
	s.Require().NoError(s.meta.compactionTaskMeta.SaveCompactionTask(ctx, mixTask))
	s.meta.SetSegmentsCompacting(ctx, mixTask.InputSegments, false)

	views, triggerID, err = s.clusteringCompactionPolicy.triggerOneCollection(ctx, collection.ID, true)
	s.Require().NoError(err)
	s.Require().Len(views, 1)
	s.Positive(triggerID)
	s.NotEqual(mixTask.TriggerID, triggerID)
	s.Equal(triggerID, views[0].GetTriggerID())
	s.Equal(label, views[0].GetGroupLabel())
	s.Require().Len(views[0].GetSegmentsView(), 2)
	s.ElementsMatch([]int64{300, 301}, []int64{views[0].GetSegmentsView()[0].ID, views[0].GetSegmentsView()[1].ID})
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionNormal() {
	paramtable.Get().Save(Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.Key, "0")
	defer paramtable.Get().Reset(Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.Key)

	testLabel := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	s.meta.metaStore.PutCollection(&collectionInfo{
		ID:     testLabel.CollectionID,
		Schema: newTestScalarClusteringKeySchema(),
	})

	segments := genSegmentsForMeta(testLabel)
	for id, segment := range segments {
		s.meta.segments.SetSegment(id, segment)
	}

	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collectionID int64) (*collectionInfo, error) {
		coll, exist := s.meta.metaStore.GetCollection(collectionID)
		if exist {
			return coll, nil
		}
		return nil, nil
	})

	// trigger
	view, _, err := s.clusteringCompactionPolicy.triggerOneCollection(context.TODO(), 1, false)
	s.Equal(1, len(view))
	s.NoError(err)
	s.Equal(testLabel, view[0].GetGroupLabel())
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionAllowsMixedSchemaVersionGroup() {
	paramtable.Get().Save(Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.Key, "0")
	defer paramtable.Get().Reset(Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.Key)

	testLabel := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	s.meta.metaStore.PutCollection(&collectionInfo{
		ID:     testLabel.CollectionID,
		Schema: newTestScalarClusteringKeySchema(),
	})

	segments := genSegmentsForMeta(testLabel)
	versionCounter := int32(0)
	for id, segment := range segments {
		versionCounter++
		segment.SchemaVersion = versionCounter
		segments[id] = segment
	}
	for id, segment := range segments {
		s.meta.segments.SetSegment(id, segment)
	}

	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collectionID int64) (*collectionInfo, error) {
		coll := s.meta.GetCollection(collectionID)
		if coll != nil {
			return coll, nil
		}
		return nil, nil
	})

	view, _, err := s.clusteringCompactionPolicy.triggerOneCollection(context.TODO(), 1, false)
	s.NoError(err)
	s.Len(view, 1)
	s.Equal(testLabel, view[0].GetGroupLabel())
}

func (s *ClusteringCompactionPolicySuite) TestGetExpectedSegmentSize() {
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
				SegmentInfo: &datapb.SegmentInfo{Stats: &datapb.Statistics{InsertBinlogSize: 1024 * 1024 * 1024 * 10}},
			},
		}, true},
		{
			"very recent partition stats and enough new data",
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
					SegmentInfo: &datapb.SegmentInfo{Stats: &datapb.Statistics{InsertBinlogSize: 1024 * 1024 * 1024 * 10}},
				},
			},
			false,
		},
		{
			"very old partition stats and not enough new data",
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
					SegmentInfo: &datapb.SegmentInfo{Stats: &datapb.Statistics{InsertBinlogSize: 1024}},
				},
			},
			true,
		},
		{
			"partition stats and enough new data",
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
					SegmentInfo: &datapb.SegmentInfo{ID: 9999, Stats: &datapb.Statistics{InsertBinlogSize: 1024 * 1024 * 1024 * 10}},
				},
			},
			true,
		},
		{
			"partition stats and not enough new data",
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
					SegmentInfo: &datapb.SegmentInfo{ID: 9999, Stats: &datapb.Statistics{InsertBinlogSize: 1024}},
				},
			},
			false,
		},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			partitionStatsMeta, err := newPartitionStatsMeta(ctx, s.catalog)
			s.NoError(err)
			for _, partitionStats := range test.partitionStats {
				seedPartitionStatsInfo(partitionStatsMeta, partitionStats)
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
