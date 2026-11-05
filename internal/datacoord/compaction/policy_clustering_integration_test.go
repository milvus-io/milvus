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
package compaction

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
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/partitionstats"
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
	handler            *meta.NMockHandler
	inspector          *MockCompactionInspector
	catalog            *mocks.DataCoordCatalog
	meta               *meta.Meta
	compactionTaskMeta *ctask.CompactionTaskMeta
	partitionStatsMeta *partitionstats.PartitionStatsMeta
	indexMeta          *index.IndexMeta

	clusteringCompactionPolicy *policy.ClusteringCompactionPolicy
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

	compactionTaskMeta, _ := ctask.NewCompactionTaskMeta(context.TODO(), s.catalog)
	partitionStatsMeta, _ := partitionstats.NewPartitionStatsMeta(context.TODO(), s.catalog)
	indexMeta, _ := index.NewIndexMeta(context.TODO(), s.catalog, nil)

	store := metacache.NewMetaStore(nil)
	mt := &meta.Meta{
		Segments:     meta.NewSegmentsInfo(store),
		MetaStore:    store,
		IndexCatalog: indexMeta,
	}
	s.meta = mt
	s.compactionTaskMeta = compactionTaskMeta
	s.partitionStatsMeta = partitionStatsMeta
	s.indexMeta = indexMeta

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocID(mock.Anything).Return(19530, nil).Maybe()
	mockHandler := meta.NewNMockHandler(s.T())
	s.handler = mockHandler
	s.clusteringCompactionPolicy = policy.NewClusteringCompactionPolicy(s.meta, s.compactionTaskMeta, s.partitionStatsMeta, mockAllocator, mockHandler)
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
	gotViews, ok := events[policy.TriggerTypeClustering]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(0, len(gotViews))
}

func (s *ClusteringCompactionPolicySuite) TestTriggerWithCollections() {
	// valid collection
	s.meta.MetaStore.PutCollection(&meta.CollectionInfo{
		ID:     1,
		Schema: newTestScalarClusteringKeySchema(),
	})
	// deleted collection
	s.meta.MetaStore.PutCollection(&meta.CollectionInfo{
		ID:     2,
		Schema: newTestScalarClusteringKeySchema(),
	})
	s.clusteringCompactionPolicy.Meta = s.meta

	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collectionID int64) (*meta.CollectionInfo, error) {
		if collectionID == 2 {
			return nil, errors.New("mock get collection fail error")
		}
		coll, exist := s.meta.MetaStore.GetCollection(collectionID)
		if exist {
			return coll, nil
		}
		return nil, nil
	})

	// trigger
	events, err := s.clusteringCompactionPolicy.Trigger(context.Background())
	s.NoError(err)
	gotViews, ok := events[policy.TriggerTypeClustering]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(0, len(gotViews))
}

func (s *ClusteringCompactionPolicySuite) TestCalculateClusteringCompactionConfig() {
	testCases := []struct {
		description       string
		coll              *meta.CollectionInfo
		view              view.CompactionView
		totalRows         int64
		maxSegmentRows    int64
		preferSegmentRows int64
		err               error
	}{
		{
			description: "",
			coll: &meta.CollectionInfo{
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
			view: &policy.ClusteringSegmentsView{
				Segments: []*view.SegmentView{
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
			expectedSegmentSize := policy.GetExpectedSegmentSize(s.indexMeta, test.coll.ID, test.coll.Schema)
			if cv, ok := test.view.(*policy.ClusteringSegmentsView); ok {
				for _, segment := range cv.Segments {
					if segment == nil || segment.NumOfRows <= 0 || test.maxSegmentRows == 0 {
						continue
					}
					segment.Size = float64(expectedSegmentSize) * float64(segment.NumOfRows) / float64(test.maxSegmentRows)
				}
			}
			totalRows, maxSegmentRows, preferSegmentRows, err := policy.CalculateClusteringCompactionConfig(test.coll, test.view, expectedSegmentSize)
			s.Equal(test.totalRows, totalRows)
			s.Equal(test.maxSegmentRows, maxSegmentRows)
			s.Equal(test.preferSegmentRows, preferSegmentRows)
			s.Equal(test.err, err)
		})
	}
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionAbnormal() {
	// mock error in handler.GetCollection
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&meta.CollectionInfo{}, errors.New("mock Error")).Once()
	views, triggerID, err := s.clusteringCompactionPolicy.TriggerOneCollection(context.TODO(), 1, false)
	s.Error(err)
	s.Nil(views)
	s.Equal(int64(0), triggerID)

	// mock "collection not exist" in handler.GetCollection
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(nil, nil).Once()
	views2, triggerID2, err2 := s.clusteringCompactionPolicy.TriggerOneCollection(context.TODO(), 1, false)
	s.NoError(err2)
	s.Nil(views2)
	s.Equal(int64(0), triggerID2)
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionSkipExternal() {
	collID := int64(100)
	s.handler.EXPECT().GetCollection(mock.Anything, collID).Return(&meta.CollectionInfo{
		ID: collID,
		Schema: func() *schemapb.CollectionSchema {
			schema := newTestScalarClusteringKeySchema()
			schema.ExternalSource = "s3://external"
			// External collections are identified by having ExternalField set on fields.
			schema.Fields[0].ExternalField = "field1_col"
			return schema
		}(),
	}, nil)

	views, triggerID, err := s.clusteringCompactionPolicy.TriggerOneCollection(context.Background(), collID, false)
	s.NoError(err)
	s.Nil(views)
	s.EqualValues(0, triggerID)
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionNoClusteringKeySchema() {
	ctx := context.Background()
	coll := &meta.CollectionInfo{
		ID:     100,
		Schema: meta.NewTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	s.compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
		State:        datapb.CompactionTaskState_executing,
	})

	views, triggerID, err := s.clusteringCompactionPolicy.TriggerOneCollection(context.TODO(), 100, false)
	s.NoError(err)
	s.Nil(views)
	s.Equal(int64(0), triggerID)
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionCompacting() {
	ctx := context.Background()
	coll := &meta.CollectionInfo{
		ID:     100,
		Schema: newTestScalarClusteringKeySchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	s.compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
		State:        datapb.CompactionTaskState_executing,
	})

	views3, triggerID3, err3 := s.clusteringCompactionPolicy.TriggerOneCollection(context.TODO(), 100, false)
	s.NoError(err3)
	s.Nil(views3)
	s.Equal(int64(1), triggerID3)
}

func (s *ClusteringCompactionPolicySuite) TestCollectionIsClusteringCompacting() {
	ctx := context.Background()
	s.Run("no collection is compacting", func() {
		compacting, triggerID := s.clusteringCompactionPolicy.CollectionIsClusteringCompacting(meta.CollID)
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
				compactionTaskMeta := ctask.NewTestCompactionTaskMeta(s.T())
				s.clusteringCompactionPolicy.Meta = &meta.Meta{}
				s.clusteringCompactionPolicy.CompactionTaskMeta = compactionTaskMeta
				compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
					TriggerID:    1,
					PlanID:       10,
					CollectionID: collID,
					State:        test.state,
				})

				compacting, triggerID := s.clusteringCompactionPolicy.CollectionIsClusteringCompacting(collID)
				s.Equal(test.isCompacting, compacting)
				s.Equal(test.triggerID, triggerID)
			})
		}
	})
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionNormal() {
	paramtable.Get().Save(Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.Key, "0")
	defer paramtable.Get().Reset(Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.Key)

	testLabel := &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	s.meta.MetaStore.PutCollection(&meta.CollectionInfo{
		ID:     testLabel.CollectionID,
		Schema: newTestScalarClusteringKeySchema(),
	})

	segments := policy.GenSegmentsForMeta(testLabel)
	for id, segment := range segments {
		s.meta.Segments.SetSegment(id, segment)
	}

	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collectionID int64) (*meta.CollectionInfo, error) {
		coll, exist := s.meta.MetaStore.GetCollection(collectionID)
		if exist {
			return coll, nil
		}
		return nil, nil
	})

	// trigger
	cv, _, err := s.clusteringCompactionPolicy.TriggerOneCollection(context.TODO(), 1, false)
	s.Equal(1, len(cv))
	s.NoError(err)
	s.Equal(testLabel, cv[0].GetGroupLabel())
}

func (s *ClusteringCompactionPolicySuite) TestTriggerOneCollectionAllowsMixedSchemaVersionGroup() {
	paramtable.Get().Save(Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.Key, "0")
	defer paramtable.Get().Reset(Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.Key)

	testLabel := &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	s.meta.MetaStore.PutCollection(&meta.CollectionInfo{
		ID:     testLabel.CollectionID,
		Schema: newTestScalarClusteringKeySchema(),
	})

	segments := policy.GenSegmentsForMeta(testLabel)
	versionCounter := int32(0)
	for id, segment := range segments {
		versionCounter++
		segment.SchemaVersion = versionCounter
		segments[id] = segment
	}
	for id, segment := range segments {
		s.meta.Segments.SetSegment(id, segment)
	}

	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collectionID int64) (*meta.CollectionInfo, error) {
		coll := s.meta.GetCollection(collectionID)
		if coll != nil {
			return coll, nil
		}
		return nil, nil
	})

	cv, _, err := s.clusteringCompactionPolicy.TriggerOneCollection(context.TODO(), 1, false)
	s.NoError(err)
	s.Len(cv, 1)
	s.Equal(testLabel, cv[0].GetGroupLabel())
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
		segments       []*meta.SegmentInfo
		succeed        bool
	}{
		{"no partition stats and not enough new data", []*datapb.PartitionStatsInfo{}, partitionstats.EmptyPartitionStatsVersion, []*meta.SegmentInfo{}, false},
		{"no partition stats and enough new data", []*datapb.PartitionStatsInfo{}, partitionstats.EmptyPartitionStatsVersion, []*meta.SegmentInfo{
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
			[]*meta.SegmentInfo{
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
			[]*meta.SegmentInfo{
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
			[]*meta.SegmentInfo{
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
			[]*meta.SegmentInfo{
				{
					SegmentInfo: &datapb.SegmentInfo{ID: 9999, Stats: &datapb.Statistics{InsertBinlogSize: 1024}},
				},
			},
			false,
		},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			partitionStatsMeta, err := partitionstats.NewPartitionStatsMeta(ctx, s.catalog)
			s.NoError(err)
			for _, partitionStats := range test.partitionStats {
				partitionstats.SeedPartitionStatsInfo(partitionStatsMeta, partitionStats)
			}
			if test.currentVersion != 0 {
				partitionStatsMeta.PartitionStatsInfos[channel][partitionID].CurrentVersion = test.currentVersion
			}

			succeed, err := policy.TriggerClusteringCompactionPolicy(ctx, partitionStatsMeta, collectionID, partitionID, channel, test.segments)
			s.NoError(err)
			s.Equal(test.succeed, succeed)
		})
	}
}
