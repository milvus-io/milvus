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

package delegator

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type TwoStageSearchSuite struct {
	suite.Suite

	collectionID  int64
	partitionIDs  []int64
	replicaID     int64
	vchannelName  string
	version       int64
	workerManager *cluster.MockManager
	manager       *segments.Manager
	loader        *segments.MockLoader
	chunkManager  storage.ChunkManager
	rootPath      string

	delegator *shardDelegator
}

func (s *TwoStageSearchSuite) SetupSuite() {
	paramtable.Init()
}

func (s *TwoStageSearchSuite) SetupTest() {
	s.collectionID = 1000
	s.partitionIDs = []int64{500, 501}
	s.replicaID = 65535
	s.vchannelName = "rootcoord-dml_1000_v0"
	s.version = 2000
	s.workerManager = &cluster.MockManager{}
	s.manager = segments.NewManager()
	s.loader = &segments.MockLoader{}
	s.rootPath = "delegator_twostage_test"

	// init schema
	s.manager.Collection.PutOrRef(s.collectionID, &schemapb.CollectionSchema{
		Name: "TestCollection",
		Fields: []*schemapb.FieldSchema{
			{
				Name:         "id",
				FieldID:      100,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       true,
			},
			{
				Name:         "vector",
				FieldID:      101,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
		},
	}, &segcorepb.CollectionIndexMeta{
		MaxIndexRowCount: 100,
		IndexMetas: []*segcorepb.FieldIndexMeta{
			{
				FieldID:      101,
				CollectionID: s.collectionID,
				IndexName:    "vector_index",
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "HNSW",
					},
					{
						Key:   common.MetricTypeKey,
						Value: metric.L2,
					},
				},
			},
		},
	}, &querypb.LoadMetaInfo{
		PartitionIDs: s.partitionIDs,
	})

	// init chunkManager
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), s.rootPath)
	s.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(context.Background())

	var err error
	delegator, err := NewShardDelegator(context.Background(), s.collectionID, s.replicaID, s.vchannelName, s.version, s.workerManager, s.manager, s.loader, 10000, nil, s.chunkManager, NewChannelQueryView(nil, nil, nil, initialTargetVersion))
	s.Require().NoError(err)
	s.delegator = delegator.(*shardDelegator)
}

func (s *TwoStageSearchSuite) TearDownTest() {
	if s.delegator != nil {
		s.delegator.Close()
		s.delegator = nil
	}
}

func (s *TwoStageSearchSuite) TestShouldUseTwoStageSearch() {
	s.Run("disabled", func() {
		// Ensure two-stage search is disabled
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "false")
		defer paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       3000,
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			},
		}
		result := s.delegator.shouldUseTwoStageSearch(req, 10)
		s.False(result, "should return false when two-stage search is disabled")
	})

	s.Run("segments_and_topk_below_threshold", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "2000")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       1000, // below min topk
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			},
		}
		// sealedNum=3 is below min segments (5) and topk=1000 is below min topk (2000)
		result := s.delegator.shouldUseTwoStageSearch(req, 3)
		s.False(result, "should return false when both segments and topk are below threshold")
	})

	s.Run("segments_above_threshold", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "2000")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       1000, // below min topk
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			},
		}
		// sealedNum=10 is above min segments (5), so it should pass even if topk is below threshold
		result := s.delegator.shouldUseTwoStageSearch(req, 10)
		s.True(result, "should return true when segments are above threshold")
	})

	s.Run("topk_above_threshold", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "2000")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       3000, // above min topk
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			},
		}
		// topk=3000 is above min topk (2000), so it should pass even if segments are below threshold
		result := s.delegator.shouldUseTwoStageSearch(req, 3)
		s.True(result, "should return true when topk is above threshold")
	})

	s.Run("wrong_search_type_no_filter", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "2000")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       3000,
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER, // wrong search type
			},
		}
		result := s.delegator.shouldUseTwoStageSearch(req, 10)
		s.False(result, "should return false for PURE_ANN_SEARCH_NO_FILTER")
	})

	s.Run("wrong_search_type_default", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "2000")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       3000,
				SearchType: internalpb.SearchType_DEFAULT, // default search type
			},
		}
		result := s.delegator.shouldUseTwoStageSearch(req, 10)
		s.False(result, "should return false for DEFAULT search type")
	})

	s.Run("all_conditions_met", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "2000")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       3000,
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			},
		}
		result := s.delegator.shouldUseTwoStageSearch(req, 10)
		s.True(result, "should return true when all conditions are met")
	})
}

// MockQueryHook implements optimizers.QueryHook for testing
type MockQueryHook struct {
	mock.Mock
}

func (m *MockQueryHook) Run(params map[string]any) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockQueryHook) Init(s string) error {
	args := m.Called(s)
	return args.Error(0)
}

func (m *MockQueryHook) InitTuningConfig(config map[string]string) error {
	args := m.Called(config)
	return args.Error(0)
}

func (m *MockQueryHook) DeleteTuningConfig(s string) error {
	args := m.Called(s)
	return args.Error(0)
}

var _ optimizers.QueryHook = (*MockQueryHook)(nil)

func (s *TwoStageSearchSuite) TestExecuteFilterStage() {
	s.delegator.Start()
	paramtable.SetNodeID(1)

	s.Run("filter_only_flag_set", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		// Verify that the request has FilterOnly=true
		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				s.True(req.GetFilterOnly(), "FilterOnly should be true in filter stage")
			}).Return(&internalpb.SearchResults{
			FilterValidCounts: []int64{100},
		}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		// Setup distribution with segments
		s.delegator.distribution.AddDistributions(SegmentEntry{
			NodeID:    1,
			SegmentID: 1000,
		})

		ctx := context.Background()
		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk: 100,
			},
			DmlChannels: []string{s.vchannelName},
		}

		sealed := []SnapshotItem{
			{
				NodeID: 1,
				Segments: []SegmentEntry{
					{SegmentID: 1000, NodeID: 1},
				},
			},
		}
		growing := []SegmentEntry{}
		sealedRowCount := map[int64]int64{1000: 10000}

		validCounts, err := s.delegator.executeFilterStage(ctx, req, sealed, growing, sealedRowCount)
		s.NoError(err)
		s.NotNil(validCounts)
		s.Len(validCounts, 1)
		s.Equal(int64(100), validCounts[0])
	})

	s.Run("filter_stage_error", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		mockErr := errors.New("filter stage error")
		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(nil, mockErr)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		// Setup distribution with segments
		s.delegator.distribution.AddDistributions(SegmentEntry{
			NodeID:    1,
			SegmentID: 1001,
		})

		ctx := context.Background()
		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk: 100,
			},
			DmlChannels: []string{s.vchannelName},
		}

		sealed := []SnapshotItem{
			{
				NodeID: 1,
				Segments: []SegmentEntry{
					{SegmentID: 1001, NodeID: 1},
				},
			},
		}
		growing := []SegmentEntry{}
		sealedRowCount := map[int64]int64{1001: 10000}

		validCounts, err := s.delegator.executeFilterStage(ctx, req, sealed, growing, sealedRowCount)
		s.Error(err)
		s.Nil(validCounts)
	})
}

func (s *TwoStageSearchSuite) TestTwoStageSearch() {
	s.delegator.Start()
	paramtable.SetNodeID(1)

	s.Run("filter_stage_fails", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		mockErr := errors.New("filter stage failed")
		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(nil, mockErr)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		// Setup distribution with segments
		s.delegator.distribution.AddDistributions(SegmentEntry{
			NodeID:    1,
			SegmentID: 2000,
		})

		ctx := context.Background()
		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk: 100,
			},
			DmlChannels: []string{s.vchannelName},
		}

		sealed := []SnapshotItem{
			{
				NodeID: 1,
				Segments: []SegmentEntry{
					{SegmentID: 2000, NodeID: 1},
				},
			},
		}
		growing := []SegmentEntry{}
		sealedRowCount := map[int64]int64{2000: 10000}

		results, err := s.delegator.twoStageSearch(ctx, req, sealed, growing, sealedRowCount)
		s.Error(err)
		s.Nil(results)
		s.Contains(err.Error(), "filter stage failed")
	})

	s.Run("two_stage_search_success", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		callCount := 0
		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				callCount++
				if callCount == 1 {
					// First call should be filter stage with FilterOnly=true
					s.True(req.GetFilterOnly(), "First call should have FilterOnly=true")
				} else {
					// Second call should be normal search with FilterOnly=false
					s.False(req.GetFilterOnly(), "Second call should have FilterOnly=false")
				}
			}).Return(&internalpb.SearchResults{
			FilterValidCounts: []int64{100},
		}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		// Setup distribution with segments
		s.delegator.distribution.AddDistributions(SegmentEntry{
			NodeID:    1,
			SegmentID: 3000,
		})

		ctx := context.Background()
		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk: 100,
			},
			DmlChannels: []string{s.vchannelName},
		}

		sealed := []SnapshotItem{
			{
				NodeID: 1,
				Segments: []SegmentEntry{
					{SegmentID: 3000, NodeID: 1},
				},
			},
		}
		growing := []SegmentEntry{}
		sealedRowCount := map[int64]int64{3000: 10000}

		results, err := s.delegator.twoStageSearch(ctx, req, sealed, growing, sealedRowCount)
		s.NoError(err)
		s.NotNil(results)
		// Should have called SearchSegments twice: once for filter stage, once for vector search
		s.Equal(2, callCount, "Should have called SearchSegments twice")
	})

	s.Run("vector_search_stage_fails", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		mockErr := errors.New("vector search failed")
		// First call for filter stage succeeds, second call for vector search fails
		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(&internalpb.SearchResults{
				FilterValidCounts: []int64{100},
			}, nil).Once()
		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(nil, mockErr).Once()

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		// Setup distribution with segments
		s.delegator.distribution.AddDistributions(SegmentEntry{
			NodeID:    1,
			SegmentID: 4000,
		})

		ctx := context.Background()
		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk: 100,
			},
			DmlChannels: []string{s.vchannelName},
		}

		sealed := []SnapshotItem{
			{
				NodeID: 1,
				Segments: []SegmentEntry{
					{SegmentID: 4000, NodeID: 1},
				},
			},
		}
		growing := []SegmentEntry{}
		sealedRowCount := map[int64]int64{4000: 10000}

		results, err := s.delegator.twoStageSearch(ctx, req, sealed, growing, sealedRowCount)
		s.Error(err)
		s.Nil(results)
	})
}

func (s *TwoStageSearchSuite) TestTwoStageSearchWithMultipleSegments() {
	s.delegator.Start()
	paramtable.SetNodeID(1)

	s.Run("multiple_segments_filter_stats_aggregation", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}
		workers[1] = worker1
		workers[2] = worker2

		// Worker 1 handles segments on node 1
		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(&internalpb.SearchResults{
				FilterValidCounts: []int64{50, 100}, // Two segments with different filter valid counts
			}, nil)

		// Worker 2 handles segments on node 2
		worker2.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(&internalpb.SearchResults{
				FilterValidCounts: []int64{200},
			}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		// Setup distribution with segments on multiple nodes
		s.delegator.distribution.AddDistributions(
			SegmentEntry{NodeID: 1, SegmentID: 5000},
			SegmentEntry{NodeID: 1, SegmentID: 5001},
			SegmentEntry{NodeID: 2, SegmentID: 5002},
		)

		ctx := context.Background()
		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk: 100,
			},
			DmlChannels: []string{s.vchannelName},
		}

		sealed := []SnapshotItem{
			{
				NodeID: 1,
				Segments: []SegmentEntry{
					{SegmentID: 5000, NodeID: 1},
					{SegmentID: 5001, NodeID: 1},
				},
			},
			{
				NodeID: 2,
				Segments: []SegmentEntry{
					{SegmentID: 5002, NodeID: 2},
				},
			},
		}
		growing := []SegmentEntry{}
		sealedRowCount := map[int64]int64{
			5000: 10000,
			5001: 10000,
			5002: 10000,
		}

		results, err := s.delegator.twoStageSearch(ctx, req, sealed, growing, sealedRowCount)
		s.NoError(err)
		s.NotNil(results)
	})
}

func (s *TwoStageSearchSuite) TestShouldUseTwoStageSearchEdgeCases() {
	s.Run("exact_threshold_segments", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "2000")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       1999, // exactly one below threshold
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			},
		}
		// sealedNum=5 is exactly at threshold
		result := s.delegator.shouldUseTwoStageSearch(req, 5)
		s.True(result, "should return true when segments equal threshold")
	})

	s.Run("exact_threshold_topk", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "2000")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       2000, // exactly at threshold
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			},
		}
		// sealedNum=4 is below threshold
		result := s.delegator.shouldUseTwoStageSearch(req, 4)
		s.True(result, "should return true when topk equals threshold")
	})

	s.Run("zero_segments", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "100")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "1")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       50, // below threshold
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			},
		}
		result := s.delegator.shouldUseTwoStageSearch(req, 0)
		s.False(result, "should return false when both segments and topk are below threshold")
	})

	s.Run("zero_topk", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "100")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
		}()

		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk:       0, // zero topk
				SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			},
		}
		// sealedNum=10 is above threshold, so it should pass
		result := s.delegator.shouldUseTwoStageSearch(req, 10)
		s.True(result, "should return true when segments above threshold even with zero topk")
	})
}

func (s *TwoStageSearchSuite) TestTwoStageSearchWithGrowingSegments() {
	s.delegator.Start()
	paramtable.SetNodeID(1)

	s.Run("growing_segments_included", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		callCount := 0
		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				callCount++
			}).Return(&internalpb.SearchResults{
			FilterValidCounts: []int64{100},
		}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		// Setup distribution with segments
		s.delegator.distribution.AddDistributions(SegmentEntry{
			NodeID:    1,
			SegmentID: 6000,
		})

		ctx := context.Background()
		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk: 100,
			},
			DmlChannels: []string{s.vchannelName},
		}

		sealed := []SnapshotItem{
			{
				NodeID: 1,
				Segments: []SegmentEntry{
					{SegmentID: 6000, NodeID: 1},
				},
			},
		}
		growing := []SegmentEntry{
			{SegmentID: 6001, NodeID: 1},
		}
		sealedRowCount := map[int64]int64{6000: 10000}

		results, err := s.delegator.twoStageSearch(ctx, req, sealed, growing, sealedRowCount)
		s.NoError(err)
		s.NotNil(results)
		// Should have called SearchSegments 3 times:
		// filter stage: 1 call for sealed only (growing not included in filter stage)
		// search stage: 1 call for sealed + 1 call for growing = 2
		s.Equal(3, callCount)
	})
}

func (s *TwoStageSearchSuite) TestTwoStageSearchZeroValidCounts() {
	s.delegator.Start()
	paramtable.SetNodeID(1)

	s.Run("zero_valid_counts_all_segments", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		workers[1] = worker1

		// Return zero valid counts - no records pass the filter
		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(&internalpb.SearchResults{
				FilterValidCounts: []int64{0, 0, 0},
			}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		// Setup distribution with segments
		s.delegator.distribution.AddDistributions(
			SegmentEntry{NodeID: 1, SegmentID: 7000},
			SegmentEntry{NodeID: 1, SegmentID: 7001},
			SegmentEntry{NodeID: 1, SegmentID: 7002},
		)

		ctx := context.Background()
		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk: 100,
			},
			DmlChannels: []string{s.vchannelName},
		}

		sealed := []SnapshotItem{
			{
				NodeID: 1,
				Segments: []SegmentEntry{
					{SegmentID: 7000, NodeID: 1},
					{SegmentID: 7001, NodeID: 1},
					{SegmentID: 7002, NodeID: 1},
				},
			},
		}
		growing := []SegmentEntry{}
		sealedRowCount := map[int64]int64{
			7000: 10000,
			7001: 10000,
			7002: 10000,
		}

		results, err := s.delegator.twoStageSearch(ctx, req, sealed, growing, sealedRowCount)
		s.NoError(err)
		s.NotNil(results)
	})
}

func (s *TwoStageSearchSuite) TestExecuteFilterStageWithMultipleNodes() {
	s.delegator.Start()
	paramtable.SetNodeID(1)

	s.Run("multiple_nodes_success", func() {
		defer func() {
			s.workerManager.ExpectedCalls = nil
		}()

		workers := make(map[int64]*cluster.MockWorker)
		worker1 := &cluster.MockWorker{}
		worker2 := &cluster.MockWorker{}
		worker3 := &cluster.MockWorker{}
		workers[1] = worker1
		workers[2] = worker2
		workers[3] = worker3

		worker1.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				s.True(req.GetFilterOnly(), "FilterOnly should be true")
			}).Return(&internalpb.SearchResults{
			FilterValidCounts: []int64{100},
		}, nil)

		worker2.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				s.True(req.GetFilterOnly(), "FilterOnly should be true")
			}).Return(&internalpb.SearchResults{
			FilterValidCounts: []int64{200},
		}, nil)

		worker3.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Run(func(_ context.Context, req *querypb.SearchRequest) {
				s.True(req.GetFilterOnly(), "FilterOnly should be true")
			}).Return(&internalpb.SearchResults{
			FilterValidCounts: []int64{300},
		}, nil)

		s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Call.Return(func(_ context.Context, nodeID int64) cluster.Worker {
			return workers[nodeID]
		}, nil)

		// Setup distribution with segments on multiple nodes
		s.delegator.distribution.AddDistributions(
			SegmentEntry{NodeID: 1, SegmentID: 8000},
			SegmentEntry{NodeID: 2, SegmentID: 8001},
			SegmentEntry{NodeID: 3, SegmentID: 8002},
		)

		ctx := context.Background()
		req := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				Topk: 100,
			},
			DmlChannels: []string{s.vchannelName},
		}

		sealed := []SnapshotItem{
			{
				NodeID: 1,
				Segments: []SegmentEntry{
					{SegmentID: 8000, NodeID: 1},
				},
			},
			{
				NodeID: 2,
				Segments: []SegmentEntry{
					{SegmentID: 8001, NodeID: 2},
				},
			},
			{
				NodeID: 3,
				Segments: []SegmentEntry{
					{SegmentID: 8002, NodeID: 3},
				},
			},
		}
		growing := []SegmentEntry{}
		sealedRowCount := map[int64]int64{
			8000: 10000,
			8001: 10000,
			8002: 10000,
		}

		validCounts, err := s.delegator.executeFilterStage(ctx, req, sealed, growing, sealedRowCount)
		s.NoError(err)
		s.NotNil(validCounts)
		s.Len(validCounts, 3)

		// Verify total valid counts
		totalValidCount := int64(0)
		for _, count := range validCounts {
			totalValidCount += count
		}
		s.Equal(int64(600), totalValidCount)
	})
}

func TestTwoStageSearchSuite(t *testing.T) {
	suite.Run(t, new(TwoStageSearchSuite))
}
