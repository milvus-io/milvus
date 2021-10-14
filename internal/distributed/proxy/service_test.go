// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package grpcproxy

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockBase struct {
}

func (m *MockBase) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State:  &internalpb.ComponentInfo{StateCode: internalpb.StateCode_Healthy},
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil
}

func (m *MockBase) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockBase) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockRootCoord struct {
	MockBase
	initErr  error
	startErr error
	regErr   error
	stopErr  error
}

func (m *MockRootCoord) Init() error {
	return m.initErr
}

func (m *MockRootCoord) Start() error {
	return m.startErr
}

func (m *MockRootCoord) Stop() error {
	return m.stopErr
}

func (m *MockRootCoord) Register() error {
	return m.regErr
}

func (m *MockRootCoord) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) CreateIndex(ctx context.Context, req *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DescribeIndex(ctx context.Context, req *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) DropIndex(ctx context.Context, req *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockIndexCoord struct {
	MockBase
	initErr  error
	startErr error
	regErr   error
	stopErr  error
}

func (m *MockIndexCoord) Init() error {
	return m.initErr
}

func (m *MockIndexCoord) Start() error {
	return m.startErr
}

func (m *MockIndexCoord) Stop() error {
	return m.stopErr
}

func (m *MockIndexCoord) Register() error {
	return m.regErr
}

func (m *MockIndexCoord) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	return nil, nil
}

func (m *MockIndexCoord) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockIndexCoord) GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
	return nil, nil
}

func (m *MockIndexCoord) GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	return nil, nil
}

func (m *MockIndexCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockQueryCoord struct {
	MockBase
	initErr  error
	startErr error
	stopErr  error
	regErr   error
}

func (m *MockQueryCoord) Init() error {
	return m.initErr
}

func (m *MockQueryCoord) Start() error {
	return m.startErr
}

func (m *MockQueryCoord) Stop() error {
	return m.stopErr
}

func (m *MockQueryCoord) Register() error {
	return m.regErr
}

func (m *MockQueryCoord) UpdateStateCode(code internalpb.StateCode) {
}

func (m *MockQueryCoord) SetRootCoord(types.RootCoord) error {
	return nil
}

func (m *MockQueryCoord) SetDataCoord(types.DataCoord) error {
	return nil
}

func (m *MockQueryCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockQueryCoord) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockQueryCoord) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockQueryCoord) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockQueryCoord) CreateQueryChannel(ctx context.Context, req *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockDataCoord struct {
	MockBase
	err      error
	initErr  error
	startErr error
	stopErr  error
	regErr   error
}

func (m *MockDataCoord) Init() error {
	return m.initErr
}

func (m *MockDataCoord) Start() error {
	return m.startErr
}

func (m *MockDataCoord) Stop() error {
	return m.stopErr
}

func (m *MockDataCoord) Register() error {
	return m.regErr
}

func (m *MockDataCoord) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockDataCoord) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockProxy struct {
	MockBase
	err      error
	initErr  error
	startErr error
	stopErr  error
	regErr   error
}

func (m *MockProxy) Init() error {
	return m.initErr
}

func (m *MockProxy) Start() error {
	return m.startErr
}

func (m *MockProxy) Stop() error {
	return m.stopErr
}

func (m *MockProxy) Register() error {
	return m.regErr
}

func (m *MockProxy) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return nil, nil
}

func (m *MockProxy) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	return nil, nil
}

func (m *MockProxy) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return nil, nil
}

func (m *MockProxy) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return nil, nil
}

func (m *MockProxy) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	return nil, nil
}

func (m *MockProxy) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return nil, nil
}

func (m *MockProxy) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	return nil, nil
}

func (m *MockProxy) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	return nil, nil
}

func (m *MockProxy) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	return nil, nil
}

func (m *MockProxy) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return nil, nil
}

func (m *MockProxy) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
	return nil, nil
}

func (m *MockProxy) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	return nil, nil
}

func (m *MockProxy) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	return nil, nil
}

func (m *MockProxy) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetQuerySegmentInfo(ctx context.Context, request *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	return nil, nil
}

func (m *MockProxy) Dummy(ctx context.Context, request *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	return nil, nil
}

func (m *MockProxy) RegisterLink(ctx context.Context, request *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

func (m *MockProxy) CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) SetRootCoordClient(rootCoord types.RootCoord) {

}

func (m *MockProxy) SetDataCoordClient(dataCoord types.DataCoord) {

}

func (m *MockProxy) SetIndexCoordClient(indexCoord types.IndexCoord) {

}

func (m *MockProxy) SetQueryCoordClient(queryCoord types.QueryCoord) {

}

func (m *MockProxy) UpdateStateCode(stateCode internalpb.StateCode) {

}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func Test_NewServer(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NotNil(t, server)
	assert.Nil(t, err)

	server.proxy = &MockProxy{}
	server.rootCoordClient = &MockRootCoord{}
	server.indexCoordClient = &MockIndexCoord{}
	server.queryCooedClient = &MockQueryCoord{}
	server.dataCoordClient = &MockDataCoord{}

	t.Run("Run", func(t *testing.T) {
		err = server.Run()
		assert.Nil(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		_, err := server.GetComponentStates(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		_, err := server.GetStatisticsChannel(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("InvalidateCollectionMetaCache", func(t *testing.T) {
		_, err := server.InvalidateCollectionMetaCache(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ReleaseDQLMessageStream", func(t *testing.T) {
		_, err := server.ReleaseDQLMessageStream(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreateCollection", func(t *testing.T) {
		_, err := server.CreateCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropCollection", func(t *testing.T) {
		_, err := server.DropCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("HasCollection", func(t *testing.T) {
		_, err := server.HasCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("LoadCollection", func(t *testing.T) {
		_, err := server.LoadCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ReleaseCollection", func(t *testing.T) {
		_, err := server.ReleaseCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DescribeCollection", func(t *testing.T) {
		_, err := server.DescribeCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetCollectionStatistics", func(t *testing.T) {
		_, err := server.GetCollectionStatistics(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ShowCollections", func(t *testing.T) {
		_, err := server.ShowCollections(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreatePartition", func(t *testing.T) {
		_, err := server.CreatePartition(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropPartition", func(t *testing.T) {
		_, err := server.DropPartition(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("HasPartition", func(t *testing.T) {
		_, err := server.HasPartition(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("LoadPartitions", func(t *testing.T) {
		_, err := server.LoadPartitions(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ReleasePartitions", func(t *testing.T) {
		_, err := server.ReleasePartitions(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetPartitionStatistics", func(t *testing.T) {
		_, err := server.GetPartitionStatistics(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ShowPartitions", func(t *testing.T) {
		_, err := server.ShowPartitions(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		_, err := server.CreateIndex(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropIndex", func(t *testing.T) {
		_, err := server.DropIndex(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		_, err := server.DescribeIndex(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		_, err := server.GetIndexBuildProgress(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		_, err := server.GetIndexState(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Insert", func(t *testing.T) {
		_, err := server.Insert(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		_, err := server.Delete(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Search", func(t *testing.T) {
		_, err := server.Search(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Flush", func(t *testing.T) {
		_, err := server.Flush(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Query", func(t *testing.T) {
		_, err := server.Query(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CalcDistance", func(t *testing.T) {
		_, err := server.CalcDistance(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetDdChannel", func(t *testing.T) {
		_, err := server.GetDdChannel(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetPersistentSegmentInfo", func(t *testing.T) {
		_, err := server.GetPersistentSegmentInfo(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetQuerySegmentInfo", func(t *testing.T) {
		_, err := server.GetQuerySegmentInfo(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Dummy", func(t *testing.T) {
		_, err := server.Dummy(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("RegisterLink", func(t *testing.T) {
		_, err := server.RegisterLink(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		_, err := server.GetMetrics(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreateAlias", func(t *testing.T) {
		_, err := server.CreateAlias(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropAlias", func(t *testing.T) {
		_, err := server.DropAlias(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("AlterAlias", func(t *testing.T) {
		_, err := server.AlterAlias(ctx, nil)
		assert.Nil(t, err)
	})

	err = server.Stop()
	assert.Nil(t, err)
}
