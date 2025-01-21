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
	"time"

	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ kv.MetaKv = &metaMemoryKV{}

type metaMemoryKV struct {
	memkv.MemoryKV
}

func NewMetaMemoryKV() *metaMemoryKV {
	return &metaMemoryKV{MemoryKV: *memkv.NewMemoryKV()}
}

func (mm *metaMemoryKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	keys, values, err := mm.MemoryKV.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return err
	}

	for i, k := range keys {
		if err := fn([]byte(k), []byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

func (mm *metaMemoryKV) GetPath(key string) string {
	panic("implement me")
}

func (mm *metaMemoryKV) Watch(ctx context.Context, key string) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) WatchWithPrefix(ctx context.Context, key string) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) WatchWithRevision(ctx context.Context, key string, revision int64) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	panic("implement me")
}

func newMemoryMeta() (*meta, error) {
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	return newMeta(context.TODO(), catalog, nil)
}

func newMockAllocator(t *testing.T) *allocator.MockAllocator {
	counter := atomic.NewInt64(0)
	mockAllocator := allocator.NewMockAllocator(t)
	mockAllocator.EXPECT().AllocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		return counter.Inc(), nil
	}).Maybe()
	mockAllocator.EXPECT().AllocTimestamp(mock.Anything).RunAndReturn(func(ctx context.Context) (uint64, error) {
		return uint64(counter.Inc()), nil
	}).Maybe()
	mockAllocator.EXPECT().AllocN(mock.Anything).RunAndReturn(func(i int64) (int64, int64, error) {
		v := counter.Add(i)
		return v, v + i, nil
	}).Maybe()
	return mockAllocator
}

func newMock0Allocator(t *testing.T) *allocator.MockAllocator {
	mock0Allocator := allocator.NewMockAllocator(t)
	mock0Allocator.EXPECT().AllocID(mock.Anything).Return(100, nil).Maybe()
	mock0Allocator.EXPECT().AllocTimestamp(mock.Anything).Return(1000, nil).Maybe()
	mock0Allocator.EXPECT().AllocN(mock.Anything).Return(100, 200, nil).Maybe()
	return mock0Allocator
}

func newTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test",
		Description: "schema for test used",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "field1", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}}},
			{FieldID: 2, Name: "field2", IsPrimaryKey: false, Description: "field no.2", DataType: schemapb.DataType_FloatVector},
		},
	}
}

func newTestScalarClusteringKeySchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test_scalar_clustering",
		Description: "schema for test scalar clustering compaction",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "field1", IsPrimaryKey: true, IsClusteringKey: true, Description: "field no.1", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "field2", IsPrimaryKey: false, Description: "field no.2", DataType: schemapb.DataType_FloatVector},
		},
	}
}

type mockDataNodeClient struct {
	id                  int64
	state               commonpb.StateCode
	ch                  chan interface{}
	compactionStateResp *datapb.CompactionStateResponse
	compactionResp      *commonpb.Status
}

func newMockDataNodeClient(id int64, ch chan interface{}) (*mockDataNodeClient, error) {
	return &mockDataNodeClient{
		id:    id,
		state: commonpb.StateCode_Initializing,
		ch:    ch,
	}, nil
}

type mockIndexNodeClient struct {
	id    int64
	state commonpb.StateCode
}

func newMockIndexNodeClient(id int64) (*mockIndexNodeClient, error) {
	return &mockIndexNodeClient{
		id:    id,
		state: commonpb.StateCode_Initializing,
	}, nil
}

func (c *mockDataNodeClient) Close() error {
	return nil
}

func (c *mockDataNodeClient) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    c.id,
			StateCode: c.state,
		},
	}, nil
}

func (c *mockDataNodeClient) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (c *mockDataNodeClient) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) FlushSegments(ctx context.Context, in *datapb.FlushSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	if c.ch != nil {
		c.ch <- in
	}
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest, opts ...grpc.CallOption) (*datapb.ResendSegmentStatsResponse, error) {
	return &datapb.ResendSegmentStatsResponse{
		Status: merr.Success(),
	}, nil
}

func (c *mockDataNodeClient) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{
		Status: merr.Success(),
	}, nil
}

func (c *mockDataNodeClient) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): change the id, though it's not important in ut
	nodeID := c.id

	nodeInfos := metricsinfo.DataNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, nodeID),
			ID:   nodeID,
		},
	}
	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, nodeID),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, nodeID),
	}, nil
}

func (c *mockDataNodeClient) CompactionV2(ctx context.Context, req *datapb.CompactionPlan, opts ...grpc.CallOption) (*commonpb.Status, error) {
	if c.ch != nil {
		c.ch <- struct{}{}
		if c.compactionResp != nil {
			return c.compactionResp, nil
		}
		return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
	}
	if c.compactionResp != nil {
		return c.compactionResp, nil
	}
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "not implemented"}, nil
}

func (c *mockDataNodeClient) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest, opts ...grpc.CallOption) (*datapb.CompactionStateResponse, error) {
	return c.compactionStateResp, nil
}

func (c *mockDataNodeClient) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) FlushChannels(ctx context.Context, req *datapb.FlushChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) NotifyChannelOperation(ctx context.Context, req *datapb.ChannelOperationsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (c *mockDataNodeClient) CheckChannelOperationProgress(ctx context.Context, req *datapb.ChannelWatchInfo, opts ...grpc.CallOption) (*datapb.ChannelOperationProgressResponse, error) {
	return &datapb.ChannelOperationProgressResponse{Status: merr.Success()}, nil
}

func (c *mockDataNodeClient) PreImport(ctx context.Context, req *datapb.PreImportRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) ImportV2(ctx context.Context, req *datapb.ImportRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) QueryPreImport(ctx context.Context, req *datapb.QueryPreImportRequest, opts ...grpc.CallOption) (*datapb.QueryPreImportResponse, error) {
	return &datapb.QueryPreImportResponse{Status: merr.Success()}, nil
}

func (c *mockDataNodeClient) QueryImport(ctx context.Context, req *datapb.QueryImportRequest, opts ...grpc.CallOption) (*datapb.QueryImportResponse, error) {
	return &datapb.QueryImportResponse{Status: merr.Success()}, nil
}

func (c *mockDataNodeClient) DropImport(ctx context.Context, req *datapb.DropImportRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) QuerySlot(ctx context.Context, req *datapb.QuerySlotRequest, opts ...grpc.CallOption) (*datapb.QuerySlotResponse, error) {
	return &datapb.QuerySlotResponse{Status: merr.Success()}, nil
}

func (c *mockDataNodeClient) DropCompactionPlan(ctx context.Context, req *datapb.DropCompactionPlanRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (c *mockDataNodeClient) Stop() error {
	c.state = commonpb.StateCode_Abnormal
	return nil
}

type mockRootCoordClient struct {
	state commonpb.StateCode
	cnt   atomic.Int64
}

func (m *mockRootCoordClient) DescribeDatabase(ctx context.Context, in *rootcoordpb.DescribeDatabaseRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
	return &rootcoordpb.DescribeDatabaseResponse{
		Status:           merr.Success(),
		DbID:             1,
		DbName:           "default",
		CreatedTimestamp: 1,
	}, nil
}

func (m *mockRootCoordClient) Close() error {
	// TODO implement me
	panic("implement me")
}

func (m *mockRootCoordClient) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	// TODO implement me
	panic("implement me")
}

func (m *mockRootCoordClient) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) DescribeAlias(ctx context.Context, req *milvuspb.DescribeAliasRequest, opts ...grpc.CallOption) (*milvuspb.DescribeAliasResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) ListAliases(ctx context.Context, req *milvuspb.ListAliasesRequest, opts ...grpc.CallOption) (*milvuspb.ListAliasesResponse, error) {
	panic("implement me")
}

func newMockRootCoordClient() *mockRootCoordClient {
	return &mockRootCoordClient{state: commonpb.StateCode_Healthy}
}

func (m *mockRootCoordClient) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *mockRootCoordClient) Stop() error {
	m.state = commonpb.StateCode_Abnormal
	return nil
}

func (m *mockRootCoordClient) Register() error {
	return nil
}

func (m *mockRootCoordClient) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    0,
			Role:      "",
			StateCode: m.state,
			ExtraInfo: []*commonpb.KeyValuePair{},
		},
		SubcomponentStates: []*milvuspb.ComponentInfo{},
		Status:             merr.Success(),
	}, nil
}

func (m *mockRootCoordClient) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	panic("not implemented") // TODO: Implement
}

// DDL request
func (m *mockRootCoordClient) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	// return not exist
	if req.CollectionID == -1 {
		err := merr.WrapErrCollectionNotFound(req.GetCollectionID())
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}
	return &milvuspb.DescribeCollectionResponse{
		Status: merr.Success(),
		Schema: &schemapb.CollectionSchema{
			Name: "test",
		},
		CollectionID:        1314,
		VirtualChannelNames: []string{"vchan1"},
	}, nil
}

func (m *mockRootCoordClient) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return m.DescribeCollection(ctx, req)
}

func (m *mockRootCoordClient) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"test"},
	}, nil
}

func (m *mockRootCoordClient) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest, opts ...grpc.CallOption) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockRootCoordClient) AlterDatabase(ctx context.Context, in *rootcoordpb.AlterDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	return &milvuspb.ShowPartitionsResponse{
		Status:         merr.Success(),
		PartitionNames: []string{"_default"},
		PartitionIDs:   []int64{0},
	}, nil
}

func (m *mockRootCoordClient) ShowPartitionsInternal(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	return m.ShowPartitions(ctx, req)
}

// global timestamp allocator
func (m *mockRootCoordClient) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	if m.state != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocTimestampResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil
	}

	val := m.cnt.Add(int64(req.Count))
	phy := time.Now().UnixNano() / int64(time.Millisecond)
	ts := tsoutil.ComposeTS(phy, val)
	return &rootcoordpb.AllocTimestampResponse{
		Status:    merr.Success(),
		Timestamp: ts,
		Count:     req.Count,
	}, nil
}

func (m *mockRootCoordClient) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	if m.state != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocIDResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil
	}
	val := m.cnt.Add(int64(req.Count))
	return &rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     val,
		Count:  req.Count,
	}, nil
}

// segment
func (m *mockRootCoordClient) DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest, opts ...grpc.CallOption) (*milvuspb.DescribeSegmentResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) GetPChannelInfo(ctx context.Context, req *rootcoordpb.GetPChannelInfoRequest, opts ...grpc.CallOption) (*rootcoordpb.GetPChannelInfoResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) DescribeSegments(ctx context.Context, req *rootcoordpb.DescribeSegmentsRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeSegmentsResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) GetDdChannel(ctx context.Context, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  "ddchannel",
	}, nil
}

func (m *mockRootCoordClient) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) InvalidateCollectionMetaCache(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockRootCoordClient) AddNewSegment(ctx context.Context, in *datapb.SegmentMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockRootCoordClient) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): change the id, though it's not important in ut
	nodeID := UniqueID(20210901)

	rootCoordTopology := metricsinfo.RootCoordTopology{
		Self: metricsinfo.RootCoordInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
				ID:   nodeID,
			},
		},
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}

	resp, err := metricsinfo.MarshalTopology(rootCoordTopology)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
	}, nil
}

func (m *mockRootCoordClient) BackupRBAC(ctx context.Context, req *milvuspb.BackupRBACMetaRequest, opts ...grpc.CallOption) (*milvuspb.BackupRBACMetaResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) RestoreRBAC(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

type mockCompactionTrigger struct {
	methods map[string]interface{}
}

// triggerSingleCompaction trigerr a compaction bundled with collection-partiiton-channel-segment
func (t *mockCompactionTrigger) triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string, blockToSendSignal bool) error {
	if f, ok := t.methods["triggerSingleCompaction"]; ok {
		if ff, ok := f.(func(collectionID int64, partitionID int64, segmentID int64, channel string) error); ok {
			return ff(collectionID, partitionID, segmentID, channel)
		}
	}
	panic("not implemented")
}

// triggerManualCompaction force to start a compaction
func (t *mockCompactionTrigger) triggerManualCompaction(collectionID int64) (UniqueID, error) {
	if f, ok := t.methods["triggerManualCompaction"]; ok {
		if ff, ok := f.(func(collectionID int64) (UniqueID, error)); ok {
			return ff(collectionID)
		}
	}
	panic("not implemented")
}

func (t *mockCompactionTrigger) start() {
	if f, ok := t.methods["start"]; ok {
		if ff, ok := f.(func()); ok {
			ff()
			return
		}
	}
	panic("not implemented")
}

func (t *mockCompactionTrigger) stop() {
	if f, ok := t.methods["stop"]; ok {
		if ff, ok := f.(func()); ok {
			ff()
			return
		}
	}
	panic("not implemented")
}

func (m *mockRootCoordClient) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil
}

func (m *mockRootCoordClient) CreatePrivilegeGroup(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) DropPrivilegeGroup(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) ListPrivilegeGroups(ctx context.Context, req *milvuspb.ListPrivilegeGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordClient) OperatePrivilegeGroup(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

type mockHandler struct {
	meta *meta
}

func newMockHandler() *mockHandler {
	return &mockHandler{}
}

func (h *mockHandler) GetQueryVChanPositions(channel RWChannel, partitionID ...UniqueID) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: channel.GetCollectionID(),
		ChannelName:  channel.GetName(),
	}
}

func (h *mockHandler) GetDataVChanPositions(channel RWChannel, partitionID UniqueID) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: channel.GetCollectionID(),
		ChannelName:  channel.GetName(),
	}
}

func (h *mockHandler) CheckShouldDropChannel(channel string) bool {
	return false
}

func (h *mockHandler) FinishDropChannel(channel string, collectionID int64) error {
	return nil
}

func (h *mockHandler) GetCollection(_ context.Context, collectionID UniqueID) (*collectionInfo, error) {
	// empty schema
	if h.meta != nil {
		return h.meta.GetCollection(collectionID), nil
	}
	return &collectionInfo{ID: collectionID}, nil
}

func (h *mockHandler) GetCurrentSegmentsView(ctx context.Context, channel RWChannel, partitionIDs ...UniqueID) *SegmentsView {
	return nil
}

func newMockHandlerWithMeta(meta *meta) *mockHandler {
	return &mockHandler{
		meta: meta,
	}
}
