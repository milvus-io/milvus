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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type metaMemoryKV struct {
	memkv.MemoryKV
}

func NewMetaMemoryKV() *metaMemoryKV {
	return &metaMemoryKV{MemoryKV: *memkv.NewMemoryKV()}
}

func (mm *metaMemoryKV) WalkWithPrefix(prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	keys, values, err := mm.MemoryKV.LoadWithPrefix(prefix)
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

func (mm *metaMemoryKV) Watch(key string) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) WatchWithPrefix(key string) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) WatchWithRevision(key string, revision int64) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) CompareVersionAndSwap(key string, version int64, target string) (bool, error) {
	panic("implement me")
}

func newMemoryMeta() (*meta, error) {
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	return newMeta(context.TODO(), catalog, nil)
}

var _ allocator = (*MockAllocator)(nil)

type MockAllocator struct {
	cnt int64
}

func (m *MockAllocator) allocTimestamp(ctx context.Context) (Timestamp, error) {
	val := atomic.AddInt64(&m.cnt, 1)
	return Timestamp(val), nil
}

func (m *MockAllocator) allocID(ctx context.Context) (UniqueID, error) {
	val := atomic.AddInt64(&m.cnt, 1)
	return val, nil
}

type MockAllocator0 struct{}

func (m *MockAllocator0) allocTimestamp(ctx context.Context) (Timestamp, error) {
	return Timestamp(0), nil
}

func (m *MockAllocator0) allocID(ctx context.Context) (UniqueID, error) {
	return 0, nil
}

var _ allocator = (*FailsAllocator)(nil)

// FailsAllocator allocator that fails
type FailsAllocator struct {
	allocTsSucceed bool
	allocIDSucceed bool
}

func (a *FailsAllocator) allocTimestamp(_ context.Context) (Timestamp, error) {
	if a.allocTsSucceed {
		return 0, nil
	}
	return 0, errors.New("always fail")
}

func (a *FailsAllocator) allocID(_ context.Context) (UniqueID, error) {
	if a.allocIDSucceed {
		return 0, nil
	}
	return 0, errors.New("always fail")
}

func newMockAllocator() *MockAllocator {
	return &MockAllocator{}
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

type mockDataNodeClient struct {
	id                   int64
	state                commonpb.StateCode
	ch                   chan interface{}
	compactionStateResp  *datapb.CompactionStateResponse
	addImportSegmentResp *datapb.AddImportSegmentResponse
	compactionResp       *commonpb.Status
}

func newMockDataNodeClient(id int64, ch chan interface{}) (*mockDataNodeClient, error) {
	return &mockDataNodeClient{
		id:    id,
		state: commonpb.StateCode_Initializing,
		ch:    ch,
		addImportSegmentResp: &datapb.AddImportSegmentResponse{
			Status: merr.Success(),
		},
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

func (c *mockDataNodeClient) Compaction(ctx context.Context, req *datapb.CompactionPlan, opts ...grpc.CallOption) (*commonpb.Status, error) {
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

func (c *mockDataNodeClient) Import(ctx context.Context, in *datapb.ImportTaskRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) AddImportSegment(ctx context.Context, req *datapb.AddImportSegmentRequest, opts ...grpc.CallOption) (*datapb.AddImportSegmentResponse, error) {
	return c.addImportSegmentResp, nil
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

func (c *mockDataNodeClient) Stop() error {
	c.state = commonpb.StateCode_Abnormal
	return nil
}

type mockRootCoordClient struct {
	state commonpb.StateCode
	cnt   int64
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
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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

	val := atomic.AddInt64(&m.cnt, int64(req.Count))
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
	val := atomic.AddInt64(&m.cnt, int64(req.Count))
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

func (m *mockRootCoordClient) Import(ctx context.Context, req *milvuspb.ImportRequest, opts ...grpc.CallOption) (*milvuspb.ImportResponse, error) {
	panic("not implemented") // TODO: Implement
}

// Check import task state from datanode
func (m *mockRootCoordClient) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest, opts ...grpc.CallOption) (*milvuspb.GetImportStateResponse, error) {
	panic("not implemented") // TODO: Implement
}

// Returns id array of all import tasks
func (m *mockRootCoordClient) ListImportTasks(ctx context.Context, in *milvuspb.ListImportTasksRequest, opts ...grpc.CallOption) (*milvuspb.ListImportTasksResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordClient) ReportImport(ctx context.Context, req *rootcoordpb.ImportResult, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

type mockCompactionHandler struct {
	methods map[string]interface{}
}

func (h *mockCompactionHandler) start() {
	if f, ok := h.methods["start"]; ok {
		if ff, ok := f.(func()); ok {
			ff()
			return
		}
	}
	panic("not implemented")
}

func (h *mockCompactionHandler) stop() {
	if f, ok := h.methods["stop"]; ok {
		if ff, ok := f.(func()); ok {
			ff()
			return
		}
	}
	panic("not implemented")
}

// execCompactionPlan start to execute plan and return immediately
func (h *mockCompactionHandler) execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) error {
	if f, ok := h.methods["execCompactionPlan"]; ok {
		if ff, ok := f.(func(signal *compactionSignal, plan *datapb.CompactionPlan) error); ok {
			return ff(signal, plan)
		}
	}
	panic("not implemented")
}

// // completeCompaction record the result of a compaction
// func (h *mockCompactionHandler) completeCompaction(result *datapb.CompactionResult) error {
// 	if f, ok := h.methods["completeCompaction"]; ok {
// 		if ff, ok := f.(func(result *datapb.CompactionResult) error); ok {
// 			return ff(result)
// 		}
// 	}
// 	panic("not implemented")
// }

// getCompaction return compaction task. If planId does not exist, return nil.
func (h *mockCompactionHandler) getCompaction(planID int64) *compactionTask {
	if f, ok := h.methods["getCompaction"]; ok {
		if ff, ok := f.(func(planID int64) *compactionTask); ok {
			return ff(planID)
		}
	}
	panic("not implemented")
}

// expireCompaction set the compaction state to expired
func (h *mockCompactionHandler) updateCompaction(ts Timestamp) error {
	if f, ok := h.methods["expireCompaction"]; ok {
		if ff, ok := f.(func(ts Timestamp) error); ok {
			return ff(ts)
		}
	}
	panic("not implemented")
}

// isFull return true if the task pool is full
func (h *mockCompactionHandler) isFull() bool {
	if f, ok := h.methods["isFull"]; ok {
		if ff, ok := f.(func() bool); ok {
			return ff()
		}
	}
	panic("not implemented")
}

// get compaction tasks by signal id
func (h *mockCompactionHandler) getCompactionTasksBySignalID(signalID int64) []*compactionTask {
	if f, ok := h.methods["getCompactionTasksBySignalID"]; ok {
		if ff, ok := f.(func(signalID int64) []*compactionTask); ok {
			return ff(signalID)
		}
	}
	panic("not implemented")
}

type mockCompactionTrigger struct {
	methods map[string]interface{}
}

// triggerCompaction trigger a compaction if any compaction condition satisfy.
func (t *mockCompactionTrigger) triggerCompaction() error {
	if f, ok := t.methods["triggerCompaction"]; ok {
		if ff, ok := f.(func() error); ok {
			return ff()
		}
	}
	panic("not implemented")
}

// triggerSingleCompaction trigerr a compaction bundled with collection-partiiton-channel-segment
func (t *mockCompactionTrigger) triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string) error {
	if f, ok := t.methods["triggerSingleCompaction"]; ok {
		if ff, ok := f.(func(collectionID int64, partitionID int64, segmentID int64, channel string) error); ok {
			return ff(collectionID, partitionID, segmentID, channel)
		}
	}
	panic("not implemented")
}

// forceTriggerCompaction force to start a compaction
func (t *mockCompactionTrigger) forceTriggerCompaction(collectionID int64) (UniqueID, error) {
	if f, ok := t.methods["forceTriggerCompaction"]; ok {
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

type mockHandler struct {
	meta *meta
}

func newMockHandler() *mockHandler {
	return &mockHandler{}
}

func (h *mockHandler) GetQueryVChanPositions(channel *channel, partitionID ...UniqueID) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: channel.CollectionID,
		ChannelName:  channel.Name,
	}
}

func (h *mockHandler) GetDataVChanPositions(channel *channel, partitionID UniqueID) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: channel.CollectionID,
		ChannelName:  channel.Name,
	}
}

func (h *mockHandler) CheckShouldDropChannel(channel string) bool {
	return false
}

func (h *mockHandler) FinishDropChannel(channel string) error {
	return nil
}

func (h *mockHandler) GetCollection(_ context.Context, collectionID UniqueID) (*collectionInfo, error) {
	// empty schema
	if h.meta != nil {
		return h.meta.GetCollection(collectionID), nil
	}
	return &collectionInfo{ID: collectionID}, nil
}

func newMockHandlerWithMeta(meta *meta) *mockHandler {
	return &mockHandler{
		meta: meta,
	}
}
