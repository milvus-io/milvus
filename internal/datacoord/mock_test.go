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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
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

func (mm *metaMemoryKV) CompareVersionAndSwap(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
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

type MockAllocator0 struct {
}

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

// a mock kv that always fail when do `Save`
type saveFailKV struct{ kv.MetaKv }

// Save override behavior
func (kv *saveFailKV) Save(key, value string) error {
	return errors.New("mocked fail")
}

func (kv *saveFailKV) MultiSave(kvs map[string]string) error {
	return errors.New("mocked fail")
}

// a mock kv that always fail when do `Remove`
type removeFailKV struct{ kv.MetaKv }

// Remove override behavior, inject error
func (kv *removeFailKV) MultiRemove(key []string) error {
	return errors.New("mocked fail")
}

func (kv *removeFailKV) Remove(key string) error {
	return errors.New("mocked fail")
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
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
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

func (c *mockDataNodeClient) Init() error {
	return nil
}

func (c *mockDataNodeClient) Start() error {
	c.state = commonpb.StateCode_Healthy
	return nil
}

func (c *mockDataNodeClient) Register() error {
	return nil
}

func (c *mockDataNodeClient) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    c.id,
			StateCode: c.state,
		},
	}, nil
}

func (c *mockDataNodeClient) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (c *mockDataNodeClient) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) FlushSegments(ctx context.Context, in *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	if c.ch != nil {
		c.ch <- in
	}
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error) {
	return &datapb.ResendSegmentStatsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (c *mockDataNodeClient) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (c *mockDataNodeClient) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
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
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, nodeID),
	}, nil
}

func (c *mockDataNodeClient) Compaction(ctx context.Context, req *datapb.CompactionPlan) (*commonpb.Status, error) {
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

func (c *mockDataNodeClient) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	return c.compactionStateResp, nil
}

func (c *mockDataNodeClient) Import(ctx context.Context, in *datapb.ImportTaskRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) AddImportSegment(ctx context.Context, req *datapb.AddImportSegmentRequest) (*datapb.AddImportSegmentResponse, error) {
	return c.addImportSegmentResp, nil
}

func (c *mockDataNodeClient) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) Stop() error {
	c.state = commonpb.StateCode_Abnormal
	return nil
}

type mockRootCoordService struct {
	state commonpb.StateCode
	cnt   int64
}

func (m *mockRootCoordService) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockRootCoordService) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordService) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordService) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordService) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func newMockRootCoordService() *mockRootCoordService {
	return &mockRootCoordService{state: commonpb.StateCode_Healthy}
}

func (m *mockRootCoordService) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *mockRootCoordService) Init() error {
	return nil
}

func (m *mockRootCoordService) Start() error {
	return nil
}

func (m *mockRootCoordService) Stop() error {
	m.state = commonpb.StateCode_Abnormal
	return nil
}

func (m *mockRootCoordService) Register() error {
	return nil
}

func (m *mockRootCoordService) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    0,
			Role:      "",
			StateCode: m.state,
			ExtraInfo: []*commonpb.KeyValuePair{},
		},
		SubcomponentStates: []*milvuspb.ComponentInfo{},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (m *mockRootCoordService) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	panic("not implemented") // TODO: Implement
}

// DDL request
func (m *mockRootCoordService) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	// return not exist
	if req.CollectionID == -1 {
		err := common.NewCollectionNotExistError(fmt.Sprintf("can't find collection: %d", req.CollectionID))
		return &milvuspb.DescribeCollectionResponse{
			// TODO: use commonpb.ErrorCode_CollectionNotExists. SDK use commonpb.ErrorCode_UnexpectedError now.
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: err.Error()},
		}, nil
	}
	return &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Schema: &schemapb.CollectionSchema{
			Name: "test",
		},
		CollectionID:        1314,
		VirtualChannelNames: []string{"vchan1"},
	}, nil
}

func (m *mockRootCoordService) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return m.DescribeCollection(ctx, req)
}

func (m *mockRootCoordService) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		CollectionNames: []string{"test"},
	}, nil
}

func (m *mockRootCoordService) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return &milvuspb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		PartitionNames: []string{"_default"},
		PartitionIDs:   []int64{0},
	}, nil
}

func (m *mockRootCoordService) ShowPartitionsInternal(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return m.ShowPartitions(ctx, req)
}

// global timestamp allocator
func (m *mockRootCoordService) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	if m.state != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocTimestampResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil
	}

	val := atomic.AddInt64(&m.cnt, int64(req.Count))
	phy := time.Now().UnixNano() / int64(time.Millisecond)
	ts := tsoutil.ComposeTS(phy, val)
	return &rootcoordpb.AllocTimestampResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Timestamp: ts,
		Count:     req.Count,
	}, nil
}

func (m *mockRootCoordService) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	if m.state != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocIDResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil
	}
	val := atomic.AddInt64(&m.cnt, int64(req.Count))
	return &rootcoordpb.AllocIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		ID:    val,
		Count: req.Count,
	}, nil
}

// segment
func (m *mockRootCoordService) DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) DescribeSegments(ctx context.Context, req *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordService) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "ddchannel",
	}, nil
}

func (m *mockRootCoordService) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) InvalidateCollectionMetaCache(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockRootCoordService) AddNewSegment(ctx context.Context, in *datapb.SegmentMsg) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (m *mockRootCoordService) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
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
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
	}, nil
}

func (m *mockRootCoordService) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	panic("not implemented") // TODO: Implement
}

// Check import task state from datanode
func (m *mockRootCoordService) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	panic("not implemented") // TODO: Implement
}

// Returns id array of all import tasks
func (m *mockRootCoordService) ListImportTasks(ctx context.Context, in *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) ReportImport(ctx context.Context, req *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
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

func (m *mockRootCoordService) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordService) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordService) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordService) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordService) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordService) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordService) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordService) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordService) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordService) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordService) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockRootCoordService) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	panic("implement me")
}

func (m *mockRootCoordService) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
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

func (h *mockHandler) CheckShouldDropChannel(channel string, collectionID UniqueID) bool {
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
