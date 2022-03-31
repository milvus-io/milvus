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
	"errors"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func newMemoryMeta(allocator allocator) (*meta, error) {
	memoryKV := memkv.NewMemoryKV()
	return newMeta(memoryKV)
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

var _ allocator = (*FailsAllocator)(nil)

// FailsAllocator allocator that fails
type FailsAllocator struct{}

func (a *FailsAllocator) allocTimestamp(_ context.Context) (Timestamp, error) {
	return 0, errors.New("always fail")
}

func (a *FailsAllocator) allocID(_ context.Context) (UniqueID, error) {
	return 0, errors.New("always fail")
}

// a mock kv that always fail when do `Save`
type saveFailKV struct{ kv.TxnKV }

// Save override behavior
func (kv *saveFailKV) Save(key, value string) error {
	return errors.New("mocked fail")
}

func (kv *saveFailKV) MultiSave(kvs map[string]string) error {
	return errors.New("mocked fail")
}

// a mock kv that always fail when do `Remove`
type removeFailKV struct{ kv.TxnKV }

// Remove override behavior, inject error
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
			{FieldID: 1, Name: "field1", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length_per_row", Value: "100"}}},
			{FieldID: 2, Name: "field2", IsPrimaryKey: false, Description: "field no.2", DataType: schemapb.DataType_FloatVector},
		},
	}
}

type mockDataNodeClient struct {
	id    int64
	state internalpb.StateCode
	ch    chan interface{}
}

func newMockDataNodeClient(id int64, ch chan interface{}) (*mockDataNodeClient, error) {
	return &mockDataNodeClient{
		id:    id,
		state: internalpb.StateCode_Initializing,
		ch:    ch,
	}, nil
}

func (c *mockDataNodeClient) Init() error {
	return nil
}

func (c *mockDataNodeClient) Start() error {
	c.state = internalpb.StateCode_Healthy
	return nil
}

func (c *mockDataNodeClient) Register() error {
	return nil
}

func (c *mockDataNodeClient) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
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

func (c *mockDataNodeClient) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): change the id, though it's not important in ut
	nodeID := UniqueID(c.id)

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
	}
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "not implemented"}, nil
}

func (c *mockDataNodeClient) Import(ctx context.Context, in *datapb.ImportTaskRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (c *mockDataNodeClient) Stop() error {
	c.state = internalpb.StateCode_Abnormal
	return nil
}

type mockRootCoordService struct {
	state internalpb.StateCode
	cnt   int64
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
	return &mockRootCoordService{state: internalpb.StateCode_Healthy}
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
	m.state = internalpb.StateCode_Abnormal
	return nil
}

func (m *mockRootCoordService) Register() error {
	return nil
}

func (m *mockRootCoordService) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			NodeID:    0,
			Role:      "",
			StateCode: m.state,
			ExtraInfo: []*commonpb.KeyValuePair{},
		},
		SubcomponentStates: []*internalpb.ComponentInfo{},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (m *mockRootCoordService) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	panic("not implemented") // TODO: Implement
}

//DDL request
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

func (m *mockRootCoordService) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		CollectionNames: []string{"test"},
	}, nil
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

//index builder service
func (m *mockRootCoordService) CreateIndex(ctx context.Context, req *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) DescribeIndex(ctx context.Context, req *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockRootCoordService) DropIndex(ctx context.Context, req *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

//global timestamp allocator
func (m *mockRootCoordService) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	if m.state != internalpb.StateCode_Healthy {
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
	if m.state != internalpb.StateCode_Healthy {
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

//segment
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

func (m *mockRootCoordService) ReleaseDQLMessageStream(ctx context.Context, req *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}
func (m *mockRootCoordService) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}
func (m *mockRootCoordService) AddNewSegment(ctx context.Context, in *datapb.SegmentMsg) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
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

// Report impot task state to rootcoord
func (m *mockRootCoordService) ReportImport(ctx context.Context, req *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
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

// completeCompaction record the result of a compaction
func (h *mockCompactionHandler) completeCompaction(result *datapb.CompactionResult) error {
	if f, ok := h.methods["completeCompaction"]; ok {
		if ff, ok := f.(func(result *datapb.CompactionResult) error); ok {
			return ff(result)
		}
	}
	panic("not implemented")
}

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
func (h *mockCompactionHandler) expireCompaction(ts Timestamp) error {
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
func (t *mockCompactionTrigger) triggerCompaction(tt *timetravel) error {
	if f, ok := t.methods["triggerCompaction"]; ok {
		if ff, ok := f.(func(tt *timetravel) error); ok {
			return ff(tt)
		}
	}
	panic("not implemented")
}

// triggerSingleCompaction trigerr a compaction bundled with collection-partiiton-channel-segment
func (t *mockCompactionTrigger) triggerSingleCompaction(collectionID int64, partitionID int64, segmentID int64, channel string, tt *timetravel) error {
	if f, ok := t.methods["triggerSingleCompaction"]; ok {
		if ff, ok := f.(func(collectionID int64, partitionID int64, segmentID int64, channel string, tt *timetravel) error); ok {
			return ff(collectionID, partitionID, segmentID, channel, tt)
		}
	}
	panic("not implemented")
}

// forceTriggerCompaction force to start a compaction
func (t *mockCompactionTrigger) forceTriggerCompaction(collectionID int64, tt *timetravel) (UniqueID, error) {
	if f, ok := t.methods["forceTriggerCompaction"]; ok {
		if ff, ok := f.(func(collectionID int64, tt *timetravel) (UniqueID, error)); ok {
			return ff(collectionID, tt)
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

type mockHandler struct {
}

func newMockHandler() *mockHandler {
	return &mockHandler{}
}

func (h *mockHandler) GetVChanPositions(channel string, collectionID UniqueID, partitionID UniqueID) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: collectionID,
		ChannelName:  channel,
	}
}

func (h *mockHandler) CheckShouldDropChannel(channel string) bool {
	return false
}

func (h *mockHandler) FinishDropChannel(channel string) {}
