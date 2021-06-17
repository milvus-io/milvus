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
package dataservice

import (
	"context"
	"sync/atomic"
	"time"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/tsoutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func newMemoryMeta(allocator allocator) (*meta, error) {
	memoryKV := memkv.NewMemoryKV()
	return newMeta(memoryKV)
}

type MockAllocator struct {
	cnt int64
}

func (m *MockAllocator) allocTimestamp() (Timestamp, error) {
	val := atomic.AddInt64(&m.cnt, 1)
	phy := time.Now().UnixNano() / int64(time.Millisecond)
	ts := tsoutil.ComposeTS(phy, val)
	return ts, nil
}

func (m *MockAllocator) allocID() (UniqueID, error) {
	val := atomic.AddInt64(&m.cnt, 1)
	return val, nil
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
			{FieldID: 1, Name: "field1", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_String},
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

func (c *mockDataNodeClient) Stop() error {
	c.state = internalpb.StateCode_Abnormal
	return nil
}

type mockMasterService struct {
	cnt int64
}

func newMockMasterService() *mockMasterService {
	return &mockMasterService{}
}

func (m *mockMasterService) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *mockMasterService) Init() error {
	return nil
}

func (m *mockMasterService) Start() error {
	return nil
}

func (m *mockMasterService) Stop() error {
	return nil
}

func (m *mockMasterService) Register() error {
	return nil
}

func (m *mockMasterService) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			NodeID:    0,
			Role:      "",
			StateCode: internalpb.StateCode_Healthy,
			ExtraInfo: []*commonpb.KeyValuePair{},
		},
		SubcomponentStates: []*internalpb.ComponentInfo{},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (m *mockMasterService) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	panic("not implemented") // TODO: Implement
}

//DDL request
func (m *mockMasterService) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
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

func (m *mockMasterService) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		CollectionNames: []string{"test"},
	}, nil
}

func (m *mockMasterService) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
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
func (m *mockMasterService) CreateIndex(ctx context.Context, req *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) DescribeIndex(ctx context.Context, req *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) DropIndex(ctx context.Context, req *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

//global timestamp allocator
func (m *mockMasterService) AllocTimestamp(ctx context.Context, req *masterpb.AllocTimestampRequest) (*masterpb.AllocTimestampResponse, error) {
	val := atomic.AddInt64(&m.cnt, int64(req.Count))
	phy := time.Now().UnixNano() / int64(time.Millisecond)
	ts := tsoutil.ComposeTS(phy, val)
	return &masterpb.AllocTimestampResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Timestamp: ts,
		Count:     req.Count,
	}, nil
}

func (m *mockMasterService) AllocID(ctx context.Context, req *masterpb.AllocIDRequest) (*masterpb.AllocIDResponse, error) {
	val := atomic.AddInt64(&m.cnt, int64(req.Count))
	return &masterpb.AllocIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		ID:    val,
		Count: req.Count,
	}, nil
}

//segment
func (m *mockMasterService) DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMasterService) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "ddchannel",
	}, nil
}

func (m *mockMasterService) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

type mockStartupPolicy struct {
}

func newMockStartupPolicy() clusterStartupPolicy {
	return &mockStartupPolicy{}
}

func (p *mockStartupPolicy) apply(oldCluster map[string]*datapb.DataNodeInfo, delta *clusterDeltaChange, buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus) {
	return nil, nil
}

type mockSessionManager struct {
	ch chan interface{}
}

func newMockSessionManager(ch chan interface{}) sessionManager {
	return &mockSessionManager{
		ch: ch,
	}
}

func (m *mockSessionManager) getOrCreateSession(addr string) (types.DataNode, error) {
	return newMockDataNodeClient(0, m.ch)
}

func (m *mockSessionManager) releaseSession(addr string) {

}
func (m *mockSessionManager) release() {
}
