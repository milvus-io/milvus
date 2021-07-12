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
package datacoord

import (
	"context"
	"sync/atomic"
	"time"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/util/tsoutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
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

type mockRootCoordService struct {
	cnt int64
}

func newMockRootCoordService() *mockRootCoordService {
	return &mockRootCoordService{}
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
