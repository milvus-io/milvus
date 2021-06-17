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

package grpcdataserviceclient

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

type mockMaster struct {
	types.MasterService
}

func (m *mockMaster) Init() error {
	return nil
}

func (m *mockMaster) Start() error {
	return nil
}

func (m *mockMaster) Stop() error {
	return fmt.Errorf("stop error")
}

func (m *mockMaster) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			StateCode: internalpb.StateCode_Healthy,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SubcomponentStates: []*internalpb.ComponentInfo{
			{
				StateCode: internalpb.StateCode_Healthy,
			},
		},
	}, nil
}

func (m *mockMaster) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "c1",
	}, nil
}

func (m *mockMaster) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		CollectionNames: []string{},
	}, nil
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	msFactory := msgstream.NewPmsFactory()
	dsServer, err := NewServer(ctx, msFactory)
	assert.Nil(t, err)

	Params.Init()
	Params.Port = 1000000
	err = dsServer.Run()
	assert.NotNil(t, err)
	assert.EqualError(t, err, "listen tcp: address 1000000: invalid port")

	Params.Port = rand.Int()%100 + 10000
	err = dsServer.Run()
	assert.Nil(t, err)

	t.Run("get component states", func(t *testing.T) {
		req := &internalpb.GetComponentStatesRequest{}
		rsp, err := dsServer.GetComponentStates(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get time tick channel", func(t *testing.T) {
		req := &internalpb.GetTimeTickChannelRequest{}
		rsp, err := dsServer.GetTimeTickChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get statistics channel", func(t *testing.T) {
		req := &internalpb.GetStatisticsChannelRequest{}
		rsp, err := dsServer.GetStatisticsChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get segment info", func(t *testing.T) {
		req := &datapb.GetSegmentInfoRequest{}
		rsp, err := dsServer.GetSegmentInfo(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	//t.Run("register node", func(t *testing.T) {
	//	req := &datapb.RegisterNodeRequest{
	//		Base: &commonpb.MsgBase{},
	//		Address: &commonpb.Address{},
	//	}
	//	rsp, err := dsServer.RegisterNode(ctx, req)
	//	assert.Nil(t, err)
	//	assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	//})

	t.Run("flush", func(t *testing.T) {
		req := &datapb.FlushRequest{}
		rsp, err := dsServer.Flush(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("assign segment id", func(t *testing.T) {
		req := &datapb.AssignSegmentIDRequest{}
		rsp, err := dsServer.AssignSegmentID(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("show segments", func(t *testing.T) {
		req := &datapb.ShowSegmentsRequest{}
		rsp, err := dsServer.ShowSegments(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get segment states", func(t *testing.T) {
		req := &datapb.GetSegmentStatesRequest{}
		rsp, err := dsServer.GetSegmentStates(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get insert binlog paths", func(t *testing.T) {
		req := &datapb.GetInsertBinlogPathsRequest{}
		rsp, err := dsServer.GetInsertBinlogPaths(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get insert channels", func(t *testing.T) {
		req := &datapb.GetInsertChannelsRequest{}
		rsp, err := dsServer.GetInsertChannels(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get collection statistics", func(t *testing.T) {
		req := &datapb.GetCollectionStatisticsRequest{}
		rsp, err := dsServer.GetCollectionStatistics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get partition statistics", func(t *testing.T) {
		req := &datapb.GetPartitionStatisticsRequest{}
		rsp, err := dsServer.GetPartitionStatistics(ctx, req)
		assert.Nil(t, err)
		assert.Nil(t, rsp)
	})

	t.Run("get segment info channel", func(t *testing.T) {
		req := &datapb.GetSegmentInfoChannelRequest{}
		rsp, err := dsServer.GetSegmentInfoChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	err = dsServer.Stop()
	assert.Nil(t, err)
}
