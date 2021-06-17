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

package grpcdatanode

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

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

func (m *mockMaster) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		CollectionNames: []string{},
	}, nil
}

type mockDataService struct {
	types.DataService
}

func (m *mockDataService) Init() error {
	return nil
}

func (m *mockDataService) Start() error {
	return nil
}

func (m *mockDataService) Stop() error {
	return fmt.Errorf("stop error")
}

func (m *mockDataService) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
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

func (m *mockDataService) RegisterNode(ctx context.Context, req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return &datapb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		InitParams: &internalpb.InitParams{
			NodeID: int64(1),
		},
	}, nil
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	msFactory := msgstream.NewPmsFactory()
	dnServer, err := NewServer(ctx, msFactory)
	assert.Nil(t, err)

	Params.Init()

	dnServer.newMasterServiceClient = func() (types.MasterService, error) {
		return &mockMaster{}, nil
	}
	dnServer.newDataServiceClient = func(string, []string, time.Duration) types.DataService {
		return &mockDataService{}
	}

	grpcPort := rand.Int()%100 + 10000
	Params.listener, _ = net.Listen("tcp", ":"+strconv.Itoa(grpcPort))

	// to let datanode init pass
	dnServer.datanode.UpdateStateCode(internalpb.StateCode_Initializing)
	dnServer.datanode.WatchDmChannels(ctx, nil)

	err = dnServer.Run()
	assert.Nil(t, err)

	t.Run("get component states", func(t *testing.T) {
		req := &internalpb.GetComponentStatesRequest{}
		rsp, err := dnServer.GetComponentStates(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get statistics channel", func(t *testing.T) {
		req := &internalpb.GetStatisticsChannelRequest{}
		rsp, err := dnServer.GetStatisticsChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("watch dm channels", func(t *testing.T) {
		req := &datapb.WatchDmChannelsRequest{}
		_, err := dnServer.WatchDmChannels(ctx, req)
		assert.NotNil(t, err)
	})

	t.Run("flush segments", func(t *testing.T) {
		req := &datapb.FlushSegmentsRequest{
			Base: &commonpb.MsgBase{},
		}
		rsp, err := dnServer.FlushSegments(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.ErrorCode, commonpb.ErrorCode_Success)
	})

	err = dnServer.Stop()
	assert.Nil(t, err)
}
