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

/*
import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

type mockRootCoord struct {
	types.RootCoord
}

func (m *mockRootCoord) Init() error {
	return nil
}

func (m *mockRootCoord) Start() error {
	return nil
}

func (m *mockRootCoord) Stop() error {
	return fmt.Errorf("stop error")
}

func (m *mockRootCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
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

func (m *mockRootCoord) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		CollectionNames: []string{},
	}, nil
}

type mockDataCoord struct {
	types.DataCoord
}

func (m *mockDataCoord) Init() error {
	return nil
}

func (m *mockDataCoord) Start() error {
	return nil
}

func (m *mockDataCoord) Stop() error {
	return fmt.Errorf("stop error")
}

func (m *mockDataCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
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

func TestRun(t *testing.T) {
	ctx := context.Background()
	msFactory := msgstream.NewPmsFactory()
	dnServer, err := NewServer(ctx, msFactory)
	assert.Nil(t, err)

	Params.Init()

	dnServer.newRootCoordClient = func(string, []string) (types.RootCoord, error) {
		return &mockRootCoord{}, nil
	}
	dnServer.newDataCoordClient = func(string, []string) (types.DataCoord, error) {
		return &mockDataCoord{}, nil
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
*/
