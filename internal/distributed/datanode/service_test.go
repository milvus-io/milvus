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
	"errors"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockDataNode struct {
	nodeID typeutil.UniqueID

	stateCode  internalpb.StateCode
	states     *internalpb.ComponentStates
	status     *commonpb.Status
	err        error
	initErr    error
	startErr   error
	stopErr    error
	regErr     error
	strResp    *milvuspb.StringResponse
	metricResp *milvuspb.GetMetricsResponse
}

func (m *MockDataNode) Init() error {
	return m.initErr
}

func (m *MockDataNode) Start() error {
	return m.startErr
}

func (m *MockDataNode) Stop() error {
	return m.stopErr
}

func (m *MockDataNode) Register() error {
	return m.regErr
}

func (m *MockDataNode) SetNodeID(id typeutil.UniqueID) {
	m.nodeID = id
}

func (m *MockDataNode) UpdateStateCode(code internalpb.StateCode) {
	m.stateCode = code
}

func (m *MockDataNode) GetStateCode() internalpb.StateCode {
	return m.stateCode
}

func (m *MockDataNode) SetRootCoord(rc types.RootCoord) error {
	return m.err
}

func (m *MockDataNode) SetDataCoord(dc types.DataCoord) error {
	return m.err
}

func (m *MockDataNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return m.states, m.err
}

func (m *MockDataNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return m.strResp, m.err
}

func (m *MockDataNode) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return m.metricResp, m.err
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type mockDataCoord struct {
	types.DataCoord
}

func (m *mockDataCoord) Init() error {
	return nil
}
func (m *mockDataCoord) Start() error {
	return nil
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
func (m *mockDataCoord) Stop() error {
	return fmt.Errorf("stop error")
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type mockRootCoord struct {
	types.RootCoord
}

func (m *mockRootCoord) Init() error {
	return nil
}
func (m *mockRootCoord) Start() error {
	return nil
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
func (m *mockRootCoord) Stop() error {
	return fmt.Errorf("stop error")
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func Test_NewServer(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, server)

	server.newRootCoordClient = func(string, []string) (types.RootCoord, error) {
		return &mockRootCoord{}, nil
	}

	server.newDataCoordClient = func(string, []string) (types.DataCoord, error) {
		return &mockDataCoord{}, nil
	}

	t.Run("Run", func(t *testing.T) {
		server.datanode = &MockDataNode{}
		err = server.Run()
		assert.Nil(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		server.datanode = &MockDataNode{
			states: &internalpb.ComponentStates{},
		}
		states, err := server.GetComponentStates(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, states)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		server.datanode = &MockDataNode{
			strResp: &milvuspb.StringResponse{},
		}
		states, err := server.GetStatisticsChannel(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, states)
	})

	t.Run("WatchDmChannels", func(t *testing.T) {
		server.datanode = &MockDataNode{
			status: &commonpb.Status{},
		}
		states, err := server.WatchDmChannels(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, states)
	})

	t.Run("FlushSegments", func(t *testing.T) {
		server.datanode = &MockDataNode{
			status: &commonpb.Status{},
		}
		states, err := server.FlushSegments(ctx, nil)
		assert.NotNil(t, err)
		assert.NotNil(t, states)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		server.datanode = &MockDataNode{
			metricResp: &milvuspb.GetMetricsResponse{},
		}
		resp, err := server.GetMetrics(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	err = server.Stop()
	assert.Nil(t, err)
}

func Test_Run(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, server)

	server.datanode = &MockDataNode{
		regErr: errors.New("error"),
	}

	server.newRootCoordClient = func(string, []string) (types.RootCoord, error) {
		return &mockRootCoord{}, nil
	}

	server.newDataCoordClient = func(string, []string) (types.DataCoord, error) {
		return &mockDataCoord{}, nil
	}

	err = server.Run()
	assert.Error(t, err)

	server.datanode = &MockDataNode{
		startErr: errors.New("error"),
	}

	err = server.Run()
	assert.Error(t, err)

	server.datanode = &MockDataNode{
		initErr: errors.New("error"),
	}

	err = server.Run()
	assert.Error(t, err)

	server.datanode = &MockDataNode{
		stopErr: errors.New("error"),
	}

	err = server.Stop()
	assert.Error(t, err)
}
