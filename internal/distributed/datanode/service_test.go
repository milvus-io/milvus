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

package grpcdatanode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockDataNode struct {
	nodeID typeutil.UniqueID

	stateCode      commonpb.StateCode
	states         *milvuspb.ComponentStates
	status         *commonpb.Status
	err            error
	initErr        error
	startErr       error
	stopErr        error
	regErr         error
	strResp        *milvuspb.StringResponse
	configResp     *internalpb.ShowConfigurationsResponse
	metricResp     *milvuspb.GetMetricsResponse
	resendResp     *datapb.ResendSegmentStatsResponse
	compactionResp *datapb.CompactionStateResponse
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

func (m *MockDataNode) UpdateStateCode(code commonpb.StateCode) {
	m.stateCode = code
}

func (m *MockDataNode) GetStateCode() commonpb.StateCode {
	return m.stateCode
}

func (m *MockDataNode) SetAddress(address string) {
}

func (m *MockDataNode) GetAddress() string {
	return ""
}

func (m *MockDataNode) GetNodeID() int64 {
	return 2
}

func (m *MockDataNode) SetRootCoordClient(rc types.RootCoordClient) error {
	return m.err
}

func (m *MockDataNode) SetDataCoordClient(dc types.DataCoordClient) error {
	return m.err
}

func (m *MockDataNode) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return m.states, m.err
}

func (m *MockDataNode) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return m.strResp, m.err
}

func (m *MockDataNode) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return m.configResp, m.err
}

func (m *MockDataNode) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return m.metricResp, m.err
}

func (m *MockDataNode) CompactionV2(ctx context.Context, req *datapb.CompactionPlan) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	return m.compactionResp, m.err
}

func (m *MockDataNode) SetEtcdClient(client *clientv3.Client) {
}

func (m *MockDataNode) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error) {
	return m.resendResp, m.err
}

func (m *MockDataNode) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) FlushChannels(ctx context.Context, req *datapb.FlushChannelsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) NotifyChannelOperation(ctx context.Context, req *datapb.ChannelOperationsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) CheckChannelOperationProgress(ctx context.Context, req *datapb.ChannelWatchInfo) (*datapb.ChannelOperationProgressResponse, error) {
	return &datapb.ChannelOperationProgressResponse{}, m.err
}

func (m *MockDataNode) PreImport(ctx context.Context, req *datapb.PreImportRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) ImportV2(ctx context.Context, req *datapb.ImportRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) QueryPreImport(ctx context.Context, req *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error) {
	return &datapb.QueryPreImportResponse{}, m.err
}

func (m *MockDataNode) QueryImport(ctx context.Context, req *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
	return &datapb.QueryImportResponse{}, m.err
}

func (m *MockDataNode) DropImport(ctx context.Context, req *datapb.DropImportRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockDataNode) QuerySlot(ctx context.Context, req *datapb.QuerySlotRequest) (*datapb.QuerySlotResponse, error) {
	return &datapb.QuerySlotResponse{}, m.err
}

func (m *MockDataNode) DropCompactionPlan(ctx context.Context, req *datapb.DropCompactionPlanRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func Test_NewServer(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	mockRootCoord := mocks.NewMockRootCoordClient(t)
	mockRootCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: merr.Success(),
		SubcomponentStates: []*milvuspb.ComponentInfo{
			{
				StateCode: commonpb.StateCode_Healthy,
			},
		},
	}, nil)
	server.newRootCoordClient = func() (types.RootCoordClient, error) {
		return mockRootCoord, nil
	}

	mockDataCoord := mocks.NewMockDataCoordClient(t)
	mockDataCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: merr.Success(),
		SubcomponentStates: []*milvuspb.ComponentInfo{
			{
				StateCode: commonpb.StateCode_Healthy,
			},
		},
	}, nil)
	server.newDataCoordClient = func() (types.DataCoordClient, error) {
		return mockDataCoord, nil
	}

	t.Run("Run", func(t *testing.T) {
		server.datanode = &MockDataNode{}
		err = server.Prepare()
		assert.NoError(t, err)
		err = server.Run()
		assert.NoError(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		server.datanode = &MockDataNode{
			states: &milvuspb.ComponentStates{},
		}
		states, err := server.GetComponentStates(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		server.datanode = &MockDataNode{
			strResp: &milvuspb.StringResponse{},
		}
		states, err := server.GetStatisticsChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("WatchDmChannels", func(t *testing.T) {
		server.datanode = &MockDataNode{
			status: &commonpb.Status{},
		}
		states, err := server.WatchDmChannels(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("FlushSegments", func(t *testing.T) {
		server.datanode = &MockDataNode{
			status: &commonpb.Status{},
		}
		states, err := server.FlushSegments(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		server.datanode = &MockDataNode{
			configResp: &internalpb.ShowConfigurationsResponse{},
		}
		resp, err := server.ShowConfigurations(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		server.datanode = &MockDataNode{
			metricResp: &milvuspb.GetMetricsResponse{},
		}
		resp, err := server.GetMetrics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("Compaction", func(t *testing.T) {
		server.datanode = &MockDataNode{
			status: &commonpb.Status{},
		}
		resp, err := server.CompactionV2(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ResendSegmentStats", func(t *testing.T) {
		server.datanode = &MockDataNode{
			resendResp: &datapb.ResendSegmentStatsResponse{},
		}
		resp, err := server.ResendSegmentStats(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("NotifyChannelOperation", func(t *testing.T) {
		server.datanode = &MockDataNode{
			status: &commonpb.Status{},
		}
		resp, err := server.NotifyChannelOperation(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("CheckChannelOperationProgress", func(t *testing.T) {
		server.datanode = &MockDataNode{
			status: &commonpb.Status{},
		}
		resp, err := server.CheckChannelOperationProgress(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("DropCompactionPlans", func(t *testing.T) {
		server.datanode = &MockDataNode{
			status: &commonpb.Status{},
		}
		resp, err := server.DropCompactionPlan(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	err = server.Stop()
	assert.NoError(t, err)
}

func Test_Run(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	mockRootCoord := mocks.NewMockRootCoordClient(t)
	mockRootCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: merr.Success(),
		SubcomponentStates: []*milvuspb.ComponentInfo{
			{
				StateCode: commonpb.StateCode_Healthy,
			},
		},
	}, nil)
	server.newRootCoordClient = func() (types.RootCoordClient, error) {
		return mockRootCoord, nil
	}

	mockDataCoord := mocks.NewMockDataCoordClient(t)
	mockDataCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: merr.Success(),
		SubcomponentStates: []*milvuspb.ComponentInfo{
			{
				StateCode: commonpb.StateCode_Healthy,
			},
		},
	}, nil)
	server.newDataCoordClient = func() (types.DataCoordClient, error) {
		return mockDataCoord, nil
	}

	server.datanode = &MockDataNode{
		regErr: errors.New("error"),
	}

	err = server.Prepare()
	assert.NoError(t, err)
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
}
