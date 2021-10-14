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

package grpcquerycoord

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockQueryCoord struct {
	states       *internalpb.ComponentStates
	status       *commonpb.Status
	err          error
	initErr      error
	startErr     error
	stopErr      error
	regErr       error
	strResp      *milvuspb.StringResponse
	showcolResp  *querypb.ShowCollectionsResponse
	showpartResp *querypb.ShowPartitionsResponse
	partResp     *querypb.GetPartitionStatesResponse
	channelResp  *querypb.CreateQueryChannelResponse
	infoResp     *querypb.GetSegmentInfoResponse
	metricResp   *milvuspb.GetMetricsResponse
}

func (m *MockQueryCoord) Init() error {
	return m.initErr
}

func (m *MockQueryCoord) Start() error {
	return m.startErr
}

func (m *MockQueryCoord) Stop() error {
	return m.stopErr
}

func (m *MockQueryCoord) Register() error {
	log.Debug("MockQueryCoord::Register")
	return m.regErr
}

func (m *MockQueryCoord) UpdateStateCode(code internalpb.StateCode) {
}

func (m *MockQueryCoord) SetRootCoord(types.RootCoord) error {
	return nil
}

func (m *MockQueryCoord) SetDataCoord(types.DataCoord) error {
	return nil
}

func (m *MockQueryCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	log.Debug("MockQueryCoord::WaitForComponentStates")
	return m.states, m.err
}

func (m *MockQueryCoord) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return m.strResp, m.err
}

func (m *MockQueryCoord) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return m.strResp, m.err
}

func (m *MockQueryCoord) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return m.showcolResp, m.err
}

func (m *MockQueryCoord) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryCoord) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryCoord) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	return m.showpartResp, m.err
}

func (m *MockQueryCoord) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	return m.partResp, m.err
}

func (m *MockQueryCoord) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryCoord) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryCoord) CreateQueryChannel(ctx context.Context, req *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error) {
	return m.channelResp, m.err
}

func (m *MockQueryCoord) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return m.infoResp, m.err
}

func (m *MockQueryCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return m.metricResp, m.err
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockRootCoord struct {
	types.RootCoord
	initErr  error
	startErr error
	regErr   error
	stopErr  error
	stateErr commonpb.ErrorCode
}

func (m *MockRootCoord) Init() error {
	return m.initErr
}

func (m *MockRootCoord) Start() error {
	return m.startErr
}

func (m *MockRootCoord) Stop() error {
	return m.stopErr
}

func (m *MockRootCoord) Register() error {
	return m.regErr
}

func (m *MockRootCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State:  &internalpb.ComponentInfo{StateCode: internalpb.StateCode_Healthy},
		Status: &commonpb.Status{ErrorCode: m.stateErr},
	}, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockDataCoord struct {
	types.DataCoord
	initErr  error
	startErr error
	stopErr  error
	regErr   error
	stateErr commonpb.ErrorCode
}

func (m *MockDataCoord) Init() error {
	return m.initErr
}

func (m *MockDataCoord) Start() error {
	return m.startErr
}

func (m *MockDataCoord) Stop() error {
	return m.stopErr
}

func (m *MockDataCoord) Register() error {
	return m.regErr
}

func (m *MockDataCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State:  &internalpb.ComponentInfo{StateCode: internalpb.StateCode_Healthy},
		Status: &commonpb.Status{ErrorCode: m.stateErr},
	}, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func Test_NewServer(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, server)

	mqc := &MockQueryCoord{
		states: &internalpb.ComponentStates{
			State:  &internalpb.ComponentInfo{StateCode: internalpb.StateCode_Healthy},
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		},
		status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		err:          nil,
		strResp:      &milvuspb.StringResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}},
		showcolResp:  &querypb.ShowCollectionsResponse{},
		showpartResp: &querypb.ShowPartitionsResponse{},
		partResp:     &querypb.GetPartitionStatesResponse{},
		channelResp:  &querypb.CreateQueryChannelResponse{},
		infoResp:     &querypb.GetSegmentInfoResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}},
		metricResp:   &milvuspb.GetMetricsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}},
	}

	mdc := &MockDataCoord{
		stateErr: commonpb.ErrorCode_Success,
	}

	mrc := &MockRootCoord{
		stateErr: commonpb.ErrorCode_Success,
	}

	t.Run("Run", func(t *testing.T) {
		server.queryCoord = mqc
		server.dataCoord = mdc
		server.rootCoord = mrc

		err = server.Run()
		assert.Nil(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		req := &internalpb.GetComponentStatesRequest{}
		states, err := server.GetComponentStates(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		req := &internalpb.GetStatisticsChannelRequest{}
		resp, err := server.GetStatisticsChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		req := &internalpb.GetTimeTickChannelRequest{}
		resp, err := server.GetTimeTickChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("ShowCollections", func(t *testing.T) {
		resp, err := server.ShowCollections(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("LoadCollection", func(t *testing.T) {
		resp, err := server.LoadCollection(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ReleaseCollection", func(t *testing.T) {
		resp, err := server.ReleaseCollection(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ShowPartitions", func(t *testing.T) {
		resp, err := server.ShowPartitions(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})
	t.Run("GetPartitionStates", func(t *testing.T) {
		resp, err := server.GetPartitionStates(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("LoadPartitions", func(t *testing.T) {
		resp, err := server.LoadPartitions(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ReleasePartitions", func(t *testing.T) {
		resp, err := server.ReleasePartitions(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("CreateQueryChannel", func(t *testing.T) {
		resp, err := server.CreateQueryChannel(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		resp, err := server.GetTimeTickChannel(ctx, nil)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetSegmentInfo", func(t *testing.T) {
		req := &querypb.GetSegmentInfoRequest{}
		resp, err := server.GetSegmentInfo(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "",
		}
		resp, err := server.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = server.Stop()
	assert.Nil(t, err)
}

func TestServer_Run1(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, server)

	server.queryCoord = &MockQueryCoord{
		regErr: errors.New("error"),
	}
	err = server.Run()
	assert.Error(t, err)

	err = server.Stop()
	assert.Nil(t, err)
}

func TestServer_Run2(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, server)

	server.queryCoord = &MockQueryCoord{}
	server.rootCoord = &MockRootCoord{
		initErr: errors.New("error"),
	}
	assert.Panics(t, func() { server.Run() })
	err = server.Stop()
	assert.Nil(t, err)
}

func TestServer_Run3(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, server)

	server.queryCoord = &MockQueryCoord{}
	server.rootCoord = &MockRootCoord{
		startErr: errors.New("error"),
	}
	assert.Panics(t, func() { server.Run() })
	err = server.Stop()
	assert.Nil(t, err)

}

func TestServer_Run4(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, server)

	server.queryCoord = &MockQueryCoord{}
	server.rootCoord = &MockRootCoord{}
	server.dataCoord = &MockDataCoord{
		initErr: errors.New("error"),
	}
	assert.Panics(t, func() { server.Run() })
	err = server.Stop()
	assert.Nil(t, err)
}

func TestServer_Run5(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, server)

	server.queryCoord = &MockQueryCoord{}
	server.rootCoord = &MockRootCoord{}
	server.dataCoord = &MockDataCoord{
		startErr: errors.New("error"),
	}
	assert.Panics(t, func() { server.Run() })
	err = server.Stop()
	assert.Nil(t, err)
}
