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

package grpcquerynode

import (
	"context"
	"errors"
	"testing"

	isc "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockQueryNode struct {
	states     *internalpb.ComponentStates
	status     *commonpb.Status
	err        error
	strResp    *milvuspb.StringResponse
	infoResp   *querypb.GetSegmentInfoResponse
	metricResp *milvuspb.GetMetricsResponse
}

func (m *MockQueryNode) Init() error {
	return m.err
}

func (m *MockQueryNode) Start() error {
	return m.err
}

func (m *MockQueryNode) Stop() error {
	return m.err
}

func (m *MockQueryNode) Register() error {
	return m.err
}

func (m *MockQueryNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return m.states, m.err
}

func (m *MockQueryNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return m.strResp, m.err
}

func (m *MockQueryNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return m.strResp, m.err
}

func (m *MockQueryNode) AddQueryChannel(ctx context.Context, req *querypb.AddQueryChannelRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryNode) RemoveQueryChannel(ctx context.Context, req *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryNode) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryNode) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryNode) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryNode) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryNode) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	return m.status, m.err
}

func (m *MockQueryNode) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return m.infoResp, m.err
}

func (m *MockQueryNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return m.metricResp, m.err
}

func (m *MockQueryNode) UpdateStateCode(code internalpb.StateCode) {
}

func (m *MockQueryNode) SetRootCoord(rc types.RootCoord) error {
	return m.err
}

func (m *MockQueryNode) SetIndexCoord(index types.IndexCoord) error {
	return m.err
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockRootCoord struct {
	rcc.Base
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
type MockIndexCoord struct {
	isc.Base
	initErr  error
	startErr error
	regErr   error
	stopErr  error
	stateErr commonpb.ErrorCode
}

func (m *MockIndexCoord) Init() error {
	return m.initErr
}

func (m *MockIndexCoord) Start() error {
	return m.startErr
}

func (m *MockIndexCoord) Stop() error {
	return m.stopErr
}

func (m *MockIndexCoord) Register() error {
	return m.regErr
}

func (m *MockIndexCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State:  &internalpb.ComponentInfo{StateCode: internalpb.StateCode_Healthy},
		Status: &commonpb.Status{ErrorCode: m.stateErr},
	}, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func Test_NewServer(t *testing.T) {
	ctx := context.Background()
	qns, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, qns)

	mqn := &MockQueryNode{
		states:     &internalpb.ComponentStates{State: &internalpb.ComponentInfo{StateCode: internalpb.StateCode_Healthy}},
		status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		err:        nil,
		strResp:    &milvuspb.StringResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}},
		infoResp:   &querypb.GetSegmentInfoResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}},
		metricResp: &milvuspb.GetMetricsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}},
	}
	err = qns.SetClient(mqn)
	assert.Nil(t, err)

	t.Run("Run", func(t *testing.T) {
		qns.rootCoord = &MockRootCoord{}
		qns.indexCoord = &MockIndexCoord{}

		err = qns.Run()
		assert.Nil(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		req := &internalpb.GetComponentStatesRequest{}
		states, err := qns.GetComponentStates(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		req := &internalpb.GetStatisticsChannelRequest{}
		resp, err := qns.GetStatisticsChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		req := &internalpb.GetTimeTickChannelRequest{}
		resp, err := qns.GetTimeTickChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("AddQueryChannel", func(t *testing.T) {
		req := &querypb.AddQueryChannelRequest{}
		resp, err := qns.AddQueryChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("RemoveQueryChannel", func(t *testing.T) {
		req := &querypb.RemoveQueryChannelRequest{}
		resp, err := qns.RemoveQueryChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("WatchDmChannels", func(t *testing.T) {
		req := &querypb.WatchDmChannelsRequest{}
		resp, err := qns.WatchDmChannels(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("LoadSegments", func(t *testing.T) {
		req := &querypb.LoadSegmentsRequest{}
		resp, err := qns.LoadSegments(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleaseCollection", func(t *testing.T) {
		req := &querypb.ReleaseCollectionRequest{}
		resp, err := qns.ReleaseCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleasePartitions", func(t *testing.T) {
		req := &querypb.ReleasePartitionsRequest{}
		resp, err := qns.ReleasePartitions(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleaseSegments", func(t *testing.T) {
		req := &querypb.ReleaseSegmentsRequest{}
		resp, err := qns.ReleaseSegments(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetSegmentInfo", func(t *testing.T) {
		req := &querypb.GetSegmentInfoRequest{}
		resp, err := qns.GetSegmentInfo(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "",
		}
		resp, err := qns.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = qns.Stop()
	assert.Nil(t, err)
}

func Test_Run(t *testing.T) {
	ctx := context.Background()
	qns, err := NewServer(ctx, nil)
	assert.Nil(t, err)
	assert.NotNil(t, qns)

	qns.rootCoord = &MockRootCoord{initErr: errors.New("Failed")}
	qns.indexCoord = &MockIndexCoord{}
	assert.Panics(t, func() { err = qns.Run() })

	qns.rootCoord = &MockRootCoord{startErr: errors.New("Failed")}
	qns.indexCoord = &MockIndexCoord{}
	assert.Panics(t, func() { err = qns.Run() })

	qns.rootCoord = &MockRootCoord{}
	qns.indexCoord = &MockIndexCoord{initErr: errors.New("Failed")}
	assert.Panics(t, func() { err = qns.Run() })

	qns.rootCoord = &MockRootCoord{}
	qns.indexCoord = &MockIndexCoord{startErr: errors.New("Failed")}
	assert.Panics(t, func() { err = qns.Run() })
}
