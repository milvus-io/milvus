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
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/stretchr/testify/assert"
)

type mockMetricDataNodeClient struct {
	types.DataNode
	mock func() (*milvuspb.GetMetricsResponse, error)
}

func (c *mockMetricDataNodeClient) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if c.mock == nil {
		return c.DataNode.GetMetrics(ctx, req)
	}
	return c.mock()
}

type mockMetricIndexNodeClient struct {
	types.IndexNode
	mock func() (*milvuspb.GetMetricsResponse, error)
}

func (m *mockMetricIndexNodeClient) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if m.mock == nil {
		return m.IndexNode.GetMetrics(ctx, req)
	}
	return m.mock()
}

func TestGetDataNodeMetrics(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	ctx := context.Background()
	req := &milvuspb.GetMetricsRequest{}
	// nil node
	_, err := svr.getDataNodeMetrics(ctx, req, nil)
	assert.Error(t, err)

	// nil client node
	_, err = svr.getDataNodeMetrics(ctx, req, NewSession(&NodeInfo{}, nil))
	assert.Error(t, err)

	creator := func(ctx context.Context, addr string) (types.DataNode, error) {
		return newMockDataNodeClient(100, nil)
	}

	// mock datanode client
	session := NewSession(&NodeInfo{}, creator)
	info, err := svr.getDataNodeMetrics(ctx, req, session)
	assert.NoError(t, err)
	assert.False(t, info.HasError)
	assert.Equal(t, metricsinfo.ConstructComponentName(typeutil.DataNodeRole, 100), info.BaseComponentInfos.Name)

	getMockFailedClientCreator := func(mockFunc func() (*milvuspb.GetMetricsResponse, error)) dataNodeCreatorFunc {
		return func(ctx context.Context, addr string) (types.DataNode, error) {
			cli, err := creator(ctx, addr)
			assert.NoError(t, err)
			return &mockMetricDataNodeClient{DataNode: cli, mock: mockFunc}, nil
		}
	}

	mockFailClientCreator := getMockFailedClientCreator(func() (*milvuspb.GetMetricsResponse, error) {
		return nil, errors.New("mocked fail")
	})

	info, err = svr.getDataNodeMetrics(ctx, req, NewSession(&NodeInfo{}, mockFailClientCreator))
	assert.NoError(t, err)
	assert.True(t, info.HasError)

	// mock status not success
	mockFailClientCreator = getMockFailedClientCreator(func() (*milvuspb.GetMetricsResponse, error) {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mocked error",
			},
		}, nil
	})

	info, err = svr.getDataNodeMetrics(ctx, req, NewSession(&NodeInfo{}, mockFailClientCreator))
	assert.NoError(t, err)
	assert.True(t, info.HasError)
	assert.Equal(t, "mocked error", info.ErrorReason)

	// mock parse error
	mockFailClientCreator = getMockFailedClientCreator(func() (*milvuspb.GetMetricsResponse, error) {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Response: `{"error_reason": 1}`,
		}, nil
	})

	info, err = svr.getDataNodeMetrics(ctx, req, NewSession(&NodeInfo{}, mockFailClientCreator))
	assert.NoError(t, err)
	assert.True(t, info.HasError)

}

func TestGetIndexNodeMetrics(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	ctx := context.Background()
	req := &milvuspb.GetMetricsRequest{}
	// nil node
	_, err := svr.getIndexNodeMetrics(ctx, req, nil)
	assert.Error(t, err)

	// return error
	info, err := svr.getIndexNodeMetrics(ctx, req, &mockMetricIndexNodeClient{mock: func() (*milvuspb.GetMetricsResponse, error) {
		return nil, errors.New("mock error")
	}})
	assert.NoError(t, err)
	assert.True(t, info.HasError)

	// failed
	info, err = svr.getIndexNodeMetrics(ctx, req, &mockMetricIndexNodeClient{
		mock: func() (*milvuspb.GetMetricsResponse, error) {
			return &milvuspb.GetMetricsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "mock fail",
				},
				Response:      "",
				ComponentName: "indexnode100",
			}, nil
		},
	})
	assert.NoError(t, err)
	assert.True(t, info.HasError)
	assert.Equal(t, metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, 100), info.BaseComponentInfos.Name)

	// return unexpected
	info, err = svr.getIndexNodeMetrics(ctx, req, &mockMetricIndexNodeClient{
		mock: func() (*milvuspb.GetMetricsResponse, error) {
			return &milvuspb.GetMetricsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				Response:      "XXXXXXXXXXXXX",
				ComponentName: "indexnode100",
			}, nil
		},
	})
	assert.NoError(t, err)
	assert.True(t, info.HasError)
	assert.Equal(t, metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, 100), info.BaseComponentInfos.Name)

	// success
	info, err = svr.getIndexNodeMetrics(ctx, req, &mockMetricIndexNodeClient{
		mock: func() (*milvuspb.GetMetricsResponse, error) {
			nodeID = UniqueID(100)

			nodeInfos := metricsinfo.DataNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					Name: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, nodeID),
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
					ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, nodeID),
				}, nil
			}

			return &milvuspb.GetMetricsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				Response:      resp,
				ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, nodeID),
			}, nil
		},
	})

	assert.NoError(t, err)
	assert.False(t, info.HasError)
	assert.Equal(t, metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, 100), info.BaseComponentInfos.Name)
}
