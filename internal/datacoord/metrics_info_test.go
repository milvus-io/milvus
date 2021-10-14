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
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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

func TestGetDataNodeMetrics(t *testing.T) {
	svr := newTestServer(t, nil)
	defer closeTestServer(t, svr)

	ctx := context.Background()
	req := &milvuspb.GetMetricsRequest{}
	// nil node
	_, err := svr.getDataNodeMetrics(ctx, req, nil)
	assert.NotNil(t, err)

	// nil client node
	_, err = svr.getDataNodeMetrics(ctx, req, NewSession(&NodeInfo{}, nil))
	assert.NotNil(t, err)

	creator := func(ctx context.Context, addr string) (types.DataNode, error) {
		return newMockDataNodeClient(100, nil)
	}

	// mock datanode client
	session := NewSession(&NodeInfo{}, creator)
	info, err := svr.getDataNodeMetrics(ctx, req, session)
	assert.Nil(t, err)
	assert.False(t, info.HasError)
	assert.Equal(t, metricsinfo.ConstructComponentName(typeutil.DataNodeRole, 100), info.BaseComponentInfos.Name)

	getMockFailedClientCreator := func(mockFunc func() (*milvuspb.GetMetricsResponse, error)) dataNodeCreatorFunc {
		return func(ctx context.Context, addr string) (types.DataNode, error) {
			cli, err := creator(ctx, addr)
			assert.Nil(t, err)
			return &mockMetricDataNodeClient{DataNode: cli, mock: mockFunc}, nil
		}
	}

	mockFailClientCreator := getMockFailedClientCreator(func() (*milvuspb.GetMetricsResponse, error) {
		return nil, errors.New("mocked fail")
	})

	info, err = svr.getDataNodeMetrics(ctx, req, NewSession(&NodeInfo{}, mockFailClientCreator))
	assert.Nil(t, err)
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
	assert.Nil(t, err)
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
	assert.Nil(t, err)
	assert.True(t, info.HasError)

}
