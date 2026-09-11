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

package coordinator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMixCoord_collectProxyFeatureUsage(t *testing.T) {
	ctx := context.Background()
	req := &internalpb.GetFeatureUsageRequest{}

	t.Run("no manager", func(t *testing.T) {
		assert.Nil(t, (&mixCoordImpl{}).collectProxyFeatureUsage(ctx, req))
	})

	t.Run("no proxies", func(t *testing.T) {
		pcm := proxyutil.NewMockProxyClientManager(t)
		pcm.EXPECT().GetProxyClients().Return(typeutil.NewConcurrentMap[int64, types.ProxyClient]())
		coord := &mixCoordImpl{proxyClientManager: pcm}
		assert.Empty(t, coord.collectProxyFeatureUsage(ctx, req))
	})

	t.Run("reachable and unreachable proxies are all reported", func(t *testing.T) {
		clients := typeutil.NewConcurrentMap[int64, types.ProxyClient]()

		healthy := mocks.NewMockProxyClient(t)
		healthy.EXPECT().GetFeatureUsage(mock.Anything, mock.Anything).Return(&internalpb.GetFeatureUsageResponse{
			Status:        merr.Success(),
			Role:          typeutil.ProxyRole,
			NodeId:        7,
			NodeStartTime: 100,
			Entries: []*internalpb.FeatureEntry{{
				Group: featureusage.GroupRequest, Name: featureusage.FeatureIterator.Name(), Value: 3, LastUsedAt: 200,
			}},
		}, nil)
		clients.Insert(7, healthy)

		dialFailed := mocks.NewMockProxyClient(t)
		dialFailed.EXPECT().GetFeatureUsage(mock.Anything, mock.Anything).Return(nil, errors.New("dial failed"))
		clients.Insert(3, dialFailed)

		notReady := mocks.NewMockProxyClient(t)
		notReady.EXPECT().GetFeatureUsage(mock.Anything, mock.Anything).Return(&internalpb.GetFeatureUsageResponse{
			Status: merr.Status(errors.New("proxy not ready")),
		}, nil)
		clients.Insert(5, notReady)

		pcm := proxyutil.NewMockProxyClientManager(t)
		pcm.EXPECT().GetProxyClients().Return(clients)
		coord := &mixCoordImpl{proxyClientManager: pcm}

		nodes := coord.collectProxyFeatureUsage(ctx, req)
		require.Len(t, nodes, 3, "an unreachable proxy is reported, never omitted")

		// Sorted by node id.
		assert.EqualValues(t, 3, nodes[0].NodeId)
		assert.False(t, nodes[0].Reachable)
		assert.Contains(t, nodes[0].Error, "dial failed")
		assert.Empty(t, nodes[0].Entries)

		assert.EqualValues(t, 5, nodes[1].NodeId)
		assert.False(t, nodes[1].Reachable)
		assert.Contains(t, nodes[1].Error, "proxy not ready")

		assert.EqualValues(t, 7, nodes[2].NodeId)
		assert.True(t, nodes[2].Reachable)
		assert.Empty(t, nodes[2].Error)
		assert.EqualValues(t, 100, nodes[2].NodeStartTime)
		require.Len(t, nodes[2].Entries, 1)
		assert.EqualValues(t, 3, nodes[2].Entries[0].Value)
		assert.EqualValues(t, 200, nodes[2].Entries[0].LastUsedAt)
		for _, n := range nodes {
			assert.Equal(t, typeutil.ProxyRole, n.Role)
		}
	})
}

func TestMixCoord_collectProxyFeatureUsage_UsesRootCoordManager(t *testing.T) {
	mockey.PatchConvey("fall back to rootcoord's proxy client manager", t, func() {
		clients := typeutil.NewConcurrentMap[int64, types.ProxyClient]()
		healthy := mocks.NewMockProxyClient(t)
		healthy.EXPECT().GetFeatureUsage(mock.Anything, mock.Anything).Return(&internalpb.GetFeatureUsageResponse{
			Status: merr.Success(), NodeId: 9, NodeStartTime: 42,
		}, nil)
		clients.Insert(9, healthy)
		pcm := proxyutil.NewMockProxyClientManager(t)
		pcm.EXPECT().GetProxyClients().Return(clients)
		mockey.Mock((*rootcoord.Core).GetProxyClientManager).Return(pcm).Build()

		coord := &mixCoordImpl{rootcoordServer: &rootcoord.Core{}}
		nodes := coord.collectProxyFeatureUsage(context.Background(), &internalpb.GetFeatureUsageRequest{})
		require.Len(t, nodes, 1)
		assert.True(t, nodes[0].Reachable)
		assert.EqualValues(t, 9, nodes[0].NodeId)
		assert.EqualValues(t, 42, nodes[0].NodeStartTime)
	})
}

func TestMixCoord_GetFeatureUsage_NotHealthy(t *testing.T) {
	coord := &mixCoordImpl{}
	coord.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err := coord.GetFeatureUsage(context.Background(), &internalpb.GetFeatureUsageRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.Empty(t, resp.GetNodes())
}
