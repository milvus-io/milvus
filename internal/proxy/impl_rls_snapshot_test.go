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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestProxyAlterCollectionRefreshesRLSEnforcement(t *testing.T) {
	ctx := context.Background()
	coord := mocks.NewMockMixCoordClient(t)
	cache, err := NewMetaCache(coord)
	require.NoError(t, err)
	shard := shardclient.NewMockShardClientManager(t)
	shard.EXPECT().InvalidateShardLeaderCache([]int64{100}).Return().Times(5)
	node := &Proxy{metaCache: cache, shardMgr: shard}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	for _, value := range []string{"", "true", "false", "true", ""} {
		var properties []*commonpb.KeyValuePair
		if value != "" {
			properties = []*commonpb.KeyValuePair{{Key: common.RLSEnabledKey, Value: value}}
		}
		coord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(), CollectionID: 100, DbName: "default",
			Schema: &schemapb.CollectionSchema{Name: "coll", Properties: properties}, Properties: properties,
		}, nil).Once()
		status, err := node.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base:   &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
			DbName: "default", CollectionName: "coll", CollectionID: 100,
		})
		require.NoError(t, merr.CheckRPCCall(status, err))
		info, err := cache.GetCollectionInfo(ctx, "default", "coll", 100)
		require.NoError(t, err)
		require.Equal(t, value == "true", info.RlsEnabled)
		fromSchema, err := common.IsRLSEnabled(info.Schema.GetProperties()...)
		require.NoError(t, err)
		require.Equal(t, info.RlsEnabled, fromSchema)
		hit, err := cache.GetCollectionInfo(ctx, "default", "coll", 100)
		require.NoError(t, err)
		require.Same(t, info, hit)
	}
}

func TestProxyRLSInvalidateRemovesSnapshots(t *testing.T) {
	ctx := context.Background()
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	for _, msgType := range []commonpb.MsgType{
		commonpb.MsgType_CreateRowPolicy,
		commonpb.MsgType_UpdateRowPolicy,
		commonpb.MsgType_DropRowPolicy,
		commonpb.MsgType_SetRLSPrincipalTags,
		commonpb.MsgType_DeleteRLSPrincipalTags,
	} {
		status, err := node.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:    msgType,
				Timestamp:  10,
				Properties: map[string]string{common.RLSPrincipalNameKey: "alice"},
			},
			DbName:         "db",
			CollectionName: "coll",
			CollectionID:   100,
		})
		require.NoError(t, err)
		require.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	}
}

func TestProxyRLSPrincipalInvalidationRequiresPrincipalName(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	status, err := node.InvalidateCollectionMetaCache(context.Background(), &proxypb.InvalidateCollMetaCacheRequest{
		Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_SetRLSPrincipalTags},
		CollectionID: 100,
	})
	require.NoError(t, err)
	require.NotEqual(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}
