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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestProxyAlterCollectionRefreshesRLSEnforcement(t *testing.T) {
	ctx := context.Background()
	coord := mocks.NewMockMixCoordClient(t)
	cache, err := NewMetaCache(coord)
	require.NoError(t, err)
	shard := shardclient.NewMockShardClientManager(t)
	shard.EXPECT().InvalidateShardLeaderCache([]int64{100}).Return().Times(5)
	previousCache := globalMetaCache
	globalMetaCache = cache
	t.Cleanup(func() { globalMetaCache = previousCache })
	coord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{Status: merr.Success()}, nil).Times(5)
	node := &Proxy{shardMgr: shard}
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
		require.Equal(t, value == "true", info.rlsEnabled)
		fromSchema, err := common.IsRLSEnabled(info.schema.GetProperties()...)
		require.NoError(t, err)
		require.Equal(t, info.rlsEnabled, fromSchema)
		hit, err := cache.GetCollectionInfo(ctx, "default", "coll", 100)
		require.NoError(t, err)
		require.Same(t, info, hit)
	}
}

func TestProxyRLSEnableWaitsForInflightSchema(t *testing.T) {
	for _, byID := range []bool{false, true} {
		name := "by_name"
		if byID {
			name = "by_id"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			coord := mocks.NewMockMixCoordClient(t)
			cache, err := NewMetaCache(coord)
			require.NoError(t, err)
			previousCache := globalMetaCache
			globalMetaCache = cache
			t.Cleanup(func() { globalMetaCache = previousCache })
			shard := shardclient.NewMockShardClientManager(t)
			shard.EXPECT().InvalidateShardLeaderCache([]int64{100}).Return().Once()
			node := &Proxy{shardMgr: shard}
			node.UpdateStateCode(commonpb.StateCode_Healthy)

			coord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				Status: merr.Success(), CollectionID: 100, DbName: "default", RequestTime: 100,
				Schema: &schemapb.CollectionSchema{Name: "coll"},
			}, nil).Once()
			properties := []*commonpb.KeyValuePair{{Key: common.RLSEnabledKey, Value: "true"}}
			coord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				Status: merr.Success(), CollectionID: 100, DbName: "default", RequestTime: 300,
				Schema: &schemapb.CollectionSchema{Name: "coll", Properties: properties}, Properties: properties,
			}, nil).Once()
			entered := make(chan struct{})
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			coord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Run(func(context.Context, *milvuspb.ShowPartitionsRequest, ...grpc.CallOption) {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}).Return(&milvuspb.ShowPartitionsResponse{Status: merr.Success()}, nil).Once()
			coord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{Status: merr.Success()}, nil).Once()

			var lookupID int64
			if byID {
				lookupID = 100
			}
			var oldInfo *collectionInfo
			var oldErr error
			filled := make(chan struct{})
			go func() {
				oldInfo, oldErr = cache.GetCollectionInfo(ctx, "default", "coll", lookupID)
				close(filled)
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("schema fill did not reach ShowPartitions")
			}
			invalidated := make(chan error, 1)
			go func() {
				status, err := node.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
					Base:   &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection, Timestamp: 200},
					DbName: "default", CollectionName: "coll", CollectionID: 100,
				})
				invalidated <- merr.CheckRPCCall(status, err)
			}()
			require.Eventually(t, func() bool {
				if cache.fillMu.TryRLock() {
					cache.fillMu.RUnlock()
					return false
				}
				return true
			}, time.Second, time.Millisecond)
			select {
			case err := <-invalidated:
				t.Fatalf("invalidation completed before the old fill: %v", err)
			default:
			}
			unblock()
			select {
			case err := <-invalidated:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("invalidation did not complete after releasing the old fill")
			}
			<-filled
			require.NoError(t, oldErr)
			require.False(t, oldInfo.rlsEnabled)
			info, err := cache.GetCollectionInfo(ctx, "default", "coll", lookupID)
			require.NoError(t, err)
			require.True(t, info.rlsEnabled)
			hit, err := cache.GetCollectionInfo(ctx, "default", "coll", lookupID)
			require.NoError(t, err)
			require.Same(t, info, hit)
		})
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
