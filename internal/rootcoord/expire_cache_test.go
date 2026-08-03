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

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func Test_expireCacheConfig_apply(t *testing.T) {
	c := proxyutil.DefaultExpireCacheConfig()
	req := &proxypb.InvalidateCollMetaCacheRequest{}
	c.Apply(req)
	assert.Equal(t, commonpb.MsgType_Undefined, req.GetBase().GetMsgType())
	opt := proxyutil.SetMsgType(commonpb.MsgType_DropCollection)
	opt(&c)
	c.Apply(req)
	assert.Equal(t, commonpb.MsgType_DropCollection, req.GetBase().GetMsgType())
}

func TestCoreExpireTransferMetaCacheInvalidatesCollectionAliasesAndID(t *testing.T) {
	const (
		dbName       = "db"
		collectionID = int64(100)
	)

	ctx := context.Background()
	core := newTestCore()
	core.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)

	var invalidatedNames []string
	proxy := newMockProxy()
	proxy.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
		require.Equal(t, dbName, req.GetDbName())
		require.Equal(t, collectionID, req.GetCollectionID())
		invalidatedNames = append(invalidatedNames, req.GetCollectionName())
		return merr.Success(), nil
	}
	core.proxyClientManager.GetProxyClients().Insert(TestProxyID, proxy)

	err := core.ExpireTransferMetaCache(
		ctx,
		dbName,
		"collection",
		[]string{"alias_1", "alias_2", "alias_1", "collection"},
		collectionID,
		123,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"collection", "alias_1", "alias_2"}, invalidatedNames)
}

func TestCoreExpireTransferMetaCacheInvalidatesByIDWithoutNames(t *testing.T) {
	const collectionID = int64(100)

	ctx := context.Background()
	core := newTestCore()
	core.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)

	var requests []*proxypb.InvalidateCollMetaCacheRequest
	proxy := newMockProxy()
	proxy.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
		requests = append(requests, req)
		return merr.Success(), nil
	}
	core.proxyClientManager.GetProxyClients().Insert(TestProxyID, proxy)

	err := core.ExpireTransferMetaCache(ctx, "db", "", nil, collectionID, 123)
	require.NoError(t, err)
	require.Len(t, requests, 1)
	require.Empty(t, requests[0].GetCollectionName())
	require.Equal(t, collectionID, requests[0].GetCollectionID())
}
