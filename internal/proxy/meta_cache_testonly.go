//go:build test
// +build test

/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proxy

import (
	"context"

	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func AddRootUserToAdminRole() {
	err := privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheAddUserToRole, OpKey: funcutil.EncodeUserRoleCache("root", "admin")})
	if err != nil {
		panic(err)
	}
}

func RemoveRootUserFromAdminRole() {
	err := privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRemoveUserFromRole, OpKey: funcutil.EncodeUserRoleCache("root", "admin")})
	if err != nil {
		panic(err)
	}
}

func InitEmptyGlobalCache() {
	var err error
	emptyMock := common.NewEmptyMockT()
	mixcoord := mocks.NewMockMixCoordClient(emptyMock)
	mixcoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.WrapErrParameterInvalidMsg("collection not found"))
	mixcoord.EXPECT().DescribeAlias(mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.WrapErrParameterInvalidMsg("alias not found"))
	globalMetaCache, err = NewMetaCache(mixcoord)
	if err != nil {
		panic(err)
	}
	mixcoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{Status: merr.Success()}, nil)
	privilege.InitPrivilegeCache(context.Background(), mixcoord)
}

type proxyComponentReadCache struct {
	Cache
	proxy types.ProxyComponent
}

func (c *proxyComponentReadCache) GetCollectionInfo(
	ctx context.Context,
	database string,
	collectionName string,
	collectionID int64,
) (*collectionInfo, error) {
	resp, err := c.proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		DbName:         database,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, merr.WrapErrServiceInternalMsg("test proxy returned a nil DescribeCollection response")
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	schema, err := newSchemaInfo(resp.GetSchema())
	if err != nil {
		return nil, err
	}

	// HTTP tests historically obtain schema through the mocked Proxy instead of
	// a real MetaCache. Supply stable non-zero identities when old mock responses
	// omit them; production never installs this test-only adapter.
	normalized := proto.Clone(resp).(*milvuspb.DescribeCollectionResponse)
	if normalized.CollectionID == 0 {
		normalized.CollectionID = 1
	}
	if normalized.DbId == 0 {
		normalized.DbId = 1
	}
	if normalized.DbName == "" {
		normalized.DbName = normalizeDBName(database)
	}
	if normalized.CollectionName == "" {
		normalized.CollectionName = schema.GetName()
	}
	if normalized.CollectionName == "" {
		normalized.CollectionName = collectionName
	}
	return newCollectionInfo(normalized, schema, false, ""), nil
}

// InitGlobalCacheFromProxyForTest lets HTTP tests exercise request-snapshot
// preprocessing while keeping their existing Proxy DescribeCollection mocks.
func InitGlobalCacheFromProxyForTest(proxyComponent types.ProxyComponent) {
	globalMetaCache = &proxyComponentReadCache{
		Cache: globalMetaCache,
		proxy: proxyComponent,
	}
}

func SetGlobalMetaCache(metaCache *MetaCache) {
	globalMetaCache = metaCache
}
