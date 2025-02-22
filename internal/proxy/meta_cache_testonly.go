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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func AddRootUserToAdminRole() {
	err := globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheAddUserToRole, OpKey: funcutil.EncodeUserRoleCache("root", "admin")})
	if err != nil {
		panic(err)
	}
}

func RemoveRootUserFromAdminRole() {
	err := globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRemoveUserFromRole, OpKey: funcutil.EncodeUserRoleCache("root", "admin")})
	if err != nil {
		panic(err)
	}
}

func InitEmptyGlobalCache() {
	var err error
	emptyMock := common.NewEmptyMockT()
	rootcoord := mocks.NewMockRootCoordClient(emptyMock)
	rootcoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("collection not found"))
	querycoord := mocks.NewMockQueryCoordClient(emptyMock)
	mgr := newShardClientMgr()
	globalMetaCache, err = NewMetaCache(rootcoord, querycoord, mgr)
	if err != nil {
		panic(err)
	}
}

func SetGlobalMetaCache(metaCache *MetaCache) {
	globalMetaCache = metaCache
}
