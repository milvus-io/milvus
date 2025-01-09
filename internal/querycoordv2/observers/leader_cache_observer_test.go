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

package observers

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type LeaderCacheObserverTestSuite struct {
	suite.Suite

	mockProxyManager *proxyutil.MockProxyClientManager

	observer *LeaderCacheObserver
}

func (suite *LeaderCacheObserverTestSuite) SetupSuite() {
	paramtable.Init()
	suite.mockProxyManager = proxyutil.NewMockProxyClientManager(suite.T())
	suite.observer = NewLeaderCacheObserver(suite.mockProxyManager)
}

func (suite *LeaderCacheObserverTestSuite) TestInvalidateShardLeaderCache() {
	suite.observer.Start(context.TODO())
	defer suite.observer.Stop()

	ret := atomic.NewBool(false)
	collectionIDs := typeutil.NewConcurrentSet[int64]()
	suite.mockProxyManager.EXPECT().InvalidateShardLeaderCache(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *proxypb.InvalidateShardLeaderCacheRequest) error {
			collectionIDs.Upsert(req.GetCollectionIDs()...)
			collectionIDs := req.GetCollectionIDs()

			if len(collectionIDs) == 1 && lo.Contains(collectionIDs, 1) {
				ret.Store(true)
			}
			return nil
		})

	suite.observer.RegisterEvent(1)
	suite.Eventually(func() bool {
		return ret.Load()
	}, 3*time.Second, 1*time.Second)

	// test batch submit events
	ret.Store(false)
	suite.mockProxyManager.ExpectedCalls = nil
	suite.mockProxyManager.EXPECT().InvalidateShardLeaderCache(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *proxypb.InvalidateShardLeaderCacheRequest) error {
			collectionIDs.Upsert(req.GetCollectionIDs()...)
			collectionIDs := req.GetCollectionIDs()

			if len(collectionIDs) == 3 && lo.Contains(collectionIDs, 1) && lo.Contains(collectionIDs, 2) && lo.Contains(collectionIDs, 3) {
				ret.Store(true)
			}
			return nil
		})
	suite.observer.RegisterEvent(1)
	suite.observer.RegisterEvent(2)
	suite.observer.RegisterEvent(3)
	suite.Eventually(func() bool {
		return ret.Load()
	}, 3*time.Second, 1*time.Second)
}

func TestLeaderCacheObserverTestSuite(t *testing.T) {
	suite.Run(t, new(LeaderCacheObserverTestSuite))
}
