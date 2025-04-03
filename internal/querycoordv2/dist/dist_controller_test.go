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

package dist

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type DistControllerTestSuite struct {
	suite.Suite
	controller    *ControllerImpl
	mockCluster   *session.MockCluster
	mockScheduler *task.MockScheduler

	kv     kv.MetaKv
	meta   *meta.Meta
	broker *meta.MockBroker

	nodeMgr *session.NodeManager
}

func (suite *DistControllerTestSuite) SetupTest() {
	paramtable.Init()

	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())

	// meta
	store := querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()

	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)

	suite.mockCluster = session.NewMockCluster(suite.T())
	distManager := meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	targetManager := meta.NewTargetManager(suite.broker, suite.meta)
	suite.mockScheduler = task.NewMockScheduler(suite.T())
	suite.mockScheduler.EXPECT().GetExecutedFlag(mock.Anything).Return(nil).Maybe()

	suite.controller = NewDistController(suite.mockCluster, suite.nodeMgr, distManager, targetManager, suite.mockScheduler, func(collectionID ...int64) {})
}

func (suite *DistControllerTestSuite) TearDownSuite() {
	suite.kv.Close()
}

func (suite *DistControllerTestSuite) TestStart() {
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	dispatchCalled := atomic.NewBool(false)
	suite.mockCluster.EXPECT().GetDataDistribution(mock.Anything, mock.Anything, mock.Anything).Return(
		&querypb.GetDataDistributionResponse{Status: merr.Success(), NodeID: 1},
		nil,
	)
	suite.mockScheduler.EXPECT().Dispatch(int64(1)).Run(func(node int64) { dispatchCalled.Store(true) })
	suite.controller.StartDistInstance(context.TODO(), 1)
	suite.Eventually(
		func() bool {
			return dispatchCalled.Load()
		},
		10*time.Second,
		500*time.Millisecond,
	)

	suite.controller.Remove(1)
	dispatchCalled.Store(false)
	suite.Never(
		func() bool {
			return dispatchCalled.Load()
		},
		3*time.Second,
		500*time.Millisecond,
	)
}

func (suite *DistControllerTestSuite) TestStop() {
	suite.controller.StartDistInstance(context.TODO(), 1)
	called := atomic.NewBool(false)
	suite.mockCluster.EXPECT().GetDataDistribution(mock.Anything, mock.Anything, mock.Anything).Maybe().Return(
		&querypb.GetDataDistributionResponse{Status: merr.Success(), NodeID: 1},
		nil,
	).Run(func(args mock.Arguments) {
		called.Store(true)
	})
	suite.mockScheduler.EXPECT().Dispatch(mock.Anything).Maybe()
	suite.controller.Stop()
	called.Store(false)
	suite.Never(
		func() bool {
			return called.Load()
		},
		3*time.Second,
		500*time.Millisecond,
	)
}

func (suite *DistControllerTestSuite) TestSyncAll() {
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.controller.StartDistInstance(context.TODO(), 1)
	suite.controller.StartDistInstance(context.TODO(), 2)

	calledSet := typeutil.NewConcurrentSet[int64]()
	suite.mockCluster.EXPECT().GetDataDistribution(mock.Anything, mock.Anything, mock.Anything).Call.Return(
		func(ctx context.Context, nodeID int64, req *querypb.GetDataDistributionRequest) *querypb.GetDataDistributionResponse {
			return &querypb.GetDataDistributionResponse{
				Status: merr.Success(),
				NodeID: nodeID,
			}
		},
		nil,
	).Run(func(args mock.Arguments) {
		calledSet.Insert(args[1].(int64))
	})
	suite.mockScheduler.EXPECT().Dispatch(mock.Anything)

	// stop inner loop
	suite.controller.handlers[1].stop()
	suite.controller.handlers[2].stop()

	calledSet.Remove(1)
	calledSet.Remove(2)

	suite.controller.SyncAll(context.TODO())
	suite.Eventually(
		func() bool {
			return calledSet.Contain(1) && calledSet.Contain(2)
		},
		5*time.Second,
		500*time.Millisecond,
	)
}

func TestDistControllerSuite(t *testing.T) {
	suite.Run(t, new(DistControllerTestSuite))
}
