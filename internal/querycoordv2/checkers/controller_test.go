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

package checkers

import (
	"context"
	"testing"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/stretchr/testify/suite"
)

type ControllerSuite struct {
	suite.Suite

	//dependency
	kv            *etcdkv.EtcdKV
	nodeMgr       *session.NodeManager
	meta          *meta.Meta
	broker        *meta.MockBroker
	distManager   *meta.DistributionManager
	targetManager *meta.TargetManager
	balancer      *balance.MockBalancer
	scheduler     *task.MockScheduler

	controller *CheckerController
}

func (suite *ControllerSuite) SetupSuite() {
	Params.Init()
}

func (suite *ControllerSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd,
		config.EtcdUseSSL,
		config.Endpoints,
		config.EtcdTLSCert,
		config.EtcdTLSKey,
		config.EtcdTLSCACert,
		config.EtcdTLSMinVersion)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)

	// meta
	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)
	suite.distManager = meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetManager = meta.NewTargetManager(suite.broker, suite.meta)

	suite.balancer = balance.NewMockBalancer(suite.T())
	suite.scheduler = task.NewMockScheduler(suite.T())
	suite.controller = NewCheckerController(suite.meta, suite.distManager, suite.targetManager, suite.balancer, suite.nodeMgr, suite.scheduler)

	suite.scheduler.EXPECT().GetSegmentTaskNum().Return(0).Maybe()
}

func (suite *ControllerSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *ControllerSuite) TestCheckAfterStop() {
	suite.controller.Start(context.Background())
	suite.controller.Stop()
	suite.controller.Check()

}

func (suite *ControllerSuite) TestCheck() {
	// test close chan
	close(suite.controller.checkCh)
}

func TestController(t *testing.T) {
	suite.Run(t, new(ControllerSuite))
}
