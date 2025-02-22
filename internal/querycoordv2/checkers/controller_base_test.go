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
	"testing"

	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ControllerBaseTestSuite struct {
	suite.Suite
	kv            kv.MetaKv
	meta          *meta.Meta
	broker        *meta.MockBroker
	nodeMgr       *session.NodeManager
	dist          *meta.DistributionManager
	targetManager *meta.TargetManager
	scheduler     *task.MockScheduler
	balancer      *balance.MockBalancer

	controller *CheckerController
}

func (suite *ControllerBaseTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *ControllerBaseTestSuite) SetupTest() {
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
	suite.dist = meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetManager = meta.NewTargetManager(suite.broker, suite.meta)

	suite.balancer = balance.NewMockBalancer(suite.T())
	suite.scheduler = task.NewMockScheduler(suite.T())

	suite.controller = NewCheckerController(suite.meta, suite.dist, suite.targetManager, suite.nodeMgr, suite.scheduler, suite.broker, func() balance.Balance { return suite.balancer })
}

func (s *ControllerBaseTestSuite) TestActivation() {
	active, err := s.controller.IsActive(utils.SegmentChecker)
	s.NoError(err)
	s.True(active)
	err = s.controller.Deactivate(utils.SegmentChecker)
	s.NoError(err)
	active, err = s.controller.IsActive(utils.SegmentChecker)
	s.NoError(err)
	s.False(active)
	err = s.controller.Activate(utils.SegmentChecker)
	s.NoError(err)
	active, err = s.controller.IsActive(utils.SegmentChecker)
	s.NoError(err)
	s.True(active)

	invalidTyp := -1
	_, err = s.controller.IsActive(utils.CheckerType(invalidTyp))
	s.Equal(errTypeNotFound, err)
}

func (s *ControllerBaseTestSuite) TestListCheckers() {
	checkers := s.controller.Checkers()
	s.Equal(5, len(checkers))
}

func TestControllerBaseTestSuite(t *testing.T) {
	suite.Run(t, new(ControllerBaseTestSuite))
}
