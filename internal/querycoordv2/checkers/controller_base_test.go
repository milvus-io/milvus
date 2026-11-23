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
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr, metacache.NewMetaStore(nil))
	suite.dist = meta.NewDistributionManager(suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetManager = meta.NewTargetManager(suite.broker, suite.meta, metacache.NewMetaStore(nil))

	suite.scheduler = task.NewMockScheduler(suite.T())

	suite.controller = NewCheckerController(suite.meta, suite.dist, suite.targetManager, suite.nodeMgr, suite.scheduler, suite.broker)
}

func (suite *ControllerBaseTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (s *ControllerBaseTestSuite) TestActivation() {
	active, err := s.controller.IsActive(utils.IndexChecker)
	s.NoError(err)
	s.True(active)
	err = s.controller.Deactivate(utils.IndexChecker)
	s.NoError(err)
	active, err = s.controller.IsActive(utils.IndexChecker)
	s.NoError(err)
	s.False(active)
	err = s.controller.Activate(utils.IndexChecker)
	s.NoError(err)
	active, err = s.controller.IsActive(utils.IndexChecker)
	s.NoError(err)
	s.True(active)

	invalidTyp := -1
	_, err = s.controller.IsActive(utils.CheckerType(invalidTyp))
	s.Equal(errTypeNotFound, err)
}

func (s *ControllerBaseTestSuite) TestListCheckers() {
	checkers := s.controller.Checkers()
	ids := make([]utils.CheckerType, 0, len(checkers))
	for _, checker := range checkers {
		ids = append(ids, checker.ID())
	}
	s.ElementsMatch([]utils.CheckerType{utils.IndexChecker, utils.LeaderChecker}, ids)
}

func TestControllerRetainedCheckersRunPeriodically(t *testing.T) {
	paramtable.Init()
	p := paramtable.Get()
	require.NoError(t, p.Save(p.QueryCoordCfg.IndexCheckInterval.Key, "10"))
	require.NoError(t, p.Save(p.QueryCoordCfg.LeaderViewUpdateInterval.Key, "1"))
	t.Cleanup(func() {
		require.NoError(t, p.Reset(p.QueryCoordCfg.IndexCheckInterval.Key))
		require.NoError(t, p.Reset(p.QueryCoordCfg.LeaderViewUpdateInterval.Key))
	})
	var indexChecks, leaderChecks atomic.Int32
	index := mockey.Mock((*IndexChecker).Check).To(func(*IndexChecker, context.Context) []task.Task {
		indexChecks.Add(1)
		return nil
	}).Build()
	t.Cleanup(func() { index.UnPatch() })
	leader := mockey.Mock((*LeaderChecker).Check).To(func(*LeaderChecker, context.Context) []task.Task {
		leaderChecks.Add(1)
		return nil
	}).Build()
	t.Cleanup(func() { leader.UnPatch() })

	controller := NewCheckerController(nil, nil, nil, nil, nil, nil)
	controller.Start()
	defer controller.Stop()
	require.Eventually(t, func() bool {
		return indexChecks.Load() > 0 && leaderChecks.Load() > 0
	}, 3*time.Second, 10*time.Millisecond)

	// Interval refresh must keep the remaining periodic checks running.
	require.NoError(t, p.Save(p.QueryCoordCfg.IndexCheckInterval.Key, "20"))
	previous := indexChecks.Load()
	require.Eventually(t, func() bool { return indexChecks.Load() >= previous+2 }, time.Second, 10*time.Millisecond)
}

func TestControllerBaseTestSuite(t *testing.T) {
	suite.Run(t, new(ControllerBaseTestSuite))
}
