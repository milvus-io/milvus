// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package observers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	etcdKV "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type ResourceObserverSuite struct {
	suite.Suite

	kv kv.MetaKv
	// dependency
	store    *mocks.QueryCoordCatalog
	meta     *meta.Meta
	observer *ResourceObserver
	nodeMgr  *session.NodeManager

	collectionID int64
	partitionID  int64

	ctx context.Context
}

func (suite *ResourceObserverSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.CheckResourceGroupInterval.Key, "3")
}

func (suite *ResourceObserverSuite) SetupTest() {
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
	suite.kv = etcdKV.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.ctx = context.Background()

	// meta
	suite.store = mocks.NewQueryCoordCatalog(suite.T())
	idAllocator := RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, suite.store, suite.nodeMgr)

	suite.observer = NewResourceObserver(suite.meta)

	suite.store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	suite.store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	for i := 0; i < 10; i++ {
		suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
		}))
		suite.meta.ResourceManager.HandleNodeUp(suite.ctx, int64(i))
	}
}

func (suite *ResourceObserverSuite) TearDownTest() {
	suite.store.ExpectedCalls = nil
}

func (suite *ResourceObserverSuite) TestObserverRecoverOperation() {
	ctx := suite.ctx
	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 4},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 6},
	})
	suite.Error(suite.meta.ResourceManager.MeetRequirement(ctx, "rg"))
	// There's 10 exists node in cluster, new incoming resource group should get 4 nodes after recover.
	suite.observer.checkAndRecoverResourceGroup(ctx)
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg"))

	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg2", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 6},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 10},
	})
	suite.Error(suite.meta.ResourceManager.MeetRequirement(ctx, "rg2"))
	// There's 10 exists node in cluster, new incoming resource group should get 6 nodes after recover.
	suite.observer.checkAndRecoverResourceGroup(ctx)
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg1"))
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg2"))

	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg3", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 1},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 1},
	})
	suite.Error(suite.meta.ResourceManager.MeetRequirement(ctx, "rg3"))
	// There's 10 exists node in cluster, but has been occupied by rg1 and rg2, new incoming resource group cannot get any node.
	suite.observer.checkAndRecoverResourceGroup(ctx)
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg1"))
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg2"))
	suite.Error(suite.meta.ResourceManager.MeetRequirement(ctx, "rg3"))
	// New node up, rg3 should get the node.
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 10,
	}))
	suite.meta.ResourceManager.HandleNodeUp(ctx, 10)
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg1"))
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg2"))
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg3"))
	// new node is down, rg3 cannot use that node anymore.
	suite.nodeMgr.Remove(10)
	suite.meta.ResourceManager.HandleNodeDown(ctx, 10)
	suite.observer.checkAndRecoverResourceGroup(ctx)
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg1"))
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg2"))
	suite.Error(suite.meta.ResourceManager.MeetRequirement(ctx, "rg3"))

	// create a new incoming node failure.
	suite.store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Unset()
	suite.store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(errors.New("failure"))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 11,
	}))
	// should be failure, so new node cannot be used by rg3.
	suite.meta.ResourceManager.HandleNodeUp(ctx, 11)
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg1"))
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg2"))
	suite.Error(suite.meta.ResourceManager.MeetRequirement(ctx, "rg3"))
	suite.store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Unset()
	suite.store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	// storage recovered, so next recover will be success.
	suite.observer.checkAndRecoverResourceGroup(ctx)
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg1"))
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg2"))
	suite.NoError(suite.meta.ResourceManager.MeetRequirement(ctx, "rg3"))
}

func (suite *ResourceObserverSuite) TestSchedule() {
	suite.observer.Start()
	defer suite.observer.Stop()
	ctx := suite.ctx

	check := func() {
		suite.Eventually(func() bool {
			rgs := suite.meta.ResourceManager.ListResourceGroups(ctx)
			for _, rg := range rgs {
				if err := suite.meta.ResourceManager.GetResourceGroup(ctx, rg).MeetRequirement(); err != nil {
					return false
				}
			}
			return true
		}, 5*time.Second, 1*time.Second)
	}

	for i := 1; i <= 4; i++ {
		suite.meta.ResourceManager.AddResourceGroup(ctx, fmt.Sprintf("rg%d", i), &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{NodeNum: int32(i)},
			Limits:   &rgpb.ResourceGroupLimit{NodeNum: int32(i)},
		})
	}
	check()

	for i := 1; i <= 4; i++ {
		suite.meta.ResourceManager.AddResourceGroup(ctx, fmt.Sprintf("rg%d", i), &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{NodeNum: 0},
			Limits:   &rgpb.ResourceGroupLimit{NodeNum: 0},
		})
	}
	check()
}

func (suite *ResourceObserverSuite) TearDownSuite() {
	suite.kv.Close()
}

func TestResourceObserver(t *testing.T) {
	suite.Run(t, new(ResourceObserverSuite))
}
