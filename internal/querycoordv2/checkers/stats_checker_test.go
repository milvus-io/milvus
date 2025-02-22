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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type StatsCheckerSuite struct {
	suite.Suite
	kv        kv.MetaKv
	checker   *StatsChecker
	meta      *meta.Meta
	broker    *meta.MockBroker
	nodeMgr   *session.NodeManager
	targetMgr *meta.MockTargetManager
}

func (suite *StatsCheckerSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *StatsCheckerSuite) SetupTest() {
	var err error
	config := params.GenerateEtcdConfig()
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
	idAllocator := params.RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)
	distManager := meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())

	suite.targetMgr = meta.NewMockTargetManager(suite.T())
	suite.checker = NewStatsChecker(suite.meta, distManager, suite.broker, suite.nodeMgr, suite.targetMgr)

	suite.targetMgr.EXPECT().GetSealedSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cid, sid int64, i3 int32) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:    sid,
			Level: datapb.SegmentLevel_L1,
		}
	}).Maybe()
}

func (suite *StatsCheckerSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *StatsCheckerSuite) TestLoadJsonIndex() {
	checker := suite.checker
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key)
	// meta
	coll := utils.CreateTestCollection(1, 1)
	coll.FieldIndexID = map[int64]int64{101: 1000}
	checker.meta.CollectionManager.PutCollection(ctx, coll)
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(200, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(ctx, 1)
	checker.meta.ResourceManager.HandleNodeUp(ctx, 2)

	// dist
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))

	// broker
	suite.broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).
		Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{
				Name: "test_loadJsonIndex",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 101, DataType: schemapb.DataType_JSON, Name: "JSON"},
				},
			},
			CollectionID:   1,
			CollectionName: "test_loadJsonIndex",
		}, nil)

	tasks := checker.Check(context.Background())
	suite.Require().Len(tasks, 1)

	t := tasks[0]
	suite.Require().Len(t.Actions(), 1)

	action, ok := t.Actions()[0].(*task.SegmentAction)
	suite.Require().True(ok)
	suite.EqualValues(200, t.ReplicaID())
	suite.Equal(task.ActionTypeStatsUpdate, action.Type())
	suite.EqualValues(2, action.GetSegmentID())

	// test skip load json index for read only node
	suite.nodeMgr.Stopping(1)
	suite.nodeMgr.Stopping(2)
	suite.meta.ResourceManager.HandleNodeStopping(ctx, 1)
	suite.meta.ResourceManager.HandleNodeStopping(ctx, 2)
	utils.RecoverAllCollection(suite.meta)
	tasks = checker.Check(context.Background())
	suite.Require().Len(tasks, 0)
}

func (suite *StatsCheckerSuite) TestJsonIndexNotMatch() {
	checker := suite.checker
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key)
	// meta
	coll := utils.CreateTestCollection(1, 1)
	coll.FieldIndexID = map[int64]int64{101: 1000}
	checker.meta.CollectionManager.PutCollection(ctx, coll)
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(200, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(ctx, 1)
	checker.meta.ResourceManager.HandleNodeUp(ctx, 2)

	// dist
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))

	// broker
	suite.broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).
		Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{
				Name: "test_loadJsonIndex",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 101, DataType: schemapb.DataType_Int16, Name: "int"},
				},
			},
			CollectionID:   1,
			CollectionName: "test_loadJsonIndex",
		}, nil)

	tasks := checker.Check(context.Background())
	suite.Require().Len(tasks, 0)
}

func (suite *StatsCheckerSuite) TestDescribeCollectionFailed() {
	checker := suite.checker
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key)
	// meta
	coll := utils.CreateTestCollection(1, 1)
	coll.FieldIndexID = map[int64]int64{101: 1000}
	checker.meta.CollectionManager.PutCollection(ctx, coll)
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(200, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(ctx, 1)
	checker.meta.ResourceManager.HandleNodeUp(ctx, 2)

	// dist
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))

	// broker
	suite.broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).
		Return(nil, errors.New("mocked error"))

	tasks := checker.Check(context.Background())
	suite.Require().Len(tasks, 0)
}

func (suite *StatsCheckerSuite) TestCreateNewJsonIndex() {
	checker := suite.checker
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key)
	// meta
	coll := utils.CreateTestCollection(1, 1)
	coll.FieldIndexID = map[int64]int64{101: 1000}
	checker.meta.CollectionManager.PutCollection(ctx, coll)
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(200, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(ctx, 1)
	checker.meta.ResourceManager.HandleNodeUp(ctx, 2)

	// dist
	segment := utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel")
	segment.JSONIndexField = []int64{102}
	checker.dist.SegmentDistManager.Update(1, segment)

	// broker
	suite.broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).
		Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{
				Name: "test_loadJsonIndex",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 101, DataType: schemapb.DataType_JSON, Name: "JSON"},
				},
			},
			CollectionID:   1,
			CollectionName: "test_loadJsonIndex",
		}, nil)

	tasks := checker.Check(context.Background())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	suite.Equal(tasks[0].Actions()[0].(*task.SegmentAction).Type(), task.ActionTypeStatsUpdate)
}

func TestStatsChecker(t *testing.T) {
	suite.Run(t, new(StatsCheckerSuite))
}
