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

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type IndexCheckerSuite struct {
	suite.Suite
	kv      kv.MetaKv
	checker *IndexChecker
	meta    *meta.Meta
	broker  *meta.MockBroker
	nodeMgr *session.NodeManager
}

func (suite *IndexCheckerSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *IndexCheckerSuite) SetupTest() {
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

	suite.checker = NewIndexChecker(suite.meta, distManager, suite.broker, suite.nodeMgr)
}

func (suite *IndexCheckerSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *IndexCheckerSuite) TestLoadIndex() {
	checker := suite.checker

	// meta
	coll := utils.CreateTestCollection(1, 1)
	coll.FieldIndexID = map[int64]int64{101: 1000}
	checker.meta.CollectionManager.PutCollection(coll)
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(200, 1, []int64{1, 2}))
	suite.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 1)
	checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 2)

	// dist
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))

	// broker
	suite.broker.EXPECT().GetIndexInfo(mock.Anything, int64(1), int64(2)).
		Return([]*querypb.FieldIndexInfo{
			{
				FieldID:        101,
				IndexID:        1000,
				EnableIndex:    true,
				IndexFilePaths: []string{"index"},
			},
		}, nil)

	tasks := checker.Check(context.Background())
	suite.Require().Len(tasks, 1)

	t := tasks[0]
	suite.Require().Len(t.Actions(), 1)

	action, ok := t.Actions()[0].(*task.SegmentAction)
	suite.Require().True(ok)
	suite.EqualValues(200, t.ReplicaID())
	suite.Equal(task.ActionTypeUpdate, action.Type())
	suite.EqualValues(2, action.SegmentID())

	// test skip load index for stopping node
	suite.nodeMgr.Stopping(1)
	suite.nodeMgr.Stopping(2)
	tasks = checker.Check(context.Background())
	suite.Require().Len(tasks, 0)
}

func (suite *IndexCheckerSuite) TestIndexInfoNotMatch() {
	checker := suite.checker

	// meta
	coll := utils.CreateTestCollection(1, 1)
	coll.FieldIndexID = map[int64]int64{101: 1000}
	checker.meta.CollectionManager.PutCollection(coll)
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(200, 1, []int64{1, 2}))
	suite.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 1)
	checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 2)

	// dist
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 3, 1, 1, "test-insert-channel"))

	// broker
	suite.broker.EXPECT().GetIndexInfo(mock.Anything, int64(1), mock.AnythingOfType("int64")).Call.
		Return(func(ctx context.Context, collectionID, segmentID int64) []*querypb.FieldIndexInfo {
			if segmentID == 2 {
				return []*querypb.FieldIndexInfo{
					{
						FieldID:     101,
						IndexID:     1000,
						EnableIndex: false,
					},
				}
			}
			if segmentID == 3 {
				return []*querypb.FieldIndexInfo{
					{
						FieldID:     101,
						IndexID:     1002,
						EnableIndex: false,
					},
				}
			}
			return nil
		}, nil)

	tasks := checker.Check(context.Background())
	suite.Require().Len(tasks, 0)
}

func (suite *IndexCheckerSuite) TestGetIndexInfoFailed() {
	checker := suite.checker

	// meta
	coll := utils.CreateTestCollection(1, 1)
	coll.FieldIndexID = map[int64]int64{101: 1000}
	checker.meta.CollectionManager.PutCollection(coll)
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(200, 1, []int64{1, 2}))
	suite.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 1)
	checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 2)

	// dist
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 3, 1, 1, "test-insert-channel"))

	// broker
	suite.broker.EXPECT().GetIndexInfo(mock.Anything, int64(1), mock.AnythingOfType("int64")).
		Return(nil, errors.New("mocked error"))

	tasks := checker.Check(context.Background())
	suite.Require().Len(tasks, 0)
}

func TestIndexChecker(t *testing.T) {
	suite.Run(t, new(IndexCheckerSuite))
}
