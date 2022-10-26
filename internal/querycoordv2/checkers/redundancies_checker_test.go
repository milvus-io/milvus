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
	"sort"
	"testing"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type RedundanciesCheckerTestSuite struct {
	suite.Suite
	kv      *etcdkv.EtcdKV
	checker *RedundanciesChecker
	broker  *meta.MockBroker
}

func (suite *RedundanciesCheckerTestSuite) SetupSuite() {
	Params.Init()
}

func (suite *RedundanciesCheckerTestSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)

	// meta
	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	testMeta := meta.NewMeta(idAllocator, store)

	distManager := meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.checker = NewRedundanciesChecker(testMeta, distManager, suite.broker)
}

func (suite *RedundanciesCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *RedundanciesCheckerTestSuite) TestReleaseGrowingSegments() {
	checker := suite.checker

	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1}))

	growingSegments := make(map[int64]*meta.Segment)
	growingSegments[1] = utils.CreateTestSegment(1, 1, 1, 1, 0, "test-insert-channel")
	growingSegments[1].StartPosition = &internalpb.MsgPosition{Timestamp: 3}

	growingSegments[2] = utils.CreateTestSegment(1, 1, 2, 1, 0, "test-insert-channel")
	growingSegments[2].StartPosition = &internalpb.MsgPosition{Timestamp: 2}

	growingSegments[3] = utils.CreateTestSegment(1, 1, 3, 1, 1, "test-insert-channel")
	growingSegments[3].StartPosition = &internalpb.MsgPosition{Timestamp: 1}
	checker.dist.LeaderViewManager.Update(1, utils.CreateTestLeaderView(1, 1, "test-insert-channel", map[int64]int64{3: 2}, growingSegments))
	checker.dist.ChannelDistManager.Update(1, utils.CreateTestChannel(1, 1, 1, "test-insert-channel"))

	vChannelInfos := make([]*datapb.VchannelInfo, 0)
	vChannelInfos = append(vChannelInfos, &datapb.VchannelInfo{
		CollectionID:        1,
		ChannelName:         "test-insert-channel",
		UnflushedSegmentIds: []int64{},
		SeekPosition:        &internalpb.MsgPosition{Timestamp: 3},
	})
	suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything, mock.Anything).Return(vChannelInfos, nil, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return([]int64{1}, nil)

	tasks := checker.Check(context.TODO())
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Actions()[0].(*task.SegmentAction).SegmentID() < tasks[i].Actions()[0].(*task.SegmentAction).SegmentID()
	})

	suite.Len(tasks, 2)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(2, action.SegmentID())
	suite.EqualValues(1, action.Node())

	suite.Len(tasks[1].Actions(), 1)
	action, ok = tasks[1].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[1].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(3, action.SegmentID())
	suite.EqualValues(1, action.Node())
}

func TestRedundanciesCheckerTestSuite(t *testing.T) {
	suite.Run(t, new(RedundanciesCheckerTestSuite))
}
