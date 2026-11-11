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

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
)

func delegatorOf(collection, node, version, replicaID int64, channel string, view *meta.LeaderView) *meta.DmChannel {
	ch := utils.CreateTestChannel(collection, node, version, channel)
	ch.ReplicaID = replicaID
	if view == nil {
		view = &meta.LeaderView{ID: node, CollectionID: collection, Channel: channel, Version: version}
	}
	ch.View = view
	return ch
}

// A delegator another replica left on one of the replica's nodes serves no
// replica. The replica removes it with a release carrying its version, so the
// release cannot remove a delegator the replica watches there afterwards.
func (suite *ChannelCheckerTestSuite) TestReleaseChannelLeftByAnotherReplica() {
	ctx := context.Background()
	checker := suite.checker
	suite.NoError(checker.meta.PutCollection(ctx, utils.CreateTestCollection(1, 1)))
	suite.NoError(checker.meta.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2})))
	suite.setNodeAvailable(1, 2)
	const channel = "test-insert-channel"

	for _, c := range []struct {
		name     string
		reported int64
		released bool
	}{
		{"left_by_another_replica", 7, true},
		{"own", 1, false},
		{"unreported", 0, false},
	} {
		suite.Run(c.name, func() {
			checker.dist.ChannelDistManager.Update(1, delegatorOf(1, 1, 3, 1, channel, nil))
			checker.dist.ChannelDistManager.Update(2, delegatorOf(1, 2, 5, c.reported, channel, nil))
			tasks := checker.Check(ctx)
			defer func() {
				for _, t := range tasks {
					t.Cancel(nil)
				}
			}()
			if !c.released {
				suite.Empty(tasks)
				return
			}
			suite.Require().Len(tasks, 1)
			suite.Equal(int64(1), tasks[0].ReplicaID())
			suite.Equal("channel left by another replica", tasks[0].GetReason())
			suite.Require().Len(tasks[0].Actions(), 1)
			action := tasks[0].Actions()[0].(*task.ChannelAction)
			suite.Equal(task.ActionTypeReduce, action.Type())
			suite.Equal(int64(2), action.Node())
			suite.Equal(channel, action.ChannelName())
			suite.Equal(int64(5), action.CopyVersion())
		})
	}
}
