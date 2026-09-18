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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// subscribeWith runs one subscribe of channel on node 1 against the given
// DescribeCollection answer. The cluster mock expects no call at all, so a
// subscribe that reaches WatchDmChannels fails the test.
func subscribeWith(t *testing.T, describe *milvuspb.DescribeCollectionResponse, channel string) error {
	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(describe, nil)
	ex := NewExecutor(1, nil, nil, broker, nil, session.NewMockCluster(t), nil)

	channelTask, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(0), 1,
		meta.NilReplica, NewChannelAction(1, ActionTypeGrow, channel))
	require.NoError(t, err)
	defer channelTask.Cancel(nil)
	return ex.subscribeChannel(channelTask, 0)
}

// C1: the channel checker decides what to watch from a shard-state view that
// may be seconds old, so a retired split source can still look listed to it
// right after adoption. The executor re-reads the collection just before the
// watch, and a vchannel it no longer lists is never watched: a rebuilt source
// has no children to front, and would serve its key range without them.
func TestSubscribeRefusesAChannelTheCollectionNoLongerLists(t *testing.T) {
	adopted := &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v1", "v2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardNormal},
			{State: schemapb.ShardState_ShardNormal},
		},
	}
	err := subscribeWith(t, adopted, "v0")
	assert.ErrorIs(t, err, merr.ErrChannelNotFound)
}

// I2: the channel checker's shard-state view may be seconds old. The executor's
// fresh describe refuses a split target that is still Creating, which only its
// source's delegator may serve until adoption.
func TestSubscribeRefusesASplitTargetNotYetAdopted(t *testing.T) {
	window := &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting},
			{State: schemapb.ShardState_ShardCreating},
		},
	}
	err := subscribeWith(t, window, "v1")
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}
