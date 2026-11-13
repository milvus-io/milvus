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

package balance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// A channel move releases the source delegator it was planned against.
func TestChannelMovePlansCarrySourceDelegatorVersion(t *testing.T) {
	ctx := context.Background()
	replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, Nodes: []int64{1, 2}})
	channel := &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "ch"},
		Node:         1,
		Version:      9,
	}
	tasks := CreateChannelTasksFromPlans(ctx, task.WrapIDSource(0), time.Minute,
		[]assign.ChannelAssignPlan{{Channel: channel, Replica: replica, From: 1, To: 2}})
	require.Len(t, tasks, 1)
	defer tasks[0].Cancel(nil)
	actions := tasks[0].Actions()
	require.Len(t, actions, 2)
	require.Equal(t, task.ActionTypeReduce, actions[1].Type())
	require.Equal(t, int64(9), actions[1].(*task.ChannelAction).CopyVersion())
}
