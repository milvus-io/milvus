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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func testReplicaWithNodes(id int64) *meta.Replica {
	return meta.NewReplica(&querypb.Replica{
		ID: id, CollectionID: membershipCollectionID,
		ResourceGroup: meta.DefaultResourceGroupName, Nodes: []int64{1, 12, 14}, RwSqNodes: []int64{1, 12, 14},
	})
}

func newTestSegmentTask(t *testing.T, replica *meta.Replica, actions ...Action) *SegmentTask {
	t.Helper()
	task, err := NewSegmentTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID,
		replica, commonpb.LoadPriority_LOW, actions...)
	require.NoError(t, err)
	t.Cleanup(func() { task.Cancel(nil) })
	return task
}

func newTestChannelTask(t *testing.T, replica *meta.Replica, actions ...Action) *ChannelTask {
	t.Helper()
	task, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID, replica, actions...)
	require.NoError(t, err)
	t.Cleanup(func() { task.Cancel(nil) })
	return task
}

func retireTaskLocked(scheduler *taskScheduler, task Task) {
	scheduler.lifecycleMu.RLock()
	defer scheduler.lifecycleMu.RUnlock()
	scheduler.collKeyLock.Lock(task.CollectionID())
	defer scheduler.collKeyLock.Unlock(task.CollectionID())
	scheduler.remove(task)
}

func TestOrphanTaskKeysAreNodeScoped(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			scheduler := newChannelMembershipScheduler(t, testReplicaWithNodes(1))
			setCollectionLoaded(t, scheduler, membershipCollectionID, false)
			for _, node := range []int64{1, 12} {
				var task Task
				if kind == "segment" {
					task = newTestSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(node, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
				} else {
					task = newTestChannelTask(t, meta.NilReplica, NewChannelAction(node, ActionTypeReduce, membershipChannel))
				}
				require.NoError(t, scheduler.Add(task), "orphan cleanup on another node must remain independent")
			}
			require.Equal(t, 2, scheduler.tasks.Len())
			if kind == "segment" {
				require.Equal(t, 2, scheduler.segmentTasks.Len())
			} else {
				require.Equal(t, 2, scheduler.channelTasks.Len())
			}
		})
	}
}
