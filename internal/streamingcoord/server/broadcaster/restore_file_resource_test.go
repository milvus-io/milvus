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

package broadcaster

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPendingRestoreFileResourcesRecoverFromBroadcast(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name      string
		state     streamingpb.BroadcastTaskState
		creation  bool
		resources []int64
		included  bool
	}{
		{"pending", streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, true, []int64{99, 100}, true},
		{"replicated", streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, true, []int64{99, 100}, true},
		{"tombstone", streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, true, []int64{99}, false},
		{"done", streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE, true, []int64{99}, false},
		{"legacy", streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, false, nil, false},
		{"no resources", streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, true, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &messagespb.RestoreSnapshotMessageBody{}
			if tc.creation {
				body.CollectionHeader = &message.CreateCollectionMessageHeader{CollectionId: 10}
				body.Collection = &message.CreateCollectionRequest{CollectionID: 10, CollectionSchema: &schemapb.CollectionSchema{FileResourceIds: tc.resources}}
			}
			msg := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10, JobId: 20}).
				WithBody(body).WithBroadcast([]string{"v1"}).MustBuildBroadcast()
			record := createNewWaitAckBroadcastTaskFromMessage(msg.WithBroadcastID(1), tc.state, []byte{0})
			encoded, err := proto.Marshal(record)
			require.NoError(t, err)
			reloaded := &streamingpb.BroadcastTask{}
			require.NoError(t, proto.Unmarshal(encoded, reloaded))
			task := newBroadcastTaskFromProto(reloaded, newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
			id, resources, included := task.PendingSchemaFileResourceSnapshot()
			require.Equal(t, tc.included, included)
			if tc.included {
				require.EqualValues(t, 10, id)
				require.Equal(t, tc.resources, resources)
				if len(resources) > 0 {
					resources[0] = -1
					_, next, _ := task.PendingSchemaFileResourceSnapshot()
					require.EqualValues(t, 99, next[0], "callers must not mutate the task's schema")
				}
			}
			bm := &broadcastTaskManager{mu: &sync.Mutex{}, tasks: map[uint64]*broadcastTask{1: task}}
			pending := bm.GetPendingSchemaFileResources()
			if tc.included && len(tc.resources) > 0 {
				require.Equal(t, map[int64][]int64{10: tc.resources}, pending)
			} else {
				require.Empty(t, pending)
			}
		})
	}
}

func TestPendingRestoreResourcesMergeWithOtherSchemaTasks(t *testing.T) {
	paramtable.Init()
	restore := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10}).
		WithBody(&messagespb.RestoreSnapshotMessageBody{
			CollectionHeader: &message.CreateCollectionMessageHeader{CollectionId: 10},
			Collection:       &message.CreateCollectionRequest{CollectionID: 10, CollectionSchema: &schemapb.CollectionSchema{FileResourceIds: []int64{99, 99, 100}}},
		}).
		WithBroadcast([]string{"v1"}).MustBuildBroadcast()
	alter := message.NewAlterCollectionMessageBuilderV2().WithHeader(&message.AlterCollectionMessageHeader{CollectionId: 10}).
		WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{FileResourceIds: []int64{100, 101}}}}).
		WithBroadcast([]string{"v1"}).MustBuildBroadcast()
	bm := &broadcastTaskManager{mu: &sync.Mutex{}, tasks: make(map[uint64]*broadcastTask)}
	for i, msg := range []message.BroadcastMutableMessage{restore, alter} {
		id := uint64(i + 1)
		record := createNewWaitAckBroadcastTaskFromMessage(msg.WithBroadcastID(id), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0})
		bm.tasks[id] = newBroadcastTaskFromProto(record, newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	}
	pending := bm.GetPendingSchemaFileResources()
	require.Len(t, pending, 1)
	require.ElementsMatch(t, []int64{99, 100, 101}, pending[10])
}
