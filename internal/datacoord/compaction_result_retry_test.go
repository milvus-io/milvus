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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCompactionUnavailableResult(t *testing.T) {
	for _, typ := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction,
		datapb.CompactionType_SortCompaction,
		datapb.CompactionType_Level0DeleteCompaction,
		datapb.CompactionType_ClusteringCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			newTask := func(meta CompactionMeta) CompactionTask {
				p := &datapb.CompactionTask{
					PlanID: 10, NodeID: 20, Type: typ,
					State:      datapb.CompactionTaskState_executing,
					RetryTimes: 1, FailReason: "previous reason",
				}
				switch typ {
				case datapb.CompactionType_Level0DeleteCompaction:
					return newL0CompactionTask(p, nil, meta)
				case datapb.CompactionType_ClusteringCompaction:
					return newClusteringCompactionTask(p, nil, meta, nil, nil, nil)
				case datapb.CompactionType_BumpSchemaVersionCompaction:
					return newBumpSchemaVersionTask(p, nil, meta, nil)
				default:
					return newMixCompactionTask(p, nil, meta, nil)
				}
			}
			query := &datapb.CompactionStateRequest{PlanID: 10}
			lostResult := merr.Wrap(merr.ErrDataIntegrity, "terminal task has no payload")

			t.Run("drop then reset without changing retry metadata", func(t *testing.T) {
				meta := NewMockCompactionMeta(t)
				cluster := session.NewMockCluster(t)
				task := newTask(meta)
				dropped := false
				cluster.EXPECT().QueryCompaction(int64(20), query).Return(nil, lostResult).Once()
				cluster.EXPECT().DropCompaction(int64(20), int64(10)).Run(func(int64, int64) {
					dropped = true
				}).Return(nil).Once()
				expected := task.ShadowClone(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
				meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
					Run(func(_ context.Context, saved *datapb.CompactionTask) {
						require.True(t, dropped)
						require.True(t, proto.Equal(expected, saved))
					}).Return(nil).Once()

				task.QueryTaskOnWorker(cluster)
				require.True(t, proto.Equal(expected, task.GetTaskProto()))
				require.Equal(t, taskcommon.Init, task.GetTaskState())
			})

			t.Run("drop failure keeps original task", func(t *testing.T) {
				meta := NewMockCompactionMeta(t)
				cluster := session.NewMockCluster(t)
				task := newTask(meta)
				original := task.ShadowClone()
				cluster.EXPECT().QueryCompaction(int64(20), query).Return(nil, lostResult).Once()
				// Even a non-retriable drop error must not fail a clustering task.
				cluster.EXPECT().DropCompaction(int64(20), int64(10)).
					Return(merr.WrapErrServiceInternalMsg("drop failed")).Once()

				task.QueryTaskOnWorker(cluster)
				require.True(t, proto.Equal(original, task.GetTaskProto()))
				meta.AssertNotCalled(t, "SaveCompactionTask", mock.Anything, mock.Anything)
			})
		})
	}
}

func TestMixCompactionDeletedErrorAfterSaveFailure(t *testing.T) {
	meta := NewMockCompactionMeta(t)
	cluster := session.NewMockCluster(t)
	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID: 10, NodeID: 20, Type: datapb.CompactionType_MixCompaction,
		State:      datapb.CompactionTaskState_executing,
		RetryTimes: 1, FailReason: "previous reason",
	}, nil, meta, nil)
	original := task.ShadowClone()
	expected := task.ShadowClone(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
	query := &datapb.CompactionStateRequest{PlanID: 10}
	cluster.EXPECT().QueryCompaction(int64(20), query).
		Return(nil, merr.WrapErrDataIntegrityMsg("terminal task has no payload")).Once()
	cluster.EXPECT().DropCompaction(int64(20), int64(10)).Return(nil).Once()
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
		Return(merr.WrapErrServiceUnavailableMsg("meta unavailable")).Once()

	task.QueryTaskOnWorker(cluster)
	require.True(t, proto.Equal(original, task.GetTaskProto()))

	// Model Hummer's reported deleted response as a generic query error.
	// This does not assert a particular Hummer error code or the DataNode protocol.
	cluster.EXPECT().QueryCompaction(int64(20), query).
		Return(nil, merr.WrapErrServiceInternalMsg("task 10 deleted")).Once()
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, saved *datapb.CompactionTask) {
			require.True(t, proto.Equal(expected, saved))
		}).Return(nil).Once()

	task.QueryTaskOnWorker(cluster)
	require.True(t, proto.Equal(expected, task.GetTaskProto()))
	require.Equal(t, taskcommon.Init, task.GetTaskState())
	cluster.AssertNumberOfCalls(t, "DropCompaction", 1)
}
