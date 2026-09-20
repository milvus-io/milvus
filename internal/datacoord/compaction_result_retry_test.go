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

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCompactionUnavailableResult(t *testing.T) {
	paramtable.Init()
	key := Params.DataCoordCfg.CompactionMaxAttempts.Key
	previous := Params.DataCoordCfg.CompactionMaxAttempts.GetValue()
	require.NoError(t, Params.Save(key, "5"))
	defer Params.Save(key, previous)

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
					PlanID: 10, NodeID: 20, TriggerID: 50, Type: typ,
					State:                  datapb.CompactionTaskState_executing,
					RetryTimes:             1,
					FailReason:             "previous reason",
					InputSegments:          []int64{5},
					PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 100, End: 101},
				}
				switch typ {
				case datapb.CompactionType_Level0DeleteCompaction:
					return newL0CompactionTask(context.Background(), p, nil, meta)
				case datapb.CompactionType_ClusteringCompaction:
					return newClusteringCompactionTask(context.Background(), p, nil, meta, nil, nil, nil)
				case datapb.CompactionType_BumpSchemaVersionCompaction:
					return newBumpSchemaVersionTask(context.Background(), p, nil, meta, nil)
				default:
					return newMixCompactionTask(context.Background(), p, nil, meta, nil)
				}
			}
			query := &datapb.CompactionStateRequest{PlanID: 10}
			lostResult := merr.WrapErrCompactionResultNotFound("terminal task has no payload")

			for _, test := range []struct {
				name    string
				dropErr error
			}{
				{name: "drop succeeds"},
				{name: "drop fails", dropErr: merr.WrapErrServiceInternalMsg("drop failed")},
			} {
				t.Run(test.name, func(t *testing.T) {
					meta := NewMockCompactionMeta(t)
					cluster := session.NewMockCluster(t)
					task := newTask(meta)
					cluster.EXPECT().QueryCompaction(int64(20), query).Return(nil, lostResult).Once()
					expected := cloneCompactionTask(task.GetTask(),
						setState(datapb.CompactionTaskState_retrying), setFailReason(lostResult.Error()))
					meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
						Run(func(_ context.Context, saved *datapb.CompactionTask) {
							require.Positive(t, saved.GetEndTime())
							expected.EndTime = saved.GetEndTime()
							require.True(t, proto.Equal(expected, saved))
						}).Return(nil).Once()

					task.QueryTaskOnWorker(cluster)
					require.True(t, proto.Equal(expected, task.GetTask()))
					require.Equal(t, taskcommon.Retry, task.GetTaskState())
					cluster.AssertNotCalled(t, "DropCompaction", mock.Anything, mock.Anything)

					// Worker cleanup follows the durable retry handoff. Its failure
					// must neither revive the old plan nor block a fresh replacement.
					cluster.EXPECT().DropCompaction(int64(20), int64(10)).Return(test.dropErr).Once()
					task.DropTaskOnWorker(cluster)
					require.True(t, proto.Equal(expected, task.GetTask()))

					alloc := allocator.NewMockAllocator(t)
					alloc.EXPECT().AllocID(mock.Anything).Return(int64(30), nil).Once()
					alloc.EXPECT().AllocN(int64(1)).Return(int64(200), int64(201), nil).Once()
					alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(10000), nil).Once()
					inspector := &compactionInspector{ctx: context.Background(), allocator: alloc}
					replacement := inspector.buildReplacement(task)
					require.NotNil(t, replacement)
					require.Equal(t, int64(30), replacement.GetPlanID())
					require.Equal(t, datapb.CompactionTaskState_pipelining, replacement.GetState())
					require.Equal(t, int64(NullNodeID), replacement.GetNodeID())
					require.Equal(t, task.GetTask().GetRetryTimes()+1, replacement.GetRetryTimes())
					require.Equal(t, task.GetTask().GetTriggerID(), replacement.GetTriggerID())
					require.Equal(t, task.GetTask().GetInputSegments(), replacement.GetInputSegments())
					require.Equal(t, &datapb.IDRange{Begin: 200, End: 201}, replacement.GetPreAllocatedSegmentIDs())
					require.Empty(t, replacement.GetFailReason())
					require.Zero(t, replacement.GetEndTime())
					require.True(t, proto.Equal(expected, task.GetTask()), "the old plan remains retired")
					meta.AssertNumberOfCalls(t, "SaveCompactionTask", 1)
				})
			}
		})
	}
}

func TestMixCompactionDeletedErrorAfterSaveFailure(t *testing.T) {
	paramtable.Init()
	key := Params.DataCoordCfg.CompactionMaxAttempts.Key
	previous := Params.DataCoordCfg.CompactionMaxAttempts.GetValue()
	require.NoError(t, Params.Save(key, "5"))
	defer Params.Save(key, previous)

	meta := NewMockCompactionMeta(t)
	cluster := session.NewMockCluster(t)
	task := newMixCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID: 10, NodeID: 20, Type: datapb.CompactionType_MixCompaction,
		State:      datapb.CompactionTaskState_executing,
		RetryTimes: 1, FailReason: "previous reason",
	}, nil, meta, nil)
	original := cloneCompactionTask(task.GetTask())
	expected := cloneCompactionTask(task.GetTask(), setState(datapb.CompactionTaskState_retrying),
		setFailReason("worker left the query unanswered"))
	query := &datapb.CompactionStateRequest{PlanID: 10}
	cluster.EXPECT().QueryCompaction(int64(20), query).
		Return(nil, merr.WrapErrCompactionResultNotFound("terminal task has no payload")).Once()
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
		Return(merr.WrapErrServiceUnavailableMsg("meta unavailable")).Once()

	task.QueryTaskOnWorker(cluster)
	require.True(t, proto.Equal(original, task.GetTask()))

	// A subsequent query may find the task already deleted by the worker.
	// Model that as a generic query error without asserting a backend error code.
	// A failed retry-state save must not prevent a later handoff to a new plan.
	cluster.EXPECT().QueryCompaction(int64(20), query).
		Return(nil, merr.WrapErrServiceInternalMsg("task 10 deleted")).Once()
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, saved *datapb.CompactionTask) {
			require.Positive(t, saved.GetEndTime())
			expected.EndTime = saved.GetEndTime()
			require.True(t, proto.Equal(expected, saved))
		}).Return(nil).Once()

	task.QueryTaskOnWorker(cluster)
	require.True(t, proto.Equal(expected, task.GetTask()))
	require.Equal(t, taskcommon.Retry, task.GetTaskState())
	cluster.AssertNotCalled(t, "DropCompaction", mock.Anything, mock.Anything)
}
