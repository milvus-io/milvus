// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCompactionEnqueueFailureRetainsCleanupOwner(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name           string
		ambiguousSave  bool
		failFailedSave bool
		failCleanSave  bool
		restart        bool
	}{
		{name: "queue full"},
		{name: "cleanup retries", failCleanSave: true},
		{name: "failure marker retries through cleanup", failFailedSave: true, failCleanSave: true},
		{name: "restart after cleanup failure", failCleanSave: true, restart: true},
		{name: "initial save applied but response lost", ambiguousSave: true},
		{name: "ambiguous save and cleanup errors", ambiguousSave: true, failFailedSave: true, failCleanSave: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalogErr := merr.WrapErrServiceUnavailableMsg("injected compaction catalog failure")
			var durable *datapb.CompactionTask
			catalog.EXPECT().ListCompactionTask(mock.Anything).RunAndReturn(func(context.Context) ([]*datapb.CompactionTask, error) {
				if durable == nil {
					return nil, nil
				}
				return []*datapb.CompactionTask{proto.Clone(durable).(*datapb.CompactionTask)}, nil
			})
			failCleanSave := tc.failCleanSave
			catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, saved *datapb.CompactionTask) error {
				if saved.GetState() == datapb.CompactionTaskState_failed && tc.failFailedSave ||
					saved.GetState() == datapb.CompactionTaskState_cleaned && failCleanSave {
					return catalogErr
				}
				durable = proto.Clone(saved).(*datapb.CompactionTask)
				if tc.ambiguousSave && saved.GetState() == datapb.CompactionTaskState_pipelining {
					// The catalog applied the initial write, but its failed response
					// prevents compactionTaskMeta from recording it in memory.
					return catalogErr
				}
				return nil
			})
			taskMeta, err := newCompactionTaskMeta(ctx, catalog)
			require.NoError(t, err)
			m := &meta{ctx: ctx, segments: NewSegmentsInfo(), compactionTaskMeta: taskMeta}
			m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
				ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1, IsSorted: true,
			}))
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(1000), nil).Once()
			inspector := newCompactionInspector(m, alloc, nil, nil, nil, nil)
			inspector.queueTasks = NewCompactionQueue(1, getPrioritizer())
			require.NoError(t, inspector.queueTasks.Enqueue(newMixCompactionTask(&datapb.CompactionTask{
				PlanID: 99, Type: datapb.CompactionType_MixCompaction,
			}, nil, m, nil)))
			err = inspector.enqueueCompaction(&datapb.CompactionTask{
				PlanID: 10, TriggerID: 10, CollectionID: 100, InputSegments: []int64{1},
				Type: datapb.CompactionType_MixCompaction, State: datapb.CompactionTaskState_pipelining,
				PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 20, End: 21},
			})
			if tc.ambiguousSave {
				require.ErrorIs(t, err, catalogErr)
			} else {
				require.ErrorIs(t, err, errFull)
			}
			require.Equal(t, 1, inspector.queueTasks.Len())
			require.Contains(t, inspector.cleaningTasks, int64(10))
			require.True(t, m.IsSegmentCompacting(1))
			// A failed status write must retain both the durable original and an
			// in-memory cleanup owner, rather than opening a second admission.
			if tc.failFailedSave {
				require.Equal(t, datapb.CompactionTaskState_pipelining, durable.GetState())
			} else {
				require.Equal(t, datapb.CompactionTaskState_failed, durable.GetState())
			}
			_, admitted := m.CheckAndSetSegmentsCompacting(ctx, []int64{1})
			require.False(t, admitted)

			if tc.failCleanSave {
				inspector.cleanFailedTasks()
				require.Contains(t, inspector.cleaningTasks, int64(10))
				require.True(t, m.IsSegmentCompacting(1))
				failCleanSave = false
			}
			if tc.restart {
				// Rebuild durable task metadata and volatile ownership as startup
				// does. The recovered failure enters the normal cleanup lifecycle.
				m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, catalog)
				require.NoError(t, err)
				m.SetSegmentsCompacting(ctx, []int64{1}, false)
				scheduler := task.NewMockGlobalScheduler(t)
				scheduler.EXPECT().Enqueue(mock.Anything).Once()
				inspector = newCompactionInspector(m, nil, nil, scheduler, scheduler, nil)
				inspector.loadMeta()
				require.True(t, m.IsSegmentCompacting(1))
				require.NoError(t, inspector.checkCompaction())
				require.Contains(t, inspector.cleaningTasks, int64(10))
			}
			inspector.cleanFailedTasks()
			require.Empty(t, inspector.cleaningTasks)
			require.Equal(t, datapb.CompactionTaskState_cleaned, durable.GetState())
			require.Equal(t, datapb.CompactionTaskState_cleaned, m.GetCompactionTasks(ctx)[10][0].GetState())
			require.False(t, m.IsSegmentCompacting(1))
		})
	}
}

func TestCompactionWorkerResultPausesForSnapshot(t *testing.T) {
	paramtable.Init()
	for _, kind := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction,
		datapb.CompactionType_SortCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction,
		datapb.CompactionType_ClusteringCompaction,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			protection := createTestSnapshotMetaLoaded(t)
			protection.SetSnapshotPending(100)
			m := &meta{ctx: context.Background(), segments: NewSegmentsInfo(), snapshotMeta: protection}
			input := &datapb.CompactionTask{
				PlanID: 10, CollectionID: 100, InputSegments: []int64{1}, NodeID: 20,
				Type: kind, State: datapb.CompactionTaskState_executing,
			}
			var compaction CompactionTask
			switch kind {
			case datapb.CompactionType_ClusteringCompaction:
				compaction = newClusteringCompactionTask(input, nil, m, nil, nil, nil)
			case datapb.CompactionType_BumpSchemaVersionCompaction:
				compaction = newBumpSchemaVersionTask(input, nil, m, nil)
			default:
				compaction = newMixCompactionTask(input, nil, m, nil)
			}
			worker := session.NewMockCluster(t)
			worker.EXPECT().QueryCompaction(int64(20), mock.Anything).Return(&datapb.CompactionPlanResult{
				State:    datapb.CompactionTaskState_completed,
				Segments: []*datapb.CompactionSegment{{SegmentID: 2}},
			}, nil).Twice()
			// Exercise the real protection producer and the worker-result
			// consumer. There is deliberately no task catalog: a failed-state
			// write would both violate the pause and fail this test.
			compaction.QueryTaskOnWorker(worker)
			compaction.QueryTaskOnWorker(worker)
			require.Equal(t, datapb.CompactionTaskState_executing, compaction.GetTaskProto().GetState())
			require.Zero(t, compaction.GetTaskProto().GetRetryTimes())
			require.Empty(t, compaction.GetTaskProto().GetFailReason())
		})
	}
}
