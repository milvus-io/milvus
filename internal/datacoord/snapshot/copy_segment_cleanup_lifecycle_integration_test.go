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

package snapshot

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRejectedCopyCleanupWorkerLossAfterAdmission(t *testing.T) {
	for _, scenario := range []struct {
		name                                   string
		admitted, failStateSave, failClearSave bool
	}{
		{name: "ordinary worker loss remains retryable"},
		{name: "interrupted completed result", admitted: true},
		{name: "failed-state save retries", admitted: true, failStateSave: true},
		{name: "worker-clear save retries", admitted: true, failClearSave: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			task, copies, m, dm := NewCopyCleanupLifecycle(t)
			require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), copysegment.UpdateCopyTaskCleanup(scenario.admitted)))
			// Crash cut: completion admission is durable; installation/terminal save is not.
			copies, err := copysegment.NewCopySegmentMeta(ctx, m.Catalog, m, dm.IndexMeta, nil, nil)
			require.NoError(t, err)
			task = copies.GetTask(ctx, task.GetTaskId()).(*copysegment.Task)
			prefixes := append([]string(nil), task.GetCleanupPrefixes()...)
			cluster := session.NewMockCluster(t)
			cluster.EXPECT().QueryCopySegment(int64(42), mock.Anything).Return(nil, merr.WrapErrNodeNotFound(42))
			if scenario.failStateSave || scenario.failClearSave {
				var original func(*kvdatacoord.Catalog, context.Context, *datapb.CopySegmentTask) error
				patch := mockey.Mock((*kvdatacoord.Catalog).SaveCopySegmentTask).Origin(&original).To(func(c *kvdatacoord.Catalog, ctx context.Context, record *datapb.CopySegmentTask) error {
					if (scenario.failStateSave && record.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed) || (scenario.failClearSave && record.GetNodeId() == meta.NullNodeID) {
						return merr.ErrServiceUnavailable
					}
					return original(c, ctx, record)
				}).Build()
				task.QueryTaskOnWorker(cluster)
				patch.UnPatch()
				require.Equal(t, taskcommon.InProgress, task.GetTaskState(), "failed saves must retain scheduler polling")
				require.EqualValues(t, 42, task.GetNodeId())
				require.True(t, task.GetCleanupRequired())
				if scenario.failClearSave {
					require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
				}
			}
			task.QueryTaskOnWorker(cluster)
			require.EqualValues(t, meta.NullNodeID, task.GetNodeId())
			require.Equal(t, prefixes, task.GetCleanupPrefixes())
			if scenario.admitted {
				require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
				require.Equal(t, taskcommon.Failed, task.GetTaskState())
				require.True(t, task.GetCleanupRequired())
				require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, copies.GetJob(ctx, task.GetJobId()).GetState())
				task.DropTaskOnWorker(cluster) // Assignment was durably released; no Drop RPC.
			} else {
				require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskPending, task.GetState())
				require.False(t, task.GetCleanupRequired())
			}
			recovered, err := copysegment.NewCopySegmentMeta(ctx, m.Catalog, m, dm.IndexMeta, nil, nil)
			require.NoError(t, err)
			require.Equal(t, task.GetState(), recovered.GetTask(ctx, task.GetTaskId()).GetState())
			require.Equal(t, scenario.admitted, recovered.GetTask(ctx, task.GetTaskId()).GetCleanupRequired())
		})
	}
}
