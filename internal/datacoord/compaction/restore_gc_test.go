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

package compaction

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRestoreCompactionProofSurvivesTaskGC(t *testing.T) {
	paramtable.Init()
	ctx := t.Context()
	catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	tasks, err := ctask.NewCompactionTaskMeta(ctx, catalog)
	require.NoError(t, err)
	plan := &datapb.CompactionTask{PlanID: 77, TriggerID: 100, Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, TerminalState: datapb.CompactionTaskState_completed, State: datapb.CompactionTaskState_cleaned, EndTime: 1}
	require.NoError(t, tasks.SaveCompactionTask(ctx, plan))
	active := true
	inspector := &Inspector{compactionTaskMeta: tasks, LookupRestoreJob: func(_ context.Context, id int64) *datapb.CopySegmentJob {
		require.EqualValues(t, 100, id)
		state := datapb.CopySegmentJobState_CopySegmentJobExecuting
		if !active {
			state = datapb.CopySegmentJobState_CopySegmentJobCompleted
		}
		return &datapb.CopySegmentJob{JobId: id, State: state}
	}}
	inspector.cleanCompactionTaskMeta()
	require.Len(t, tasks.GetCompactionTasksByTriggerID(100), 1)
	active = false
	inspector.cleanCompactionTaskMeta()
	require.Empty(t, tasks.GetCompactionTasksByTriggerID(100))
}
