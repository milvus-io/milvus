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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

// TestImportV3JobTransitToRecordsLeftStageLatency pins the centralized stage
// attribution: the transition records the stage of the state being LEFT, and a
// state that has no stage (Committing) records nothing.
func TestImportV3JobTransitToRecordsLeftStageLatency(t *testing.T) {
	const version internalpb.ImportVersion = 4242 // unique label, isolated from other tests
	newJob := func(state internalpb.ImportJobState) *importJob {
		return &importJob{
			ImportJob: &datapb.ImportJob{JobID: 1, State: state, Version: version},
			tr:        timerecord.NewTimeRecorder("job"),
		}
	}

	// Leaving a staged state (Importing) creates the left state's series.
	importMeta := NewMockImportMeta(t)
	job := newJob(internalpb.ImportJobState_Importing)
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything).Return(nil).Once()
	before := testutil.CollectAndCount(metrics.ImportJobLatency)
	require.NoError(t, ImportV3JobTransitTo(context.Background(), importMeta, job, internalpb.ImportJobState_IndexBuilding))
	require.Equal(t, before+1, testutil.CollectAndCount(metrics.ImportJobLatency))

	// Leaving a stageless state (Committing) records nothing.
	importMeta = NewMockImportMeta(t)
	job = newJob(internalpb.ImportJobState_Committing)
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything).Return(nil).Once()
	before = testutil.CollectAndCount(metrics.ImportJobLatency)
	require.NoError(t, ImportV3JobTransitTo(context.Background(), importMeta, job, internalpb.ImportJobState_Completed))
	require.Equal(t, before, testutil.CollectAndCount(metrics.ImportJobLatency))
}

// TestPreImportingHandlerFailsJobOnFailedTask pins the preimport propagation: a
// Failed PreImportV3 task (which can never reach Completed) must fail the job
// with the task's reason on the checker's next tick.
func TestPreImportingHandlerFailsJobOnFailedTask(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{
		ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_PreImporting},
		tr:        timerecord.NewTimeRecorder("job"),
	}
	task := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2,
		State: datapb.ImportTaskStateV2_Failed, Reason: "worker blew up",
	}, importMeta)

	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ int64, actions ...UpdateJobAction) {
			applied := &importJob{ImportJob: &datapb.ImportJob{}}
			for _, action := range actions {
				action(applied)
			}
			require.Equal(t, internalpb.ImportJobState_Failed, applied.GetState())
			require.Contains(t, applied.GetReason(), "worker blew up")
		}).Return(nil).Once()

	require.NoError(t, preImportingHandler{}.Handle(newImportV3JobContext(checker, job)))
	require.Equal(t, internalpb.ImportJobState_PreImporting, job.GetState(),
		"the handler must not mutate its snapshot in place")
}
