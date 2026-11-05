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

package copysegment

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/dataview"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newRestoreTargetDropChecker(t *testing.T, state datapb.CopySegmentJobState, copied bool) (*Checker, CopySegmentJob) {
	t.Helper()
	task := CreateTestCopyTask(100, 2001).(*Task)
	copies, meta := NewCopySegmentTaskTestMeta(t, task)
	job := NewTestCopyJob(100, state).(*Job)
	job.CreatesCollection = true
	job.IdMappings = task.GetIdMappings()
	job.PinId = 42
	require.NoError(t, copies.AddJob(t.Context(), job))
	if copied {
		require.NoError(t, copies.UpdateTask(t.Context(), task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted)))
	}
	// Rebuild from catalog: no process-local dropped marker is available.
	recovered, err := NewCopySegmentMeta(t.Context(), meta.Catalog, meta, nil, &restoreSnapshotSource{}, nil)
	require.NoError(t, err)
	return &Checker{Ctx: t.Context(), Meta: meta, CopyMeta: recovered, CollectionRestorer: &restoreCollectionTarget{}}, recovered.GetJob(t.Context(), job.GetJobId())
}

func TestRestoreTargetDeletionConvergesAfterRecovery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  datapb.CopySegmentJobState
		copied bool
	}{
		{"pending", datapb.CopySegmentJobState_CopySegmentJobPending, false},
		{"copying", datapb.CopySegmentJobState_CopySegmentJobExecuting, false},
		{"copied awaiting publication", datapb.CopySegmentJobState_CopySegmentJobExecuting, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey("authoritative target deletion terminates every active stage", t, func() {
				checker, job := newRestoreTargetDropChecker(t, tc.state, tc.copied)
				lookup := mockey.Mock((*restoreCollectionTarget).IsCollectionDropped).Return(true, nil).Build()
				unpin := mockey.Mock((*restoreSnapshotSource).UnpinSnapshot).To(func(_ *restoreSnapshotSource, _ context.Context, id int64) (int64, string, int, error) {
					require.EqualValues(t, 42, id)
					return 0, "", 0, nil
				}).Build()
				if tc.state == datapb.CopySegmentJobState_CopySegmentJobPending {
					checker.checkPendingJob(job)
				} else {
					checker.checkCopyingJob(job)
				}
				require.Equal(t, 1, lookup.Times())
				require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, checker.CopyMeta.GetJob(t.Context(), job.GetJobId()).GetState())
				require.Contains(t, checker.CopyMeta.GetJob(t.Context(), job.GetJobId()).GetReason(), "target collection dropped")
				require.Equal(t, 1, unpin.Times())
				require.False(t, checker.checkRestoreTarget(job), "a stale checker cannot double-unpin")
				require.Equal(t, 1, unpin.Times())
				recovered, err := NewCopySegmentMeta(t.Context(), checker.Meta.Catalog, checker.Meta, nil, &restoreSnapshotSource{}, nil)
				require.NoError(t, err)
				require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, recovered.GetJob(t.Context(), job.GetJobId()).GetState())
			})
		})
	}
}

func TestRestoreTargetCheckPreservesLiveAndUnknownTargets(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"live including restoring", nil},
		{"lookup unavailable", merr.ErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey("missing publication is not deletion proof", t, func() {
				checker, job := newRestoreTargetDropChecker(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, true)
				mockey.Mock((*restoreCollectionTarget).IsCollectionDropped).Return(false, tc.err).Build()
				unpin := mockey.Mock((*restoreSnapshotSource).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
				require.Equal(t, tc.err == nil, checker.checkRestoreTarget(job))
				require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, checker.CopyMeta.GetJob(t.Context(), job.GetJobId()).GetState())
				require.Zero(t, unpin.Times())
			})
		})
	}
}

func TestRestoreTargetDeletionRetriesDurableFailure(t *testing.T) {
	mockey.PatchConvey("persist terminal state before releasing its snapshot", t, func() {
		checker, job := newRestoreTargetDropChecker(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, true)
		mockey.Mock((*restoreCollectionTarget).IsCollectionDropped).Return(true, nil).Build()
		unpin := mockey.Mock((*restoreSnapshotSource).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
		save := mockey.Mock((*kvdatacoord.Catalog).SaveCopySegmentJob).Return(merr.ErrServiceUnavailable).Build()
		require.False(t, checker.checkRestoreTarget(job))
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, checker.CopyMeta.GetJob(t.Context(), job.GetJobId()).GetState())
		require.Zero(t, unpin.Times())
		save.UnPatch()
		require.False(t, checker.checkRestoreTarget(job))
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, checker.CopyMeta.GetJob(t.Context(), job.GetJobId()).GetState())
		require.Equal(t, 1, unpin.Times())
		require.NoError(t, checker.CopyMeta.UpdateJobStateAndReleaseRef(t.Context(), job.GetJobId(), UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted)))
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, checker.CopyMeta.GetJob(t.Context(), job.GetJobId()).GetState())
	})
}

func TestRestoreTargetDeletionRacesPublication(t *testing.T) {
	for _, missingView := range []bool{true, false} {
		t.Run(map[bool]string{true: "DataView absent", false: "RootCoord completion rejected"}[missingView], func(t *testing.T) {
			mockey.PatchConvey("recheck deletion when publication cannot finish", t, func() {
				checker, job := newRestoreTargetDropChecker(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, true)
				dropped := false
				mockey.Mock((*restoreCollectionTarget).IsCollectionDropped).To(func(_ *restoreCollectionTarget, _ context.Context, _ int64) (bool, error) { return dropped, nil }).Build()
				unpin := mockey.Mock((*restoreSnapshotSource).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
				require.True(t, checker.checkRestoreTarget(job))
				if missingView {
					manager, err := dataview.RecoverManager(t.Context(), checker.Meta.Catalog.(*kvdatacoord.Catalog),
						func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
					require.NoError(t, err)
					_, err = manager.OnDropCollection(t.Context(), job.GetCollectionId())
					require.NoError(t, err)
					checker.Meta.DataViewManager = manager
				} else {
					mockey.Mock((*restoreCollectionTarget).CompleteRestoreCollection).Return(merr.ErrCollectionNotFound).Build()
				}
				dropped = true
				checker.publishRestoredCollection(job, 10)
				require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, checker.CopyMeta.GetJob(t.Context(), job.GetJobId()).GetState())
				require.Equal(t, 1, unpin.Times())
			})
		})
	}
}

func TestRestoreCompletedJobSurvivesTargetDeletion(t *testing.T) {
	mockey.PatchConvey("a later target drop does not change an already completed job", t, func() {
		checker, job := newRestoreTargetDropChecker(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, true)
		unpin := mockey.Mock((*restoreSnapshotSource).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
		require.NoError(t, checker.CopyMeta.UpdateJobStateAndReleaseRef(t.Context(), job.GetJobId(), UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted)))
		require.NoError(t, checker.CopyMeta.FailJobForDroppedCollection(t.Context(), job.GetJobId()))
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobCompleted, checker.CopyMeta.GetJob(t.Context(), job.GetJobId()).GetState())
		require.Equal(t, 1, unpin.Times())
	})
}
