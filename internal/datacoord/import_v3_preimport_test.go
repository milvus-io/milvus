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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

// newPreimportV3TestSchema is a minimal schema with an explicit primary key, the
// precondition for importV3NeedsIDRanges to report true.
func newPreimportV3TestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
}

// applyJobActions runs UpdateJobAction values against a fresh job and returns it
// so a test can assert the persisted state/reason without touching a live job.
func applyJobActions(actions ...UpdateJobAction) *importJob {
	applied := &importJob{ImportJob: &datapb.ImportJob{}}
	for _, action := range actions {
		action(applied)
	}
	return applied
}

// TestImportV3NeedsIDRanges pins the V3 range-need predicate: an explicit- or
// auto-PK ordinary job needs ranges, while a job without a resolvable PK or one
// that carries its own PK/RowID (backup, L0) does not.
func TestImportV3NeedsIDRanges(t *testing.T) {
	pkSchema := newPreimportV3TestSchema()
	require.True(t, importV3NeedsIDRanges(&importJob{ImportJob: &datapb.ImportJob{Schema: pkSchema}}))

	require.False(t, importV3NeedsIDRanges(&importJob{ImportJob: &datapb.ImportJob{
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 1, DataType: schemapb.DataType_Int64}}},
	}}), "a job without a resolvable primary key cannot be ranged")

	require.False(t, importV3NeedsIDRanges(&importJob{ImportJob: &datapb.ImportJob{
		Schema: pkSchema, Options: []*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}},
	}}), "a backup job carries its own PK/RowID")
	require.False(t, importV3NeedsIDRanges(&importJob{ImportJob: &datapb.ImportJob{
		Schema: pkSchema, Options: []*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}},
	}}), "an L0 import has no rows to range")
}

// TestFinishPreImportV3DivergenceFailsJob pins the cross-cluster divergence gate:
// when a ranged file's local preimport count differs from the reserved range
// width, the job fails with both numbers before any segment is written.
func TestFinishPreImportV3DivergenceFailsJob(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_AssigningIDRange,
		Schema: newPreimportV3TestSchema(),
		Files:  []*internalpb.ImportFile{{Id: 7, IdRange: &commonpb.IDRange{Begin: 0, End: 5}}},
	}, tr: timerecord.NewTimeRecorder("import job")}
	require.True(t, importV3NeedsIDRanges(job))

	task := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Completed,
		FileStats: []*datapb.ImportV3FileStats{{FileId: 7, TotalRows: 9}},
	}, importMeta)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1), mock.Anything).Return([]ImportTask{task}).Once()
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ int64, actions ...UpdateJobAction) {
			applied := applyJobActions(actions...)
			require.Equal(t, internalpb.ImportJobState_Failed, applied.GetState())
			require.Contains(t, applied.GetReason(), "cross-cluster file divergence")
			require.Contains(t, applied.GetReason(), "local row count 9")
			require.Contains(t, applied.GetReason(), "reserved ID range size 5")
		}).Return(nil).Once()

	require.NoError(t, finishPreImportV3(newImportV3JobContext(checker, job)))
}

// TestFinishPreImportV3BackupZeroRowsFallsThroughToReshard pins that a backup
// job legitimately reporting zero rows must NOT take the zero-row exit: its
// rows are sized on the DataNode, so it falls through to Reshard.
func TestFinishPreImportV3BackupZeroRowsFallsThroughToReshard(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_PreImporting,
		Schema:  newPreimportV3TestSchema(),
		Options: []*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}},
		Files:   []*internalpb.ImportFile{{Id: 1, Paths: []string{"insert", "delta"}}},
	}, tr: timerecord.NewTimeRecorder("import job")}
	require.False(t, importV3NeedsIDRanges(job), "a backup job needs no ranges")

	preimport := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Completed,
		FileStats: []*datapb.ImportV3FileStats{{FileId: 1, TotalRows: 0, FileSize: 4096}},
	}, importMeta)
	// createReshardTasks first re-queries the preimport set for packing sizes,
	// then the reshard set for covered sources. The file is already covered, so
	// BFD is skipped and only the state advances.
	covered := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 20, CollectionId: 2, State: datapb.ImportTaskStateV2_Completed, FileIds: []int64{1},
	}, importMeta, nil, nil)
	preimportProbe := newPreImportTaskV3(&datapb.PreImportTaskV3{}, importMeta)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1), mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, filters ...ImportTaskFilter) []ImportTask {
			if len(filters) == 1 && filters[0](preimportProbe) {
				return []ImportTask{preimport}
			}
			return []ImportTask{covered}
		}).Twice()
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything).
		Run(func(_ context.Context, _ int64, actions ...UpdateJobAction) {
			require.Equal(t, internalpb.ImportJobState_Resharding, applyJobActions(actions...).GetState())
		}).Return(nil).Once()

	require.NoError(t, finishPreImportV3(newImportV3JobContext(checker, job)))
}

// TestFinishPreImportV3ZeroRowsOrdinaryTakesEmptyExit pins that a zero-row
// ordinary (ranged) job takes the shared empty exit instead of creating a
// Reshard task. auto_commit=false parks it in Uncommitted.
func TestFinishPreImportV3ZeroRowsOrdinaryTakesEmptyExit(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_PreImporting,
		Schema: newPreimportV3TestSchema(),
		Files:  []*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}},
	}, tr: timerecord.NewTimeRecorder("import job")}
	require.True(t, importV3NeedsIDRanges(job))

	preimport := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Completed,
		FileStats: []*datapb.ImportV3FileStats{{FileId: 1, TotalRows: 0}},
	}, importMeta)
	// The strict mock has no reshard query or AddTask expectation: reaching the
	// Reshard stage would fail the test.
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1), mock.Anything).Return([]ImportTask{preimport}).Once()
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything).
		Run(func(_ context.Context, _ int64, actions ...UpdateJobAction) {
			require.Equal(t, internalpb.ImportJobState_Uncommitted, applyJobActions(actions...).GetState())
		}).Return(nil).Once()

	require.NoError(t, finishPreImportV3(newImportV3JobContext(checker, job)))
}

// newAssigningIDRangeJobContext builds a checker/job pair for the AssigningIDRange
// tests: the job carries the given files and the completed PreImportV3 task the
// given per-file row counts.
func newAssigningIDRangeJobContext(t *testing.T, importMeta *MockImportMeta, files []*internalpb.ImportFile, fileRows map[int64]int64) (*importCheckerV3, *importJob) {
	t.Helper()
	stats := make([]*datapb.ImportV3FileStats, 0, len(fileRows))
	for id, rows := range fileRows {
		stats = append(stats, &datapb.ImportV3FileStats{FileId: id, TotalRows: rows})
	}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_AssigningIDRange,
		Schema: newPreimportV3TestSchema(),
		Files:  files,
	}, tr: timerecord.NewTimeRecorder("import job")}
	checker := &importCheckerV3{ctx: context.Background(), importMeta: importMeta}
	task := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Completed,
		FileStats: stats,
	}, importMeta)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1), mock.Anything).Return([]ImportTask{task}).Maybe()
	return checker, job
}

// TestEnsurePreImportV3IDRangesPrimaryBroadcasts pins the primary path: the
// primary reads the exact per-file counts and hands them to the
// assignImportIDRange hook (which allocates and broadcasts the authoritative
// ranges that every cluster then applies).
func TestEnsurePreImportV3IDRangesPrimaryBroadcasts(t *testing.T) {
	importMeta := NewMockImportMeta(t)
	checker, job := newAssigningIDRangeJobContext(t, importMeta,
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}, {Id: 2, Paths: []string{"b.json"}}},
		map[int64]int64{1: 100, 2: 0})
	var assigned []int64
	checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}
	checker.hooks.assignImportIDRange = func(_ context.Context, j ImportJob, fileRows []int64) error {
		require.Equal(t, int64(1), j.GetJobID())
		assigned = append([]int64(nil), fileRows...)
		return nil
	}

	ensurePreImportV3IDRanges(newImportV3JobContext(checker, job))
	require.Equal(t, []int64{100, 0}, assigned, "counts must align with job.GetFiles() order")
}

// TestEnsurePreImportV3IDRangesSecondaryWaits pins that a secondary never
// allocates locally: it waits for the replicated UpdateImport, so neither the
// role-agnostic task read nor the assign hook runs.
func TestEnsurePreImportV3IDRangesSecondaryWaits(t *testing.T) {
	importMeta := NewMockImportMeta(t)
	checker, job := newAssigningIDRangeJobContext(t, importMeta,
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}},
		map[int64]int64{1: 100})
	assignCalls := 0
	checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RoleSecondary, true, nil
	}
	checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}

	ensurePreImportV3IDRanges(newImportV3JobContext(checker, job))
	require.Equal(t, 0, assignCalls, "a secondary must never allocate/broadcast ranges")
}

// TestEnsurePreImportV3IDRangesRoleFlipWaits pins that a broadcaster ErrNotPrimary
// (this cluster was demoted mid-broadcast) is treated as a wait, not a failure.
func TestEnsurePreImportV3IDRangesRoleFlipWaits(t *testing.T) {
	importMeta := NewMockImportMeta(t)
	checker, job := newAssigningIDRangeJobContext(t, importMeta,
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}},
		map[int64]int64{1: 100})
	checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}
	checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		return broadcaster.ErrNotPrimary
	}
	// No UpdateJob expectation: the flip must not fail the job.

	ensurePreImportV3IDRanges(newImportV3JobContext(checker, job))
}

// TestEnsurePreImportV3IDRangesInputErrorFailsJob pins that an input-class
// assign error is permanent (the request content itself cannot be ranged), so
// the job fails immediately with the precise reason.
func TestEnsurePreImportV3IDRangesInputErrorFailsJob(t *testing.T) {
	importMeta := NewMockImportMeta(t)
	checker, job := newAssigningIDRangeJobContext(t, importMeta,
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}},
		map[int64]int64{1: 100})
	checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}
	checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		return merr.WrapErrParameterInvalidMsg("import file 0 exceeds one allocation batch; split the file")
	}
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ int64, actions ...UpdateJobAction) {
			applied := applyJobActions(actions...)
			require.Equal(t, internalpb.ImportJobState_Failed, applied.GetState())
			require.Contains(t, applied.GetReason(), "split the file")
		}).Return(nil).Once()

	ensurePreImportV3IDRanges(newImportV3JobContext(checker, job))
}

// TestAssigningIDRangeHandlerDispatch pins the handler's two branches: with the
// ranges still unset it drives the allocation/broadcast; once every file carries
// a range it runs the shared preimport tail (here surfaced as the divergence
// gate failing a mismatched job).
func TestAssigningIDRangeHandlerDispatch(t *testing.T) {
	t.Run("ranges unset allocates", func(t *testing.T) {
		importMeta := NewMockImportMeta(t)
		checker, job := newAssigningIDRangeJobContext(t, importMeta,
			[]*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}},
			map[int64]int64{1: 100})
		checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
			return replicateutil.RolePrimary, true, nil
		}
		checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
			return merr.WrapErrParameterInvalidMsg("cannot range")
		}
		importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything, mock.Anything).
			Run(func(_ context.Context, _ int64, actions ...UpdateJobAction) {
				require.Equal(t, internalpb.ImportJobState_Failed, applyJobActions(actions...).GetState())
			}).Return(nil).Once()

		require.NoError(t, assigningIDRangeHandler{}.Handle(newImportV3JobContext(checker, job)))
	})

	t.Run("ranges set runs the tail", func(t *testing.T) {
		importMeta := NewMockImportMeta(t)
		checker, job := newAssigningIDRangeJobContext(t, importMeta,
			[]*internalpb.ImportFile{{Id: 1, IdRange: &commonpb.IDRange{Begin: 0, End: 5}}},
			map[int64]int64{1: 9})
		importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything, mock.Anything).
			Run(func(_ context.Context, _ int64, actions ...UpdateJobAction) {
				applied := applyJobActions(actions...)
				require.Equal(t, internalpb.ImportJobState_Failed, applied.GetState())
				require.Contains(t, applied.GetReason(), "cross-cluster file divergence")
			}).Return(nil).Once()

		require.NoError(t, assigningIDRangeHandler{}.Handle(newImportV3JobContext(checker, job)))
	})
}
