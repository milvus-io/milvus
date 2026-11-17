package externalrefresh

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func preparedRefreshResult() message.BroadcastResultRefreshExternalCollectionMessageV2 {
	msg := message.NewRefreshExternalCollectionMessageBuilderV2().WithHeader(&message.RefreshExternalCollectionMessageHeader{CollectionId: 100, CollectionName: "external", JobId: 7}).WithBody(&message.RefreshExternalCollectionMessageBody{Schema: &schemapb.CollectionSchema{Version: 2, ExternalSource: "s3://new", ExternalSpec: `{"format":"parquet"}`}, Tasks: []*datapb.ExternalCollectionRefreshTask{{TaskId: 8, JobId: 7, CollectionId: 100, OwnershipPlanVersion: externalRefreshOwnershipPlanVersion, State: indexpb.JobState_JobStateInit}}}).WithBroadcast([]string{"v1"}).MustBuildBroadcast()
	return message.BroadcastResultRefreshExternalCollectionMessageV2{Message: message.MustAsBroadcastRefreshExternalCollectionMessageV2(msg), Results: map[string]*message.AppendResult{"v1": {TimeTick: 50}}}
}

func TestPreparedRefreshReplaysWithoutExploreOrReapply(t *testing.T) {
	paramtable.Init()
	m := newManagerWithChunkManager(t, &recordingChunkManager{})
	applied := 0
	apply := mockey.Mock((*Manager).applyFinishedJobSegments).To(func(_ *Manager, _ context.Context, _ *datapb.ExternalCollectionRefreshJob) error {
		applied++
		return nil
	}).Build()
	defer apply.UnPatch()
	stamp := mockey.Mock((*Manager).stampRefreshSchema).Return(nil).Build()
	defer stamp.UnPatch()
	state := mockey.Mock((*ExternalCollectionRefreshMeta).AggregateJobStateFromTasks).Return(indexpb.JobState_JobStateFinished, int64(100), nil).Build()
	defer state.UnPatch()
	result := preparedRefreshResult()
	publishCount := 0
	publish := func(context.Context, message.BroadcastResultRefreshExternalCollectionMessageV2) error {
		publishCount++
		if publishCount == 1 {
			return merr.ErrServiceNotReady
		}
		return nil
	}
	require.ErrorIs(t, m.RunPreparedRefresh(t.Context(), result, publish), merr.ErrServiceNotReady)
	job := m.refreshMeta.GetJob(7)
	require.True(t, job.GetSegmentsApplied())
	require.Equal(t, "s3://new", job.GetExternalSource())
	require.EqualValues(t, 50, job.GetTransformStartAfterTimeticks()["v1"])
	require.NoError(t, m.RunPreparedRefresh(t.Context(), result, publish))
	require.Equal(t, 1, applied)
	require.Equal(t, 2, publishCount)
	require.Equal(t, indexpb.JobState_JobStateFinished, m.refreshMeta.GetJob(7).GetState())
	require.NoError(t, m.RunPreparedRefresh(t.Context(), result, publish))
	require.Equal(t, 2, publishCount)
}

func TestPreparedRefreshCancellationAndFailure(t *testing.T) {
	paramtable.Init()
	m := newManagerWithChunkManager(t, &recordingChunkManager{})
	result := preparedRefreshResult()
	publish := func(context.Context, message.BroadcastResultRefreshExternalCollectionMessageV2) error {
		t.Fatal("failed or unfinished tasks must not publish")
		return nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, m.RunPreparedRefresh(ctx, result, publish), context.Canceled)
	_, err := m.refreshMeta.UpdateJobState(7, indexpb.JobState_JobStateFailed, "corrupted parquet")
	require.NoError(t, err)
	require.NoError(t, m.RunPreparedRefresh(t.Context(), result, publish))
	require.Equal(t, indexpb.JobState_JobStateFailed, m.refreshMeta.GetJob(7).GetState())
}

func TestPrepareRefreshRunsBeforeJobCreation(t *testing.T) {
	m := newManagerWithChunkManager(t, &recordingChunkManager{})
	target := &schemapb.CollectionSchema{ExternalSource: "s3://new", ExternalSpec: `{"format":"parquet"}`}
	plan := mockey.Mock((*Manager).planTasksForJob).To(func(_ *Manager, _ context.Context, job *datapb.ExternalCollectionRefreshJob) ([]*datapb.ExternalCollectionRefreshTask, error) {
		require.Equal(t, target.ExternalSource, job.GetExternalSource())
		return []*datapb.ExternalCollectionRefreshTask{{TaskId: 8}}, nil
	}).Build()
	tasks, err := m.PrepareRefresh(t.Context(), 7, 100, "external", target)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Nil(t, m.refreshMeta.GetJob(7))
	plan.UnPatch()
	fail := mockey.Mock((*Manager).planTasksForJob).Return(nil, merr.ErrDataIntegrity).Build()
	defer fail.UnPatch()
	_, err = m.PrepareRefresh(t.Context(), 9, 100, "external", target)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Nil(t, m.refreshMeta.GetJob(9))
	prefixes, _ := m.chunkManager.(*recordingChunkManager).snapshot()
	require.Equal(t, []string{"__explore_temp__/coord_9/"}, prefixes)
}

func TestPreparedRefreshStampRetainedSegments(t *testing.T) {
	m := newManagerWithChunkManager(t, &recordingChunkManager{})
	segments := []*meta.SegmentInfo{{SegmentInfo: &datapb.SegmentInfo{ID: 1, SchemaVersion: 1}}, {SegmentInfo: &datapb.SegmentInfo{ID: 2, SchemaVersion: 2}}}
	selectSegments := mockey.Mock((*meta.Meta).SelectSegments).Return(segments).Build()
	defer selectSegments.UnPatch()
	writes := 0
	update := mockey.Mock((*meta.Meta).UpdateSegmentsInfo).To(func(_ context.Context, ops ...meta.UpdateOperator) error {
		writes++
		pack := &meta.UpdateSegmentPack{Segments: map[int64]*meta.SegmentInfo{1: segments[0], 2: segments[1]}}
		for _, op := range ops {
			require.True(t, op(pack))
		}
		return nil
	}).Build()
	defer update.UnPatch()
	job := &datapb.ExternalCollectionRefreshJob{CollectionId: 100}
	require.NoError(t, m.stampRefreshSchema(t.Context(), job, 2))
	require.EqualValues(t, 2, segments[0].SchemaVersion)
	require.NoError(t, m.stampRefreshSchema(t.Context(), job, 2))
	require.Equal(t, 1, writes, "replay must preserve already stamped segments")
}

func TestPreparedRefreshPersistenceErrors(t *testing.T) {
	paramtable.Init()
	for _, failure := range []string{"job", "tasks", "aggregate", "apply", "stamp", "finish"} {
		t.Run(failure, func(t *testing.T) {
			m := newManagerWithChunkManager(t, &recordingChunkManager{})
			var patches []*mockey.Mocker
			defer func() {
				for _, p := range patches {
					p.UnPatch()
				}
			}()
			switch failure {
			case "job":
				patches = append(patches, mockey.Mock((*ExternalCollectionRefreshMeta).AddJob).Return(merr.ErrServiceNotReady).Build())
			case "tasks":
				patches = append(patches, mockey.Mock((*ExternalCollectionRefreshMeta).AddTasksToJob).Return(merr.ErrServiceNotReady).Build())
			}
			var aggregateErr error
			if failure == "aggregate" {
				aggregateErr = merr.ErrServiceNotReady
			}
			patches = append(patches, mockey.Mock((*ExternalCollectionRefreshMeta).AggregateJobStateFromTasks).Return(indexpb.JobState_JobStateFinished, int64(100), aggregateErr).Build())
			var applyErr error
			if failure == "apply" {
				applyErr = merr.ErrServiceNotReady
			}
			patches = append(patches, mockey.Mock((*ExternalCollectionRefreshMeta).BeginPublication).Return(true, applyErr).Build())
			var stampErr error
			if failure == "stamp" {
				stampErr = merr.ErrServiceNotReady
			}
			patches = append(patches, mockey.Mock((*Manager).stampRefreshSchema).Return(stampErr).Build())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if failure == "finish" {
				patches = append(patches, mockey.Mock((*externalCollectionRefreshChecker).finishAppliedJob).To(func(*datapb.ExternalCollectionRefreshJob) { cancel() }).Build())
			}
			err := m.RunPreparedRefresh(ctx, preparedRefreshResult(), func(context.Context, message.BroadcastResultRefreshExternalCollectionMessageV2) error { return nil })
			if failure == "finish" {
				require.ErrorIs(t, err, context.Canceled)
				require.NotEqual(t, indexpb.JobState_JobStateFinished, m.refreshMeta.GetJob(7).GetState())
			} else {
				require.ErrorIs(t, err, merr.ErrServiceNotReady)
			}
		})
	}
}

func TestPreparedRefreshWALPlanIsImmutable(t *testing.T) {
	m := newManagerWithChunkManager(t, &recordingChunkManager{})
	result := preparedRefreshResult()
	enqueue := mockey.Mock((*stubScheduler).Enqueue).To(func(task globalTask.Task) {
		task.(*refreshExternalCollectionTask).SetState(indexpb.JobState_JobStateInProgress, "")
	}).Build()
	defer enqueue.UnPatch()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, m.RunPreparedRefresh(ctx, result, nil), context.Canceled)
	require.Equal(t, indexpb.JobState_JobStateInit, result.Message.MustBody().GetTasks()[0].GetState())
}

func TestPreparedRefreshWaitsForIndexes(t *testing.T) {
	paramtable.Init()
	m := newManagerWithChunkManager(t, &recordingChunkManager{})
	patches := []*mockey.Mocker{
		mockey.Mock((*Manager).applyFinishedJobSegments).Return(nil).Build(),
		mockey.Mock((*Manager).stampRefreshSchema).Return(nil).Build(),
		mockey.Mock((*ExternalCollectionRefreshMeta).AggregateJobStateFromTasks).Return(indexpb.JobState_JobStateFinished, int64(100), nil).Build(),
		mockey.Mock((*externalCollectionRefreshChecker).indexWaitEnabled).Return(true).Build(),
	}
	defer func() {
		for _, p := range patches {
			p.UnPatch()
		}
	}()
	checks := 0
	done := mockey.Mock((*externalCollectionRefreshChecker).indexWaitDone).To(func(job *datapb.ExternalCollectionRefreshJob) bool {
		require.NotZero(t, job.GetIndexWaitStartedTime())
		checks++
		return checks > 1
	}).Build()
	defer done.UnPatch()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.RunPreparedRefresh(ctx, preparedRefreshResult(), func(context.Context, message.BroadcastResultRefreshExternalCollectionMessageV2) error { return nil }))
	require.Equal(t, 2, checks)
	require.Equal(t, indexpb.JobState_JobStateFinished, m.refreshMeta.GetJob(7).GetState())
}

func TestPreparedRefreshCheckerDefersCompletionAndGC(t *testing.T) {
	paramtable.Init()
	m := newManagerWithChunkManager(t, &recordingChunkManager{})
	result := preparedRefreshResult()
	job := &datapb.ExternalCollectionRefreshJob{JobId: 7, CollectionId: 100, State: indexpb.JobState_JobStateInit}
	require.NoError(t, m.refreshMeta.AddJob(job))
	target := mockey.Mock(broadcast.GetPendingSchemaTarget).Return(&broadcaster.SchemaTarget{RefreshJobID: 7}, nil).Build()
	defer target.UnPatch()
	m.ensureTasksForInitJob(7)
	require.Empty(t, m.refreshMeta.GetJob(7).GetTaskIds(), "checker must not rediscover a prepared plan")
	require.NoError(t, m.refreshMeta.AddTasksToJob(7, result.Message.MustBody().GetTasks()))
	require.NoError(t, m.refreshMeta.UpdateTaskState(8, indexpb.JobState_JobStateFinished, ""))
	job = m.refreshMeta.GetJob(7)
	m.checker.AggregateJobState(job)
	require.NotEqual(t, indexpb.JobState_JobStateFinished, m.refreshMeta.GetJob(7).GetState())
	_, err := m.refreshMeta.UpdateJobState(7, indexpb.JobState_JobStateFinished, "")
	require.NoError(t, err)
	m.checker.onJobFinished = func(context.Context, *datapb.ExternalCollectionRefreshJob) {
		t.Fatal("checker must not own publication")
	}
	m.checker.ensureJobFinishedNotified(m.refreshMeta.GetJob(7))
	m.checker.checkGC(m.refreshMeta.GetJob(7))
	require.NotNil(t, m.refreshMeta.GetJob(7), "pending callback must retain its job anchor")
}

func TestPreparedRefreshSegmentRemovedBeforeStamp(t *testing.T) {
	m := newManagerWithChunkManager(t, &recordingChunkManager{})
	selected := mockey.Mock((*meta.Meta).SelectSegments).Return([]*meta.SegmentInfo{{SegmentInfo: &datapb.SegmentInfo{ID: 1, SchemaVersion: 1}}}).Build()
	defer selected.UnPatch()
	missing := mockey.Mock((*meta.UpdateSegmentPack).Get).Return(nil).Build()
	defer missing.UnPatch()
	update := mockey.Mock((*meta.Meta).UpdateSegmentsInfo).To(func(_ context.Context, ops ...meta.UpdateOperator) error {
		require.Len(t, ops, 1)
		require.False(t, ops[0](&meta.UpdateSegmentPack{}), "segment removed after selection must not be recreated")
		return nil
	}).Build()
	defer update.UnPatch()
	require.NoError(t, m.stampRefreshSchema(t.Context(), &datapb.ExternalCollectionRefreshJob{CollectionId: 100}, 2))
}
