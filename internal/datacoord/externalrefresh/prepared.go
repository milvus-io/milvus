package externalrefresh

import (
	"context"
	"maps"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// PrepareRefresh runs the coordinator's validation, Explore and ownership
// planning under the collection guard, before anything is submitted to WAL.
func (m *Manager) PrepareRefresh(ctx context.Context, jobID, collectionID int64, name string, schema *schemapb.CollectionSchema) ([]*datapb.ExternalCollectionRefreshTask, error) {
	job := &datapb.ExternalCollectionRefreshJob{JobId: jobID, CollectionId: collectionID, CollectionName: name, ExternalSource: schema.GetExternalSource(), ExternalSpec: schema.GetExternalSpec()}
	tasks, err := m.planTasksForJob(ctx, job)
	if err != nil {
		m.cleanupExploreTempForJob(jobID)
	}
	return tasks, err
}

// refreshCallbackOwnsJob is backed by the durable broadcaster task and survives
// coordinator restart. Legacy WAL messages without a schema retain their path.
func refreshCallbackOwnsJob(job *datapb.ExternalCollectionRefreshJob) bool {
	target, err := broadcast.GetPendingSchemaTarget(job.GetCollectionId())
	return err == nil && target != nil && target.RefreshJobID == job.GetJobId()
}

// RunPreparedRefresh is called only by the original DDL callback. It persists
// the plan before dispatch, waits for worker results, then publishes exactly once.
func (m *Manager) RunPreparedRefresh(ctx context.Context, result message.BroadcastResultRefreshExternalCollectionMessageV2, publish func(context.Context, message.BroadcastResultRefreshExternalCollectionMessageV2) error) error {
	header, body := result.Message.Header(), result.Message.MustBody()
	starts := make(map[string]uint64)
	for channel, fence := range result.Results {
		if !funcutil.IsControlChannel(channel) {
			starts[channel] = fence.TimeTick
		}
	}
	job := m.refreshMeta.GetJob(header.GetJobId())
	if job == nil {
		job = &datapb.ExternalCollectionRefreshJob{JobId: header.GetJobId(), CollectionId: header.GetCollectionId(), CollectionName: header.GetCollectionName(), ExternalSource: body.GetSchema().GetExternalSource(), ExternalSpec: body.GetSchema().GetExternalSpec(), State: indexpb.JobState_JobStateInit, StartTime: time.Now().UnixMilli(), TransformStartAfterTimeticks: maps.Clone(starts)}
		if err := m.refreshMeta.AddJob(job); err != nil {
			return err
		}
	}
	if job.GetState() != indexpb.JobState_JobStateFinished && job.GetState() != indexpb.JobState_JobStateFailed && len(job.GetTaskIds()) == 0 {
		if err := m.refreshMeta.AddTasksToJob(job.GetJobId(), body.GetTasks()); err != nil {
			return err
		}
		for _, task := range body.GetTasks() {
			// Scheduler tasks are mutable; never let them mutate the WAL plan.
			m.scheduler.Enqueue(m.wrapTask(proto.Clone(task).(*datapb.ExternalCollectionRefreshTask)))
		}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		job = m.refreshMeta.GetJob(header.GetJobId())
		switch job.GetState() {
		case indexpb.JobState_JobStateFailed:
			// TODO(#55380): Refresh can fail after StreamingNodes installed its schema.
			// There is no rollback/reconciliation. The cluster can be inconsistent and
			// requires manual intervention before subsequent collection operations.
			mlog.Error(ctx, "external refresh failed after WAL submission; cluster may be inconsistent, manual intervention required", mlog.FieldJobID(job.GetJobId()), mlog.FieldCollectionID(job.GetCollectionId()), mlog.String("reason", job.GetFailReason()))
			return nil
		case indexpb.JobState_JobStateFinished:
			m.handleJobFinished(ctx, job)
			return nil
		}
		state, _, err := m.refreshMeta.AggregateJobStateFromTasks(job.GetJobId())
		if err != nil {
			return err
		}
		if state == indexpb.JobState_JobStateFinished {
			if _, err := m.refreshMeta.BeginPublication(job.GetJobId(), func(current *datapb.ExternalCollectionRefreshJob) error {
				return m.applyFinishedJobSegments(ctx, current)
			}); err != nil {
				return err
			}
			if m.refreshMeta.GetJob(job.GetJobId()).GetState() == indexpb.JobState_JobStateFailed {
				continue
			}
			if err := m.stampRefreshSchema(ctx, job, body.GetSchema().GetVersion()); err != nil {
				return err
			}
			if err := publish(ctx, result); err != nil {
				return err
			}
			if m.checker.indexWaitEnabled() {
				if _, err := m.refreshMeta.BeginIndexWait(job.GetJobId(), nil); err != nil {
					return err
				}
			}
			job = m.refreshMeta.GetJob(job.GetJobId())
			if !m.checker.indexWaitEnabled() || m.checker.indexWaitDone(job) {
				m.checker.finishAppliedJob(job)
				// The final catalog write can fail; keep the callback and its
				// guard alive until the terminal state is actually durable.
				if m.refreshMeta.GetJob(job.GetJobId()).GetState() == indexpb.JobState_JobStateFinished {
					m.handleJobFinished(ctx, job)
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// External refresh changes reader configuration, not field encoding. Retained
// segment bases are valid under the target after all task results have applied.
func (m *Manager) stampRefreshSchema(ctx context.Context, job *datapb.ExternalCollectionRefreshJob, version int32) error {
	segments := m.mt.SelectSegments(ctx, meta.CollectionFilter(job.GetCollectionId()), meta.SegmentFilterFunc(meta.IsSegmentHealthy))
	operators := make([]meta.UpdateOperator, 0, len(segments))
	for _, segment := range segments {
		if segment.GetSchemaVersion() >= version {
			continue
		}
		id := segment.GetID()
		operators = append(operators, func(pack *meta.UpdateSegmentPack) bool {
			seg := pack.Get(id)
			if seg == nil {
				return false
			}
			seg.SchemaVersion = version
			return true
		})
	}
	if len(operators) == 0 {
		return nil
	}
	return m.mt.UpdateSegmentsInfo(ctx, operators...)
}
