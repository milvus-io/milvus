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
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// errExternalRefreshTaskPlanNotPublishable is an in-process control-flow
// signal for task plans rejected before any catalog write is attempted.
// createTasksForJob uses it to distinguish a definitive rejection from an
// ambiguous catalog error, where deleting the Explore manifest could break a
// plan that was committed despite the client observing an error.
var errExternalRefreshTaskPlanNotPublishable = errors.New("external refresh task plan is not publishable")

// externalCollectionRefreshMeta manages both Job and Task metadata for external collection refresh.
// Job represents user-initiated refresh operations (API level), while Task represents
// execution units dispatched to workers (scheduler level).
//
// Index structures:
// - jobs: jobID -> Job (for API queries by jobID)
// - collectionJobs: collectionID -> {jobID -> Job} (for queries by collection)
// - tasks: taskID -> Task (for scheduler)
// - jobTasks: jobID -> {taskID -> Task} (for job-task association)
type externalCollectionRefreshMeta struct {
	ctx         context.Context
	catalog     metastore.DataCoordCatalog
	resultStore *externalCollectionRefreshResultStore

	// Job lock (by collectionID)
	jobLock *lock.KeyLock[UniqueID]
	// Task lock (by jobID)
	taskLock *lock.KeyLock[int64]

	// ============ Job Indexes ============
	// jobID -> Job
	jobs *typeutil.ConcurrentMap[int64, *datapb.ExternalCollectionRefreshJob]
	// collectionID -> (jobID -> Job)
	collectionJobs *typeutil.ConcurrentMap[UniqueID, *typeutil.ConcurrentMap[int64, *datapb.ExternalCollectionRefreshJob]]

	// ============ Task Indexes ============
	// taskID -> Task
	tasks *typeutil.ConcurrentMap[int64, *datapb.ExternalCollectionRefreshTask]
	// jobID -> (taskID -> Task)
	jobTasks *typeutil.ConcurrentMap[int64, *typeutil.ConcurrentMap[int64, *datapb.ExternalCollectionRefreshTask]]
}

type externalCollectionRefreshMetaOption func(*externalCollectionRefreshMeta)

func withExternalCollectionRefreshResultStore(
	resultStore *externalCollectionRefreshResultStore,
) externalCollectionRefreshMetaOption {
	return func(meta *externalCollectionRefreshMeta) {
		meta.resultStore = resultStore
	}
}

func newExternalCollectionRefreshMeta(
	ctx context.Context,
	catalog metastore.DataCoordCatalog,
	options ...externalCollectionRefreshMetaOption,
) (*externalCollectionRefreshMeta, error) {
	m := &externalCollectionRefreshMeta{
		ctx:            ctx,
		catalog:        catalog,
		jobLock:        lock.NewKeyLock[UniqueID](),
		taskLock:       lock.NewKeyLock[int64](),
		jobs:           typeutil.NewConcurrentMap[int64, *datapb.ExternalCollectionRefreshJob](),
		collectionJobs: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[int64, *datapb.ExternalCollectionRefreshJob]](),
		tasks:          typeutil.NewConcurrentMap[int64, *datapb.ExternalCollectionRefreshTask](),
		jobTasks:       typeutil.NewConcurrentMap[int64, *typeutil.ConcurrentMap[int64, *datapb.ExternalCollectionRefreshTask]](),
	}
	for _, option := range options {
		option(m)
	}
	if err := m.reloadFromKV(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *externalCollectionRefreshMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("externalCollectionRefreshMeta-reloadFromKV")

	// Load jobs
	jobs, err := m.catalog.ListExternalCollectionRefreshJobs(m.ctx)
	if err != nil {
		mlog.Error(m.ctx, "failed to load external collection refresh jobs", mlog.Err(err))
		return err
	}
	for _, job := range jobs {
		m.jobs.Insert(job.GetJobId(), job)
		m.addToCollectionJobs(job)
	}

	// Load tasks
	tasks, err := m.catalog.ListExternalCollectionRefreshTasks(m.ctx)
	if err != nil {
		mlog.Error(m.ctx, "failed to load external collection refresh tasks", mlog.Err(err))
		return err
	}
	for _, task := range tasks {
		m.tasks.Insert(task.GetTaskId(), task)
		m.addToJobTasks(task)
	}

	mlog.Info(m.ctx, "externalCollectionRefreshMeta reloadFromKV done",
		mlog.Int("jobCount", len(jobs)),
		mlog.Int("taskCount", len(tasks)),
		mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

// ==================== Internal Helper Methods ====================

func (m *externalCollectionRefreshMeta) addToCollectionJobs(job *datapb.ExternalCollectionRefreshJob) {
	jobMap, _ := m.collectionJobs.GetOrInsert(
		job.GetCollectionId(),
		typeutil.NewConcurrentMap[int64, *datapb.ExternalCollectionRefreshJob](),
	)
	jobMap.Insert(job.GetJobId(), job)
}

func (m *externalCollectionRefreshMeta) removeFromCollectionJobs(collectionID int64, jobID int64) {
	if jobMap, ok := m.collectionJobs.Get(collectionID); ok {
		jobMap.Remove(jobID)
		if jobMap.Len() == 0 {
			m.collectionJobs.Remove(collectionID)
		}
	}
}

func (m *externalCollectionRefreshMeta) addToJobTasks(task *datapb.ExternalCollectionRefreshTask) {
	taskMap, _ := m.jobTasks.GetOrInsert(
		task.GetJobId(),
		typeutil.NewConcurrentMap[int64, *datapb.ExternalCollectionRefreshTask](),
	)
	taskMap.Insert(task.GetTaskId(), task)
}

func (m *externalCollectionRefreshMeta) removeFromJobTasks(jobID int64, taskID int64) {
	if taskMap, ok := m.jobTasks.Get(jobID); ok {
		taskMap.Remove(taskID)
		if taskMap.Len() == 0 {
			m.jobTasks.Remove(jobID)
		}
	}
}

func cloneProtoSegments(segments []*datapb.SegmentInfo) []*datapb.SegmentInfo {
	if len(segments) == 0 {
		return nil
	}
	cloned := make([]*datapb.SegmentInfo, 0, len(segments))
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		cloned = append(cloned, proto.Clone(segment).(*datapb.SegmentInfo))
	}
	return cloned
}

// ==================== Job Operations ====================

// AddJob adds a new job to meta
func (m *externalCollectionRefreshMeta) AddJob(job *datapb.ExternalCollectionRefreshJob) error {
	m.jobLock.Lock(job.GetCollectionId())
	defer m.jobLock.Unlock(job.GetCollectionId())

	mlog.Info(m.ctx, "add refresh job",
		mlog.Int64("jobID", job.GetJobId()),
		mlog.Int64("collectionID", job.GetCollectionId()),
		mlog.String("collectionName", job.GetCollectionName()))

	if err := m.catalog.SaveExternalCollectionRefreshJob(m.ctx, job); err != nil {
		mlog.Warn(m.ctx, "save refresh job failed",
			mlog.Int64("jobID", job.GetJobId()),
			mlog.Err(err))
		return err
	}

	m.jobs.Insert(job.GetJobId(), job)
	m.addToCollectionJobs(job)

	mlog.Info(m.ctx, "add refresh job success",
		mlog.Int64("jobID", job.GetJobId()),
		mlog.Int64("collectionID", job.GetCollectionId()))
	return nil
}

// GetJob returns job by jobID
func (m *externalCollectionRefreshMeta) GetJob(jobID int64) *datapb.ExternalCollectionRefreshJob {
	job, ok := m.jobs.Get(jobID)
	if !ok {
		return nil
	}
	return proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)
}

// GetActiveJobByCollectionID returns the active (non-terminal) job for a collection
// If there are multiple active jobs (unexpected), returns the one with the newest StartTime
func (m *externalCollectionRefreshMeta) GetActiveJobByCollectionID(collectionID int64) *datapb.ExternalCollectionRefreshJob {
	m.jobLock.Lock(collectionID)
	defer m.jobLock.Unlock(collectionID)

	jobMap, ok := m.collectionJobs.Get(collectionID)
	if !ok {
		return nil
	}

	var newestJob *datapb.ExternalCollectionRefreshJob
	jobMap.Range(func(_ int64, job *datapb.ExternalCollectionRefreshJob) bool {
		switch job.GetState() {
		case indexpb.JobState_JobStateInit, indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateInProgress:
			if newestJob == nil || job.GetStartTime() > newestJob.GetStartTime() {
				newestJob = job
			}
		}
		return true
	})
	if newestJob != nil {
		return proto.Clone(newestJob).(*datapb.ExternalCollectionRefreshJob)
	}
	return nil
}

// ListJobsByCollectionID returns all jobs for a collection, sorted by start_time descending
func (m *externalCollectionRefreshMeta) ListJobsByCollectionID(collectionID int64) []*datapb.ExternalCollectionRefreshJob {
	m.jobLock.Lock(collectionID)
	defer m.jobLock.Unlock(collectionID)

	jobMap, ok := m.collectionJobs.Get(collectionID)
	if !ok {
		return nil
	}

	jobs := make([]*datapb.ExternalCollectionRefreshJob, 0)
	jobMap.Range(func(_ int64, job *datapb.ExternalCollectionRefreshJob) bool {
		jobs = append(jobs, proto.Clone(job).(*datapb.ExternalCollectionRefreshJob))
		return true
	})

	// Sort by StartTime descending (most recent first)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetStartTime() > jobs[j].GetStartTime()
	})

	return jobs
}

// ListAllJobs returns all jobs, sorted by start_time descending.
func (m *externalCollectionRefreshMeta) ListAllJobs() []*datapb.ExternalCollectionRefreshJob {
	jobs := make([]*datapb.ExternalCollectionRefreshJob, 0, m.jobs.Len())
	m.jobs.Range(func(_ int64, job *datapb.ExternalCollectionRefreshJob) bool {
		jobs = append(jobs, proto.Clone(job).(*datapb.ExternalCollectionRefreshJob))
		return true
	})

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetStartTime() > jobs[j].GetStartTime()
	})

	return jobs
}

// GetAllJobs returns all jobs
func (m *externalCollectionRefreshMeta) GetAllJobs() map[int64]*datapb.ExternalCollectionRefreshJob {
	result := make(map[int64]*datapb.ExternalCollectionRefreshJob)
	m.jobs.Range(func(jobID int64, job *datapb.ExternalCollectionRefreshJob) bool {
		result[jobID] = proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)
		return true
	})
	return result
}

// mutateJob applies a persisted in-place mutation to a refresh job under the
// collection-scoped lock. It centralizes the lock → refetch → clone → mutate →
// save → reindex pattern that every Job mutator needs.
//
// The mutate callback receives a cloned job and may return:
//   - (false, nil)  -> apply: save & reindex the clone; returns (true, nil)
//   - (true,  nil)  -> skip: no-op (e.g. terminal-state guard); returns (false, nil)
//   - (_,     err)  -> abort: propagate err (no save); returns (false, err)
//
// The first return value is whether the mutation was actually persisted, so
// callers can conditionally log success without running the log on skip paths.
func (m *externalCollectionRefreshMeta) mutateJob(
	jobID int64,
	opName string,
	mutate func(*datapb.ExternalCollectionRefreshJob) (skip bool, err error),
) (applied bool, err error) {
	job, ok := m.jobs.Get(jobID)
	if !ok {
		return false, merr.WrapErrServiceInternalMsg("job %d not found", jobID)
	}

	m.jobLock.Lock(job.GetCollectionId())
	defer m.jobLock.Unlock(job.GetCollectionId())

	// Re-fetch after lock
	job, ok = m.jobs.Get(jobID)
	if !ok {
		return false, merr.WrapErrServiceInternalMsg("job %d not found", jobID)
	}

	cloneJob := proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)
	skip, err := mutate(cloneJob)
	if err != nil {
		return false, err
	}
	if skip {
		return false, nil
	}

	if err := m.catalog.SaveExternalCollectionRefreshJob(m.ctx, cloneJob); err != nil {
		mlog.Warn(m.ctx,
			opName+" failed",
			mlog.Int64("jobID", jobID),
			mlog.Err(err))
		return false, err
	}

	m.jobs.Insert(jobID, cloneJob)
	m.addToCollectionJobs(cloneJob)
	return true, nil
}

// UpdateJobState updates job state.
//
// Returns (applied, err):
//   - applied=true means the state was actually persisted.
//   - applied=false, err=nil means the terminal-state guard skipped the write
//     because the job already reached Finished/Failed. Callers that perform
//     follow-up actions conditional on the transition (fire onJobFailed, mark
//     tasks as failed, etc.) MUST check applied and short-circuit when false.
//   - applied=false, err!=nil means a persistence / lookup failure.
func (m *externalCollectionRefreshMeta) UpdateJobState(jobID int64, state indexpb.JobState, failReason string) (bool, error) {
	applied, err := m.mutateJob(jobID, "update job state", func(job *datapb.ExternalCollectionRefreshJob) (bool, error) {
		// Terminal-state guard: once a job has reached Finished or Failed it must
		// not be transitioned again. Without this guard a stale-snapshot caller
		// could silently overwrite a transition persisted by another checker path.
		if job.GetState() == indexpb.JobState_JobStateFinished ||
			job.GetState() == indexpb.JobState_JobStateFailed {
			mlog.Info(m.ctx, "skip update job state, already in terminal state",
				mlog.Int64("jobID", jobID),
				mlog.String("currentState", job.GetState().String()),
				mlog.String("requestedState", state.String()))
			return true, nil
		}

		job.State = state
		job.FailReason = failReason
		if state == indexpb.JobState_JobStateFinished || state == indexpb.JobState_JobStateFailed {
			job.EndTime = time.Now().UnixMilli()
			if state == indexpb.JobState_JobStateFinished {
				job.Progress = 100
			}
		}
		return false, nil
	})
	if applied {
		mlog.Info(m.ctx, "update job state success",
			mlog.Int64("jobID", jobID),
			mlog.String("state", state.String()))
	}
	return applied, err
}

// UpdateJobStateWithPreApply runs preApply and persists the requested state
// while holding the collection-scoped job lock. It is used for Finished
// refresh jobs so concurrent eager checker paths cannot apply segment results
// more than once before the job reaches a terminal state.
func (m *externalCollectionRefreshMeta) UpdateJobStateWithPreApply(
	jobID int64,
	state indexpb.JobState,
	failReason string,
	preApply func(*datapb.ExternalCollectionRefreshJob) error,
) (bool, error) {
	job, ok := m.jobs.Get(jobID)
	if !ok {
		return false, merr.WrapErrServiceInternalMsg("job %d not found", jobID)
	}

	m.jobLock.Lock(job.GetCollectionId())
	defer m.jobLock.Unlock(job.GetCollectionId())

	// Re-fetch after lock so a concurrent eager path that already persisted a
	// terminal state owns the one-time side effects.
	job, ok = m.jobs.Get(jobID)
	if !ok {
		return false, merr.WrapErrServiceInternalMsg("job %d not found", jobID)
	}
	if job.GetState() == indexpb.JobState_JobStateFinished ||
		job.GetState() == indexpb.JobState_JobStateFailed {
		mlog.Info(m.ctx, "skip update job state with pre-apply, already in terminal state",
			mlog.Int64("jobID", jobID),
			mlog.String("currentState", job.GetState().String()),
			mlog.String("requestedState", state.String()))
		return false, nil
	}

	if preApply != nil {
		if err := preApply(job); err != nil {
			cloneJob := proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)
			cloneJob.State = indexpb.JobState_JobStateFailed
			cloneJob.FailReason = err.Error()
			cloneJob.EndTime = time.Now().UnixMilli()
			if saveErr := m.catalog.SaveExternalCollectionRefreshJob(m.ctx, cloneJob); saveErr != nil {
				mlog.Warn(m.ctx, "update job state after pre-apply failed",
					mlog.Int64("jobID", jobID),
					mlog.Err(saveErr))
				return false, merr.Wrapf(err, "pre-apply failed; additionally failed to persist Failed job state: %v", saveErr)
			}
			m.jobs.Insert(jobID, cloneJob)
			m.addToCollectionJobs(cloneJob)
			mlog.Info(m.ctx, "update job state success",
				mlog.Int64("jobID", jobID),
				mlog.String("state", indexpb.JobState_JobStateFailed.String()))
			return true, err
		}
	}

	cloneJob := proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)
	cloneJob.State = state
	cloneJob.FailReason = failReason
	if state == indexpb.JobState_JobStateFinished || state == indexpb.JobState_JobStateFailed {
		cloneJob.EndTime = time.Now().UnixMilli()
		if state == indexpb.JobState_JobStateFinished {
			cloneJob.Progress = 100
		}
	}

	if err := m.catalog.SaveExternalCollectionRefreshJob(m.ctx, cloneJob); err != nil {
		mlog.Warn(m.ctx, "update job state with pre-apply failed",
			mlog.Int64("jobID", jobID),
			mlog.Err(err))
		return false, err
	}

	m.jobs.Insert(jobID, cloneJob)
	m.addToCollectionJobs(cloneJob)
	mlog.Info(m.ctx, "update job state success",
		mlog.Int64("jobID", jobID),
		mlog.String("state", state.String()))
	return true, nil
}

// UpdateJobProgress updates job progress. Terminal jobs are skipped: a
// terminal state owns its progress (Finished pins 100), and a late in-flight
// progress write racing the terminal transition - the index gate's held-
// progress write and the eager path's Finished persist run on different
// goroutines - must not land, or a Finished job would report 95/99 forever
// and completion pollers waiting for 100 would never observe it.
func (m *externalCollectionRefreshMeta) UpdateJobProgress(jobID int64, progress int64) error {
	_, err := m.mutateJob(jobID, "update job progress", func(job *datapb.ExternalCollectionRefreshJob) (bool, error) {
		if job.GetState() == indexpb.JobState_JobStateFinished ||
			job.GetState() == indexpb.JobState_JobStateFailed {
			return true, nil
		}
		job.Progress = progress
		return false, nil
	})
	return err
}

// AddTasksToJob persists task records first and the job task list last as the
// publication marker. The 3.0 catalog does not support master's composite
// update API, so the job and task locks are held across the pre-write guards,
// sequential task saves, and final job save. A partial catalog write therefore
// remains unpublished, and in-memory state changes only after the job save
// succeeds.
func (m *externalCollectionRefreshMeta) AddTasksToJob(jobID int64, tasks []*datapb.ExternalCollectionRefreshTask) error {
	job, ok := m.jobs.Get(jobID)
	if !ok {
		return merr.WrapErrServiceInternalMsg("job %d not found", jobID)
	}

	m.jobLock.Lock(job.GetCollectionId())
	defer m.jobLock.Unlock(job.GetCollectionId())
	m.taskLock.Lock(jobID)
	defer m.taskLock.Unlock(jobID)

	// Re-fetch after lock so the persisted job carries the freshest TaskIds.
	job, ok = m.jobs.Get(jobID)
	if !ok {
		return merr.WrapErrServiceInternalMsg("job %d not found", jobID)
	}
	// This is the production publication boundary. The checks must happen after
	// re-fetching under jobLock so timeout/failure and publication are ordered:
	// whichever acquires the lock first wins, and a late Explore result cannot
	// append tasks to a terminal or already-published job.
	if job.GetState() != indexpb.JobState_JobStateInit {
		return merr.Wrapf(
			errExternalRefreshTaskPlanNotPublishable,
			"cannot publish external refresh task plan for job %d in state %s",
			jobID,
			job.GetState().String(),
		)
	}
	if len(tasks) == 0 {
		return merr.Wrapf(errExternalRefreshTaskPlanNotPublishable, "cannot publish empty task plan for job %d", jobID)
	}
	if len(job.GetTaskIds()) > 0 {
		return merr.Wrapf(errExternalRefreshTaskPlanNotPublishable, "job %d already has a published task plan", jobID)
	}

	cloneJob := proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)
	for _, task := range tasks {
		if err := m.catalog.SaveExternalCollectionRefreshTask(m.ctx, task); err != nil {
			mlog.Warn(m.ctx, "save task while publishing refresh plan failed",
				mlog.Int64("jobID", jobID),
				mlog.Int64("taskID", task.GetTaskId()),
				mlog.Err(err))
			return err
		}
		cloneJob.TaskIds = append(cloneJob.TaskIds, task.GetTaskId())
	}

	if err := m.catalog.SaveExternalCollectionRefreshJob(m.ctx, cloneJob); err != nil {
		mlog.Warn(m.ctx, "publish external refresh task plan failed",
			mlog.Int64("jobID", jobID),
			mlog.Err(err))
		return err
	}

	for _, task := range tasks {
		m.tasks.Insert(task.GetTaskId(), task)
		m.addToJobTasks(task)
	}
	m.jobs.Insert(jobID, cloneJob)
	m.addToCollectionJobs(cloneJob)
	return nil
}

// DropJob removes a job and all its associated tasks
func (m *externalCollectionRefreshMeta) DropJob(ctx context.Context, jobID int64) error {
	job, ok := m.jobs.Get(jobID)
	if !ok {
		mlog.Info(ctx, "drop job success, job already not exist", mlog.Int64("jobID", jobID))
		return nil
	}

	m.jobLock.Lock(job.GetCollectionId())
	defer m.jobLock.Unlock(job.GetCollectionId())

	// Re-fetch after lock
	job, ok = m.jobs.Get(jobID)
	if !ok {
		mlog.Info(ctx, "drop job success, job already not exist", mlog.Int64("jobID", jobID))
		return nil
	}

	// Drop all associated tasks first
	if taskMap, ok := m.jobTasks.Get(jobID); ok {
		var dropErr error
		taskMap.Range(func(taskID int64, _ *datapb.ExternalCollectionRefreshTask) bool {
			if err := m.catalog.DropExternalCollectionRefreshTask(ctx, taskID); err != nil {
				mlog.Warn(ctx, "drop task failed during job drop",
					mlog.Int64("jobID", jobID),
					mlog.Int64("taskID", taskID),
					mlog.Err(err))
				dropErr = err
				return false
			}
			m.tasks.Remove(taskID)
			return true
		})
		if dropErr != nil {
			return dropErr
		}
		m.jobTasks.Remove(jobID)
	}

	// Drop job
	if err := m.catalog.DropExternalCollectionRefreshJob(ctx, jobID); err != nil {
		mlog.Warn(ctx, "drop job failed",
			mlog.Int64("jobID", jobID),
			mlog.Err(err))
		return err
	}

	m.jobs.Remove(jobID)
	m.removeFromCollectionJobs(job.GetCollectionId(), jobID)
	if m.resultStore != nil {
		if err := m.resultStore.RemoveJob(ctx, job.GetCollectionId(), jobID); err != nil {
			mlog.Warn(ctx, "failed to remove external refresh job results",
				mlog.FieldJobID(jobID),
				mlog.FieldCollectionID(job.GetCollectionId()),
				mlog.Err(err))
		}
	}

	mlog.Info(ctx, "drop job success",
		mlog.Int64("jobID", jobID),
		mlog.Int64("collectionID", job.GetCollectionId()))
	return nil
}

// ==================== Task Operations ====================

// AddTask adds a new task to meta
func (m *externalCollectionRefreshMeta) AddTask(task *datapb.ExternalCollectionRefreshTask) error {
	m.taskLock.Lock(task.GetJobId())
	defer m.taskLock.Unlock(task.GetJobId())

	mlog.Info(m.ctx, "add refresh task",
		mlog.Int64("taskID", task.GetTaskId()),
		mlog.Int64("jobID", task.GetJobId()),
		mlog.Int64("collectionID", task.GetCollectionId()))

	if err := m.catalog.SaveExternalCollectionRefreshTask(m.ctx, task); err != nil {
		mlog.Warn(m.ctx, "save refresh task failed",
			mlog.Int64("taskID", task.GetTaskId()),
			mlog.Err(err))
		return err
	}

	m.tasks.Insert(task.GetTaskId(), task)
	m.addToJobTasks(task)

	mlog.Info(m.ctx, "add refresh task success",
		mlog.Int64("taskID", task.GetTaskId()),
		mlog.Int64("jobID", task.GetJobId()))
	return nil
}

// GetTask returns task by taskID
func (m *externalCollectionRefreshMeta) GetTask(taskID int64) *datapb.ExternalCollectionRefreshTask {
	task, ok := m.tasks.Get(taskID)
	if !ok {
		return nil
	}
	return proto.Clone(task).(*datapb.ExternalCollectionRefreshTask)
}

// GetTasksByJobID returns all tasks for a job
func (m *externalCollectionRefreshMeta) GetTasksByJobID(jobID int64) []*datapb.ExternalCollectionRefreshTask {
	m.taskLock.Lock(jobID)
	defer m.taskLock.Unlock(jobID)

	taskMap, ok := m.jobTasks.Get(jobID)
	if !ok {
		return nil
	}

	tasks := make([]*datapb.ExternalCollectionRefreshTask, 0)
	taskMap.Range(func(_ int64, task *datapb.ExternalCollectionRefreshTask) bool {
		tasks = append(tasks, proto.Clone(task).(*datapb.ExternalCollectionRefreshTask))
		return true
	})
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].GetTaskId() < tasks[j].GetTaskId()
	})
	return tasks
}

// GetCommittedTasksByJobID resolves tasks through the parent job's ordered
// task_ids list. Task records absent from that list are unpublished and must
// not be scheduled, aggregated, applied, or have their results cleared.
func (m *externalCollectionRefreshMeta) GetCommittedTasksByJobID(jobID int64) ([]*datapb.ExternalCollectionRefreshTask, error) {
	job := m.GetJob(jobID)
	if job == nil {
		return nil, merr.WrapErrServiceInternalMsg("job %d not found", jobID)
	}
	if len(job.GetTaskIds()) == 0 {
		return nil, nil
	}

	m.taskLock.Lock(jobID)
	defer m.taskLock.Unlock(jobID)
	return m.getCommittedTasksLocked(job)
}

// GetCommittedTaskResultsByJobID resolves the committed task headers first,
// then loads external result payloads without holding the job-scoped task lock.
// Callers that only inspect task state should use GetCommittedTasksByJobID to
// avoid object-storage I/O.
func (m *externalCollectionRefreshMeta) GetCommittedTaskResultsByJobID(jobID int64) ([]*datapb.ExternalCollectionRefreshTask, error) {
	tasks, err := m.GetCommittedTasksByJobID(jobID)
	if err != nil {
		return nil, err
	}
	for _, task := range tasks {
		switch task.GetResultStorageVersion() {
		case 0:
			if task.GetResultPath() != "" || len(task.GetResultChecksum()) != 0 {
				return nil, merr.WrapErrDataIntegrityMsg(
					"external refresh task %d has a result reference without a storage version",
					task.GetTaskId(),
				)
			}
			if task.GetOwnershipPlanVersion() == externalRefreshOwnershipPlanVersion && task.GetResultReady() {
				return nil, merr.WrapErrDataIntegrityMsg(
					"external refresh task %d has an inline result under ownership plan version %d",
					task.GetTaskId(),
					task.GetOwnershipPlanVersion(),
				)
			}
		case externalRefreshTaskResultStorageVersion:
			if !task.GetResultReady() {
				return nil, merr.WrapErrDataIntegrityMsg(
					"external refresh task %d has an unpublished external result",
					task.GetTaskId(),
				)
			}
			if m.resultStore == nil {
				return nil, merr.WrapErrServiceInternalMsg(
					"external refresh task %d requires an unconfigured result store",
					task.GetTaskId(),
				)
			}
			result, err := m.resultStore.Load(m.ctx, task)
			if err != nil {
				return nil, err
			}
			task.KeptSegments = append([]int64(nil), result.GetKeptSegments()...)
			task.UpdatedSegments = cloneProtoSegments(result.GetUpdatedSegments())
		default:
			return nil, merr.WrapErrServiceInternalMsg(
				"external refresh task %d has unsupported result storage version %d",
				task.GetTaskId(),
				task.GetResultStorageVersion(),
			)
		}
	}
	return tasks, nil
}

// getCommittedTasksLocked resolves a job's published task list while the
// caller holds taskLock for that job.
func (m *externalCollectionRefreshMeta) getCommittedTasksLocked(job *datapb.ExternalCollectionRefreshJob) ([]*datapb.ExternalCollectionRefreshTask, error) {
	jobID := job.GetJobId()
	tasks := make([]*datapb.ExternalCollectionRefreshTask, 0, len(job.GetTaskIds()))
	seen := make(map[int64]struct{}, len(job.GetTaskIds()))
	for _, taskID := range job.GetTaskIds() {
		if _, ok := seen[taskID]; ok {
			return nil, merr.WrapErrServiceInternalMsg("job %d references duplicate task %d", jobID, taskID)
		}
		seen[taskID] = struct{}{}

		task, ok := m.tasks.Get(taskID)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("job %d references missing task %d", jobID, taskID)
		}
		if task.GetJobId() != jobID {
			return nil, merr.WrapErrServiceInternalMsg("job %d references task %d owned by job %d", jobID, taskID, task.GetJobId())
		}
		tasks = append(tasks, proto.Clone(task).(*datapb.ExternalCollectionRefreshTask))
	}
	return tasks, nil
}

// GetAllTasks returns all tasks (for inspector)
func (m *externalCollectionRefreshMeta) GetAllTasks() map[int64]*datapb.ExternalCollectionRefreshTask {
	result := make(map[int64]*datapb.ExternalCollectionRefreshTask)
	m.tasks.Range(func(taskID int64, task *datapb.ExternalCollectionRefreshTask) bool {
		result[taskID] = proto.Clone(task).(*datapb.ExternalCollectionRefreshTask)
		return true
	})
	return result
}

// GetTaskState returns task state
func (m *externalCollectionRefreshMeta) GetTaskState(taskID int64) indexpb.JobState {
	task, ok := m.tasks.Get(taskID)
	if !ok {
		return indexpb.JobState_JobStateNone
	}
	return task.GetState()
}

// mutateTask is the Task counterpart of mutateJob: it applies a persisted
// in-place mutation to a refresh task under the jobID-scoped task lock.
// See mutateJob for the skip/apply/abort return semantics.
func (m *externalCollectionRefreshMeta) mutateTask(
	taskID int64,
	opName string,
	mutate func(*datapb.ExternalCollectionRefreshTask) (skip bool, err error),
) (applied bool, cloned *datapb.ExternalCollectionRefreshTask, err error) {
	task, ok := m.tasks.Get(taskID)
	if !ok {
		return false, nil, merr.WrapErrServiceInternalMsg("task %d not found", taskID)
	}

	m.taskLock.Lock(task.GetJobId())
	defer m.taskLock.Unlock(task.GetJobId())

	// Re-fetch after lock
	task, ok = m.tasks.Get(taskID)
	if !ok {
		return false, nil, merr.WrapErrServiceInternalMsg("task %d not found", taskID)
	}

	cloneTask := proto.Clone(task).(*datapb.ExternalCollectionRefreshTask)
	skip, err := mutate(cloneTask)
	if err != nil {
		return false, nil, err
	}
	if skip {
		return false, nil, nil
	}

	if err := m.catalog.SaveExternalCollectionRefreshTask(m.ctx, cloneTask); err != nil {
		mlog.Warn(m.ctx,
			opName+" failed",
			mlog.FieldJobID(cloneTask.GetJobId()),
			mlog.FieldTaskID(taskID),
			mlog.Int("taskMetaBytes", proto.Size(cloneTask)),
			mlog.Err(err))
		return false, nil, err
	}

	m.tasks.Insert(taskID, cloneTask)
	m.addToJobTasks(cloneTask)
	return true, cloneTask, nil
}

// UpdateTaskState updates task state
func (m *externalCollectionRefreshMeta) UpdateTaskState(taskID int64, state indexpb.JobState, failReason string) error {
	applied, _, err := m.mutateTask(taskID, "update task state", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		task.State = state
		task.FailReason = failReason
		if state == indexpb.JobState_JobStateFinished {
			task.Progress = 100
		}
		return false, nil
	})
	if applied {
		mlog.Info(m.ctx, "update task state success",
			mlog.Int64("taskID", taskID),
			mlog.String("state", state.String()))
	}
	return err
}

// UpdateTaskProgress updates task progress
func (m *externalCollectionRefreshMeta) UpdateTaskProgress(taskID int64, progress int64) error {
	_, _, err := m.mutateTask(taskID, "update task progress", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		task.Progress = progress
		return false, nil
	})
	return err
}

// UpdateTaskResult persists the terminal worker response for job-level aggregation.
func (m *externalCollectionRefreshMeta) UpdateTaskResult(
	taskID int64,
	state indexpb.JobState,
	failReason string,
	keptSegments []int64,
	updatedSegments []*datapb.SegmentInfo,
) error {
	currentTask := m.GetTask(taskID)
	if currentTask == nil {
		return merr.WrapErrServiceInternalMsg("task %d not found", taskID)
	}

	storeExternally := false
	switch currentTask.GetOwnershipPlanVersion() {
	case 0:
		// Version zero is retained for legacy test fixtures. Runtime tasks with
		// no ownership plan are rejected before worker dispatch.
	case externalRefreshOwnershipPlanVersion:
		storeExternally = true
	default:
		return merr.WrapErrServiceInternalMsg(
			"external refresh task %d has unsupported ownership plan version %d",
			taskID,
			currentTask.GetOwnershipPlanVersion(),
		)
	}

	var resultRef externalCollectionRefreshResultRef
	if storeExternally {
		if m.resultStore == nil {
			return merr.WrapErrServiceInternalMsg(
				"external refresh task %d requires an unconfigured result store",
				taskID,
			)
		}
		var err error
		resultRef, err = m.resultStore.Save(m.ctx, currentTask, keptSegments, updatedSegments)
		if err != nil {
			return err
		}
	}

	applied, cloned, err := m.mutateTask(taskID, "update task result", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		if storeExternally &&
			(task.GetVersion() != currentTask.GetVersion() ||
				task.GetOwnershipPlanVersion() != currentTask.GetOwnershipPlanVersion()) {
			return false, merr.WrapErrServiceInternalMsg(
				"external refresh task %d changed while persisting result",
				taskID,
			)
		}
		task.State = state
		task.FailReason = failReason
		if storeExternally {
			task.KeptSegments = nil
			task.UpdatedSegments = nil
			task.ResultStorageVersion = externalRefreshTaskResultStorageVersion
			task.ResultPath = resultRef.path
			task.ResultChecksum = append([]byte(nil), resultRef.checksum...)
		} else {
			task.KeptSegments = append([]int64(nil), keptSegments...)
			task.UpdatedSegments = cloneProtoSegments(updatedSegments)
			task.ResultStorageVersion = 0
			task.ResultPath = ""
			task.ResultChecksum = nil
		}
		task.ResultReady = true
		if state == indexpb.JobState_JobStateFinished {
			task.Progress = 100
		}
		return false, nil
	})
	if applied {
		mlog.Info(m.ctx, "update task result success",
			mlog.FieldJobID(cloned.GetJobId()),
			mlog.FieldTaskID(taskID),
			mlog.String("state", state.String()),
			mlog.Int("ownedSegments", len(cloned.GetOwnedSegmentIds())),
			mlog.Int("keptSegments", len(keptSegments)),
			mlog.Int("updatedSegments", len(updatedSegments)),
			mlog.Int32("resultStorageVersion", cloned.GetResultStorageVersion()),
			mlog.Int("resultBytes", resultRef.size),
			mlog.Int("taskMetaBytes", proto.Size(cloned)))
	}
	return err
}

// ClearTaskResult clears stored task result payload after the owning job has
// persisted Finished. The task state/progress remain intact for progress and
// history queries until the job retention GC drops the task.
func (m *externalCollectionRefreshMeta) ClearTaskResult(taskID int64) error {
	var resultPath string
	applied, cloned, err := m.mutateTask(taskID, "clear task result", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		if len(task.GetKeptSegments()) == 0 &&
			len(task.GetUpdatedSegments()) == 0 &&
			task.GetResultStorageVersion() == 0 &&
			task.GetResultPath() == "" &&
			len(task.GetResultChecksum()) == 0 {
			return true, nil
		}
		resultPath = task.GetResultPath()
		task.KeptSegments = nil
		task.UpdatedSegments = nil
		task.ResultStorageVersion = 0
		task.ResultPath = ""
		task.ResultChecksum = nil
		return false, nil
	})
	if applied {
		mlog.Info(m.ctx, "clear task result success",
			mlog.Int64("taskID", taskID),
			mlog.String("state", cloned.GetState().String()))
		// The durable reference is cleared first. A failed object deletion is
		// safe to leave for the job-prefix cleanup performed by DropJob.
		if resultPath != "" {
			if m.resultStore == nil {
				mlog.Warn(m.ctx, "cannot remove external refresh task result without result store",
					mlog.FieldTaskID(taskID),
					mlog.String("resultPath", resultPath))
			} else if err := m.resultStore.Remove(m.ctx, resultPath); err != nil {
				mlog.Warn(m.ctx, "failed to remove external refresh task result",
					mlog.FieldTaskID(taskID),
					mlog.String("resultPath", resultPath),
					mlog.Err(err))
			}
		}
	}
	return err
}

func (m *externalCollectionRefreshMeta) ClearTaskResultsByJobID(jobID int64) error {
	tasks, err := m.GetCommittedTasksByJobID(jobID)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if err := m.ClearTaskResult(task.GetTaskId()); err != nil {
			return err
		}
	}
	return nil
}

// UpdateTaskVersion updates task version and nodeID
func (m *externalCollectionRefreshMeta) UpdateTaskVersion(taskID, nodeID int64) error {
	applied, cloned, err := m.mutateTask(taskID, "update task version", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		task.Version++
		task.NodeId = nodeID
		return false, nil
	})
	if applied {
		mlog.Info(m.ctx, "update task version success",
			mlog.Int64("taskID", taskID),
			mlog.Int64("nodeID", nodeID),
			mlog.Int64("newVersion", cloned.GetVersion()))
	}
	return err
}

// DropTask removes a task
func (m *externalCollectionRefreshMeta) DropTask(ctx context.Context, taskID int64) error {
	task, ok := m.tasks.Get(taskID)
	if !ok {
		mlog.Info(ctx, "drop task success, task already not exist", mlog.Int64("taskID", taskID))
		return nil
	}

	m.taskLock.Lock(task.GetJobId())
	defer m.taskLock.Unlock(task.GetJobId())

	task, ok = m.tasks.Get(taskID)
	if !ok {
		mlog.Info(ctx, "drop task success, task already not exist", mlog.Int64("taskID", taskID))
		return nil
	}

	if err := m.catalog.DropExternalCollectionRefreshTask(ctx, taskID); err != nil {
		mlog.Warn(ctx, "drop task failed",
			mlog.Int64("taskID", taskID),
			mlog.Err(err))
		return err
	}

	m.tasks.Remove(taskID)
	m.removeFromJobTasks(task.GetJobId(), taskID)

	mlog.Info(ctx, "drop task success",
		mlog.Int64("taskID", taskID),
		mlog.Int64("jobID", task.GetJobId()))
	return nil
}

// ==================== Aggregation Operations ====================

// AggregateJobStateFromTasks calculates job state and progress from the
// committed task plan.
func (m *externalCollectionRefreshMeta) AggregateJobStateFromTasks(jobID int64) (state indexpb.JobState, progress int64, err error) {
	tasks, err := m.GetCommittedTasksByJobID(jobID)
	if err != nil {
		return indexpb.JobState_JobStateNone, 0, err
	}
	if len(tasks) == 0 {
		return indexpb.JobState_JobStateNone, 0, nil
	}

	var hasInit, hasRetry, hasInProgress, hasFailed bool
	var totalProgress int64

	for _, task := range tasks {
		taskProgress := task.GetProgress()
		// Finished tasks should always count as 100% regardless of stored value
		if task.GetState() == indexpb.JobState_JobStateFinished {
			taskProgress = 100
		}
		totalProgress += taskProgress
		switch task.GetState() {
		case indexpb.JobState_JobStateInit:
			hasInit = true
		case indexpb.JobState_JobStateRetry:
			hasRetry = true
		case indexpb.JobState_JobStateInProgress:
			hasInProgress = true
		case indexpb.JobState_JobStateFailed:
			hasFailed = true
		}
	}

	// Priority: Failed > InProgress > Retry > Init > Finished
	// With multiple tasks, prefer "more active" state for better user perception
	if hasFailed {
		state = indexpb.JobState_JobStateFailed
	} else if hasInProgress {
		state = indexpb.JobState_JobStateInProgress
	} else if hasRetry {
		state = indexpb.JobState_JobStateRetry
	} else if hasInit {
		state = indexpb.JobState_JobStateInit
	} else {
		state = indexpb.JobState_JobStateFinished
	}

	progress = totalProgress / int64(len(tasks))
	return state, progress, nil
}
