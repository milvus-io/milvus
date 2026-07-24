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
	ctx     context.Context
	catalog metastore.DataCoordCatalog

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

func newExternalCollectionRefreshMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*externalCollectionRefreshMeta, error) {
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
//     This is the critical signal that prevents tryTimeoutJob from racing an
//     eager Finished transition and poisoning the manager's notifiedJobs map.
//   - applied=false, err!=nil means a persistence / lookup failure.
func (m *externalCollectionRefreshMeta) UpdateJobState(jobID int64, state indexpb.JobState, failReason string) (bool, error) {
	applied, err := m.mutateJob(jobID, "update job state", func(job *datapb.ExternalCollectionRefreshJob) (bool, error) {
		// Terminal-state guard: once a job has reached Finished or Failed it must
		// not be transitioned again. Without this guard a stale-snapshot caller
		// (e.g. tryTimeoutJob using a job pointer captured before aggregateJobState
		// transitioned the job to Finished in the same processJob cycle) could
		// silently overwrite a successful Finished as Failed("timeout").
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
			// A stale-manifest conflict or a mid-retry not-ready apply is a
			// transient concurrency condition, not a terminal failure. Leave the
			// job non-terminal so the checker can drive the retry (reset tasks /
			// wait a tick); the per-job timeout is the ultimate bound. The retry
			// decision lives in the checker, so just surface the signal here.
			if errors.Is(err, errExternalRefreshStaleManifest) ||
				errors.Is(err, errExternalRefreshNotReady) {
				return false, err
			}
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

// UpdateJobProgress updates job progress
func (m *externalCollectionRefreshMeta) UpdateJobProgress(jobID int64, progress int64) error {
	_, err := m.mutateJob(jobID, "update job progress", func(job *datapb.ExternalCollectionRefreshJob) (bool, error) {
		job.Progress = progress
		return false, nil
	})
	return err
}

// AddTaskIDToJob adds a taskID to job's task_ids list
func (m *externalCollectionRefreshMeta) AddTaskIDToJob(jobID int64, taskID int64) error {
	_, err := m.mutateJob(jobID, "add taskID to job", func(job *datapb.ExternalCollectionRefreshJob) (bool, error) {
		job.TaskIds = append(job.TaskIds, taskID)
		return false, nil
	})
	return err
}

// AddTasksToJob persists a batch of newly-created tasks together with the
// job's updated TaskIds list as a single composite catalog write, then applies
// the in-memory bookkeeping of both. It replaces the per-task pair of writes
// createTasksForJob used to do (AddTask followed by AddTaskIDToJob), which -
// being 2N independent txns - could leave the job's TaskIds disagreeing with
// the persisted task set on a partial failure.
//
// The job - the failover anchor for its tasks - is written LAST as the commit
// marker, mirroring DropJob's ordering: a persisted job always references only
// tasks that are themselves persisted. Both the job lock (collectionID) and
// the task lock (jobID) are held across the whole compute -> catalog.Update ->
// in-memory apply sequence so a concurrent AddTask / AddTaskIDToJob cannot
// interleave and desync the job's TaskIds from its task set. (jobLock is taken
// before taskLock; no path takes them in the opposite order, so this cannot
// deadlock.) In-memory state is applied only after the write succeeds.
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

	// Mirror AddTaskIDToJob: mutate a clone (append every new task ID) and
	// persist that as the job record, so on-disk TaskIds cover all saved tasks.
	cloneJob := proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)
	actions := make([]metastore.UpdateAction, 0, len(tasks)+1)
	for _, task := range tasks {
		actions = append(actions, metastore.AddRefreshTask(task))
		cloneJob.TaskIds = append(cloneJob.TaskIds, task.GetTaskId())
	}
	actions = append(actions, metastore.SaveRefreshJob(cloneJob))

	if err := m.catalog.Update(m.ctx, actions...); err != nil {
		mlog.Warn(m.ctx, "add tasks to job failed",
			mlog.Int64("jobID", jobID),
			mlog.Int("taskCount", len(tasks)),
			mlog.Err(err))
		return err
	}

	// Mirror AddTask's memory writes (task inserted as-is, no clone) and
	// mutateJob's (the mutated clone replaces the in-memory job).
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

	// Collect associated task IDs, then persist the drop of every task and
	// the job as a single composite catalog write, with the job (the
	// failover anchor) landing last. Memory is only mutated after the write
	// succeeds, so a failed write leaves the in-memory state consistent with
	// what is actually on disk.
	var taskIDs []int64
	if taskMap, ok := m.jobTasks.Get(jobID); ok {
		taskMap.Range(func(taskID int64, _ *datapb.ExternalCollectionRefreshTask) bool {
			taskIDs = append(taskIDs, taskID)
			return true
		})
	}

	actions := make([]metastore.UpdateAction, 0, len(taskIDs)+1)
	for _, taskID := range taskIDs {
		actions = append(actions, metastore.DropRefreshTask(taskID))
	}
	actions = append(actions, metastore.DropRefreshJob(jobID))

	if err := m.catalog.Update(ctx, actions...); err != nil {
		mlog.Warn(ctx, "drop job and tasks failed",
			mlog.Int64("jobID", jobID),
			mlog.Err(err))
		return err
	}

	for _, taskID := range taskIDs {
		m.tasks.Remove(taskID)
	}
	m.jobTasks.Remove(jobID)

	m.jobs.Remove(jobID)
	m.removeFromCollectionJobs(job.GetCollectionId(), jobID)

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
			mlog.Int64("taskID", taskID),
			mlog.Err(err))
		return false, nil, err
	}

	m.tasks.Insert(taskID, cloneTask)
	m.addToJobTasks(cloneTask)
	return true, cloneTask, nil
}

// UpdateTaskState updates task state. expectedVersion fences the write to a
// specific attempt (0 = unconditional job-scoped write). Returns whether the
// write landed so callers can skip attempt-scoped side effects on a superseded
// write.
func (m *externalCollectionRefreshMeta) UpdateTaskState(taskID, expectedVersion int64, state indexpb.JobState, failReason string) (bool, error) {
	applied, _, err := m.mutateTask(taskID, "update task state", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		if taskVersionMismatch(task, expectedVersion) {
			return true, nil // skip: superseded attempt
		}
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
	return applied, err
}

// UpdateTaskProgress updates task progress
func (m *externalCollectionRefreshMeta) UpdateTaskProgress(taskID int64, progress int64) error {
	_, _, err := m.mutateTask(taskID, "update task progress", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		task.Progress = progress
		return false, nil
	})
	return err
}

// taskVersionMismatch reports whether an attempt-scoped write for expectedVersion
// must be skipped because the persisted task has been re-dispatched under a newer
// version. expectedVersion == 0 means "unconditional" (a job-scoped write such as
// timeout that is not tied to a specific attempt).
func taskVersionMismatch(task *datapb.ExternalCollectionRefreshTask, expectedVersion int64) bool {
	return expectedVersion != 0 && task.GetVersion() != expectedVersion
}

// ResetTaskForRetry atomically returns a task to Init for re-dispatch and clears
// its transient result payload and progress in the same write. This mirrors the
// stats retry path: a re-dispatchable task must not leave a stale result behind
// (job-level aggregation would otherwise adopt it) nor report Progress=100 while
// the rebuild is still running. expectedVersion fences the write to a specific
// attempt (0 = unconditional).
func (m *externalCollectionRefreshMeta) ResetTaskForRetry(taskID, expectedVersion int64, reason string) (bool, error) {
	return m.mutateTaskApplied(taskID, "reset task for retry", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		if taskVersionMismatch(task, expectedVersion) {
			return true, nil // skip: superseded attempt
		}
		task.State = indexpb.JobState_JobStateInit
		task.FailReason = reason
		task.Progress = 0
		task.ResultReady = false
		task.KeptSegments = nil
		task.UpdatedSegments = nil
		return false, nil
	})
}

// mutateTaskApplied is mutateTask returning only (applied, error) for callers that
// need to know whether a version-fenced write actually landed.
func (m *externalCollectionRefreshMeta) mutateTaskApplied(taskID int64, opName string, mutate func(*datapb.ExternalCollectionRefreshTask) (bool, error)) (bool, error) {
	applied, _, err := m.mutateTask(taskID, opName, mutate)
	return applied, err
}

// UpdateTaskResult persists the terminal worker response for job-level aggregation.
func (m *externalCollectionRefreshMeta) UpdateTaskResult(
	taskID int64,
	expectedVersion int64,
	state indexpb.JobState,
	failReason string,
	keptSegments []int64,
	updatedSegments []*datapb.SegmentInfo,
) (bool, error) {
	applied, cloned, err := m.mutateTask(taskID, "update task result", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		if taskVersionMismatch(task, expectedVersion) {
			// A superseded attempt's result — the task was re-dispatched under a
			// newer version. Drop the write so a stale/late Query response cannot
			// overwrite the current attempt's state.
			mlog.Info(m.ctx, "drop refresh task result from a superseded attempt",
				mlog.Int64("taskID", taskID),
				mlog.Int64("resultVersion", expectedVersion),
				mlog.Int64("currentVersion", task.GetVersion()))
			return true, nil
		}
		task.State = state
		task.FailReason = failReason
		task.KeptSegments = append([]int64(nil), keptSegments...)
		task.UpdatedSegments = cloneProtoSegments(updatedSegments)
		task.ResultReady = true
		if state == indexpb.JobState_JobStateFinished {
			task.Progress = 100
		}
		return false, nil
	})
	if applied {
		mlog.Info(m.ctx, "update task result success",
			mlog.Int64("taskID", taskID),
			mlog.String("state", state.String()),
			mlog.Int("keptSegments", len(cloned.GetKeptSegments())),
			mlog.Int("updatedSegments", len(cloned.GetUpdatedSegments())))
	}
	return applied, err
}

// ClearTaskResult clears stored task result payload after the owning job has
// persisted Finished. The task state/progress remain intact for progress and
// history queries until the job retention GC drops the task.
func (m *externalCollectionRefreshMeta) ClearTaskResult(taskID int64) error {
	applied, cloned, err := m.mutateTask(taskID, "clear task result", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		if len(task.GetKeptSegments()) == 0 && len(task.GetUpdatedSegments()) == 0 {
			return true, nil
		}
		task.KeptSegments = nil
		task.UpdatedSegments = nil
		return false, nil
	})
	if applied {
		mlog.Info(m.ctx, "clear task result success",
			mlog.Int64("taskID", taskID),
			mlog.String("state", cloned.GetState().String()))
	}
	return err
}

func (m *externalCollectionRefreshMeta) ClearTaskResultsByJobID(jobID int64) error {
	tasks := m.GetTasksByJobID(jobID)
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

// ==================== Aggregation Operations ====================

// AggregateJobStateFromTasks calculates job state and progress from its tasks
func (m *externalCollectionRefreshMeta) AggregateJobStateFromTasks(jobID int64) (state indexpb.JobState, progress int64) {
	tasks := m.GetTasksByJobID(jobID)
	if len(tasks) == 0 {
		return indexpb.JobState_JobStateNone, 0
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
	return state, progress
}
