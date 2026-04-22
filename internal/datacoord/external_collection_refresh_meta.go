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
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
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
		log.Error("failed to load external collection refresh jobs", zap.Error(err))
		return err
	}
	for _, job := range jobs {
		m.jobs.Insert(job.GetJobId(), job)
		m.addToCollectionJobs(job)
	}

	// Load tasks
	tasks, err := m.catalog.ListExternalCollectionRefreshTasks(m.ctx)
	if err != nil {
		log.Error("failed to load external collection refresh tasks", zap.Error(err))
		return err
	}
	for _, task := range tasks {
		m.tasks.Insert(task.GetTaskId(), task)
		m.addToJobTasks(task)
	}

	log.Info("externalCollectionRefreshMeta reloadFromKV done",
		zap.Int("jobCount", len(jobs)),
		zap.Int("taskCount", len(tasks)),
		zap.Duration("duration", record.ElapseSpan()))
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

// ==================== Job Operations ====================

// AddJob adds a new job to meta
func (m *externalCollectionRefreshMeta) AddJob(job *datapb.ExternalCollectionRefreshJob) error {
	m.jobLock.Lock(job.GetCollectionId())
	defer m.jobLock.Unlock(job.GetCollectionId())

	log.Ctx(m.ctx).Info("add refresh job",
		zap.Int64("jobID", job.GetJobId()),
		zap.Int64("collectionID", job.GetCollectionId()),
		zap.String("collectionName", job.GetCollectionName()))

	if err := m.catalog.SaveExternalCollectionRefreshJob(m.ctx, job); err != nil {
		log.Warn("save refresh job failed",
			zap.Int64("jobID", job.GetJobId()),
			zap.Error(err))
		return err
	}

	m.jobs.Insert(job.GetJobId(), job)
	m.addToCollectionJobs(job)

	log.Info("add refresh job success",
		zap.Int64("jobID", job.GetJobId()),
		zap.Int64("collectionID", job.GetCollectionId()))
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
		return false, fmt.Errorf("job %d not found", jobID)
	}

	m.jobLock.Lock(job.GetCollectionId())
	defer m.jobLock.Unlock(job.GetCollectionId())

	// Re-fetch after lock
	job, ok = m.jobs.Get(jobID)
	if !ok {
		return false, fmt.Errorf("job %d not found", jobID)
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
		log.Warn(opName+" failed",
			zap.Int64("jobID", jobID),
			zap.Error(err))
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
			log.Info("skip update job state, already in terminal state",
				zap.Int64("jobID", jobID),
				zap.String("currentState", job.GetState().String()),
				zap.String("requestedState", state.String()))
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
		log.Info("update job state success",
			zap.Int64("jobID", jobID),
			zap.String("state", state.String()))
	}
	return applied, err
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

// DropJob removes a job and all its associated tasks
func (m *externalCollectionRefreshMeta) DropJob(ctx context.Context, jobID int64) error {
	job, ok := m.jobs.Get(jobID)
	if !ok {
		log.Ctx(ctx).Info("drop job success, job already not exist", zap.Int64("jobID", jobID))
		return nil
	}

	m.jobLock.Lock(job.GetCollectionId())
	defer m.jobLock.Unlock(job.GetCollectionId())

	// Re-fetch after lock
	job, ok = m.jobs.Get(jobID)
	if !ok {
		log.Ctx(ctx).Info("drop job success, job already not exist", zap.Int64("jobID", jobID))
		return nil
	}

	// Drop all associated tasks first
	if taskMap, ok := m.jobTasks.Get(jobID); ok {
		var dropErr error
		taskMap.Range(func(taskID int64, _ *datapb.ExternalCollectionRefreshTask) bool {
			if err := m.catalog.DropExternalCollectionRefreshTask(ctx, taskID); err != nil {
				log.Warn("drop task failed during job drop",
					zap.Int64("jobID", jobID),
					zap.Int64("taskID", taskID),
					zap.Error(err))
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
		log.Warn("drop job failed",
			zap.Int64("jobID", jobID),
			zap.Error(err))
		return err
	}

	m.jobs.Remove(jobID)
	m.removeFromCollectionJobs(job.GetCollectionId(), jobID)

	log.Info("drop job success",
		zap.Int64("jobID", jobID),
		zap.Int64("collectionID", job.GetCollectionId()))
	return nil
}

// ==================== Task Operations ====================

// AddTask adds a new task to meta
func (m *externalCollectionRefreshMeta) AddTask(task *datapb.ExternalCollectionRefreshTask) error {
	m.taskLock.Lock(task.GetJobId())
	defer m.taskLock.Unlock(task.GetJobId())

	log.Ctx(m.ctx).Info("add refresh task",
		zap.Int64("taskID", task.GetTaskId()),
		zap.Int64("jobID", task.GetJobId()),
		zap.Int64("collectionID", task.GetCollectionId()))

	if err := m.catalog.SaveExternalCollectionRefreshTask(m.ctx, task); err != nil {
		log.Warn("save refresh task failed",
			zap.Int64("taskID", task.GetTaskId()),
			zap.Error(err))
		return err
	}

	m.tasks.Insert(task.GetTaskId(), task)
	m.addToJobTasks(task)

	log.Info("add refresh task success",
		zap.Int64("taskID", task.GetTaskId()),
		zap.Int64("jobID", task.GetJobId()))
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
		return false, nil, fmt.Errorf("task %d not found", taskID)
	}

	m.taskLock.Lock(task.GetJobId())
	defer m.taskLock.Unlock(task.GetJobId())

	// Re-fetch after lock
	task, ok = m.tasks.Get(taskID)
	if !ok {
		return false, nil, fmt.Errorf("task %d not found", taskID)
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
		log.Warn(opName+" failed",
			zap.Int64("taskID", taskID),
			zap.Error(err))
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
		log.Info("update task state success",
			zap.Int64("taskID", taskID),
			zap.String("state", state.String()))
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

// UpdateTaskVersion updates task version and nodeID
func (m *externalCollectionRefreshMeta) UpdateTaskVersion(taskID, nodeID int64) error {
	applied, cloned, err := m.mutateTask(taskID, "update task version", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		task.Version++
		task.NodeId = nodeID
		return false, nil
	})
	if applied {
		log.Info("update task version success",
			zap.Int64("taskID", taskID),
			zap.Int64("nodeID", nodeID),
			zap.Int64("newVersion", cloned.GetVersion()))
	}
	return err
}

// DropTask removes a task
func (m *externalCollectionRefreshMeta) DropTask(ctx context.Context, taskID int64) error {
	task, ok := m.tasks.Get(taskID)
	if !ok {
		log.Ctx(ctx).Info("drop task success, task already not exist", zap.Int64("taskID", taskID))
		return nil
	}

	m.taskLock.Lock(task.GetJobId())
	defer m.taskLock.Unlock(task.GetJobId())

	task, ok = m.tasks.Get(taskID)
	if !ok {
		log.Ctx(ctx).Info("drop task success, task already not exist", zap.Int64("taskID", taskID))
		return nil
	}

	if err := m.catalog.DropExternalCollectionRefreshTask(ctx, taskID); err != nil {
		log.Warn("drop task failed",
			zap.Int64("taskID", taskID),
			zap.Error(err))
		return err
	}

	m.tasks.Remove(taskID)
	m.removeFromJobTasks(task.GetJobId(), taskID)

	log.Info("drop task success",
		zap.Int64("taskID", taskID),
		zap.Int64("jobID", task.GetJobId()))
	return nil
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
