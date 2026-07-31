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

// errExternalRefreshJobTerminal is an in-process control-flow signal used when
// asynchronous task creation loses a race with a terminal job transition. The
// manager catches it before it can cross a component boundary.
var errExternalRefreshJobTerminal = errors.New("external refresh job is already terminal")

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

	// jobLock guards the whole job aggregate - the job record AND its tasks -
	// keyed by collectionID. One lock rather than a job/task pair: every
	// interesting transition (finish-with-apply, stale-manifest rebuild,
	// task-set creation, drop) spans both, so a two-tier scheme only bought a
	// lock-ordering rule and a window in which a job could be observed
	// half-transitioned. collectionID is the key because the registry
	// operations (AddJob, GetActiveJobByCollectionID) need collection-wide
	// exclusion to keep "at most one active job per collection"; a collection
	// holds at most one active job, so this is job-granular in practice.
	//
	// The lock is NOT reentrant, so anything reachable from inside a critical
	// section must not take it: helpers meant to run there are named
	// `...Locked`, and the read paths over the ConcurrentMap indexes (GetJob,
	// GetTask, GetTasksByJobID, ...) take no lock at all.
	jobLock *lock.KeyLock[UniqueID]

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
	committedTaskIDs := make(map[int64]int64)
	for _, job := range jobs {
		for _, taskID := range job.GetTaskIds() {
			committedTaskIDs[taskID] = job.GetJobId()
		}
	}

	// Load tasks
	tasks, err := m.catalog.ListExternalCollectionRefreshTasks(m.ctx)
	if err != nil {
		mlog.Error(m.ctx, "failed to load external collection refresh tasks", mlog.Err(err))
		return err
	}
	loadedTaskCount := 0
	ignoredTaskCount := 0
	for _, task := range tasks {
		jobID, committed := committedTaskIDs[task.GetTaskId()]
		if !committed {
			// Compatibility for historical records created before TaskIds became
			// the enforced visibility marker. A job that has already advanced out
			// of Init necessarily ran with its task set visible, so keep those
			// records recoverable. The partial-write case this fence addresses
			// leaves the pre-created job in Init with an empty TaskIds list.
			if legacyJob, ok := m.jobs.Get(task.GetJobId()); ok &&
				len(legacyJob.GetTaskIds()) == 0 &&
				legacyJob.GetState() != indexpb.JobState_JobStateInit {
				jobID = task.GetJobId()
				committed = true
			}
		}
		job, jobExists := m.jobs.Get(jobID)
		if !committed || !jobExists || task.GetJobId() != jobID ||
			(task.GetCollectionId() != 0 && task.GetCollectionId() != job.GetCollectionId()) {
			// Task records are written before the Job.task_ids commit marker when
			// a large create falls back to chunked etcd writes. A failed flush may
			// therefore leave a prefix of Task records behind. They are deliberately
			// invisible: only IDs published by the persisted Job are committed.
			// A later job retry allocates a fresh task set; stale records can be
			// garbage-collected independently because no scheduler/aggregator sees them.
			// TODO: add a best-effort catalog cleanup pass for these unreachable
			// records so repeated failed task-set creation cannot accumulate KV garbage.
			ignoredTaskCount++
			continue
		}
		m.tasks.Insert(task.GetTaskId(), task)
		m.addToJobTasks(task)
		loadedTaskCount++
	}

	mlog.Info(m.ctx, "externalCollectionRefreshMeta reloadFromKV done",
		mlog.Int("jobCount", len(jobs)),
		mlog.Int("taskCount", loadedTaskCount),
		mlog.Int("ignoredUncommittedTaskCount", ignoredTaskCount),
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

func cloneRefreshUpdatedSegments(segments []*datapb.SegmentInfo) []*datapb.SegmentInfo {
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

func cloneRefreshBaseManifests(baseManifests map[int64]string) map[int64]string {
	if len(baseManifests) == 0 {
		return nil
	}
	cloned := make(map[int64]string, len(baseManifests))
	for segmentID, manifest := range baseManifests {
		cloned[segmentID] = manifest
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

// isTerminalJobState reports whether a refresh job/task state is terminal.
func isTerminalJobState(state indexpb.JobState) bool {
	return state == indexpb.JobState_JobStateFinished || state == indexpb.JobState_JobStateFailed
}

// stampJobState applies a state transition to a job record. Every commit path
// goes through it so the terminal bookkeeping (EndTime, Progress=100) is
// defined in exactly one place.
func stampJobState(job *datapb.ExternalCollectionRefreshJob, state indexpb.JobState, failReason string) {
	job.State = state
	job.FailReason = failReason
	if isTerminalJobState(state) {
		job.EndTime = time.Now().UnixMilli()
		if state == indexpb.JobState_JobStateFinished {
			job.Progress = 100
		}
	}
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
		if isTerminalJobState(job.GetState()) {
			mlog.Info(m.ctx, "skip update job state, already in terminal state",
				mlog.Int64("jobID", jobID),
				mlog.String("currentState", job.GetState().String()),
				mlog.String("requestedState", state.String()))
			return true, nil
		}

		stampJobState(job, state, failReason)
		return false, nil
	})
	if applied {
		mlog.Info(m.ctx, "update job state success",
			mlog.Int64("jobID", jobID),
			mlog.String("state", state.String()))
	}
	return applied, err
}

// FinishJobWithApply runs the whole "every task is done -> apply the segment
// results -> commit Finished" transition as one critical section over the job
// aggregate: the terminal-state guard, the all-tasks-finished check, preApply,
// and - when preApply loses a manifest race - the rollback of those tasks to
// Init. Deriving the aggregate here rather than trusting one the caller computed
// outside the lock is what collapses the outcomes to two: a caller holding a
// pre-rollback snapshot stops at the aggregate check instead of reaching
// preApply and needing a separate "not ready" signal to back off.
//
// Returns (applied, err):
//   - (true,  nil)  Finished committed and preApply's segment update landed.
//   - (true,  err)  preApply failed for real; the job is persisted Failed and
//     the caller should run its job-failed cleanup.
//   - (false, nil)  nothing happened and nothing is owed: the job is gone or
//     already terminal, its tasks are not all finished, or the apply raced a
//     concurrent manifest commit and the tasks were just reset for a rebuild.
//   - (false, err)  lookup / persistence failure; a later tick retries.
func (m *externalCollectionRefreshMeta) FinishJobWithApply(
	jobID int64,
	preApply func(currentJob, finishedJob *datapb.ExternalCollectionRefreshJob) error,
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
	if isTerminalJobState(job.GetState()) {
		mlog.Info(m.ctx, "skip finish job with apply, already in terminal state",
			mlog.Int64("jobID", jobID),
			mlog.String("currentState", job.GetState().String()))
		return false, nil
	}

	// A concurrent rebuild may have reset some of these tasks to Init since the
	// caller sampled the aggregate; applying a partial result set would drop the
	// segments of the tasks being rebuilt.
	if state, _ := m.AggregateJobStateFromTasks(jobID); state != indexpb.JobState_JobStateFinished {
		mlog.Info(m.ctx, "skip finish job with apply, tasks are not all finished",
			mlog.Int64("jobID", jobID),
			mlog.String("aggregatedState", state.String()))
		return false, nil
	}

	cloneJob := proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)
	stampJobState(cloneJob, indexpb.JobState_JobStateFinished, "")

	if preApply != nil {
		// preApply owns the atomic catalog transaction that writes both segment
		// mutations and cloneJob as the Finished visibility marker.
		if err := preApply(job, cloneJob); err != nil {
			if errors.Is(err, errExternalRefreshStaleManifest) ||
				errors.Is(err, errExternalRefreshUnverifiableAttempt) ||
				errors.Is(err, errExternalRefreshTransientCommit) {
				// Roll the finished tasks back to Init in the SAME critical section,
				// so the job is never observable as "all tasks finished" on a result
				// set that was already rejected. It stays non-terminal on purpose:
				// the inspector re-dispatches, the worker rebuilds on the current
				// manifest, and tryTimeoutJob bounds the loop.
				//
				// errExternalRefreshTransientCommit joins the two CAS sentinels here
				// because it too says "this attempt did not land", not "this result
				// is wrong": a catalog write failed and the next tick may succeed.
				// Every other error stays terminal — a deterministic apply failure
				// would otherwise spin here until the job timeout with no signal.
				m.resetFinishedTasksLocked(jobID, err)
				return false, nil
			}
			cloneJob := proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)
			stampJobState(cloneJob, indexpb.JobState_JobStateFailed, err.Error())
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

	// Task result payloads deliberately remain intact. Clearing them before the
	// Finished job write would make an over-limit, chunked catalog update unsafe:
	// a prefix of task clears could persist while the terminal job marker fails.
	// After restart those tasks would still look Finished/ResultReady but no longer
	// carry the collection-wide apply input. The tasks are removed with the job by
	// retention GC, so keeping the payload is the simplest correct lifetime model.
	if preApply == nil {
		if err := m.catalog.SaveExternalCollectionRefreshJob(m.ctx, cloneJob); err != nil {
			mlog.Warn(m.ctx, "finish job with apply failed",
				mlog.Int64("jobID", jobID),
				mlog.Err(err))
			return false, err
		}
	}

	m.jobs.Insert(jobID, cloneJob)
	m.addToCollectionJobs(cloneJob)
	mlog.Info(m.ctx, "update job state success",
		mlog.Int64("jobID", jobID),
		mlog.String("state", indexpb.JobState_JobStateFinished.String()))
	return true, nil
}

// resetFinishedTasksLocked rolls every finished task of a job back to Init as a
// single composite catalog write, so the inspector re-dispatches them and the
// worker rebuilds on the current manifest. Caller MUST hold jobLock for the
// job's collection.
//
// It resets ALL finished tasks, not just the one that lost the CAS: the apply is
// one collection-wide update built from every task's results, so rebuilding a
// single task would still leave its siblings' results computed against the
// superseded manifest and the next apply would lose the same race.
//
// Per-task payload clearing mirrors ResetTaskForRetry - a re-dispatchable task
// must not leave a stale result behind for job aggregation to adopt, nor report
// Progress=100 while its rebuild is still running. No version fence is needed
// here (unlike ResetTaskForRetry, which runs unlocked from the scheduler path):
// every task mutator takes jobLock, so the task set cannot change underneath
// this. Version stays put; the re-dispatch bumps it in UpdateTaskVersion.
//
// A failed write leaves memory untouched and the job non-terminal, so a later
// tick re-runs the apply and retries the reset. A partial reset self-heals the
// same way: the still-finished siblings simply wait for the reset ones to
// rebuild before the aggregate reads Finished again.
func (m *externalCollectionRefreshMeta) resetFinishedTasksLocked(jobID int64, cause error) {
	const reason = "manifest advanced during apply; rebuilding on the current manifest"

	// GetTasksByJobID hands back clones, so these are safe to mutate in place.
	clones := make([]*datapb.ExternalCollectionRefreshTask, 0)
	actions := make([]metastore.UpdateAction, 0)
	for _, task := range m.GetTasksByJobID(jobID) {
		if task.GetState() != indexpb.JobState_JobStateFinished {
			continue
		}
		task.State = indexpb.JobState_JobStateInit
		task.FailReason = reason
		task.Progress = 0
		task.ResultReady = false
		task.KeptSegments = nil
		task.UpdatedSegments = nil
		task.BaseManifests = nil
		clones = append(clones, task)
		// ActionAdd on a refresh task is an upsert - the same encoding as
		// SaveExternalCollectionRefreshTask.
		actions = append(actions, metastore.AddRefreshTask(task))
	}
	if len(actions) == 0 {
		return
	}

	if err := m.catalog.Update(m.ctx, actions...); err != nil {
		mlog.Warn(m.ctx, "failed to reset refresh tasks for stale-manifest rebuild",
			mlog.Int64("jobID", jobID),
			mlog.Int("taskCount", len(actions)),
			mlog.Err(err))
		return
	}

	for _, task := range clones {
		m.tasks.Insert(task.GetTaskId(), task)
		m.addToJobTasks(task)
	}
	mlog.Warn(m.ctx, "external refresh apply raced a concurrent manifest commit; reset tasks to rebuild on the current manifest",
		mlog.Int64("jobID", jobID),
		mlog.Int("taskCount", len(clones)),
		mlog.Err(cause))
}

// UpdateJobProgress updates job progress
func (m *externalCollectionRefreshMeta) UpdateJobProgress(jobID int64, progress int64) error {
	_, err := m.mutateJob(jobID, "update job progress", func(job *datapb.ExternalCollectionRefreshJob) (bool, error) {
		job.Progress = progress
		return false, nil
	})
	return err
}

// AddTasksToJob persists a batch of newly-created tasks together with the
// job's updated TaskIds list as a single composite catalog write, then applies
// the in-memory bookkeeping of both. Doing it as 2N independent txns instead
// could leave the job's TaskIds disagreeing with the persisted task set on a
// partial failure.
//
// The job - the failover anchor for its tasks - is written LAST as the commit
// marker, mirroring DropJob and FinishJobWithApply: a persisted job always
// references only tasks that are themselves persisted. jobLock covers the job
// record and its tasks alike, so holding it across the whole
// compute -> catalog.Update -> in-memory apply sequence is what stops a
// concurrent task write from desyncing TaskIds from the task set. In-memory
// state is applied only after the write succeeds.
func (m *externalCollectionRefreshMeta) AddTasksToJob(jobID int64, tasks []*datapb.ExternalCollectionRefreshTask) error {
	job, ok := m.jobs.Get(jobID)
	if !ok {
		return merr.WrapErrServiceInternalMsg("job %d not found", jobID)
	}

	m.jobLock.Lock(job.GetCollectionId())
	defer m.jobLock.Unlock(job.GetCollectionId())

	// Re-fetch after lock so the persisted job carries the freshest TaskIds.
	job, ok = m.jobs.Get(jobID)
	if !ok {
		return merr.WrapErrServiceInternalMsg("job %d not found", jobID)
	}
	if isTerminalJobState(job.GetState()) {
		return errors.Wrapf(errExternalRefreshJobTerminal,
			"job %d reached %s before task creation committed", jobID, job.GetState())
	}

	// Append every new task ID to a job clone and persist that as the job
	// record, so on-disk TaskIds cover all saved tasks.
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
	m.jobLock.Lock(task.GetCollectionId())
	defer m.jobLock.Unlock(task.GetCollectionId())

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

// GetTasksByJobID returns all tasks for a job, sorted by taskID.
//
// Lock-free by design, like GetJob / GetTask / GetAllTasks: the indexes are
// ConcurrentMaps and every mutator clone-then-replaces a task, so each element
// returned is internally consistent. It must stay lock-free because
// FinishJobWithApply calls it while already holding jobLock, which is not
// reentrant. Callers that need the task set itself to be stable against a
// concurrent mutation must run inside jobLock themselves — which is exactly why
// the all-tasks-finished decision was moved in there.
func (m *externalCollectionRefreshMeta) GetTasksByJobID(jobID int64) []*datapb.ExternalCollectionRefreshTask {
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
// in-place mutation to a refresh task under the job aggregate's lock.
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

	// A task's collectionID is immutable, so reading it before the lock and
	// re-fetching the task inside is safe.
	m.jobLock.Lock(task.GetCollectionId())
	defer m.jobLock.Unlock(task.GetCollectionId())

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
		if state == indexpb.JobState_JobStateInProgress {
			job, ok := m.jobs.Get(task.GetJobId())
			if ok && isTerminalJobState(job.GetState()) {
				return false, errors.Wrapf(errExternalRefreshJobTerminal,
					"job %d reached %s before task %d entered InProgress",
					job.GetJobId(), job.GetState(), task.GetTaskId())
			}
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
		task.BaseManifests = nil
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
		task.UpdatedSegments = cloneRefreshUpdatedSegments(updatedSegments)
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
			mlog.Int("updatedSegments", len(cloned.GetUpdatedSegments())),
			mlog.Int("baseManifests", len(cloned.GetBaseManifests())))
	}
	return applied, err
}

// UpdateTaskVersion atomically reserves one worker attempt: the version, node,
// InProgress state, preallocated ID range, target path, and existing-segment
// manifest snapshot are one persisted fence. The ID range proves new-segment
// ownership; baseManifests is the coordinator-owned CAS expectation for patches.
func (m *externalCollectionRefreshMeta) UpdateTaskVersion(
	taskID, nodeID int64,
	idRange *datapb.IDRange,
	targetSegmentBase string,
	baseManifests map[int64]string,
) error {
	if idRange == nil || idRange.GetBegin() >= idRange.GetEnd() {
		return merr.WrapErrServiceInternalMsg("task %d has invalid preallocated ID range", taskID)
	}
	if targetSegmentBase == "" {
		return merr.WrapErrServiceInternalMsg("task %d has empty target segment base path", taskID)
	}
	applied, cloned, err := m.mutateTask(taskID, "update task version", func(task *datapb.ExternalCollectionRefreshTask) (bool, error) {
		job, ok := m.jobs.Get(task.GetJobId())
		if ok && isTerminalJobState(job.GetState()) {
			return false, errors.Wrapf(errExternalRefreshJobTerminal,
				"job %d reached %s before task %d attempt was reserved",
				job.GetJobId(), job.GetState(), task.GetTaskId())
		}
		task.Version++
		task.NodeId = nodeID
		task.State = indexpb.JobState_JobStateInProgress
		task.FailReason = ""
		task.PreallocatedSegmentIds = proto.Clone(idRange).(*datapb.IDRange)
		task.TargetSegmentBase = targetSegmentBase
		task.BaseManifests = cloneRefreshBaseManifests(baseManifests)
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
	if job, ok := m.jobs.Get(jobID); ok && len(job.GetTaskIds()) > 0 && len(tasks) != len(job.GetTaskIds()) {
		// A committed job must recover its complete authoritative task set.
		// Never aggregate a subset to Finished: doing so could apply incomplete
		// collection results. Leave it active so timeout/recovery surfaces the
		// metadata loss instead of silently dropping data.
		return indexpb.JobState_JobStateInit, 0
	}
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
