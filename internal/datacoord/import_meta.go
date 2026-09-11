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
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/samber/lo"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type ImportMeta interface {
	AddJob(ctx context.Context, job ImportJob) error
	UpdateJob(ctx context.Context, jobID int64, actions ...UpdateJobAction) error
	GetJob(ctx context.Context, jobID int64) ImportJob
	GetJobBy(ctx context.Context, filters ...ImportJobFilter) []ImportJob
	CountJobBy(ctx context.Context, filters ...ImportJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error
	HandleCommitVchannel(ctx context.Context, jobID int64, vchannel string, callback func() error) error

	AddTask(ctx context.Context, task ImportTask) error
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateAction) error
	GetTask(ctx context.Context, taskID int64) ImportTask
	GetTaskBy(ctx context.Context, filters ...ImportTaskFilter) []ImportTask
	GetTaskByJob(ctx context.Context, jobID int64, filters ...ImportTaskFilter) []ImportTask
	RemoveTask(ctx context.Context, taskID int64) error
	TaskStatsJSON(ctx context.Context) string
}

type importTasks struct {
	tasks          map[int64]ImportTask
	taskIDsByJobID map[int64]map[int64]struct{}
	taskStats      *expirable.LRU[int64, ImportTask]
}

func newImportTasks() *importTasks {
	return &importTasks{
		tasks:          make(map[int64]ImportTask),
		taskIDsByJobID: make(map[int64]map[int64]struct{}),
		taskStats:      expirable.NewLRU[UniqueID, ImportTask](512, nil, time.Minute*30),
	}
}

func (t *importTasks) get(taskID int64) ImportTask {
	ret, ok := t.tasks[taskID]
	if !ok {
		return nil
	}
	return ret
}

func (t *importTasks) add(task ImportTask) {
	taskID := task.GetTaskID()
	jobID := task.GetJobID()
	if oldTask, ok := t.tasks[taskID]; ok && oldTask.GetJobID() != jobID {
		t.removeFromJob(oldTask.GetJobID(), taskID)
	}
	t.tasks[taskID] = task
	if _, ok := t.taskIDsByJobID[jobID]; !ok {
		t.taskIDsByJobID[jobID] = make(map[int64]struct{})
	}
	t.taskIDsByJobID[jobID][taskID] = struct{}{}
	t.taskStats.Add(taskID, task)
}

func (t *importTasks) remove(taskID int64) {
	task, ok := t.tasks[taskID]
	if ok {
		delete(t.tasks, taskID)
		t.removeFromJob(task.GetJobID(), taskID)
		t.taskStats.Add(task.GetTaskID(), task)
	}
}

func (t *importTasks) removeFromJob(jobID, taskID int64) {
	taskIDs := t.taskIDsByJobID[jobID]
	delete(taskIDs, taskID)
	if len(taskIDs) == 0 {
		delete(t.taskIDsByJobID, jobID)
	}
}

func (t *importTasks) listTasks() []ImportTask {
	return maps.Values(t.tasks)
}

func (t *importTasks) listTasksByJob(jobID int64) []ImportTask {
	taskIDs := t.taskIDsByJobID[jobID]
	tasks := make([]ImportTask, 0, len(taskIDs))
	for taskID := range taskIDs {
		if task, ok := t.tasks[taskID]; ok {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (t *importTasks) listTaskStats() []ImportTask {
	return t.taskStats.Values()
}

type importMeta struct {
	mu      lock.RWMutex // guards jobs and tasks
	ctx     context.Context
	jobs    map[int64]ImportJob
	tasks   *importTasks
	catalog metastore.DataCoordCatalog
}

func NewImportMeta(ctx context.Context, catalog metastore.DataCoordCatalog, alloc allocator.Allocator, meta *meta) (ImportMeta, error) {
	restoredPreImportTasks, err := catalog.ListPreImportTasks(ctx)
	if err != nil {
		return nil, err
	}
	restoredImportTasks, err := catalog.ListImportTasks(ctx)
	if err != nil {
		return nil, err
	}
	restoredJobs, err := catalog.ListImportJobs(ctx)
	if err != nil {
		return nil, err
	}

	tasks := newImportTasks()
	importMeta := &importMeta{ctx: ctx}

	for _, task := range restoredPreImportTasks {
		t := &preImportTask{
			alloc:      alloc,
			importMeta: importMeta,
			tr:         timerecord.NewTimeRecorder("preimport task"),
			times:      taskcommon.NewTimes(),
		}
		t.task.Store(task)
		tasks.add(t)
	}
	for _, task := range restoredImportTasks {
		t := &importTask{
			ctx:        ctx,
			alloc:      alloc,
			meta:       meta,
			importMeta: importMeta,
			tr:         timerecord.NewTimeRecorder("import task"),
			times:      taskcommon.NewTimes(),
		}
		t.task.Store(task)
		tasks.add(t)
	}

	jobs := make(map[int64]ImportJob)
	for _, job := range restoredJobs {
		jobs[job.GetJobID()] = &importJob{
			ImportJob: job,
			tr:        timerecord.NewTimeRecorder("import job"),
		}
	}

	importMeta.jobs = jobs
	importMeta.tasks = tasks
	importMeta.catalog = catalog
	return importMeta, nil
}

// replacePreImportRetryTask removes the old retry record and publishes its
// fresh-ID replacement in one catalog update. The old execution may remain on
// its DataNode, but its task ID no longer has a coordinator-side query path.
func (m *importMeta) replacePreImportRetryTask(ctx context.Context, oldTask, replacement *preImportTask) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	currentTask := m.tasks.get(oldTask.GetTaskID())
	if currentTask == nil || currentTask.GetType() != PreImportTaskType {
		return false, nil
	}
	current := currentTask.(*preImportTask)
	if current.GetState() != datapb.ImportTaskStateV2_Retry {
		return false, nil
	}
	job := m.jobs[current.GetJobID()]
	if job == nil || job.GetState() != internalpb.ImportJobState_PreImporting {
		return false, nil
	}

	if err := m.catalog.Update(ctx,
		metastore.DropPreImportTask(current.GetTaskID()),
		metastore.SavePreImportTask(replacement.task.Load())); err != nil {
		return false, err
	}

	m.tasks.remove(current.GetTaskID())
	m.tasks.add(replacement)
	return true, nil
}

// replaceRetryTask deletes the old task and creates a fresh task with fresh
// output segments in one catalog update. The old execution may still exist on
// its DataNode, but its task ID no longer has a coordinator-side commit path.
func (m *importMeta) replaceRetryTask(ctx context.Context, segmentMeta *meta, oldTask, replacement *importTask, newSegments []*SegmentInfo) (bool, error) {
	// HandleCommitVchannel already holds m.mu while its visibility callback
	// updates segment meta. Keep the same import-meta -> segment-meta lock order
	// here so planning/retry cannot form an ABBA cycle with commit.
	m.mu.Lock()
	defer m.mu.Unlock()

	currentTask := m.tasks.get(oldTask.GetTaskID())
	if currentTask == nil || currentTask.GetType() != ImportTaskType {
		return false, nil
	}
	current := currentTask.(*importTask)
	if current.GetState() != datapb.ImportTaskStateV2_Retry {
		return false, nil
	}
	// Import attempts are retryable only while their job is in the Importing
	// stage. Re-check under the same import-meta lock used for the swap: an
	// inspector may have read Retry just before the checker made the job
	// terminal, and must not publish a replacement for that stale snapshot.
	job := m.jobs[current.GetJobID()]
	if job == nil || job.GetState() != internalpb.ImportJobState_Importing {
		return false, nil
	}

	segmentMeta.segMu.Lock()
	defer segmentMeta.segMu.Unlock()

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	droppedSegments := make([]*SegmentInfo, 0, len(current.GetSegmentIDs()))
	for _, segmentID := range current.GetSegmentIDs() {
		segment := segmentMeta.segments.GetSegment(segmentID)
		if segment == nil {
			continue
		}
		updated := segment.Clone()
		updateSegStateAndPrepareMetrics(updated, commonpb.SegmentState_Dropped, metricMutation)
		updated.IsImporting = false
		droppedSegments = append(droppedSegments, updated)
	}

	newIDs := make([]int64, 0, len(newSegments))
	for _, segment := range newSegments {
		segmentID := segment.GetID()
		newIDs = append(newIDs, segmentID)
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(),
			segment.GetStorageVersion(), segmentMetricFormatLabel(segment), segment.GetNumOfRows())
	}

	replacementProto := replacement.task.Load()
	replacementProto.SegmentIDs = newIDs

	actions := make([]metastore.UpdateAction, 0, len(droppedSegments)+len(newSegments)+2)
	actions = append(actions, metastore.DropImportTask(current.GetTaskID()))
	for _, segment := range droppedSegments {
		actions = append(actions, metastore.AlterSegment(segment.SegmentInfo))
	}
	for _, segment := range newSegments {
		actions = append(actions, metastore.AddSegment(segment.SegmentInfo))
	}
	actions = append(actions, metastore.SaveImportTask(replacementProto))
	if err := m.catalog.Update(ctx, actions...); err != nil {
		return false, err
	}

	metricMutation.commit()
	for _, segment := range droppedSegments {
		segmentMeta.segments.SetSegment(segment.GetID(), segment)
	}
	for _, segment := range newSegments {
		segmentMeta.segments.SetSegment(segment.GetID(), segment)
	}
	m.tasks.remove(current.GetTaskID())
	m.tasks.add(replacement)

	// This object is no longer authoritative, but the scheduler still holds it
	// until the current callback returns. Make it terminal so the scheduler
	// releases it and performs the best-effort DataNode drop.
	oldTaskState := current.Clone().(*importTask)
	oldTaskState.task.Load().State = datapb.ImportTaskStateV2_Failed
	current.task.Store(oldTaskState.task.Load())
	return true, nil
}

// addImportTasks persists newly planned import tasks together with all of
// their segments, then publishes the same objects to the in-memory metadata.
// Nothing is added to memory when the catalog write fails.
func (m *importMeta) addImportTasks(ctx context.Context, segmentMeta *meta, tasks []ImportTask, segments []*SegmentInfo) error {
	// See replaceRetryTask: every operation that needs both locks takes import
	// meta first, matching HandleCommitVchannel's visibility callback.
	m.mu.Lock()
	defer m.mu.Unlock()
	segmentMeta.segMu.Lock()
	defer segmentMeta.segMu.Unlock()

	actions := make([]metastore.UpdateAction, 0, len(segments)+len(tasks))
	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	for _, segment := range segments {
		actions = append(actions, metastore.AddSegment(segment.SegmentInfo))
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(),
			segment.GetStorageVersion(), segmentMetricFormatLabel(segment), segment.GetNumOfRows())
	}
	for _, task := range tasks {
		actions = append(actions, metastore.SaveImportTask(task.(*importTask).task.Load()))
	}
	if err := m.catalog.Update(ctx, actions...); err != nil {
		return err
	}

	metricMutation.commit()
	for _, segment := range segments {
		segmentMeta.segments.SetSegment(segment.GetID(), segment)
	}
	for _, task := range tasks {
		m.tasks.add(task)
	}
	return nil
}

func (m *importMeta) AddJob(ctx context.Context, job ImportJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	originJob := m.jobs[job.GetJobID()]
	if originJob != nil {
		originJob := originJob.Clone()
		internalJob := originJob.(*importJob).ImportJob
		internalJob.ReadyVchannels = lo.Union(originJob.GetReadyVchannels(), job.GetReadyVchannels())
		job = originJob
	}
	err := m.catalog.SaveImportJob(ctx, job.(*importJob).ImportJob)
	if err != nil {
		return err
	}
	m.jobs[job.GetJobID()] = job
	return nil
}

func (m *importMeta) UpdateJob(ctx context.Context, jobID int64, actions ...UpdateJobAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job, ok := m.jobs[jobID]; ok {
		if job.GetState() == internalpb.ImportJobState_Completed ||
			job.GetState() == internalpb.ImportJobState_Failed {
			// import job is already completed or failed, no need to update
			return nil
		}
		updatedJob := job.Clone()
		for _, action := range actions {
			action(updatedJob)
		}
		// Once commit has started, some vchannels may already be visible. A
		// stale timeout/failure decision must not move the job back out of the
		// commit phase and strand the remaining vchannels.
		if job.GetState() == internalpb.ImportJobState_Committing &&
			updatedJob.GetState() == internalpb.ImportJobState_Failed {
			return nil
		}
		err := m.catalog.SaveImportJob(ctx, updatedJob.(*importJob).ImportJob)
		if err != nil {
			state := updatedJob.GetState()
			if (state == internalpb.ImportJobState_Completed || state == internalpb.ImportJobState_Failed) &&
				m.ctx != nil && m.ctx.Err() == nil {
				// A terminal write may already be durable even when its response is
				// lost. Fail-stop while holding m.mu so no stale in-memory state can
				// overwrite that authoritative result before the process exits.
				mlog.Fatal(m.ctx, "import terminal job publication failed; terminating process",
					mlog.FieldJobID(jobID),
					mlog.String("state", state.String()),
					mlog.Err(err))
			}
			return err
		}
		m.jobs[updatedJob.GetJobID()] = updatedJob
	}
	return nil
}

func (m *importMeta) GetJob(ctx context.Context, jobID int64) ImportJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[jobID]
}

func (m *importMeta) GetJobBy(ctx context.Context, filters ...ImportJobFilter) []ImportJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getJobBy(filters...)
}

func (m *importMeta) getJobBy(filters ...ImportJobFilter) []ImportJob {
	ret := make([]ImportJob, 0)
OUTER:
	for _, job := range m.jobs {
		for _, f := range filters {
			if !f(job) {
				continue OUTER
			}
		}
		ret = append(ret, job)
	}
	return ret
}

func (m *importMeta) CountJobBy(ctx context.Context, filters ...ImportJobFilter) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.getJobBy(filters...))
}

func (m *importMeta) RemoveJob(ctx context.Context, jobID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[jobID]; ok {
		err := m.catalog.DropImportJob(ctx, jobID)
		if err != nil {
			return err
		}
		delete(m.jobs, jobID)
	}
	return nil
}

func (m *importMeta) AddTask(ctx context.Context, task ImportTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch task.GetType() {
	case PreImportTaskType:
		err := m.catalog.SavePreImportTask(ctx, task.(*preImportTask).task.Load())
		if err != nil {
			return err
		}
		m.tasks.add(task)
	case ImportTaskType:
		err := m.catalog.SaveImportTask(ctx, task.(*importTask).task.Load())
		if err != nil {
			return err
		}
		m.tasks.add(task)
	}
	return nil
}

func (m *importMeta) UpdateTask(ctx context.Context, taskID int64, actions ...UpdateAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		updatedTask := task.Clone()
		for _, action := range actions {
			action(updatedTask)
		}
		var err error
		switch updatedTask.GetType() {
		case PreImportTaskType:
			err = m.catalog.SavePreImportTask(ctx, updatedTask.(*preImportTask).task.Load())
		case ImportTaskType:
			err = m.catalog.SaveImportTask(ctx, updatedTask.(*importTask).task.Load())
		}
		if err != nil {
			oldState := task.GetState()
			newState := updatedTask.GetState()
			activeToCompleted := oldState != datapb.ImportTaskStateV2_Completed &&
				oldState != datapb.ImportTaskStateV2_Failed &&
				newState == datapb.ImportTaskStateV2_Completed
			if activeToCompleted && m.ctx != nil && m.ctx.Err() == nil {
				// The completion write may already be durable even when its response is
				// lost. Fail-stop while holding m.mu so a timeout cannot publish Failed
				// from the stale in-memory task state.
				mlog.Fatal(m.ctx, "import task completion publication failed; terminating process",
					mlog.FieldTaskID(taskID),
					mlog.String("taskType", updatedTask.GetType().String()),
					mlog.Err(err))
			}
			return err
		}
		switch updatedTask.GetType() {
		case PreImportTaskType:
			task.(*preImportTask).task.Store(updatedTask.(*preImportTask).task.Load())
		case ImportTaskType:
			task.(*importTask).task.Store(updatedTask.(*importTask).task.Load())
		}
	}
	return nil
}

func (m *importMeta) GetTask(ctx context.Context, taskID int64) ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks.get(taskID)
}

func (m *importMeta) GetTaskBy(ctx context.Context, filters ...ImportTaskFilter) []ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return filterImportTasks(m.tasks.listTasks(), filters...)
}

func (m *importMeta) GetTaskByJob(ctx context.Context, jobID int64, filters ...ImportTaskFilter) []ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return filterImportTasks(m.tasks.listTasksByJob(jobID), filters...)
}

func filterImportTasks(tasks []ImportTask, filters ...ImportTaskFilter) []ImportTask {
	ret := make([]ImportTask, 0)
OUTER:
	for _, task := range tasks {
		for _, f := range filters {
			if !f(task) {
				continue OUTER
			}
		}
		ret = append(ret, task)
	}
	return ret
}

func (m *importMeta) RemoveTask(ctx context.Context, taskID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		switch task.GetType() {
		case PreImportTaskType:
			err := m.catalog.DropPreImportTask(ctx, taskID)
			if err != nil {
				return err
			}
		case ImportTaskType:
			err := m.catalog.DropImportTask(ctx, taskID)
			if err != nil {
				return err
			}
		}
		m.tasks.remove(taskID)
	}
	return nil
}

func (m *importMeta) TaskStatsJSON(ctx context.Context) string {
	tasks := m.tasks.listTaskStats()

	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}

func (m *importMeta) HandleCommitVchannel(ctx context.Context, jobID int64, vchannel string, callback func() error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job := m.jobs[jobID]
	if job == nil {
		return merr.WrapErrImportSysFailedMsg("job %d not found", jobID)
	}
	switch job.GetState() {
	case internalpb.ImportJobState_Uncommitted, internalpb.ImportJobState_Committing:
		// continue
	case internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed:
		return nil
	default:
		// Do not record committed_vchannels while the import task is still
		// importing. The caller must retry after the job becomes Uncommitted;
		// otherwise a later retry would treat this vchannel as committed even
		// though the visibility callback has not run.
		return merr.WrapErrImportSysFailedMsg("job %d is in state %s, waiting for Uncommitted", jobID, job.GetState())
	}
	if !lo.Contains(job.GetVchannels(), vchannel) {
		return merr.WrapErrImportSysFailedMsg("vchannel %s does not belong to import job %d", vchannel, jobID)
	}
	// Idempotency: if vchannel already committed, skip.
	for _, c := range job.GetCommittedVchannels() {
		if c == vchannel {
			return nil
		}
	}
	if job.GetState() == internalpb.ImportJobState_Uncommitted {
		updatedJob := job.Clone()
		updatedJob.(*importJob).State = internalpb.ImportJobState_Committing
		if err := m.catalog.SaveImportJob(ctx, updatedJob.(*importJob).ImportJob); err != nil {
			// The write may have reached catalog even when its response was lost.
			// Restart before an in-memory Uncommitted snapshot can overwrite the
			// durable Committing fence with a timeout failure.
			if ctx != nil && ctx.Err() == nil {
				mlog.Fatal(ctx, "import commit-phase publication failed; terminating process", mlog.Err(err))
			}
			return err
		}
		m.jobs[jobID] = updatedJob
		job = updatedJob
	}
	// Move the job into commit phase before making any segment visible, then
	// execute the callback before persisting the committed vchannel.
	// If callback fails, we return error without persisting committed_vchannels;
	// the caller retries and the callback will be invoked again. This avoids the
	// scenario where committed_vchannels is persisted but callback fails, causing
	// the idempotency check to skip the callback on retry (data stays invisible
	// forever).
	// The callback (setting is_importing=false) is idempotent, so re-execution on
	// retry after a persist failure is safe.
	//
	// Visibility ordering note: the callback clears segment meta (is_importing=false)
	// before this function persists job meta (committed_vchannels). Therefore a
	// vchannel's imported data can become visible before the job-level transition
	// to Completed (which happens later in checkCommittingJob once all vchannels
	// have been recorded here). This is inherent to per-vchannel commit fences —
	// 2PC for import is per-vchannel-atomic, not job-atomic. See MEP
	// (milvus-io/milvus-design-docs#29) "Segment Visibility" section.
	if err := callback(); err != nil {
		return err
	}
	updatedJob := job.Clone()
	updatedJob.(*importJob).CommittedVchannels = append(updatedJob.GetCommittedVchannels(), vchannel)
	if err := m.catalog.SaveImportJob(ctx, updatedJob.(*importJob).ImportJob); err != nil {
		return err
	}
	m.jobs[jobID] = updatedJob
	return nil
}
