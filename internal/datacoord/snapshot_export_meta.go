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
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var errSnapshotExportJobPersistence = errors.New("snapshot export job metadata persistence failed")

type snapshotExportJobPersistenceError struct{ error }

func (e *snapshotExportJobPersistenceError) Unwrap() error { return e.error }

func (e *snapshotExportJobPersistenceError) Is(target error) bool {
	return target == errSnapshotExportJobPersistence
}

// snapshotExportJobView joins durable job and task records for local operations.
// Keep record named: embedding a protobuf would also expose its serialization API.
type snapshotExportJobView struct {
	record       *datapb.ExportSnapshotJob
	copyTasks    []*datapb.SnapshotExportCopyTask
	retiredTasks []*datapb.SnapshotExportCopyTask
}

func (job *snapshotExportJobView) clone() *snapshotExportJobView {
	if job == nil {
		return nil
	}
	clone := &snapshotExportJobView{record: proto.Clone(job.record).(*datapb.ExportSnapshotJob)}
	for _, task := range job.copyTasks {
		clone.copyTasks = append(clone.copyTasks, proto.Clone(task).(*datapb.SnapshotExportCopyTask))
	}
	for _, task := range job.retiredTasks {
		clone.retiredTasks = append(clone.retiredTasks, proto.Clone(task).(*datapb.SnapshotExportCopyTask))
	}
	return clone
}

type snapshotExportMeta struct {
	catalog metastore.DataCoordCatalog
	jobs    *typeutil.ConcurrentMap[int64, *datapb.ExportSnapshotJob]
	locks   *lock.KeyLock[int64]
	// mu protects coherent cache snapshots; it is never held during catalog I/O.
	mu    sync.RWMutex
	tasks map[int64]map[int64]*datapb.SnapshotExportCopyTask
	// jobID -> func(context.Context) error, serialized by the job lock. Keep
	// uncertain task mutations intact: a timed-out write may already be durable.
	pending sync.Map
}

func newSnapshotExportMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*snapshotExportMeta, error) {
	jobs, err := catalog.ListExportSnapshotJobs(ctx)
	if err != nil {
		return nil, merr.Wrap(err, "failed to load snapshot export jobs")
	}

	meta := &snapshotExportMeta{
		catalog: catalog,
		jobs:    typeutil.NewConcurrentMap[int64, *datapb.ExportSnapshotJob](),
		locks:   lock.NewKeyLock[int64](),
		tasks:   make(map[int64]map[int64]*datapb.SnapshotExportCopyTask),
	}
	for _, job := range jobs {
		if job == nil || job.GetJobId() == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("invalid snapshot export job record")
		}
		if _, exists := meta.jobs.Get(job.GetJobId()); exists {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate snapshot export job %d", job.GetJobId())
		}
		tasks, err := catalog.ListExportSnapshotTasks(ctx, job.GetJobId())
		if err != nil {
			return nil, merr.Wrap(err, "failed to load snapshot export tasks")
		}
		records := make(map[int64]*datapb.SnapshotExportCopyTask, len(tasks))
		for _, task := range tasks {
			if task.GetTaskId() <= 0 || records[task.GetTaskId()] != nil {
				return nil, merr.WrapErrDataIntegrityMsg("invalid or duplicate snapshot export task")
			}
			records[task.GetTaskId()] = proto.Clone(task).(*datapb.SnapshotExportCopyTask)
		}
		meta.tasks[job.GetJobId()] = records
		meta.jobs.Insert(job.GetJobId(), exportJobRecord(job))
	}
	mlog.Info(ctx, "snapshot export jobs loaded", mlog.Int("jobCount", len(jobs)))
	return meta, nil
}

func (m *snapshotExportMeta) CreateJob(ctx context.Context, job *snapshotExportJobView) error {
	if job == nil || job.record.GetJobId() == 0 {
		return merr.WrapErrServiceInternalMsg("snapshot export job is invalid")
	}

	m.locks.Lock(job.record.GetJobId())
	defer m.locks.Unlock(job.record.GetJobId())
	if _, ok := m.jobs.Get(job.record.GetJobId()); ok {
		return merr.WrapErrServiceInternalMsg("snapshot export job %d already exists", job.record.GetJobId())
	}

	record := exportJobRecord(job.record)
	tasks := exportTaskRecords(job)
	if err := m.save(ctx, job.record.GetJobId(), record, tasks, nil); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs.Insert(record.GetJobId(), record)
	m.tasks[record.GetJobId()] = make(map[int64]*datapb.SnapshotExportCopyTask)
	for _, task := range tasks {
		m.tasks[record.GetJobId()][task.GetTaskId()] = task
	}
	return nil
}

// Task lists exist only in the local view. Task polling asks for just its
// own record, avoiding a clone of every sibling on each scheduler tick.
func (m *snapshotExportMeta) GetJob(jobID int64) (*snapshotExportJobView, bool) {
	return m.getJob(jobID, 0)
}

func (m *snapshotExportMeta) getJob(jobID, taskID int64) (*snapshotExportJobView, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs.Get(jobID)
	if !ok {
		return nil, false
	}
	clone := &snapshotExportJobView{record: proto.Clone(job).(*datapb.ExportSnapshotJob)}
	if clone.record.CopyRequest != nil {
		clone.record.ExternalSpec = clone.record.CopyRequest.GetExternalSpec()
	}
	appendTask := func(task *datapb.SnapshotExportCopyTask) {
		if task == nil {
			return
		}
		t := proto.Clone(task).(*datapb.SnapshotExportCopyTask)
		if t.GetRetired() {
			clone.retiredTasks = append(clone.retiredTasks, t)
		} else {
			clone.copyTasks = append(clone.copyTasks, t)
		}
	}
	if taskID != 0 {
		appendTask(m.tasks[jobID][taskID])
	} else {
		for _, task := range m.tasks[jobID] {
			appendTask(task)
		}
		sort.Slice(clone.copyTasks, func(i, j int) bool {
			return clone.copyTasks[i].GetSegmentStart() < clone.copyTasks[j].GetSegmentStart()
		})
		sort.Slice(clone.retiredTasks, func(i, j int) bool {
			return clone.retiredTasks[i].GetTaskId() < clone.retiredTasks[j].GetTaskId()
		})
	}
	return clone, true
}

func (m *snapshotExportMeta) GetJobs() []*snapshotExportJobView {
	jobs := make([]*snapshotExportJobView, 0, m.jobs.Len())
	m.jobs.Range(func(id int64, _ *datapb.ExportSnapshotJob) bool {
		if job, ok := m.GetJob(id); ok {
			jobs = append(jobs, job)
		}
		return true
	})
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].record.GetStartTime() == jobs[j].record.GetStartTime() {
			return jobs[i].record.GetJobId() < jobs[j].record.GetJobId()
		}
		return jobs[i].record.GetStartTime() < jobs[j].record.GetStartTime()
	})
	return jobs
}

func (m *snapshotExportMeta) UpdateJob(
	ctx context.Context,
	jobID int64,
	mutate func(*snapshotExportJobView) (skip bool, err error),
) (*snapshotExportJobView, bool, error) {
	return m.updateJob(ctx, jobID, mutate)
}

func (m *snapshotExportMeta) TryUpdateJob(
	ctx context.Context,
	jobID int64,
	mutate func(*snapshotExportJobView) (skip bool, err error),
) (*snapshotExportJobView, bool, bool, error) {
	if !m.locks.TryLock(jobID) {
		return nil, false, false, nil
	}
	defer m.locks.Unlock(jobID)
	job, applied, err := m.updateJobLocked(ctx, jobID, 0, mutate)
	return job, true, applied, err
}

func (m *snapshotExportMeta) updateJob(
	ctx context.Context,
	jobID int64,
	mutate func(*snapshotExportJobView) (skip bool, err error),
) (*snapshotExportJobView, bool, error) {
	m.locks.Lock(jobID)
	defer m.locks.Unlock(jobID)
	return m.updateJobLocked(ctx, jobID, 0, mutate)
}

func (m *snapshotExportMeta) updateJobLocked(
	ctx context.Context,
	jobID, taskID int64,
	mutate func(*snapshotExportJobView) (skip bool, err error),
) (*snapshotExportJobView, bool, error) {
	if pending, ok := m.pending.Load(jobID); ok {
		if err := pending.(func(context.Context) error)(ctx); err != nil {
			return nil, false, err
		}
		m.pending.Delete(jobID)
		// The caller may have allocated a replacement from the old cache. Make
		// it reread the confirmed state instead of applying that stale mutation.
		return nil, false, &snapshotExportJobPersistenceError{error: merr.WrapErrServiceUnavailable("snapshot export metadata recovered; retry with refreshed state")}
	}
	job, ok := m.getJob(jobID, taskID)
	if !ok {
		return nil, false, merr.WrapErrServiceInternalMsg("snapshot export job %d not found", jobID)
	}
	clone := job.clone()
	skip, err := mutate(clone)
	if err != nil {
		return nil, false, err
	}
	if skip {
		return job.clone(), false, nil
	}
	before, after := exportJobRecord(job.record), exportJobRecord(clone.record)
	var jobUpdate *datapb.ExportSnapshotJob
	if !proto.Equal(before, after) {
		jobUpdate = after
	}
	previous := make(map[int64]*datapb.SnapshotExportCopyTask)
	for _, task := range exportTaskRecords(job) {
		previous[task.GetTaskId()] = task
	}
	var changed []*datapb.SnapshotExportCopyTask
	for _, task := range exportTaskRecords(clone) {
		if !proto.Equal(previous[task.GetTaskId()], task) {
			changed = append(changed, task)
		}
		delete(previous, task.GetTaskId())
	}
	if taskID != 0 {
		m.mu.RLock()
		collision := false
		for _, task := range changed {
			if task.GetTaskId() != taskID && m.tasks[jobID][task.GetTaskId()] != nil {
				collision = true
			}
		}
		m.mu.RUnlock()
		if collision {
			return nil, false, merr.WrapErrDataIntegrityMsg("snapshot export replacement task ID already exists")
		}
	}
	var removed []int64
	for id := range previous {
		removed = append(removed, id)
	}
	persist := func(ctx context.Context) error {
		if err := m.save(ctx, jobID, jobUpdate, changed, removed); err != nil {
			return err
		}
		m.mu.Lock()
		m.jobs.Insert(jobID, after)
		for _, task := range changed {
			m.tasks[jobID][task.GetTaskId()] = proto.Clone(task).(*datapb.SnapshotExportCopyTask)
		}
		for _, id := range removed {
			delete(m.tasks[jobID], id)
		}
		m.mu.Unlock()
		return nil
	}
	if err := persist(ctx); err != nil {
		if (len(changed) != 0 || len(removed) != 0) && errors.Is(err, errSnapshotExportJobPersistence) {
			m.pending.Store(jobID, persist)
		}
		return nil, false, err
	}
	return clone.clone(), true, nil
}

func (m *snapshotExportMeta) DropJob(ctx context.Context, jobID int64) error {
	m.locks.Lock(jobID)
	defer m.locks.Unlock(jobID)
	// Keep the job until its task prefix is removed; a restart can retry cleanup.
	if err := m.catalog.DropExportSnapshotTasks(ctx, jobID); err != nil {
		return merr.Wrap(err, "failed to drop snapshot export tasks")
	}
	if err := m.catalog.DropExportSnapshotJob(ctx, jobID); err != nil {
		return merr.Wrap(err, "failed to drop snapshot export job")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, jobID)
	m.jobs.Remove(jobID)
	m.pending.Delete(jobID)
	return nil
}

// UpdateTask shares the job lock with terminal/publication transitions. The
// callback sees only this task and may atomically replace it with a successor.
func (m *snapshotExportMeta) UpdateTask(ctx context.Context, jobID, taskID int64, mutate func(*snapshotExportJobView) (bool, error)) (*snapshotExportJobView, bool, error) {
	m.locks.Lock(jobID)
	defer m.locks.Unlock(jobID)
	return m.updateJobLocked(ctx, jobID, taskID, mutate)
}

func exportJobRecord(job *datapb.ExportSnapshotJob) *datapb.ExportSnapshotJob {
	record := proto.Clone(job).(*datapb.ExportSnapshotJob)
	if record.CopyRequest != nil {
		record.ExternalSpec = ""
	}
	return record
}

func exportTaskRecords(job *snapshotExportJobView) []*datapb.SnapshotExportCopyTask {
	records := make([]*datapb.SnapshotExportCopyTask, 0, len(job.copyTasks)+len(job.retiredTasks))
	for _, tasks := range [][]*datapb.SnapshotExportCopyTask{job.copyTasks, job.retiredTasks} {
		for _, task := range tasks {
			record := proto.Clone(task).(*datapb.SnapshotExportCopyTask)
			records = append(records, record)
		}
	}
	for i := len(job.copyTasks); i < len(records); i++ {
		records[i].Retired = true
	}
	return records
}

func (m *snapshotExportMeta) save(ctx context.Context, jobID int64, job *datapb.ExportSnapshotJob, tasks []*datapb.SnapshotExportCopyTask, removed []int64) error {
	// Leave headroom below etcd's default request limit, including keys and txn
	// framing. Planning writes one task at a time; replacement writes two.
	size := 128 * (1 + len(tasks) + len(removed))
	if job != nil {
		size += proto.Size(job)
	}
	for _, task := range tasks {
		size += proto.Size(task)
	}
	if size > 1<<20 {
		return merr.WrapErrServiceInternalMsg("snapshot export metadata exceeds size limit")
	}
	var err error
	switch {
	case len(tasks) != 0 || len(removed) != 0:
		err = m.catalog.SaveExportSnapshotTasks(ctx, jobID, job, tasks, removed)
	case job != nil:
		err = m.catalog.SaveExportSnapshotJob(ctx, job)
	default:
		return nil
	}
	if err == nil {
		return nil
	}
	// A permanent request-size error must not enter the persistence retry loop.
	// etcdKV.putEtcdMeta and executeTxn currently stringify the etcd cause
	// into ErrIoFailed. Recognize that exact server diagnostic too, without
	// changing the shared KV error contract or treating other I/O errors as final.
	if errors.Is(err, rpctypes.ErrRequestTooLarge) ||
		(errors.Is(err, merr.ErrIoFailed) && strings.Contains(err.Error(), rpctypes.ErrRequestTooLarge.Error())) {
		return merr.Wrap(err, "snapshot export metadata exceeds store limit")
	}
	return &snapshotExportJobPersistenceError{error: merr.Wrap(err, "failed to persist snapshot export metadata")}
}
