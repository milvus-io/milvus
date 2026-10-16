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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// One scheduler task owns a segment range. Progress is volatile.
type snapshotExportCopyTask struct {
	manager    *snapshotExportManager
	request    *datapb.SnapshotExportCopyRequest
	mu         sync.Mutex
	times      map[taskcommon.TimeType]time.Time
	nextAction time.Time
	retryDelay time.Duration
}

var _ task.Task = (*snapshotExportCopyTask)(nil)

type snapshotExportProgress struct {
	copied int64
	total  int64
}

// Freeze only the pinned metadata identity; no object-store I/O belongs here.
func (m *snapshotExportManager) prepareWorkerRequest(ctx context.Context, job *datapb.ExportSnapshotJob) (*datapb.ExportSnapshotJob, error) {
	if err := ensureSnapshotExportCanAdvance(ctx, job); err != nil {
		return nil, err
	}
	if job.GetCopyRequest() != nil {
		return job, nil
	}
	info, err := m.snapshotManager.snapshotMeta.GetSnapshot(ctx, job.GetCollectionId(), job.GetSnapshotName())
	if err != nil {
		return nil, err
	}
	if info.GetId() <= 0 || info.GetS3Location() == "" || info.GetCollectionId() != job.GetCollectionId() {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot export source identity is incomplete")
	}
	req := &datapb.SnapshotExportCopyRequest{TaskSlot: Params.DataCoordCfg.SnapshotTaskSlotUsage.GetAsInt64(), CopyConcurrency: Params.DataCoordCfg.SnapshotCopyConcurrency.GetAsInt32(), ClusterId: Params.CommonCfg.ClusterPrefix.GetValue(), JobId: job.GetJobId(), DeadlineTime: job.GetDeadlineTime(), TargetUri: job.GetTargetS3Path(), ExternalSpec: job.GetExternalSpec(), SourceStorageConfig: snapshotstorage.StorageConfigFromInstance(Params), SnapshotMetadataPath: info.GetS3Location(), SnapshotId: info.GetId(), CollectionId: info.GetCollectionId()}
	if proto.Size(req)+32 > snapshotstorage.SnapshotExportMaxRequestBytes {
		return nil, merr.WrapErrServiceInternalMsg("snapshot export request exceeds size limit")
	}
	updated, _, err := m.meta.UpdateJob(ctx, job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
		if err := ensureSnapshotExportCanAdvance(ctx, latest); err != nil {
			return false, err
		}
		if latest.GetCopyRequest() != nil {
			return true, nil
		}
		latest.CopyRequest = req
		latest.CopyCursor, latest.CopiedFiles, latest.TotalFiles = 0, 0, 0
		latest.Progress = 5
		return false, nil
	})
	return updated, err
}

func (m *snapshotExportManager) copyOnWorker(ctx context.Context, job *datapb.ExportSnapshotJob) error {
	if job.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing {
		return nil
	}
	if m.scheduler == nil || m.cluster == nil {
		return merr.WrapErrServiceInternalMsg("snapshot export worker scheduler unavailable")
	}
	prepareCtx, cancel := m.withJobDeadline(ctx, job.GetJobId())
	job, err := m.prepareWorkerRequest(prepareCtx, job)
	cancel()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(snapshotExportReconcileInterval)
	defer ticker.Stop()
	for {
		if err := ensureSnapshotExportCanAdvance(ctx, job); err != nil {
			return err
		}
		if job.GetCopyRequest().GetGenerationId() == 0 {
			job, err = m.prepareExportGeneration(ctx, job)
			if err != nil {
				return err
			}
		}
		m.enqueueRetiredExportTasks(job)
		ready := true
		for _, shard := range job.GetCopyTasks() {
			if shard.GetResult() != nil && shard.GetNodeId() == 0 {
				continue
			}
			ready = false
			// Azure conditionally creates destinations and joins existing copies,
			// so ranges can run together without replacing another poller's CopyID.
			m.enqueueExportTask(job, shard)
		}
		if ready {
			return classifySnapshotExportPublicationError(ctx, m.finalizeExportGeneration(ctx, job))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		var ok bool
		job, ok = m.meta.GetJob(job.GetJobId())
		if !ok {
			return errSnapshotExportJobStopped
		}
		if job.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing {
			return nil
		}
	}
}

func (t *snapshotExportCopyTask) current() (*datapb.ExportSnapshotJob, *datapb.SnapshotExportCopyTask) {
	job, ok := t.manager.meta.GetJob(t.request.GetJobId())
	if !ok {
		return nil, nil
	}
	active := findExportTask(job, t.GetTaskID())
	if active == nil {
		return nil, nil
	}
	return job, active
}
func (t *snapshotExportCopyTask) GetTaskID() int64             { return t.request.GetTaskId() }
func (t *snapshotExportCopyTask) GetTaskType() taskcommon.Type { return taskcommon.SnapshotExportCopy }
func (t *snapshotExportCopyTask) GetTaskSlot() int64 {
	// Persisted requests created before task_slot was added consume one slot.
	return max(t.request.GetTaskSlot(), 1)
}
func (t *snapshotExportCopyTask) GetTaskVersion() int64 { return 1 }
func (t *snapshotExportCopyTask) SetTaskTime(kind taskcommon.TimeType, value time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.times[kind] = value
}
func (t *snapshotExportCopyTask) GetTaskTime(kind taskcommon.TimeType) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.times[kind]
}
func (t *snapshotExportCopyTask) GetTaskState() taskcommon.State {
	job, active := t.current()
	if active == nil || (active.GetNodeId() == 0 && (active.GetResult() != nil || isSnapshotExportTerminal(job.GetState()))) {
		return taskcommon.None
	}
	if isSnapshotExportTerminal(job.GetState()) {
		// Retain the scheduler entry and its backoff until remote cleanup is
		// acknowledged. The export job, not this cleanup task, is terminal.
		return taskcommon.InProgress
	}
	if active.GetNodeId() != 0 || job.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing {
		return taskcommon.InProgress
	}
	return taskcommon.Init
}

func (t *snapshotExportCopyTask) backoff() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.retryDelay == 0 {
		t.retryDelay = time.Second
	} else {
		t.retryDelay *= 2
	}
	if t.retryDelay > time.Minute {
		t.retryDelay = time.Minute
	}
	t.nextAction = time.Now().Add(t.retryDelay)
}
func (t *snapshotExportCopyTask) ready() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !time.Now().Before(t.nextAction)
}
func (t *snapshotExportCopyTask) resetBackoff() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retryDelay = 0
	t.nextAction = time.Time{}
}

func (t *snapshotExportCopyTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	_, _, err := t.manager.meta.UpdateJob(t.manager.ctx, t.request.GetJobId(), func(job *datapb.ExportSnapshotJob) (bool, error) {
		if err := ensureSnapshotExportCanAdvance(t.manager.ctx, job); err != nil {
			return false, err
		}
		a := findExportTask(job, t.GetTaskID())
		if a == nil || t.request.GetGenerationId() != job.GetCopyRequest().GetGenerationId() || isRetiredExportTask(job, t.GetTaskID()) || a.GetResult() != nil || a.GetTaskId() != t.GetTaskID() || a.GetNodeId() != 0 {
			return true, nil
		}
		a.NodeId = nodeID
		return false, nil
	})
	if err != nil {
		return
	}
	job, active := t.current()
	if active == nil || active.GetNodeId() != nodeID || active.GetResult() != nil || isRetiredExportTask(job, t.GetTaskID()) || job.GetCopyRequest().GetGenerationId() != t.request.GetGenerationId() || ensureSnapshotExportCanAdvance(t.manager.ctx, job) != nil {
		return
	}
	t.create(nodeID)
}
func (t *snapshotExportCopyTask) create(nodeID int64) {
	if err := t.manager.cluster.CreateSnapshotExportCopy(nodeID, t.request); err != nil {
		if errors.Is(err, merr.ErrServiceUnimplemented) || errors.Is(err, merr.ErrServiceInternal) || errors.Is(err, merr.ErrDataIntegrity) {
			t.fail(err)
		} else {
			t.backoff()
		}
	}
}
func (t *snapshotExportCopyTask) fail(err error) {
	if t.manager.ctx.Err() != nil {
		return
	}
	job, _ := t.current()
	if job != nil && !isRetiredExportTask(job, t.GetTaskID()) {
		t.manager.updateFailedJob(job.GetJobId(), sanitizeSnapshotExportReason(err, job.GetExternalSpec()), false, false, t.GetTaskID())
	}
}
func (t *snapshotExportCopyTask) QueryTaskOnWorker(cluster session.Cluster) {
	job, active := t.current()
	if active == nil || !t.ready() {
		return
	}
	if active.GetResult() != nil || isRetiredExportTask(job, t.GetTaskID()) || job.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing || isSnapshotExportTerminal(job.GetState()) {
		if !t.manager.cleanupExportCopyTask(job.GetJobId(), active, t.manager.cluster) {
			t.backoff()
		}
		return
	}
	if err := ensureSnapshotExportCanAdvance(t.manager.ctx, job); err != nil {
		t.fail(err)
		return
	}
	response, err := t.manager.cluster.QuerySnapshotExportCopy(active.GetNodeId(), active.GetTaskId())
	if err != nil {
		if errors.Is(err, merr.ErrNodeNotFound) || errors.Is(err, merr.ErrImportSysFailed) {
			// Query may finish after another reconciliation accepted or retired this
			// execution. Do not allocate or replace anything for a stale response.
			current, assigned := t.current()
			if assigned == nil || assigned.GetResult() != nil || assigned.GetNodeId() != active.GetNodeId() || isRetiredExportTask(current, t.GetTaskID()) || current.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting {
				return
			}
			ctx, cancel := t.manager.withJobDeadline(t.manager.ctx, job.GetJobId())
			defer cancel()
			newID, allocErr := t.manager.snapshotManager.allocator.AllocID(ctx)
			if allocErr != nil {
				t.backoff()
				return
			}
			_, _, updateErr := t.manager.meta.UpdateJob(t.manager.ctx, job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
				a := findExportTask(latest, t.GetTaskID())
				if latest.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting || a == nil || a.GetResult() != nil || a.GetNodeId() != active.GetNodeId() || isRetiredExportTask(latest, t.GetTaskID()) || latest.GetCopyRequest().GetGenerationId() != t.request.GetGenerationId() {
					return true, nil
				}
				if err := ensureSnapshotExportCanAdvance(ctx, latest); err != nil {
					return false, err
				}
				replaceExportTask(latest, t.GetTaskID(), newID)
				return false, nil
			})
			if updateErr != nil {
				t.backoff()
			}
		} else {
			t.backoff()
		}
		return
	}
	if response.GetTaskId() != t.GetTaskID() || response.GetCopiedFiles() < 0 || response.GetCopiedFiles() > response.GetTotalFiles() || response.GetCopiedBytes() < 0 {
		t.fail(merr.WrapErrDataIntegrityMsg("snapshot export worker result mismatch"))
		return
	}
	if response.GetState() == datapb.ImportTaskStateV2_Failed {
		t.fail(merr.WrapErrServiceInternalMsg("snapshot export worker failed: %s", response.GetReason()))
		return
	}
	if response.GetState() != datapb.ImportTaskStateV2_Completed {
		t.resetBackoff()
		t.manager.copyProgress.Store(snapshotExportProgressKey{job.GetJobId(), t.GetTaskID()}, snapshotExportProgress{copied: response.GetCopiedFiles(), total: response.GetTotalFiles()})
		return
	}
	if err := snapshotstorage.ValidateSnapshotExportResult(t.request, response); err != nil {
		t.fail(err)
		return
	}
	_, _, err = t.manager.meta.UpdateJob(t.manager.ctx, job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
		if err := ensureSnapshotExportCanAdvance(t.manager.ctx, latest); err != nil {
			return false, err
		}
		a := findExportTask(latest, t.GetTaskID())
		if a == nil || t.request.GetGenerationId() != latest.GetCopyRequest().GetGenerationId() || isRetiredExportTask(latest, t.GetTaskID()) || a.GetTaskId() != active.GetTaskId() || a.GetNodeId() != active.GetNodeId() {
			return true, nil
		}
		a.Result = proto.Clone(response).(*datapb.SnapshotExportCopyResponse)
		return false, nil
	})
	if err != nil {
		t.backoff()
	} else {
		t.resetBackoff()
		t.manager.copyProgress.Delete(snapshotExportProgressKey{job.GetJobId(), t.GetTaskID()})
	}
}
func (m *snapshotExportManager) cleanupExportCopyTask(jobID int64, active *datapb.SnapshotExportCopyTask, cluster session.SnapshotExportCluster) bool {
	if cluster == nil {
		return false
	}
	_, ok := m.meta.GetJob(jobID)
	if !ok {
		return true
	}
	if active.GetNodeId() != 0 {
		// Workers that reject this task type cannot own an export writer. Treat
		// their explicit capability error as absence, but retain cleanup on
		// ambiguous transport failures so live writers keep their source pins.
		if err := cluster.DropSnapshotExportCopy(active.GetNodeId(), active.GetTaskId()); err != nil &&
			!errors.Is(err, merr.ErrNodeNotFound) && !errors.Is(err, merr.ErrServiceUnimplemented) {
			return false
		}
	}
	_, _, err := m.meta.UpdateJob(m.ctx, jobID, func(job *datapb.ExportSnapshotJob) (bool, error) {
		a := findExportTask(job, active.GetTaskId())
		if a == nil || a.GetNodeId() != active.GetNodeId() {
			return true, nil
		}
		if isRetiredExportTask(job, a.GetTaskId()) {
			retained := job.RetiredCopyTasks[:0]
			for _, old := range job.RetiredCopyTasks {
				if old.GetTaskId() != a.GetTaskId() {
					retained = append(retained, old)
				}
			}
			job.RetiredCopyTasks = retained
		} else {
			if a.GetResult() == nil && !isSnapshotExportTerminal(job.GetState()) {
				return true, nil
			}
			a.NodeId = 0
		}
		return false, nil
	})
	if err == nil {
		m.copyProgress.Delete(snapshotExportProgressKey{jobID, active.GetTaskId()})
	}
	return err == nil
}
func (t *snapshotExportCopyTask) DropTaskOnWorker(cluster session.Cluster) {
	job, active := t.current()
	if active != nil {
		t.manager.cleanupExportCopyTask(job.GetJobId(), active, t.manager.cluster)
	}
}
