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
	"io"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type snapshotExportProgressKey struct{ jobID, taskID int64 }

func findExportTask(job *snapshotExportJobView, id int64) *datapb.SnapshotExportCopyTask {
	if job == nil {
		return nil
	}
	for _, tasks := range [][]*datapb.SnapshotExportCopyTask{job.copyTasks, job.retiredTasks} {
		for _, task := range tasks {
			if task.GetTaskId() == id {
				return task
			}
		}
	}
	return nil
}

func isRetiredExportTask(job *snapshotExportJobView, id int64) bool {
	if job == nil {
		return false
	}
	for _, task := range job.retiredTasks {
		if task.GetTaskId() == id {
			return true
		}
	}
	return false
}

// Replace only the lost execution. Frozen source objects and deterministic target
// paths allow its range to be replayed without discarding other accepted results.
// A fresh task ID keeps delayed Drop calls and worker tombstones off the retry.
func replaceExportTask(job *snapshotExportJobView, oldID, newID int64) {
	if job == nil {
		return
	}
	for i, task := range job.copyTasks {
		if task.GetTaskId() == oldID {
			job.retiredTasks = append(job.retiredTasks, task)
			job.copyTasks[i] = &datapb.SnapshotExportCopyTask{
				TaskId: newID, SegmentStart: task.GetSegmentStart(), SegmentEnd: task.GetSegmentEnd(),
			}
			return
		}
	}
}

func (m *snapshotExportManager) prepareExportCopyPlan(ctx context.Context, job *snapshotExportJobView) (_ *snapshotExportJobView, err error) {
	ctx, cancel := m.withJobDeadline(ctx, job.record.GetJobId())
	defer cancel()
	req := snapshotExportExecutionRequest(job.record.GetCopyRequest())
	defer func() { err = redactSnapshotExportError(err, req) }()
	// A positive frozen partition size marks initialization, including empty jobs.
	// Completed plans need no repartitioning.
	if !job.record.GetCopyPlanComplete() && job.record.GetCopySegmentsPerTask() == 0 {
		// Only metadata.json is read here. Native and Avro manifests stay on DataNode.
		cfg := snapshotstorage.ObjectStorageConfigFromIndexConfig(req.GetSourceStorageConfig())
		cfg.SkipBucketCheck = true
		source, err := storage.NewRemoteChunkManager(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if closer, ok := source.UnderlyingObjectStorage().(io.Closer); ok {
			defer closer.Close()
		}
		snapshot, err := snapshotstorage.NewSnapshotReader(source).ReadSnapshot(ctx, req.GetSnapshotMetadataPath(), false)
		if err != nil {
			return nil, err
		}
		if snapshot.SnapshotInfo.GetId() != req.GetSnapshotId() || snapshot.SnapshotInfo.GetCollectionId() != req.GetCollectionId() {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot export source identity changed")
		}
		// Empty snapshots skip the copy workers, so reject unsupported schemas here
		// before either dispatch or coordinator-only metadata publication can occur.
		if typeutil.IsExternalCollection(snapshot.Collection.GetSchema()) {
			return nil, merr.WrapErrParameterInvalidMsg("exporting external collections is not supported")
		}
		limit := Params.DataCoordCfg.MaxSegmentsPerCopyTask.GetAsInt()
		if limit <= 0 {
			return nil, merr.WrapErrParameterInvalidMsg("maxSegmentsPerCopyTask must be positive")
		}
		// TEXT uses the same ranges. Shared LOBs may be copied by several tasks;
		// sources are immutable and dropping an Export task never deletes objects.
		job, _, err = m.meta.UpdateJob(ctx, job.record.GetJobId(), func(latest *snapshotExportJobView) (bool, error) {
			if err := ensureSnapshotExportCanAdvance(ctx, latest); err != nil {
				return false, err
			}
			if latest.record.GetCopyPlanComplete() || latest.record.GetCopySegmentsPerTask() != 0 {
				return true, nil
			}
			latest.record.CopySegmentCount = int64(len(snapshot.ManifestPaths))
			latest.record.CopySegmentsPerTask = int64(limit)
			return false, nil
		})
		if err != nil {
			return nil, err
		}
	}
	// Like Restore, retain successfully persisted ranges and create only gaps.
	// Unlike Restore, dispatch waits for the complete plan to be durable.
	if job.record.GetCopyPlanComplete() {
		return job, nil
	}
	limit, count := job.record.GetCopySegmentsPerTask(), job.record.GetCopySegmentCount()
	if limit <= 0 || count < 0 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid snapshot export copy plan")
	}
	covered := make(map[int64]struct{}, len(job.copyTasks))
	for _, task := range job.copyTasks {
		if task.GetSegmentStart() < 0 || task.GetSegmentStart() >= count || task.GetSegmentStart()%limit != 0 || task.GetSegmentEnd() != task.GetSegmentStart()+min(limit, count-task.GetSegmentStart()) {
			return nil, merr.WrapErrDataIntegrityMsg("invalid persisted export range")
		}
		if _, exists := covered[task.GetSegmentStart()]; exists {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate export range")
		}
		covered[task.GetSegmentStart()] = struct{}{}
	}
	for start := int64(0); start < count; {
		end := start + min(limit, count-start)
		if _, exists := covered[start]; !exists {
			id, err := m.snapshotManager.allocator.AllocID(ctx)
			if err != nil {
				return nil, err
			}
			_, _, err = m.meta.UpdateTask(ctx, job.record.GetJobId(), id, func(latest *snapshotExportJobView) (bool, error) {
				if err := ensureSnapshotExportCanAdvance(ctx, latest); err != nil {
					return false, err
				}
				latest.copyTasks = append(latest.copyTasks, &datapb.SnapshotExportCopyTask{TaskId: id, SegmentStart: start, SegmentEnd: end})
				return false, nil
			})
			if err != nil {
				return nil, err
			}
		}
		start = end
	}
	updated, _, err := m.meta.UpdateJob(ctx, job.record.GetJobId(), func(latest *snapshotExportJobView) (bool, error) {
		if err := ensureSnapshotExportCanAdvance(ctx, latest); err != nil {
			return false, err
		}
		if err := validateExportCopyPlan(latest); err != nil {
			return false, err
		}
		latest.record.CopyPlanComplete = true
		return false, nil
	})
	if err == nil {
		m.inspector.Wake()
	}
	return updated, err
}

func validateExportCopyPlan(job *snapshotExportJobView) error {
	next := int64(0)
	for _, task := range job.copyTasks {
		if task.GetTaskId() <= 0 || task.GetSegmentStart() != next || task.GetSegmentEnd() <= next || task.GetSegmentEnd() > job.record.GetCopySegmentCount() {
			return merr.WrapErrDataIntegrityMsg("snapshot export tasks do not cover the copy plan")
		}
		next = task.GetSegmentEnd()
	}
	if next != job.record.GetCopySegmentCount() {
		return merr.WrapErrDataIntegrityMsg("snapshot export copy plan is incomplete")
	}
	return nil
}

// Completed shard results are durable; running counts remain volatile.
func (m *snapshotExportManager) exportShardProgress(job *snapshotExportJobView) (copied, total int64) {
	for _, task := range job.copyTasks {
		if result := task.GetResult(); result != nil {
			copied += result.GetCopiedFiles()
			total += result.GetTotalFiles()
			continue
		}
		if value, ok := m.copyProgress.Load(snapshotExportProgressKey{job.record.GetJobId(), task.GetTaskId()}); ok {
			p := value.(snapshotExportProgress)
			copied += p.copied
			total += p.total
		}
	}
	return
}

// Finalization runs in the bounded export-job goroutine, not a worker task.
// Persist Publishing only after staging readback succeeds. Before that durable
// transition a restart may rebuild staging; afterwards publication is source-free.
func (m *snapshotExportManager) finalizeExportCopyPlan(ctx context.Context, job *snapshotExportJobView) (err error) {
	ctx, cancel := m.withJobDeadline(ctx, job.record.GetJobId())
	defer cancel()
	req := snapshotExportExecutionRequest(job.record.GetCopyRequest())
	defer func() { err = redactSnapshotExportError(err, req) }()
	cfg := snapshotstorage.ObjectStorageConfigFromIndexConfig(req.GetSourceStorageConfig())
	cfg.SkipBucketCheck = true
	source, err := storage.NewRemoteChunkManager(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeExportStorageClient(source)
	resolved, err := snapshotstorage.ResolveForeignStorage(ctx, cfg, snapshotstorage.DirectionExport, req.GetTargetUri(), req.GetExternalSpec())
	if err != nil {
		return err
	}
	defer closeExportStorageClient(resolved.ForeignCM)
	result, err := snapshotstorage.FinalizeSnapshotExport(ctx, req, job.copyTasks, source, resolved)
	if err != nil {
		return err
	}
	_, _, err = m.meta.UpdateJob(ctx, job.record.GetJobId(), func(latest *snapshotExportJobView) (bool, error) {
		if err := ensureSnapshotExportCanAdvance(ctx, latest); err != nil {
			return false, err
		}
		if !latest.record.GetCopyPlanComplete() {
			return false, errSnapshotExportJobStopped
		}
		if err := validateExportCopyPlan(latest); err != nil {
			return false, err
		}
		for _, task := range latest.copyTasks {
			if task.GetResult() == nil || task.GetNodeId() != 0 {
				return false, errSnapshotExportJobStopped
			}
		}
		latest.record.State = datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing
		latest.record.TargetS3Path = req.GetTargetUri()
		latest.record.SnapshotMetadataUri = result.MetadataURI
		latest.record.TotalBytes, latest.record.TotalFiles = result.TotalBytes, result.TotalFiles
		latest.record.CopyCursor, latest.record.CopiedFiles = result.TotalFiles, result.TotalFiles
		latest.record.Progress = 99
		return false, nil
	})
	return err
}

func closeExportStorageClient(cm storage.ChunkManager) {
	if remote, ok := cm.(*storage.RemoteChunkManager); ok {
		if closer, ok := remote.UnderlyingObjectStorage().(io.Closer); ok {
			closer.Close()
		}
	}
}
