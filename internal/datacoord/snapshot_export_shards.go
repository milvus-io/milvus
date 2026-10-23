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
	"time"

	"google.golang.org/protobuf/proto"

	snapshotadapter "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	snapshotstorage "github.com/milvus-io/milvus/pkg/v3/snapshotio"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type snapshotExportProgressKey struct{ jobID, taskID int64 }

func findExportTask(job *datapb.ExportSnapshotJob, id int64) *datapb.SnapshotExportCopyTask {
	for _, tasks := range [][]*datapb.SnapshotExportCopyTask{job.GetCopyTasks(), job.GetRetiredCopyTasks()} {
		for _, task := range tasks {
			if task.GetTaskId() == id {
				return task
			}
		}
	}
	return nil
}
func isRetiredExportTask(job *datapb.ExportSnapshotJob, id int64) bool {
	for _, task := range job.GetRetiredCopyTasks() {
		if task.GetTaskId() == id {
			return true
		}
	}
	return false
}

// Replace only the lost execution. Frozen source objects and deterministic target
// paths allow its range to be replayed without discarding other accepted results.
// A fresh task ID keeps delayed Drop calls and worker tombstones off the retry.
func replaceExportTask(job *datapb.ExportSnapshotJob, oldID, newID int64) {
	for i, task := range job.GetCopyTasks() {
		if task.GetTaskId() == oldID {
			job.RetiredCopyTasks = append(job.RetiredCopyTasks, task)
			job.CopyTasks[i] = &datapb.SnapshotExportCopyTask{
				TaskId: newID, SegmentStart: task.GetSegmentStart(), SegmentEnd: task.GetSegmentEnd(),
			}
			return
		}
	}
}

func (m *snapshotExportManager) prepareExportGeneration(ctx context.Context, job *datapb.ExportSnapshotJob) (*datapb.ExportSnapshotJob, error) {
	ctx, cancel := m.withJobDeadline(ctx, job.GetJobId())
	defer cancel()
	req := job.GetCopyRequest()
	generation, err := m.snapshotManager.allocator.AllocID(ctx)
	if err != nil {
		return nil, err
	}
	// Only metadata.json is read here. Native and Avro manifests stay on DataNode.
	cfg := snapshotadapter.ObjectStorageConfigFromIndexConfig(req.GetSourceStorageConfig())
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
	var shards []*datapb.SnapshotExportCopyTask
	for start := 0; start < len(snapshot.ManifestPaths); start += limit {
		id, err := m.snapshotManager.allocator.AllocID(ctx)
		if err != nil {
			return nil, err
		}
		shards = append(shards, &datapb.SnapshotExportCopyTask{TaskId: id, SegmentStart: int64(start), SegmentEnd: int64(min(start+limit, len(snapshot.ManifestPaths)))})
	}
	updated, _, err := m.meta.UpdateJob(ctx, job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
		if err := ensureSnapshotExportCanAdvance(ctx, latest); err != nil {
			return false, err
		}
		if latest.GetCopyRequest().GetGenerationId() != 0 {
			return true, nil
		}
		latest.CopyRequest.GenerationId = generation
		latest.CopyTasks = shards
		return false, nil
	})
	return updated, err
}

func (m *snapshotExportManager) enqueueExportTask(job *datapb.ExportSnapshotJob, task *datapb.SnapshotExportCopyTask) {
	req := proto.Clone(job.GetCopyRequest()).(*datapb.SnapshotExportCopyRequest)
	req.TaskId = task.GetTaskId()
	req.SegmentStart = task.GetSegmentStart()
	req.SegmentEnd = task.GetSegmentEnd()
	m.scheduler.Enqueue(&snapshotExportCopyTask{manager: m, request: req, times: make(map[taskcommon.TimeType]time.Time)})
}

func (m *snapshotExportManager) enqueueRetiredExportTasks(job *datapb.ExportSnapshotJob) {
	for _, task := range job.GetRetiredCopyTasks() {
		m.scheduler.Enqueue(&snapshotExportCopyTask{manager: m, request: &datapb.SnapshotExportCopyRequest{JobId: job.GetJobId(), TaskId: task.GetTaskId()}, times: make(map[taskcommon.TimeType]time.Time)})
	}
}

// Completed shard results are durable; running counts remain volatile.
func (m *snapshotExportManager) exportShardProgress(job *datapb.ExportSnapshotJob) (copied, total int64) {
	for _, task := range job.GetCopyTasks() {
		if result := task.GetResult(); result != nil {
			copied += result.GetCopiedFiles()
			total += result.GetTotalFiles()
			continue
		}
		if value, ok := m.copyProgress.Load(snapshotExportProgressKey{job.GetJobId(), task.GetTaskId()}); ok {
			p := value.(snapshotExportProgress)
			copied += p.copied
			total += p.total
		}
	}
	return
}

// Cleanup is independent of successful publication for isolated old generations.
func (m *snapshotExportManager) enqueueExportCleanup(job *datapb.ExportSnapshotJob) bool {
	pending := false
	for _, tasks := range [][]*datapb.SnapshotExportCopyTask{job.GetCopyTasks(), job.GetRetiredCopyTasks()} {
		for _, task := range tasks {
			m.copyProgress.Delete(snapshotExportProgressKey{job.GetJobId(), task.GetTaskId()})
			if task.GetNodeId() == 0 {
				continue
			}
			pending = true
			if m.scheduler != nil {
				m.scheduler.Enqueue(&snapshotExportCopyTask{manager: m, request: &datapb.SnapshotExportCopyRequest{JobId: job.GetJobId(), TaskId: task.GetTaskId()}, times: make(map[taskcommon.TimeType]time.Time)})
			}
		}
	}
	return pending
}

// Finalization runs in the bounded export-job goroutine, not a worker task.
// Persist Publishing only after staging readback succeeds. Before that durable
// transition a restart may rebuild staging; afterwards publication is source-free.
func (m *snapshotExportManager) finalizeExportGeneration(ctx context.Context, job *datapb.ExportSnapshotJob) error {
	ctx, cancel := m.withJobDeadline(ctx, job.GetJobId())
	defer cancel()
	req := job.GetCopyRequest()
	cfg := snapshotadapter.ObjectStorageConfigFromIndexConfig(req.GetSourceStorageConfig())
	cfg.SkipBucketCheck = true
	source, err := storage.NewRemoteChunkManager(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeExportStorageClient(source)
	resolved, err := snapshotadapter.ResolveForeignStorage(ctx, cfg, snapshotstorage.DirectionExport, snapshotstorage.SnapshotExportAttemptURI(req), req.GetExternalSpec())
	if err != nil {
		return err
	}
	defer closeExportStorageClient(resolved.ForeignCM)
	result, err := snapshotstorage.FinalizeSnapshotExport(ctx, req, job.GetCopyTasks(), source, resolved)
	if err != nil {
		return err
	}
	_, _, err = m.meta.UpdateJob(ctx, job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
		if err := ensureSnapshotExportCanAdvance(ctx, latest); err != nil {
			return false, err
		}
		if latest.GetCopyRequest().GetGenerationId() != req.GetGenerationId() {
			return false, errSnapshotExportJobStopped
		}
		for _, task := range latest.GetCopyTasks() {
			if task.GetResult() == nil || task.GetNodeId() != 0 {
				return false, errSnapshotExportJobStopped
			}
		}
		latest.State = datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing
		latest.TargetS3Path = snapshotstorage.SnapshotExportAttemptURI(req)
		latest.SnapshotMetadataUri = result.MetadataURI
		latest.TotalBytes, latest.TotalFiles = result.TotalBytes, result.TotalFiles
		latest.CopyCursor, latest.CopiedFiles = result.TotalFiles, result.TotalFiles
		latest.Progress = 99
		return false, nil
	})
	return err
}

func closeExportStorageClient(cm snapshotstorage.ChunkManager) {
	if remote, ok := cm.(*storage.RemoteChunkManager); ok {
		if closer, ok := remote.UnderlyingObjectStorage().(io.Closer); ok {
			closer.Close()
		}
	}
}
