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
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	snapshotExportReconcileInterval = time.Second
	snapshotExportPinSafetyMargin   = 5 * time.Minute
	snapshotExportNamespaceSubPath  = "exports"
)

var (
	errSnapshotExportJobStopped         = errors.New("snapshot export job is no longer executing")
	errSnapshotExportPublicationPending = errors.New("snapshot export metadata publication is pending")
)

type snapshotExportPublicationPendingError struct{ error }

func (e *snapshotExportPublicationPendingError) Unwrap() error { return e.error }

func (e *snapshotExportPublicationPendingError) Is(target error) bool {
	return target == errSnapshotExportPublicationPending
}

type snapshotExportManager struct {
	ctx             context.Context
	cancel          context.CancelFunc
	meta            *snapshotExportMeta
	snapshotManager *snapshotManager
	scheduler       task.GlobalScheduler
	cluster         session.SnapshotExportCluster
	copyProgress    sync.Map
	checker         *snapshotExportChecker
	inspector       *snapshotExportInspector

	wakeCh    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup

	runningMu sync.Mutex
	running   map[int64]context.CancelFunc

	targetMu    sync.Mutex
	targetLocks map[snapshotExportTarget]*snapshotExportTargetLock
}

type snapshotExportTargetLock struct {
	semaphore chan struct{}
	refs      int
}

func newSnapshotExportManager(
	ctx context.Context,
	meta *snapshotExportMeta,
	snapshotManager *snapshotManager,
) *snapshotExportManager {
	managerCtx, cancel := context.WithCancel(ctx)
	m := &snapshotExportManager{
		ctx:             managerCtx,
		cancel:          cancel,
		meta:            meta,
		snapshotManager: snapshotManager,
		wakeCh:          make(chan struct{}, 1),
		running:         make(map[int64]context.CancelFunc),
		targetLocks:     make(map[snapshotExportTarget]*snapshotExportTargetLock),
	}
	m.checker = &snapshotExportChecker{manager: m}
	m.inspector = &snapshotExportInspector{manager: m, cleanups: make(map[int64]*snapshotExportCopyTask), wakeCh: make(chan struct{}, 1)}
	return m
}

func (m *snapshotExportManager) Start() {
	m.startOnce.Do(func() {
		m.wg.Add(1)
		go m.checker.run()
		m.inspector.Start()
	})
}

func (m *snapshotExportManager) Close() {
	m.closeOnce.Do(func() {
		m.cancel()
		m.wg.Wait()
	})
}

func (m *snapshotExportManager) Wake() {
	m.inspector.Wake()
	select {
	case m.wakeCh <- struct{}{}:
	default:
	}
}

func (m *snapshotExportManager) Submit(
	ctx context.Context,
	collectionID int64,
	snapshotName string,
	dbName string,
	collectionName string,
	targetPath string,
	externalSpec string,
) (int64, error) {
	if strings.TrimSpace(targetPath) == "" {
		return 0, merr.WrapErrParameterMissingMsg("target_s3_path is required")
	}
	// Reject oversized user fields before allocating or pinning a job. The full
	// worker request is checked separately because it also carries server config.
	input := &datapb.SnapshotExportCopyRequest{TargetUri: targetPath, ExternalSpec: externalSpec}
	if proto.Size(input) > snapshotstorage.SnapshotExportMaxRequestBytes {
		return 0, merr.WrapErrParameterInvalidMsg("snapshot export target path and external spec exceed %d bytes", snapshotstorage.SnapshotExportMaxRequestBytes)
	}
	instanceCfg := snapshotstorage.InstanceConfigFromParamtable(Params)
	if err := snapshotstorage.ValidateForeignStorageRequest(
		instanceCfg,
		snapshotstorage.DirectionExport,
		targetPath,
		externalSpec,
	); err != nil {
		return 0, err
	}
	if _, err := m.snapshotManager.snapshotMeta.GetSnapshot(ctx, collectionID, snapshotName); err != nil {
		return 0, err
	}
	jobID, err := m.snapshotManager.allocator.AllocID(ctx)
	if err != nil {
		return 0, merr.Wrap(err, "failed to allocate snapshot export job ID")
	}
	exportNamespace, err := uuid.NewRandom()
	if err != nil {
		return 0, merr.Wrap(err, "failed to generate snapshot export namespace")
	}
	// Persist the effective bundle root before starting any object-store work so
	// retries and recovery always reuse the same cross-cluster-safe namespace.
	targetPath = namespacedSnapshotExportTarget(targetPath, exportNamespace.String())

	timeout := Params.DataCoordCfg.SnapshotExportJobTimeout.GetAsDuration(time.Second)
	pinTTL := Params.DataCoordCfg.SnapshotRestorePinTTLSeconds.GetAsInt64()
	exportPinTTL := int64((timeout + snapshotExportPinSafetyMargin + time.Second - 1) / time.Second)
	if exportPinTTL > pinTTL {
		pinTTL = exportPinTTL
	}
	pinID, activePins, err := m.snapshotManager.snapshotMeta.PinSnapshot(
		ctx,
		collectionID,
		snapshotName,
		pinTTL,
	)
	if err != nil {
		return 0, merr.Wrap(err, "failed to pin source snapshot for export")
	}
	setSnapshotActivePinsGauge(collectionID, snapshotName, activePins)

	startTime := time.Now()
	job := &snapshotExportJobView{
		record: &datapb.ExportSnapshotJob{
			JobId:          jobID,
			SnapshotName:   snapshotName,
			CollectionId:   collectionID,
			DbName:         dbName,
			CollectionName: collectionName,
			TargetS3Path:   targetPath,
			ExternalSpec:   externalSpec,
			State:          datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
			StartTime:      uint64(startTime.UnixMilli()),
			DeadlineTime:   uint64(startTime.Add(timeout).UnixMilli()),
			PinId:          pinID,
		},
	}
	if err := m.meta.CreateJob(ctx, job); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotPinCleanupTimeout)
		defer cancel()
		collID, snapName, remaining, unpinErr := m.snapshotManager.snapshotMeta.UnpinSnapshot(cleanupCtx, pinID)
		if unpinErr != nil {
			mlog.Warn(cleanupCtx, "failed to release snapshot export pin after job persistence failure",
				mlog.FieldJobID(jobID),
				mlog.Int64("pinID", pinID),
				mlog.Err(unpinErr))
		} else if snapName != "" {
			setSnapshotActivePinsGauge(collID, snapName, remaining)
		}
		return 0, err
	}
	mlog.Info(ctx, "snapshot export job accepted",
		mlog.FieldJobID(jobID),
		mlog.FieldCollectionID(collectionID),
		mlog.String("snapshotName", snapshotName))
	m.Wake()
	return jobID, nil
}

func namespacedSnapshotExportTarget(targetPath, namespace string) string {
	return strings.TrimRight(targetPath, "/") + "/" + snapshotExportNamespaceSubPath + "/" + namespace
}

func (m *snapshotExportManager) GetJobInfo(jobID int64) (*datapb.ExportSnapshotJobInfo, error) {
	job, ok := m.meta.GetJob(jobID)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("snapshot export job %d not found", jobID)
	}
	now := uint64(time.Now().UnixMilli())
	end := job.record.GetEndTime()
	if end == 0 {
		end = now
	}
	timeCost := uint64(0)
	if end >= job.record.GetStartTime() {
		timeCost = end - job.record.GetStartTime()
	}
	metadataURI := ""
	totalBytes := int64(0)
	if job.record.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted {
		metadataURI = job.record.GetSnapshotMetadataUri()
		totalBytes = job.record.GetTotalBytes()
	}
	copied, progress, total := job.record.GetCopiedFiles(), job.record.GetProgress(), job.record.GetTotalFiles()
	if job.record.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting {
		copied = 0
		if len(job.copyTasks) > 0 {
			copied, total = m.exportShardProgress(job)
			progress = snapshotExportCopyProgress(copied, total)
		}
	}
	return &datapb.ExportSnapshotJobInfo{
		JobId:               job.record.GetJobId(),
		SnapshotName:        job.record.GetSnapshotName(),
		DbName:              job.record.GetDbName(),
		CollectionName:      job.record.GetCollectionName(),
		State:               job.record.GetState(),
		Progress:            progress,
		Reason:              job.record.GetReason(),
		StartTime:           job.record.GetStartTime(),
		TimeCost:            timeCost,
		TotalFiles:          total,
		CopiedFiles:         copied,
		SnapshotMetadataUri: metadataURI,
		TotalBytes:          totalBytes,
	}, nil
}

// Worker slots and DataNode copy permits bound resources across jobs. The
// running registry only prevents duplicate runners for the same job.
func (m *snapshotExportManager) tryStartJob(jobID int64) {
	m.runningMu.Lock()
	defer m.runningMu.Unlock()
	if _, ok := m.running[jobID]; ok {
		return
	}
	workerCtx, cancel := context.WithCancel(m.ctx)
	m.running[jobID] = cancel
	m.wg.Add(1)
	go func() {
		defer cancel()
		m.runJob(workerCtx, jobID)
	}()
}

func (m *snapshotExportManager) runJob(ctx context.Context, jobID int64) {
	defer m.wg.Done()
	defer func() {
		m.runningMu.Lock()
		delete(m.running, jobID)
		m.runningMu.Unlock()
		// The reconciliation ticker starts queued jobs and retries persistence
		// failures. Waking immediately here would spin while the catalog is down.
	}()

	ctx, span := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "DataCoord-ExportSnapshotJob", trace.WithAttributes(
		attribute.Int64("jobID", jobID),
	))
	defer span.End()
	transitionCtx := ctx
	cancel := func() {}
	current, ok := m.meta.GetJob(jobID)
	if ok && current.record.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing {
		transitionCtx, cancel = m.withJobDeadline(ctx, jobID)
	}
	defer cancel()

	job, _, err := m.meta.UpdateJob(transitionCtx, jobID, func(job *snapshotExportJobView) (bool, error) {
		if isSnapshotExportTerminal(job.record.GetState()) {
			return true, nil
		}
		if job.record.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing {
			return true, nil
		}
		if err := snapshotExportAdvanceError(transitionCtx, job); err != nil {
			return false, err
		}
		if job.record.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting {
			return true, nil
		}
		if job.record.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPending {
			return false, merr.WrapErrServiceInternalMsg(
				"snapshot export job %d has invalid active state %s",
				jobID,
				job.record.GetState().String(),
			)
		}
		job.record.State = datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting
		return false, nil
	})
	if err != nil {
		mlog.Warn(ctx, "failed to start snapshot export job", mlog.FieldJobID(jobID), mlog.Err(err))
		return
	}
	if isSnapshotExportTerminal(job.record.GetState()) {
		return
	}
	mlog.Info(ctx, "snapshot export job started", mlog.FieldJobID(jobID), mlog.FieldCollectionID(job.record.GetCollectionId()))
	metrics.DataCoordSnapshotExportActiveJobs.Inc()
	defer metrics.DataCoordSnapshotExportActiveJobs.Dec()
	if err := m.executeJob(ctx, jobID); err != nil {
		if m.ctx.Err() != nil || errors.Is(err, errSnapshotExportJobStopped) {
			return
		}
		if errors.Is(err, errSnapshotExportJobPersistence) {
			mlog.RatedWarn(ctx, 1, "snapshot export job will retry after metadata persistence failure",
				mlog.FieldJobID(jobID),
				mlog.Err(err))
			return
		}
		if errors.Is(err, errSnapshotExportPublicationPending) {
			mlog.RatedWarn(ctx, 1, "snapshot export metadata publication will retry",
				mlog.FieldJobID(jobID),
				mlog.Err(err))
			return
		}
		latest, _ := m.meta.GetJob(jobID)
		externalSpec := ""
		if latest != nil {
			externalSpec = latest.record.GetExternalSpec()
		}
		m.failJob(jobID, m.snapshotExportFailureReason(latest, err, externalSpec))
	}
}

func (m *snapshotExportManager) snapshotExportFailureReason(
	job *snapshotExportJobView,
	err error,
	externalSpec string,
) string {
	if job != nil && job.record.GetCopyRequest() != nil {
		return snapshotstorage.SanitizeSnapshotExportReason(err, job.record.GetCopyRequest())
	}
	return sanitizeSnapshotExportReason(err, externalSpec)
}

func (m *snapshotExportManager) withJobDeadline(ctx context.Context, jobID int64) (context.Context, context.CancelFunc) {
	job, ok := m.meta.GetJob(jobID)
	if !ok || job.record.GetDeadlineTime() == 0 {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, time.UnixMilli(int64(job.record.GetDeadlineTime())))
}

func (m *snapshotExportManager) executeJob(ctx context.Context, jobID int64) error {
	job, ok := m.meta.GetJob(jobID)
	if !ok {
		return merr.WrapErrServiceInternalMsg("snapshot export job %d not found", jobID)
	}
	switch job.record.GetState() {
	case datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting:
		return m.executeSnapshotExport(ctx, job)
	case datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing:
		return m.executeSnapshotExportPublication(ctx, job)
	default:
		return errSnapshotExportJobStopped
	}
}

func (m *snapshotExportManager) executeSnapshotExport(
	ctx context.Context,
	job *snapshotExportJobView,
) error {
	if err := m.copyOnWorker(ctx, job); err != nil {
		return err
	}
	latest, ok := m.meta.GetJob(job.record.GetJobId())
	if !ok {
		return errSnapshotExportJobStopped
	}
	return m.executeSnapshotExportPublication(ctx, latest)
}

func (m *snapshotExportManager) executeSnapshotExportPublication(
	ctx context.Context,
	job *snapshotExportJobView,
) (err error) {
	if err := validateSnapshotExportPublishingJob(ctx, job); err != nil {
		return err
	}
	// Publication is source-independent, including recovery after the deadline.
	req := snapshotExportExecutionRequest(job.record.GetCopyRequest())
	if req == nil {
		// Legacy Publishing records have no frozen storage identity.
		req = &datapb.SnapshotExportCopyRequest{SourceStorageConfig: snapshotstorage.StorageConfigFromInstance(Params), ExternalSpec: job.record.GetExternalSpec()}
	}
	defer func() { err = redactSnapshotExportError(err, req) }()
	instanceCfg := snapshotstorage.ObjectStorageConfigFromIndexConfig(req.GetSourceStorageConfig())
	resolved, err := snapshotstorage.ResolveForeignStorage(ctx, instanceCfg, snapshotstorage.DirectionExport, job.record.GetTargetS3Path(), req.GetExternalSpec())
	if err != nil {
		return classifySnapshotExportPublicationError(ctx, err)
	}
	targetRoot := strings.TrimSuffix(snapshotstorage.NormalizeSnapshotObjectPath(job.record.GetTargetS3Path()), "/")
	releaseTarget, err := m.lockTarget(ctx, snapshotExportTarget{
		bucket: strings.TrimSpace(resolved.ForeignBucket),
		root:   strings.Trim(targetRoot, "/"),
	})
	if err != nil {
		return err
	}
	defer releaseTarget()
	return m.completeSnapshotExportPublication(ctx, job, resolved.ForeignCM, targetRoot)
}

func (m *snapshotExportManager) completeSnapshotExportPublication(
	ctx context.Context,
	job *snapshotExportJobView,
	targetCM storage.ChunkManager,
	targetRoot string,
) error {
	if err := validateSnapshotExportPublishingJob(ctx, job); err != nil {
		return err
	}
	if err := commitSnapshotExportMetadata(ctx, targetCM, targetRoot, job.record.GetSnapshotMetadataUri()); err != nil {
		return classifySnapshotExportPublicationError(ctx, err)
	}
	completed, _, err := m.meta.UpdateJob(ctx, job.record.GetJobId(), func(latest *snapshotExportJobView) (bool, error) {
		if err := ensureSnapshotExportCanPublish(ctx, latest); err != nil {
			return false, err
		}
		if latest.record.GetSnapshotMetadataUri() != job.record.GetSnapshotMetadataUri() {
			return false, merr.WrapErrDataIntegrityMsg(
				"snapshot export job %d metadata URI changed during publication",
				job.record.GetJobId(),
			)
		}
		if latest.record.GetTotalBytes() != job.record.GetTotalBytes() {
			return false, merr.WrapErrDataIntegrityMsg(
				"snapshot export job %d total bytes changed during publication",
				job.record.GetJobId(),
			)
		}
		latest.record.State = datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted
		latest.record.Progress = 100
		latest.record.EndTime = uint64(time.Now().UnixMilli())
		latest.record.ExternalSpec = ""
		latest.record.CopyRequest = nil
		return false, nil
	})
	if err != nil {
		return err
	}
	observeSnapshotExportTerminal(completed)
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotPinCleanupTimeout)
	defer cancel()
	if err := cleanupSnapshotExportStagingMetadata(cleanupCtx, targetCM, targetRoot); err != nil {
		mlog.Warn(cleanupCtx, "failed to remove staged snapshot export metadata",
			mlog.FieldJobID(job.record.GetJobId()),
			mlog.Err(err))
	}
	mlog.Info(ctx, "snapshot export job completed",
		mlog.FieldJobID(job.record.GetJobId()),
		mlog.String("snapshotMetadataURI", snapshotstorage.RedactSnapshotObjectPath(job.record.GetSnapshotMetadataUri())))
	return nil
}

func classifySnapshotExportPublicationError(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil || isPermanentSnapshotError(err) {
		return err
	}
	return &snapshotExportPublicationPendingError{
		error: merr.Wrap(err, "snapshot export metadata publication is not yet verified"),
	}
}

func validateSnapshotExportPublishingJob(ctx context.Context, job *snapshotExportJobView) error {
	if err := ensureSnapshotExportCanPublish(ctx, job); err != nil {
		return err
	}
	if strings.TrimSpace(job.record.GetTargetS3Path()) == "" || strings.TrimSpace(job.record.GetSnapshotMetadataUri()) == "" {
		return merr.WrapErrDataIntegrityMsg("publishing snapshot export job is missing its target paths")
	}
	if job.record.GetCopyCursor() != job.record.GetTotalFiles() || job.record.GetCopiedFiles() != job.record.GetTotalFiles() {
		return merr.WrapErrDataIntegrityMsg("publishing snapshot export job has an incomplete copy plan")
	}
	if job.record.GetTotalBytes() < 0 {
		return merr.WrapErrDataIntegrityMsg("publishing snapshot export job has negative copied data size")
	}
	return nil
}

func (m *snapshotExportManager) failJob(jobID int64, reason string) bool {
	return m.updateFailedJob(jobID, reason, false, true, 0)
}

func (m *snapshotExportManager) tryFailJob(jobID int64, reason string) bool {
	return m.updateFailedJob(jobID, reason, true, false, 0)
}

func (m *snapshotExportManager) updateFailedJob(jobID int64, reason string, tryLock bool, allowPublishing bool, taskID int64) bool {
	ctx, cancel := context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
	defer cancel()
	mutate := func(job *snapshotExportJobView) (bool, error) {
		// Check assignment under the same lock as the terminal transition. A
		// late failure from a retired task must not fail its replacement.
		if taskID != 0 && (findExportTask(job, taskID) == nil || isRetiredExportTask(job, taskID)) {
			return true, nil
		}
		if isSnapshotExportTerminal(job.record.GetState()) ||
			(!allowPublishing && job.record.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing) {
			return true, nil
		}
		now := uint64(time.Now().UnixMilli())
		job.record.Reason = reason
		// Classify against the locked record, so checker and worker failures
		// agree even when the deadline passes while waiting for this update.
		if job.record.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing &&
			job.record.GetDeadlineTime() != 0 && now >= job.record.GetDeadlineTime() {
			job.record.Reason = "snapshot export job timed out"
		}
		job.record.State = datapb.ExportSnapshotJobState_ExportSnapshotJobFailed
		job.record.EndTime = now
		job.record.SnapshotMetadataUri = ""
		job.record.ExternalSpec = ""
		job.record.CopyRequest = nil
		return false, nil
	}
	var (
		job     *snapshotExportJobView
		applied bool
		err     error
	)
	if tryLock {
		var acquired bool
		job, acquired, applied, err = m.meta.TryUpdateJob(ctx, jobID, mutate)
		if !acquired {
			return false
		}
	} else {
		job, applied, err = m.meta.UpdateJob(ctx, jobID, mutate)
	}
	if err != nil {
		mlog.Warn(ctx, "failed to persist snapshot export failure",
			mlog.FieldJobID(jobID),
			mlog.Err(err))
		return false
	}
	if applied {
		observeSnapshotExportTerminal(job)
		mlog.Warn(ctx, "snapshot export job failed",
			mlog.FieldJobID(jobID),
			mlog.String("reason", job.record.GetReason()))
	}
	return applied
}

func (m *snapshotExportManager) cancelRunningJob(jobID int64) {
	m.runningMu.Lock()
	cancel := m.running[jobID]
	m.runningMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (m *snapshotExportManager) lockTarget(
	ctx context.Context,
	target snapshotExportTarget,
) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.targetMu.Lock()
	targetLock, ok := m.targetLocks[target]
	if !ok {
		targetLock = &snapshotExportTargetLock{semaphore: make(chan struct{}, 1)}
		m.targetLocks[target] = targetLock
	}
	targetLock.refs++
	m.targetMu.Unlock()

	select {
	case targetLock.semaphore <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-targetLock.semaphore
				m.releaseTargetLockRef(target, targetLock)
			})
		}, nil
	case <-ctx.Done():
		m.releaseTargetLockRef(target, targetLock)
		return nil, ctx.Err()
	}
}

func (m *snapshotExportManager) releaseTargetLockRef(target snapshotExportTarget, targetLock *snapshotExportTargetLock) {
	m.targetMu.Lock()
	defer m.targetMu.Unlock()
	current, ok := m.targetLocks[target]
	if !ok || current != targetLock {
		return
	}
	targetLock.refs--
	if targetLock.refs == 0 {
		delete(m.targetLocks, target)
	}
}

func snapshotExportCopyProgress(copied, total int64) int32 {
	if total <= 0 {
		return 5
	}
	progress := int32(5 + copied*90/total)
	if progress > 95 {
		return 95
	}
	return progress
}

func isSnapshotExportTerminal(state datapb.ExportSnapshotJobState) bool {
	return state == datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted ||
		state == datapb.ExportSnapshotJobState_ExportSnapshotJobFailed
}

func snapshotExportAdvanceError(ctx context.Context, job *snapshotExportJobView) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job != nil && job.record.GetDeadlineTime() != 0 && uint64(time.Now().UnixMilli()) >= job.record.GetDeadlineTime() {
		return context.DeadlineExceeded
	}
	return nil
}

func ensureSnapshotExportCanAdvance(ctx context.Context, job *snapshotExportJobView) error {
	if job == nil || job.record.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting {
		return errSnapshotExportJobStopped
	}
	return snapshotExportAdvanceError(ctx, job)
}

func ensureSnapshotExportCanPublish(ctx context.Context, job *snapshotExportJobView) error {
	if job == nil || job.record.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing {
		return errSnapshotExportJobStopped
	}
	return ctx.Err()
}

func observeSnapshotExportTerminal(job *snapshotExportJobView) {
	if job == nil || !isSnapshotExportTerminal(job.record.GetState()) {
		return
	}
	state := "failed"
	if job.record.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted {
		state = "completed"
	}
	metrics.DataCoordSnapshotExportTerminalJobs.WithLabelValues(state).Inc()
	if job.record.GetEndTime() >= job.record.GetStartTime() {
		metrics.DataCoordSnapshotExportJobLatency.WithLabelValues(state).
			Observe(float64(job.record.GetEndTime() - job.record.GetStartTime()))
	}
}

func sanitizeSnapshotExportReason(err error, externalSpec string) string {
	return snapshotstorage.SanitizeSnapshotExportReason(err,
		&datapb.SnapshotExportCopyRequest{ExternalSpec: externalSpec})
}
