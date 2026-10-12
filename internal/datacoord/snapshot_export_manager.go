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
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	snapshotExportCheckpointBatchSize = 256
	snapshotExportReconcileInterval   = time.Second
	snapshotExportPinSafetyMargin     = 5 * time.Minute
	snapshotExportFailureReasonLimit  = 1024
	snapshotExportNamespaceSubPath    = "exports"
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
	return &snapshotExportManager{
		ctx:             managerCtx,
		cancel:          cancel,
		meta:            meta,
		snapshotManager: snapshotManager,
		wakeCh:          make(chan struct{}, 1),
		running:         make(map[int64]context.CancelFunc),
		targetLocks:     make(map[snapshotExportTarget]*snapshotExportTargetLock),
	}
}

func (m *snapshotExportManager) Start() {
	m.startOnce.Do(func() {
		m.wg.Add(1)
		go m.run()
	})
}

func (m *snapshotExportManager) Close() {
	m.closeOnce.Do(func() {
		m.cancel()
		m.wg.Wait()
	})
}

func (m *snapshotExportManager) Wake() {
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
	job := &datapb.ExportSnapshotJob{
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
	end := job.GetEndTime()
	if end == 0 {
		end = now
	}
	timeCost := uint64(0)
	if end >= job.GetStartTime() {
		timeCost = end - job.GetStartTime()
	}
	metadataURI := ""
	totalBytes := int64(0)
	if job.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted {
		metadataURI = job.GetSnapshotMetadataUri()
		totalBytes = job.GetTotalBytes()
	}
	return &datapb.ExportSnapshotJobInfo{
		JobId:               job.GetJobId(),
		SnapshotName:        job.GetSnapshotName(),
		DbName:              job.GetDbName(),
		CollectionName:      job.GetCollectionName(),
		State:               job.GetState(),
		Progress:            job.GetProgress(),
		Reason:              job.GetReason(),
		StartTime:           job.GetStartTime(),
		TimeCost:            timeCost,
		TotalFiles:          job.GetTotalFiles(),
		CopiedFiles:         job.GetCopiedFiles(),
		SnapshotMetadataUri: metadataURI,
		TotalBytes:          totalBytes,
	}, nil
}

func (m *snapshotExportManager) run() {
	defer m.wg.Done()
	ticker := time.NewTicker(snapshotExportReconcileInterval)
	defer ticker.Stop()
	m.reconcile()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wakeCh:
			m.reconcile()
		case <-ticker.C:
			m.reconcile()
		}
	}
}

func (m *snapshotExportManager) reconcile() {
	jobs := m.meta.GetJobs()
	now := uint64(time.Now().UnixMilli())
	for _, job := range jobs {
		if isSnapshotExportTerminal(job.GetState()) {
			m.cleanupTerminalJob(job, now)
			continue
		}
		if job.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing &&
			job.GetDeadlineTime() != 0 && now >= job.GetDeadlineTime() {
			// Persist the timeout transition before canceling the worker. If the
			// worker has already entered Publishing, tryFailJob observes that state
			// under the job lock and leaves publication running.
			if m.tryFailJob(job.GetJobId(), "snapshot export job timed out") {
				m.cancelRunningJob(job.GetJobId())
			}
		}
	}

	maxConcurrent := Params.DataCoordCfg.SnapshotExportMaxConcurrentJobs.GetAsInt()
	for _, job := range m.meta.GetJobs() {
		if isSnapshotExportTerminal(job.GetState()) {
			continue
		}
		if job.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing &&
			job.GetDeadlineTime() != 0 && now >= job.GetDeadlineTime() {
			continue
		}
		if !m.tryStartJob(job.GetJobId(), maxConcurrent) {
			continue
		}
	}
}

func (m *snapshotExportManager) tryStartJob(jobID int64, maxConcurrent int) bool {
	m.runningMu.Lock()
	defer m.runningMu.Unlock()
	if _, ok := m.running[jobID]; ok || len(m.running) >= maxConcurrent {
		return false
	}
	workerCtx, cancel := context.WithCancel(m.ctx)
	m.running[jobID] = cancel
	m.wg.Add(1)
	go func() {
		defer cancel()
		m.runJob(workerCtx, jobID)
	}()
	return true
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
	if ok && current.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing {
		transitionCtx, cancel = m.withJobDeadline(ctx, jobID)
	}
	defer cancel()

	job, _, err := m.meta.UpdateJob(transitionCtx, jobID, func(job *datapb.ExportSnapshotJob) (bool, error) {
		if isSnapshotExportTerminal(job.GetState()) {
			return true, nil
		}
		if job.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing {
			return true, nil
		}
		if err := snapshotExportAdvanceError(transitionCtx, job); err != nil {
			return false, err
		}
		if job.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting {
			return true, nil
		}
		if job.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPending {
			return false, merr.WrapErrServiceInternalMsg(
				"snapshot export job %d has invalid active state %s",
				jobID,
				job.GetState().String(),
			)
		}
		job.State = datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting
		return false, nil
	})
	if err != nil {
		mlog.Warn(ctx, "failed to start snapshot export job", mlog.FieldJobID(jobID), mlog.Err(err))
		return
	}
	if isSnapshotExportTerminal(job.GetState()) {
		return
	}
	mlog.Info(ctx, "snapshot export job started", mlog.FieldJobID(jobID), mlog.FieldCollectionID(job.GetCollectionId()))
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
			externalSpec = latest.GetExternalSpec()
		}
		m.failJob(jobID, m.snapshotExportFailureReason(latest, err, externalSpec))
	}
}

func (m *snapshotExportManager) snapshotExportFailureReason(
	job *datapb.ExportSnapshotJob,
	err error,
	externalSpec string,
) string {
	if job != nil &&
		job.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing &&
		job.GetDeadlineTime() != 0 &&
		uint64(time.Now().UnixMilli()) >= job.GetDeadlineTime() {
		return "snapshot export job timed out"
	}
	return sanitizeSnapshotExportReason(err, externalSpec)
}

func (m *snapshotExportManager) withJobDeadline(ctx context.Context, jobID int64) (context.Context, context.CancelFunc) {
	job, ok := m.meta.GetJob(jobID)
	if !ok || job.GetDeadlineTime() == 0 {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, time.UnixMilli(int64(job.GetDeadlineTime())))
}

func (m *snapshotExportManager) executeJob(ctx context.Context, jobID int64) error {
	job, ok := m.meta.GetJob(jobID)
	if !ok {
		return merr.WrapErrServiceInternalMsg("snapshot export job %d not found", jobID)
	}
	switch job.GetState() {
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
	job *datapb.ExportSnapshotJob,
) error {
	jobID := job.GetJobId()
	operationCtx, cancel := m.withJobDeadline(ctx, jobID)
	defer cancel()
	if err := ensureSnapshotExportCanAdvance(operationCtx, job); err != nil {
		return err
	}

	instanceCfg := snapshotstorage.InstanceConfigFromParamtable(Params)
	resolved, err := snapshotstorage.ResolveForeignStorage(
		operationCtx,
		instanceCfg,
		snapshotstorage.DirectionExport,
		job.GetTargetS3Path(),
		job.GetExternalSpec(),
	)
	if err != nil {
		return err
	}
	targetRoot := strings.TrimSuffix(snapshotstorage.NormalizeSnapshotObjectPath(job.GetTargetS3Path()), "/")
	releaseTarget, err := m.lockTarget(operationCtx, snapshotExportTarget{
		bucket: strings.TrimSpace(resolved.ForeignBucket),
		root:   strings.Trim(targetRoot, "/"),
	})
	if err != nil {
		return err
	}
	defer releaseTarget()

	snapshot, err := m.snapshotManager.ReadSnapshotData(operationCtx, job.GetCollectionId(), job.GetSnapshotName())
	if err != nil {
		return err
	}
	plan, err := buildSnapshotExportPlan(
		operationCtx,
		m.snapshotManager.snapshotMeta.chunkManager,
		resolved.ForeignCM,
		instanceCfg.BucketName,
		resolved.ForeignBucket,
		snapshot,
		job.GetTargetS3Path(),
		resolved.ForeignStorageConfig,
	)
	if err != nil {
		return err
	}

	job, err = m.persistOrValidatePlan(operationCtx, jobID, plan)
	if err != nil {
		return err
	}
	copyConcurrency := Params.DataCoordCfg.SnapshotExportCopyConcurrency.GetAsInt()
	for cursor := job.GetCopyCursor(); cursor < int64(len(plan.items)); {
		end := cursor + snapshotExportCheckpointBatchSize
		if end > int64(len(plan.items)) {
			end = int64(len(plan.items))
		}
		if err := copySnapshotExportPlan(
			operationCtx,
			resolved.Copier,
			instanceCfg.BucketName,
			resolved.ForeignBucket,
			plan.items[cursor:end],
			copyConcurrency,
		); err != nil {
			return err
		}
		updated, _, err := m.meta.UpdateJob(operationCtx, jobID, func(latest *datapb.ExportSnapshotJob) (bool, error) {
			if err := ensureSnapshotExportCanAdvance(operationCtx, latest); err != nil {
				return false, err
			}
			if latest.GetCopyCursor() != cursor {
				return false, merr.WrapErrDataIntegrityMsg(
					"snapshot export job %d copy cursor changed from %d to %d",
					jobID,
					cursor,
					latest.GetCopyCursor(),
				)
			}
			latest.CopyCursor = end
			latest.CopiedFiles = end
			latest.Progress = snapshotExportCopyProgress(end, int64(len(plan.items)))
			return false, nil
		})
		if err != nil {
			return err
		}
		cursor = updated.GetCopyCursor()
		mlog.Info(operationCtx, "snapshot export checkpoint persisted",
			mlog.FieldJobID(jobID),
			mlog.Int64("copiedFiles", cursor),
			mlog.Int64("totalFiles", int64(len(plan.items))))
	}

	totalBytes, err := prepareSnapshotExportPlanWithSize(
		operationCtx,
		resolved.ForeignCM,
		snapshot,
		plan,
	)
	if err != nil {
		return err
	}
	job, _, err = m.meta.UpdateJob(operationCtx, jobID, func(latest *datapb.ExportSnapshotJob) (bool, error) {
		if err := ensureSnapshotExportCanAdvance(operationCtx, latest); err != nil {
			return false, err
		}
		if latest.GetCopyCursor() != int64(len(plan.items)) ||
			latest.GetCopiedFiles() != int64(len(plan.items)) {
			return false, merr.WrapErrDataIntegrityMsg(
				"snapshot export job %d cannot publish an incomplete copy plan",
				jobID,
			)
		}
		latest.State = datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing
		latest.Progress = 99
		latest.SnapshotMetadataUri = plan.metadataURI
		latest.TotalBytes = totalBytes
		return false, nil
	})
	if err != nil {
		return err
	}
	return m.completeSnapshotExportPublication(ctx, job, resolved.ForeignCM, targetRoot)
}

func (m *snapshotExportManager) executeSnapshotExportPublication(
	ctx context.Context,
	job *datapb.ExportSnapshotJob,
) error {
	if err := validateSnapshotExportPublishingJob(ctx, job); err != nil {
		return err
	}
	instanceCfg := snapshotstorage.InstanceConfigFromParamtable(Params)
	resolved, err := snapshotstorage.ResolveForeignStorage(
		ctx,
		instanceCfg,
		snapshotstorage.DirectionExport,
		job.GetTargetS3Path(),
		job.GetExternalSpec(),
	)
	if err != nil {
		return classifySnapshotExportPublicationError(ctx, err)
	}
	targetRoot := strings.TrimSuffix(snapshotstorage.NormalizeSnapshotObjectPath(job.GetTargetS3Path()), "/")
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
	job *datapb.ExportSnapshotJob,
	targetCM storage.ChunkManager,
	targetRoot string,
) error {
	if err := validateSnapshotExportPublishingJob(ctx, job); err != nil {
		return err
	}
	if err := commitSnapshotExportMetadata(ctx, targetCM, targetRoot, job.GetSnapshotMetadataUri()); err != nil {
		return classifySnapshotExportPublicationError(ctx, err)
	}
	completed, _, err := m.meta.UpdateJob(ctx, job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
		if err := ensureSnapshotExportCanPublish(ctx, latest); err != nil {
			return false, err
		}
		if latest.GetSnapshotMetadataUri() != job.GetSnapshotMetadataUri() {
			return false, merr.WrapErrDataIntegrityMsg(
				"snapshot export job %d metadata URI changed during publication",
				job.GetJobId(),
			)
		}
		if latest.GetTotalBytes() != job.GetTotalBytes() {
			return false, merr.WrapErrDataIntegrityMsg(
				"snapshot export job %d total bytes changed during publication",
				job.GetJobId(),
			)
		}
		latest.State = datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted
		latest.Progress = 100
		latest.EndTime = uint64(time.Now().UnixMilli())
		latest.ExternalSpec = ""
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
			mlog.FieldJobID(job.GetJobId()),
			mlog.Err(err))
	}
	mlog.Info(ctx, "snapshot export job completed",
		mlog.FieldJobID(job.GetJobId()),
		mlog.String("snapshotMetadataURI", snapshotstorage.RedactSnapshotObjectPath(job.GetSnapshotMetadataUri())))
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

func validateSnapshotExportPublishingJob(ctx context.Context, job *datapb.ExportSnapshotJob) error {
	if err := ensureSnapshotExportCanPublish(ctx, job); err != nil {
		return err
	}
	if strings.TrimSpace(job.GetTargetS3Path()) == "" || strings.TrimSpace(job.GetSnapshotMetadataUri()) == "" {
		return merr.WrapErrDataIntegrityMsg("publishing snapshot export job is missing its target paths")
	}
	if job.GetCopyCursor() != job.GetTotalFiles() || job.GetCopiedFiles() != job.GetTotalFiles() {
		return merr.WrapErrDataIntegrityMsg("publishing snapshot export job has an incomplete copy plan")
	}
	if job.GetTotalBytes() <= 0 {
		return merr.WrapErrDataIntegrityMsg("publishing snapshot export job has no prepared bundle size")
	}
	return nil
}

func (m *snapshotExportManager) persistOrValidatePlan(
	ctx context.Context,
	jobID int64,
	plan *snapshotExportPlan,
) (*datapb.ExportSnapshotJob, error) {
	updated, _, err := m.meta.UpdateJob(ctx, jobID, func(job *datapb.ExportSnapshotJob) (bool, error) {
		if job.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting {
			return false, errSnapshotExportJobStopped
		}
		if err := ensureSnapshotExportCanAdvance(ctx, job); err != nil {
			return false, err
		}
		if job.GetPlanFingerprint() == "" {
			job.PlanVersion = plan.version
			job.PlanFingerprint = plan.fingerprint
			job.SnapshotFingerprint = plan.snapshotFingerprint
			job.TotalFiles = int64(len(plan.items))
			job.CopyCursor = 0
			job.CopiedFiles = 0
			job.Progress = 5
			return false, nil
		}
		if job.GetPlanVersion() != plan.version ||
			job.GetPlanFingerprint() != plan.fingerprint ||
			job.GetSnapshotFingerprint() != plan.snapshotFingerprint ||
			job.GetTotalFiles() != int64(len(plan.items)) {
			return false, merr.WrapErrDataIntegrityMsg("snapshot export plan changed during recovery")
		}
		if job.GetCopyCursor() < 0 || job.GetCopyCursor() > job.GetTotalFiles() ||
			job.GetCopiedFiles() != job.GetCopyCursor() {
			return false, merr.WrapErrDataIntegrityMsg("snapshot export checkpoint is invalid")
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (m *snapshotExportManager) failJob(jobID int64, reason string) bool {
	return m.updateFailedJob(jobID, reason, false, true)
}

func (m *snapshotExportManager) tryFailJob(jobID int64, reason string) bool {
	return m.updateFailedJob(jobID, reason, true, false)
}

func (m *snapshotExportManager) updateFailedJob(jobID int64, reason string, tryLock bool, allowPublishing bool) bool {
	ctx, cancel := context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
	defer cancel()
	mutate := func(job *datapb.ExportSnapshotJob) (bool, error) {
		if isSnapshotExportTerminal(job.GetState()) ||
			(!allowPublishing && job.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing) {
			return true, nil
		}
		job.State = datapb.ExportSnapshotJobState_ExportSnapshotJobFailed
		job.Reason = reason
		job.EndTime = uint64(time.Now().UnixMilli())
		job.SnapshotMetadataUri = ""
		job.ExternalSpec = ""
		return false, nil
	}
	var (
		job     *datapb.ExportSnapshotJob
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
			mlog.String("reason", reason))
	}
	return applied
}

func (m *snapshotExportManager) cleanupTerminalJob(job *datapb.ExportSnapshotJob, now uint64) {
	if job.GetExternalSpec() != "" {
		ctx, cancel := context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
		_, _, err := m.meta.UpdateJob(ctx, job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
			if !isSnapshotExportTerminal(latest.GetState()) || latest.GetExternalSpec() == "" {
				return true, nil
			}
			latest.ExternalSpec = ""
			return false, nil
		})
		cancel()
		if err != nil {
			mlog.RatedWarn(m.ctx, 1, "failed to clear terminal snapshot export credentials",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
		}
		return
	}
	if job.GetPinId() != 0 {
		ctx, cancel := context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
		collID, snapshotName, remaining, err := m.snapshotManager.snapshotMeta.UnpinSnapshot(ctx, job.GetPinId())
		cancel()
		if err != nil {
			mlog.RatedWarn(m.ctx, 1, "failed to release terminal snapshot export pin",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Int64("pinID", job.GetPinId()),
				mlog.Err(err))
			return
		}
		if snapshotName != "" {
			setSnapshotActivePinsGauge(collID, snapshotName, remaining)
		}
		ctx, cancel = context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
		_, _, err = m.meta.UpdateJob(ctx, job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
			if latest.GetPinId() == 0 {
				return true, nil
			}
			latest.PinId = 0
			return false, nil
		})
		cancel()
		if err != nil {
			mlog.RatedWarn(m.ctx, 1, "failed to clear terminal snapshot export pin",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
		}
		return
	}

	retention := Params.DataCoordCfg.SnapshotExportJobRetention.GetAsDuration(time.Second)
	if job.GetEndTime() == 0 || now < job.GetEndTime()+uint64(retention.Milliseconds()) {
		return
	}
	ctx, cancel := context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
	defer cancel()
	if err := m.meta.DropJob(ctx, job.GetJobId()); err != nil {
		mlog.RatedWarn(m.ctx, 1, "failed to remove expired snapshot export job",
			mlog.FieldJobID(job.GetJobId()),
			mlog.Err(err))
	}
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

func snapshotExportAdvanceError(ctx context.Context, job *datapb.ExportSnapshotJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job.GetDeadlineTime() != 0 && uint64(time.Now().UnixMilli()) >= job.GetDeadlineTime() {
		return context.DeadlineExceeded
	}
	return nil
}

func ensureSnapshotExportCanAdvance(ctx context.Context, job *datapb.ExportSnapshotJob) error {
	if job.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting {
		return errSnapshotExportJobStopped
	}
	return snapshotExportAdvanceError(ctx, job)
}

func ensureSnapshotExportCanPublish(ctx context.Context, job *datapb.ExportSnapshotJob) error {
	if job.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing {
		return errSnapshotExportJobStopped
	}
	return ctx.Err()
}

func observeSnapshotExportTerminal(job *datapb.ExportSnapshotJob) {
	if job == nil || !isSnapshotExportTerminal(job.GetState()) {
		return
	}
	state := "failed"
	if job.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted {
		state = "completed"
	}
	metrics.DataCoordSnapshotExportTerminalJobs.WithLabelValues(state).Inc()
	if job.GetEndTime() >= job.GetStartTime() {
		metrics.DataCoordSnapshotExportJobLatency.WithLabelValues(state).
			Observe(float64(job.GetEndTime() - job.GetStartTime()))
	}
}

func sanitizeSnapshotExportReason(err error, externalSpec string) string {
	if err == nil {
		return ""
	}
	reason := strings.TrimSpace(err.Error())
	for _, secret := range snapshotExportSecretValues(externalSpec) {
		reason = strings.ReplaceAll(reason, secret, "<redacted>")
	}
	if len(reason) > snapshotExportFailureReasonLimit {
		reason = reason[:snapshotExportFailureReasonLimit]
	}
	return reason
}

func snapshotExportSecretValues(externalSpec string) []string {
	if strings.TrimSpace(externalSpec) == "" {
		return nil
	}
	values := []string{externalSpec}
	var spec struct {
		Extfs map[string]json.RawMessage `json:"extfs"`
	}
	if err := json.Unmarshal([]byte(externalSpec), &spec); err != nil {
		return values
	}
	for _, key := range []string{
		externalspec.ExtfsKeyAccessKeyID,
		externalspec.ExtfsKeyAccessKeyValue,
		externalspec.ExtfsKeySSLCACert,
		externalspec.ExtfsKeyExternalID,
		"credential_json",
		// Azure source-read SAS: SDK copy errors can echo the SAS-bearing
		// source URL, so scrub the token from failure reasons too.
		"source_sas_token",
	} {
		var value string
		if err := json.Unmarshal(spec.Extfs[key], &value); err == nil && value != "" {
			values = append(values, value)
		}
	}
	return values
}
