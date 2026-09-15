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
	"path"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type statsTask struct {
	*indexpb.StatsTask

	taskSlot int64

	times *taskcommon.Times

	meta      *meta
	handler   Handler
	allocator allocator.Allocator
	ievm      IndexEngineVersionManager
}

var _ globalTask.Task = (*statsTask)(nil)

var (
	errStatsResultStale       = errors.New("stale stats result")
	errStatsResultDiscarded   = errors.New("discarded stats result")
	errJSONStatsResultInvalid = errors.New("JSON stats result does not satisfy the requested format")
)

func newStatsTask(t *indexpb.StatsTask,
	taskSlot int64,
	mt *meta,
	handler Handler,
	allocator allocator.Allocator,
	ievm IndexEngineVersionManager,
) *statsTask {
	return &statsTask{
		StatsTask: t,
		taskSlot:  taskSlot,
		times:     taskcommon.NewTimes(),
		meta:      mt,
		handler:   handler,
		allocator: allocator,
		ievm:      ievm,
	}
}

func (st *statsTask) GetTaskID() int64 {
	return st.TaskID
}

func (st *statsTask) GetTaskType() taskcommon.Type {
	return taskcommon.Stats
}

func (st *statsTask) GetTaskState() taskcommon.State {
	return st.GetState()
}

func (st *statsTask) GetTaskSlot() int64 {
	return st.taskSlot
}

func (st *statsTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	st.times.SetTaskTime(timeType, time)
}

func (st *statsTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(st.times)
}

func (st *statsTask) GetTaskVersion() int64 {
	return st.GetVersion()
}

func (st *statsTask) getJSONStatsDataFormat() int64 {
	format := st.GetJsonStatsDataFormat()
	if format == 0 {
		// StatsTask predates the persisted format field. Such a task was created
		// while V3 was the only writable format. Never reinterpret its output
		// as V4; migration retires the old task before creating a new task ID.
		return common.JSONStatsDataFormatV3
	}
	return format
}

func (st *statsTask) requiresJSONStatsV4() bool {
	return st.GetSubJobType() == indexpb.StatsSubJob_JsonKeyIndexJob &&
		st.getJSONStatsDataFormat() == common.JSONStatsDataFormatV4
}

// CanRunOnNode pauses JSON stats when shredding is disabled and gates persisted
// V4 tasks on coordinator compatibility: reader/writer capabilities alone
// cannot protect V3 files from an old GC.
func (st *statsTask) CanRunOnNode(nodeID int64) bool {
	if st.GetSubJobType() != indexpb.StatsSubJob_JsonKeyIndexJob {
		return true
	}
	if st.meta != nil {
		segment := st.meta.GetHealthySegment(context.TODO(), st.GetSegmentID())
		if segment == nil || segment.GetNumOfRows() == 0 {
			// Local completion/cleanup does not write a JSON stats artifact.
			return true
		}
	}
	if !Params.CommonCfg.EnabledJSONKeyStats.GetAsBool() || jsonShreddingDisabledByDeprecatedConfig() {
		return false
	}
	if !st.requiresJSONStatsV4() {
		return true
	}
	if Params.DataCoordCfg.JSONStatsFormatVersion.GetAsInt64() != common.JSONStatsDataFormatV4 {
		return false
	}
	versions, ok := st.ievm.(JSONStatsVersionManager)
	return ok && versions.SupportsJSONStatsReaders() && versions.SupportsJSONStatsWriter(nodeID)
}

// validateJSONStatsResult runs before any metadata/manifest publication. Old
// DataNodes can report success without output for an unknown requested format.
// Admitted JSON attempts always request shredding; disabling new work must not
// relax their output contract or prevent valid in-flight results from finishing.
func (st *statsTask) validateJSONStatsResult(ctx context.Context, result *workerpb.StatsResult) error {
	if !st.requiresJSONStatsV4() {
		return nil
	}
	segment := st.meta.GetHealthySegment(ctx, st.GetSegmentID())
	if segment == nil || segment.GetNumOfRows() == 0 {
		return nil
	}
	if Params.DataCoordCfg.JSONStatsFormatVersion.GetAsInt64() != common.JSONStatsDataFormatV4 {
		return merr.WrapErrServiceNotReadyMsg("waiting for V4 JSON stats version gate before publication")
	}
	collection, err := st.handler.GetCollection(ctx, segment.GetCollectionID())
	if err != nil {
		return merr.Wrap(err, "get schema to validate JSON stats result")
	}
	if collection == nil || collection.Schema == nil {
		return merr.WrapErrServiceNotReadyMsg("collection schema unavailable for JSON stats validation")
	}
	stats := result.GetJsonKeyStatsLogs()
	currentFields := make(map[int64]struct{})
	for _, fieldID := range getJSONStatsFieldIDs(collection) {
		currentFields[fieldID] = struct{}{}
	}
	if len(stats) == 0 && len(currentFields) > 0 {
		return merr.Wrap(errJSONStatsResultInvalid, "missing JSON stats output")
	}
	for _, fieldID := range st.GetJsonStatsFieldIds() {
		// A removed/disabled field no longer needs an artifact. Fields added
		// after task creation are not part of this attempt's contract.
		if _, enabled := currentFields[fieldID]; enabled && stats[fieldID] == nil {
			return merr.Wrapf(errJSONStatsResultInvalid, "missing stats for field %d", fieldID)
		}
	}
	for fieldID, info := range stats {
		if info == nil || info.GetFieldID() != fieldID ||
			info.GetJsonKeyStatsDataFormat() != st.getJSONStatsDataFormat() ||
			info.GetBuildID() != st.GetTaskID() || len(info.GetFiles()) == 0 {
			return merr.Wrapf(errJSONStatsResultInvalid, "invalid stats for field %d: expected format %d and build %d",
				fieldID, st.getJSONStatsDataFormat(), st.GetTaskID())
		}
	}
	versions, ok := st.ievm.(JSONStatsVersionManager)
	if !ok || !versions.SupportsJSONStatsReaders() {
		// Hold the completed worker result until readers catch up. No rebuild
		// is needed and no manifest may be published during this window.
		return merr.WrapErrServiceNotReadyMsg("waiting for V4 JSON stats readers before publication")
	}
	return nil
}

func (st *statsTask) SetState(state indexpb.JobState, failReason string) {
	st.State = state
	st.FailReason = failReason
}

func (st *statsTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	if err := st.meta.statsTaskMeta.UpdateTaskState(st.GetTaskID(), state, failReason); err != nil {
		mlog.Warn(context.TODO(), "update stats task state failed", mlog.FieldTaskID(st.GetTaskID()),
			mlog.String("state", state.String()), mlog.String("failReason", failReason),
			mlog.Err(err))
		return err
	}
	st.SetState(state, failReason)
	return nil
}

func (st *statsTask) UpdateTaskVersion(nodeID int64) error {
	if err := st.meta.statsTaskMeta.UpdateVersion(st.GetTaskID(), nodeID); err != nil {
		return err
	}
	st.Version++
	st.NodeID = nodeID
	return nil
}

func (st *statsTask) resetTask(ctx context.Context, reason string) {
	// reset state to init
	st.UpdateStateWithMeta(indexpb.JobState_JobStateInit, reason)
}

func (st *statsTask) dropAndResetTaskOnWorker(ctx context.Context, cluster session.Cluster, reason string) {
	if err := st.tryDropTaskOnWorker(cluster); err != nil {
		return
	}
	st.resetTask(ctx, reason)
}

// Retire only idle, background-owned V3 tasks once V4 can be built safely.
// Otherwise a legacy parser failure can keep retrying forever and block both
// the inspector and migration compaction. Published V3 artifacts are untouched:
// the inspector creates a fresh V4 task for missing stats, while compaction
// owns the gradual replacement of segments that already have V3 stats.
func (st *statsTask) retireLegacyJSONStatsTask(ctx context.Context, nodeID int64, cluster session.Cluster) (bool, error) {
	if st.GetState() != indexpb.JobState_JobStateInit || !st.GetCanRecycle() ||
		st.GetSubJobType() != indexpb.StatsSubJob_JsonKeyIndexJob ||
		st.getJSONStatsDataFormat() != common.JSONStatsDataFormatV3 ||
		!Params.CommonCfg.EnabledJSONKeyStats.GetAsBool() || jsonShreddingDisabledByDeprecatedConfig() ||
		Params.DataCoordCfg.JSONStatsFormatVersion.GetAsInt64() != common.JSONStatsDataFormatV4 {
		return false, nil
	}
	versions, ok := st.ievm.(JSONStatsVersionManager)
	if !ok || !versions.SupportsJSONStatsReaders() || !versions.SupportsJSONStatsWriter(nodeID) {
		return false, nil
	}
	if st.GetNodeID() != 0 {
		if err := st.tryDropTaskOnWorker(cluster); err != nil {
			return false, merr.Wrap(err, "drop legacy JSON stats attempt before migration")
		}
	}
	if err := st.meta.statsTaskMeta.DropStatsTask(ctx, st.GetTaskID()); err != nil {
		return false, merr.Wrap(err, "retire legacy JSON stats task before migration")
	}
	st.SetState(indexpb.JobState_JobStateNone, "legacy JSON stats task retired for automatic V4 migration")
	mlog.Info(ctx, "retired legacy JSON stats task for automatic V4 migration",
		mlog.FieldTaskID(st.GetTaskID()), mlog.FieldSegmentID(st.GetSegmentID()))
	return true, nil
}

func (st *statsTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx, cancel := context.WithTimeout(context.Background(), Params.DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second))
	defer cancel()

	log := mlog.With(
		mlog.FieldTaskID(st.GetTaskID()),
		mlog.FieldSegmentID(st.GetSegmentID()),
		mlog.Int64("targetSegmentID", st.GetTargetSegmentID()),
		mlog.String("subJobType", st.GetSubJobType().String()),
	)

	var err error
	defer func() {
		if err != nil {
			st.resetTask(ctx, err.Error())
		}
	}()

	// Handle empty segment case
	segment := st.meta.GetHealthySegment(ctx, st.GetSegmentID())
	if segment == nil {
		log.Warn(context.TODO(), "segment is not healthy, skipping stats task")
		if err := st.meta.statsTaskMeta.DropStatsTask(ctx, st.GetTaskID()); err != nil {
			log.Warn(context.TODO(), "remove stats task failed, will retry later", mlog.Err(err))
			return
		}
		st.SetState(indexpb.JobState_JobStateNone, "segment is not healthy")
		return
	}

	if st.shouldDropExternalJSONStatsTask(segment) {
		log.Warn(ctx, "external json stats task is no longer buildable, dropping stats task")
		if err := st.meta.statsTaskMeta.DropStatsTask(ctx, st.GetTaskID()); err != nil {
			log.Warn(ctx, "remove stats task failed, will retry later", mlog.Err(err))
			return
		}
		st.SetState(indexpb.JobState_JobStateNone, "external json stats task is no longer buildable")
		return
	}

	if segment.GetNumOfRows() == 0 {
		if err := st.handleEmptySegment(ctx); err != nil {
			log.Warn(context.TODO(), "failed to handle empty segment", mlog.Err(err))
		}
		return
	}

	retired, err := st.retireLegacyJSONStatsTask(ctx, nodeID, cluster)
	if err != nil {
		log.Warn(ctx, "failed to retire legacy JSON stats task, will retry", mlog.Err(err))
		return
	}
	if retired {
		return
	}

	// Recheck after selection, before assigning an attempt or making an RPC.
	if !st.CanRunOnNode(nodeID) {
		err = merr.WrapErrServiceNotReadyMsg("waiting for enabled JSON shredding, compatible readers and a compatible DataNode")
		return
	}

	// Update task version
	if err := st.UpdateTaskVersion(nodeID); err != nil {
		log.Warn(context.TODO(), "failed to update stats task version", mlog.Err(err))
		return
	}

	// Prepare request
	req, err := st.prepareJobRequest(ctx, segment)
	if err != nil {
		log.Warn(context.TODO(), "failed to prepare stats request", mlog.Err(err))
		return
	}

	// Use defer for cleanup on error
	defer func() {
		if err != nil {
			st.tryDropTaskOnWorker(cluster)
		}
	}()
	// Execute task creation
	if err = cluster.CreateStats(nodeID, req); err != nil {
		log.Warn(context.TODO(), "failed to create stats task on worker", mlog.Err(err))
		return
	}
	log.Info(context.TODO(), "assign stats task to worker successfully", mlog.FieldTaskID(st.GetTaskID()))

	if err = st.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn(context.TODO(), "failed to update stats task state to InProgress", mlog.Err(err))
		return
	}

	log.Info(context.TODO(), "stats task update state to InProgress successfully", mlog.Int64("task version", st.GetVersion()))
}

func (st *statsTask) shouldDropExternalJSONStatsTask(segment *SegmentInfo) bool {
	if st.GetSubJobType() != indexpb.StatsSubJob_JsonKeyIndexJob || canBuildExternalJSONKeyIndex(segment) {
		return false
	}
	if st.meta == nil || st.meta.collections == nil {
		return false
	}
	// External-table source data may stay unchanged, so the segment may never
	// become dropped. Drop unrebuildable reloaded JSON stats tasks immediately
	// instead of relying on dropped-segment GC to unblock future scheduling.
	collection := st.meta.GetCollection(segment.GetCollectionID())
	return collection != nil && collection.IsExternal()
}

func (st *statsTask) QueryTaskOnWorker(cluster session.Cluster) {
	ctx := context.TODO()
	log := mlog.With(
		mlog.FieldTaskID(st.GetTaskID()),
		mlog.FieldSegmentID(st.GetSegmentID()),
		mlog.FieldNodeID(st.NodeID),
	)

	// The segment may have been dropped (collection/partition drop, compaction)
	// while the task is in flight. Cancel the worker-side job instead of
	// letting it finish stats nobody will read.
	if st.meta.GetHealthySegment(ctx, st.GetSegmentID()) == nil {
		log.Info(ctx, "segment dropped while stats task in progress, aborting")
		st.abortForDroppedSegment(ctx, cluster)
		return
	}

	// Query task status
	results, err := cluster.QueryStats(st.NodeID, &workerpb.QueryJobsRequest{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []int64{st.GetTaskID()},
	})
	if err != nil {
		log.Warn(context.TODO(), "query stats task result failed", mlog.Err(err))
		st.dropAndResetTaskOnWorker(ctx, cluster, err.Error())
		return
	}

	// Process query results
	for _, result := range results.GetResults() {
		if result.GetTaskID() != st.GetTaskID() {
			continue
		}

		state := result.GetState()
		// Handle different task states
		switch state {
		case indexpb.JobState_JobStateFinished:
			err := st.SetJobInfo(ctx, result)
			if errors.Is(err, errJSONStatsResultInvalid) {
				if versions, ok := st.ievm.(JSONStatsVersionManager); ok {
					versions.RejectJSONStatsWriter(st.NodeID)
				}
				log.Warn(ctx, "reject JSON stats result and retry on a compatible worker", mlog.Err(err))
				if err := st.cleanupInvalidJSONStatsResultFiles(ctx, result); err != nil {
					// Keep the completed result available for another cleanup attempt.
					return
				}
				st.dropAndResetTaskOnWorker(ctx, cluster, err.Error())
				return
			}
			if errors.Is(err, errStatsResultStale) {
				st.discardRejectedStatsResult(ctx, cluster, result, "stale stats result discarded")
				return
			}
			if errors.Is(err, errStatsResultDiscarded) {
				st.discardRejectedStatsResult(ctx, cluster, result, "stats result discarded")
				return
			}
			if err != nil {
				return
			}
			st.UpdateStateWithMeta(state, result.GetFailReason())
		case indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateNone:
			st.dropAndResetTaskOnWorker(ctx, cluster, result.GetFailReason())
		case indexpb.JobState_JobStateFailed:
			st.UpdateStateWithMeta(state, result.GetFailReason())
		}
		// Otherwise (inProgress or unissued/init), keep InProgress state
		return
	}

	log.Warn(context.TODO(), "task not found in results")
	st.resetTask(ctx, "task not found in results")
}

// abortForDroppedSegment cancels an in-flight stats task whose origin segment
// is gone: drop the job on the worker first, then remove the task meta. Either
// step failing leaves the task InProgress so the next check round retries.
// Files the worker already uploaded are reclaimed by the orphan binlog scans.
func (st *statsTask) abortForDroppedSegment(ctx context.Context, cluster session.Cluster) {
	if err := st.tryDropTaskOnWorker(cluster); err != nil {
		return
	}
	if err := st.meta.statsTaskMeta.DropStatsTask(ctx, st.GetTaskID()); err != nil {
		mlog.Warn(ctx, "remove stats task of dropped segment failed, will retry later",
			mlog.FieldTaskID(st.GetTaskID()), mlog.FieldSegmentID(st.GetSegmentID()), mlog.Err(err))
		return
	}
	st.SetState(indexpb.JobState_JobStateNone, "segment is not healthy")
}

func (st *statsTask) tryDropTaskOnWorker(cluster session.Cluster) error {
	log := mlog.With(
		mlog.FieldTaskID(st.GetTaskID()),
		mlog.FieldSegmentID(st.GetSegmentID()),
		mlog.FieldNodeID(st.NodeID),
	)

	err := cluster.DropStats(st.NodeID, st.GetTaskID())
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		log.Warn(context.TODO(), "failed to drop stats task on worker", mlog.Err(err))
		return err
	}

	log.Info(context.TODO(), "stats task dropped successfully")
	return nil
}

func (st *statsTask) discardRejectedStatsResult(ctx context.Context, cluster session.Cluster, result *workerpb.StatsResult, reason string) {
	log := mlog.With(
		mlog.FieldTaskID(st.GetTaskID()),
		mlog.FieldSegmentID(st.GetSegmentID()),
		mlog.String("subJobType", st.GetSubJobType().String()),
	)

	if st.shouldCleanupRejectedStatsResultFiles() {
		// Do not defer rejected V3 stats cleanup to dropped-segment GC for
		// external collections. External collection segments are patched only
		// when the source changes; if the source stays stable, the segment can
		// remain active forever and stale stats files would never be removed by
		// dropped-segment GC. Keep this best-effort deletion external-only so
		// internal collections continue to rely on existing GC ownership rules.
		st.cleanupRejectedStatsResultFiles(ctx, result)
	}
	if err := st.tryDropTaskOnWorker(cluster); err != nil {
		log.Warn(ctx, "failed to drop rejected stats task on worker", mlog.Err(err))
	}
	if err := st.meta.statsTaskMeta.DropStatsTask(ctx, st.GetTaskID()); err != nil {
		log.Warn(ctx, "failed to drop rejected stats task meta", mlog.Err(err))
		return
	}
	st.SetState(indexpb.JobState_JobStateNone, reason)
	log.Info(ctx, "discard rejected stats result", mlog.String("reason", reason))
}

func (st *statsTask) shouldCleanupRejectedStatsResultFiles() bool {
	if st.meta == nil || st.meta.collections == nil {
		return false
	}
	collection, ok := st.meta.collections.Get(st.GetCollectionID())
	if !ok {
		return false
	}
	return collection.IsExternal()
}

// Invalid worker output is not trusted as a deletion manifest. Only clean
// this attempt's requested fields inside the live external segment's base path.
func (st *statsTask) cleanupInvalidJSONStatsResultFiles(ctx context.Context, result *workerpb.StatsResult) error {
	if !st.shouldCleanupRejectedStatsResultFiles() || st.meta.chunkManager == nil ||
		result.GetTaskID() != st.GetTaskID() || result.GetSegmentID() != st.GetSegmentID() ||
		result.GetCollectionID() != st.GetCollectionID() || result.GetPartitionID() != st.GetPartitionID() {
		return nil
	}
	locks := st.meta.getSegmentManifestLocks()
	locks.Lock(st.GetSegmentID())
	defer locks.Unlock(st.GetSegmentID())
	segment := st.meta.GetSegment(ctx, st.GetSegmentID())
	if !canCommitStatsManifestDelta(segment) || segment.GetCollectionID() != st.GetCollectionID() ||
		segment.GetPartitionID() != st.GetPartitionID() {
		return nil
	}
	if snapshots := st.meta.GetSnapshotMeta(); snapshots != nil &&
		snapshots.IsSegmentGCBlocked(segment.GetCollectionID(), segment.GetID()) {
		return nil
	}
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil || basePath == "" {
		return nil
	}
	resultManifest := result.GetBaseManifest()
	if resultManifest == "" {
		resultManifest = result.GetManifest()
	}
	resultBase, _, err := packed.UnmarshalManifestPath(resultManifest)
	if err != nil || strings.TrimSuffix(resultBase, "/") != strings.TrimSuffix(basePath, "/") {
		return nil
	}
	// Manifest publication shares this lock. A file referenced by the current
	// stats must survive, even if an invalid result reports it as its own output.
	var referenced []string
	for _, stats := range segment.GetJsonKeyStats() {
		if stats == nil {
			continue
		}
		prefix := metautil.BuildJSONKeyStatsBasePath("", segment, stats)
		for _, file := range metautil.BuildStatsFilePaths(prefix, stats.GetFiles()) {
			referenced = append(referenced, path.Clean(file))
		}
	}
	sort.Strings(referenced)
	owned := &workerpb.StatsResult{
		BaseManifest:     segment.GetManifestPath(),
		JsonKeyStatsLogs: make(map[int64]*datapb.JsonKeyStats),
	}
	for _, fieldID := range st.GetJsonStatsFieldIds() {
		stats := result.GetJsonKeyStatsLogs()[fieldID]
		if stats == nil || stats.GetFieldID() != fieldID || stats.GetBuildID() != st.GetTaskID() ||
			stats.GetVersion() != st.GetVersion() {
			continue
		}
		prefix := path.Join(basePath, "_stats", fmt.Sprintf("json_stats.%d", fieldID))
		files := make([]string, 0, len(stats.GetFiles()))
		for _, file := range stats.GetFiles() {
			if (path.IsAbs(file) || metautil.IsJSONKeyStatsFullPath(file)) && !strings.HasPrefix(file, prefix+"/") {
				continue
			}
			if !strings.HasPrefix(file, prefix+"/") {
				file = path.Join(prefix, file)
			}
			file = path.Clean(file)
			if !strings.HasPrefix(file, prefix+"/") {
				continue
			}
			// Some backends remove prefixes (and local removal is recursive).
			// Protect any candidate that could also remove a referenced key.
			next := sort.SearchStrings(referenced, file)
			if next == len(referenced) || !strings.HasPrefix(referenced[next], file) {
				files = append(files, file)
			}
		}
		owned.JsonKeyStatsLogs[fieldID] = &datapb.JsonKeyStats{FieldID: fieldID, Files: files}
	}
	// The `owned` files are already complete storage keys (relative names were
	// resolved against the live segment's base path while filtering). Flatten
	// and dedupe them directly instead of routing through
	// collectRejectedStatsResultFiles, whose path computation assumes raw
	// relative JSON file names and would re-prefix these complete keys.
	files := make([]string, 0)
	seen := make(map[string]struct{})
	for _, stats := range owned.JsonKeyStatsLogs {
		for _, file := range stats.GetFiles() {
			if _, ok := seen[file]; ok {
				continue
			}
			seen[file] = struct{}{}
			files = append(files, file)
		}
	}
	for _, file := range files {
		if err := st.meta.chunkManager.Remove(ctx, file); err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
			mlog.RatedWarn(ctx, 1.0, "failed to clean invalid JSON stats output before retry",
				mlog.FieldTaskID(st.GetTaskID()), mlog.FieldSegmentID(st.GetSegmentID()),
				mlog.String("file", file), mlog.Err(err))
			return err
		}
	}
	return nil
}

func (st *statsTask) cleanupRejectedStatsResultFiles(ctx context.Context, result *workerpb.StatsResult) {
	if st.meta == nil || st.meta.chunkManager == nil {
		return
	}

	expectedSegmentID := st.GetSegmentID()
	if st.GetTargetSegmentID() != 0 {
		expectedSegmentID = st.GetTargetSegmentID()
	}
	if result.GetCollectionID() != st.GetCollectionID() ||
		result.GetPartitionID() != st.GetPartitionID() ||
		result.GetSegmentID() != expectedSegmentID {
		mlog.Warn(ctx, "refuse to cleanup rejected stats files for a mismatched segment",
			mlog.FieldTaskID(st.GetTaskID()),
			mlog.Int64("resultCollectionID", result.GetCollectionID()),
			mlog.Int64("resultPartitionID", result.GetPartitionID()),
			mlog.Int64("resultSegmentID", result.GetSegmentID()))
		return
	}

	basePath, files, err := collectRejectedStatsResultFiles(
		result, st.meta.chunkManager.RootPath(),
		st.GetCollectionID(), st.GetPartitionID(), expectedSegmentID)
	if err != nil {
		mlog.Warn(ctx, "failed to collect rejected stats result files",
			mlog.FieldTaskID(st.GetTaskID()),
			mlog.FieldSegmentID(st.GetSegmentID()),
			mlog.Err(err))
		return
	}
	if len(files) == 0 {
		return
	}
	// Complete keys are absolute filesystem paths locally and bucket-relative
	// object keys remotely. Never resolve a local base against the process CWD
	// or pass a filesystem path to remote storage.
	if _, local := st.meta.chunkManager.(*storage.LocalChunkManager); local != path.IsAbs(basePath) {
		mlog.Warn(ctx, "refuse to cleanup rejected stats files with a manifest base for another storage namespace",
			mlog.FieldTaskID(st.GetTaskID()),
			mlog.String("basePath", basePath))
		return
	}
	if err := st.meta.chunkManager.MultiRemove(ctx, files); err != nil {
		mlog.Warn(ctx, "failed to cleanup rejected stats result files",
			mlog.FieldTaskID(st.GetTaskID()),
			mlog.FieldSegmentID(st.GetSegmentID()),
			mlog.Strings("files", files),
			mlog.Err(err))
	}
}

func collectRejectedStatsResultFiles(
	result *workerpb.StatsResult,
	rootPath string,
	collectionID, partitionID, segmentID int64,
) (string, []string, error) {
	files := make([]string, 0)
	seen := make(map[string]struct{})
	addStatsFiles := func(statsBasePath string, statFiles []string, relative bool) error {
		local := path.IsAbs(statsBasePath)
		statsPrefix := statsBasePath + "/"
		if local {
			statsPrefix = path.Clean(statsBasePath) + "/"
		}
		for _, file := range statFiles {
			if strings.TrimSpace(file) == "" {
				continue
			}
			// JSON reports names relative to the field's stats directory;
			// TEXT reports complete keys. Decode before the shared check.
			if relative {
				if path.IsAbs(file) {
					return merr.WrapErrDataIntegrityMsg("rejected JSON stats file must be relative to its stats directory")
				}
				file = statsBasePath + "/" + file
			}
			checkedFile := file
			if local {
				checkedFile = path.Clean(file)
			}
			// Workers report complete TEXT stats keys, including an absolute
			// filesystem path on local storage. Do not guess a prefix for a
			// relative filename or a key outside this field's stats directory.
			if !strings.HasPrefix(checkedFile, statsPrefix) {
				return merr.WrapErrDataIntegrityMsg("rejected stats file escapes its stats directory")
			}
			// Clean only for the local directory check. Remote object keys must be
			// deleted exactly as reported, not rewritten as filesystem paths.
			if _, ok := seen[file]; ok {
				continue
			}
			seen[file] = struct{}{}
			files = append(files, file)
		}
		return nil
	}
	manifest := result.GetBaseManifest()
	if manifest == "" {
		manifest = result.GetManifest()
	}
	basePath := rootPath
	if manifest != "" {
		var err error
		basePath, _, err = packed.UnmarshalManifestPath(manifest)
		if err != nil {
			return "", nil, err
		}
		if err := validateStatsManifestBase(basePath, collectionID, partitionID, segmentID); err != nil {
			return "", nil, err
		}
		if result.GetManifest() != "" && result.GetManifest() != manifest {
			resultBase, _, err := packed.UnmarshalManifestPath(result.GetManifest())
			if err != nil {
				return "", nil, err
			}
			sameBase := resultBase == basePath
			if path.IsAbs(basePath) {
				sameBase = path.Clean(resultBase) == path.Clean(basePath)
			}
			if !sameBase {
				return "", nil, merr.WrapErrDataIntegrityMsg("rejected stats result changes manifest base path")
			}
		}
	}

	// Manifest-less V2 results use build/version directories; V3 uses _stats.
	validateLegacyLocation := func(fieldID, buildID, version int64) error {
		if buildID <= 0 || version <= 0 {
			return merr.WrapErrDataIntegrityMsg(
				"rejected stats result for field %d carries no build ID or version to locate its stats directory", fieldID)
		}
		return nil
	}
	for fieldID, stats := range result.GetTextStatsLogs() {
		statsBase := basePath + fmt.Sprintf("/_stats/text_index.%d", fieldID)
		if manifest == "" {
			if err := validateLegacyLocation(fieldID, stats.GetBuildID(), stats.GetVersion()); err != nil {
				return "", nil, err
			}
			statsBase = metautil.BuildTextIndexPrefix(rootPath, stats.GetBuildID(), stats.GetVersion(),
				collectionID, partitionID, segmentID, fieldID)
		}
		if err := addStatsFiles(statsBase, stats.GetFiles(), false); err != nil {
			return "", nil, err
		}
	}
	for fieldID, stats := range result.GetJsonKeyStatsLogs() {
		statsBase := metautil.BuildJSONKeyStatsV3Prefix(basePath, fieldID)
		if manifest == "" {
			if err := validateLegacyLocation(fieldID, stats.GetBuildID(), stats.GetVersion()); err != nil {
				return "", nil, err
			}
			statsBase = metautil.BuildJSONKeyStatsPrefix(rootPath,
				stats.GetJsonKeyStatsDataFormat(), stats.GetBuildID(), stats.GetVersion(),
				collectionID, partitionID, segmentID, fieldID)
		}
		if err := addStatsFiles(statsBase, stats.GetFiles(), true); err != nil {
			return "", nil, err
		}
	}
	return basePath, files, nil
}

func validateStatsManifestBase(basePath string, collectionID, partitionID, segmentID int64) error {
	// Manifests carry complete storage keys, not provider URIs.  A URI would
	// be passed verbatim to ChunkManager cleanup and is not a valid object key.
	if strings.Contains(basePath, "://") {
		return merr.WrapErrDataIntegrityMsg("stats manifest base must be a storage key")
	}
	if path.IsAbs(basePath) {
		basePath = path.Clean(basePath)
	}
	if !segmentBaseMatches(basePath, collectionID, partitionID, segmentID) {
		return merr.WrapErrDataIntegrityMsg("stats manifest base does not match task segment identity")
	}
	return nil
}

func (st *statsTask) DropTaskOnWorker(cluster session.Cluster) {
	st.tryDropTaskOnWorker(cluster)
}

// Helper for empty segment handling
func (st *statsTask) handleEmptySegment(ctx context.Context) error {
	result := &workerpb.StatsResult{
		TaskID:       st.GetTaskID(),
		State:        st.GetState(),
		FailReason:   st.GetFailReason(),
		CollectionID: st.GetCollectionID(),
		PartitionID:  st.GetPartitionID(),
		SegmentID:    st.GetSegmentID(),
		Channel:      st.GetInsertChannel(),
		NumRows:      0,
	}

	if err := st.SetJobInfo(ctx, result); err != nil {
		return err
	}

	if err := st.UpdateStateWithMeta(indexpb.JobState_JobStateFinished, "segment num row is zero"); err != nil {
		return err
	}

	return nil
}

// Prepare the stats request
func (st *statsTask) prepareJobRequest(ctx context.Context, segment *SegmentInfo) (*workerpb.CreateStatsRequest, error) {
	// Snapshot once for this attempt: the switch can change after node selection
	// or while preparing the request. Never dispatch a dedicated JSON job that
	// tells the worker to skip the very output we require on completion.
	enableJSONKeyStats := Params.CommonCfg.EnabledJSONKeyStats.GetAsBool()
	if st.GetSubJobType() == indexpb.StatsSubJob_JsonKeyIndexJob &&
		(!enableJSONKeyStats || jsonShreddingDisabledByDeprecatedConfig()) {
		return nil, merr.WrapErrServiceNotReadyMsg("JSON shredding is disabled")
	}

	collInfo, err := st.handler.GetCollection(ctx, segment.GetCollectionID())
	if err != nil {
		return nil, merr.Wrap(err, "failed to get collection info")
	}
	// GetCollection can return (nil, nil) on a cache miss; merr.Wrap(nil) would
	// be nil and silently submit a malformed request, so guard collInfo
	// separately with a typed not-found.
	if collInfo == nil {
		return nil, merr.WrapErrCollectionNotFound(segment.GetCollectionID())
	}
	if collInfo.Schema == nil || len(collInfo.Schema.GetFields()) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("collection schema is nil or has no fields, collectionID: %d", segment.GetCollectionID())
	}

	// Calculate binlog allocation
	binlogNum := (segment.getSegmentSize()/Params.DataNodeCfg.BinLogMaxSize.GetAsInt64() + 1) *
		int64(len(collInfo.Schema.GetFields())) *
		paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt64()

	// Allocate IDs
	start, end, err := st.allocator.AllocN(binlogNum + int64(len(collInfo.Schema.GetFunctions())) + 1)
	if err != nil {
		return nil, merr.Wrap(err, "failed to allocate log IDs")
	}

	// Create the request
	req := &workerpb.CreateStatsRequest{
		ClusterID:       Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskID:          st.GetTaskID(),
		CollectionID:    segment.GetCollectionID(),
		PartitionID:     segment.GetPartitionID(),
		InsertChannel:   segment.GetInsertChannel(),
		SegmentID:       segment.GetID(),
		StorageConfig:   createStorageConfig(),
		Schema:          collInfo.Schema,
		SubJobType:      st.GetSubJobType(),
		TargetSegmentID: st.GetTargetSegmentID(),
		InsertLogs:      segment.GetBinlogs(),
		StartLogID:      start,
		EndLogID:        end,
		NumRows:         segment.GetNumOfRows(),
		// update version after check
		TaskVersion:                      st.GetVersion(),
		EnableJsonKeyStats:               enableJSONKeyStats,
		JsonKeyStatsDataFormat:           st.getJSONStatsDataFormat(),
		TaskSlot:                         st.taskSlot,
		StorageVersion:                   segment.StorageVersion,
		CurrentScalarIndexVersion:        st.ievm.ResolveScalarIndexVersion(),
		JsonStatsMaxShreddingColumns:     Params.DataCoordCfg.JSONStatsMaxShreddingColumns.GetAsInt64(),
		JsonStatsShreddingRatioThreshold: Params.DataCoordCfg.JSONStatsShreddingRatioThreshold.GetAsFloat(),
		JsonStatsWriteBatchSize:          Params.DataCoordCfg.JSONStatsWriteBatchSize.GetAsInt64(),
		ManifestPath:                     segment.GetManifestPath(),
	}
	WrapPluginContext(segment.GetCollectionID(), collInfo.Schema.GetProperties(), req)

	return req, nil
}

func (st *statsTask) SetJobInfo(ctx context.Context, result *workerpb.StatsResult) error {
	if err := st.validateJSONStatsResult(ctx, result); err != nil {
		return err
	}

	var err error
	switch st.GetSubJobType() {
	case indexpb.StatsSubJob_TextIndexJob:
		err = st.commitTextIndexStats(ctx, result)
		if err != nil {
			mlog.Warn(ctx, "save text index stats result failed", mlog.FieldTaskID(st.GetTaskID()),
				mlog.FieldSegmentID(st.GetSegmentID()), mlog.Err(err))
			break
		}
	case indexpb.StatsSubJob_JsonKeyIndexJob:
		err = st.commitJSONKeyStats(ctx, result)
		if err != nil {
			mlog.Warn(ctx, "save json key index stats result failed", mlog.Int64("taskId", st.GetTaskID()),
				mlog.FieldSegmentID(st.GetSegmentID()), mlog.Err(err))
			break
		}
	case indexpb.StatsSubJob_Sort:
		// For V2 segments (no manifest), persist statsLogs and bm25Logs.
		// For V3 segments (manifest set), stats are already in manifest.
		segment := st.meta.GetHealthySegment(ctx, st.GetTargetSegmentID())
		if segment != nil && segment.GetManifestPath() == "" {
			var operators []SegmentOperator
			if len(result.GetStatsLogs()) > 0 {
				operators = append(operators, SetStatslogs(result.GetStatsLogs()))
			}
			if len(result.GetBm25Logs()) > 0 {
				operators = append(operators, SetBm25Statslogs(result.GetBm25Logs()))
			}
			if len(operators) > 0 {
				err = st.meta.UpdateSegment(st.GetTargetSegmentID(), operators...)
				if err != nil {
					mlog.Warn(ctx, "save sort stats result failed", mlog.FieldTaskID(st.GetTaskID()),
						mlog.FieldSegmentID(st.GetTargetSegmentID()), mlog.Err(err))
					break
				}
			}
		}
	case indexpb.StatsSubJob_BM25Job:
	// bm25 logs are generated during with segment flush.
	default:
		mlog.Warn(ctx, "unexpected sub job type", mlog.String("type", st.GetSubJobType().String()))
	}

	// if segment is not found, it means the segment is already dropped,
	// so we can ignore the error and mark task as finished.
	if err != nil && !errors.Is(err, merr.ErrSegmentNotFound) {
		return err
	}

	// Update segment manifest version so subsequent stats tasks use the latest version.
	if manifest := result.GetManifest(); manifest != "" &&
		st.GetSubJobType() != indexpb.StatsSubJob_TextIndexJob &&
		st.GetSubJobType() != indexpb.StatsSubJob_JsonKeyIndexJob {
		segID := st.GetSegmentID()
		if st.GetSubJobType() == indexpb.StatsSubJob_Sort {
			segID = st.GetTargetSegmentID()
		}
		var updateErr error
		if st.shouldPublishPreparedManifest(ctx, segID, result) {
			updateErr = classifyStatsManifestCommitError(st.meta.CommitSegmentManifest(ctx, SegmentManifestCommit{
				SegmentID:        segID,
				ExpectedManifest: result.GetBaseManifest(),
				Mutation: ManifestMutation{
					Type:         ManifestMutationNoop,
					ManifestPath: manifest,
				},
			}))
		} else {
			updateErr = st.meta.UpdateSegmentsInfo(ctx, UpdateManifest(segID, manifest))
		}
		if updateErr != nil {
			mlog.Warn(ctx, "failed to update manifest after stats task",
				mlog.FieldTaskID(st.GetTaskID()),
				mlog.FieldSegmentID(segID),
				mlog.Err(updateErr))
			if !errors.Is(updateErr, merr.ErrSegmentNotFound) {
				return updateErr
			}
		}
	}

	mlog.Info(ctx, "SetJobInfo for stats task success", mlog.FieldTaskID(st.GetTaskID()),
		mlog.Int64("oldSegmentID", st.GetSegmentID()), mlog.Int64("targetSegmentID", st.GetTargetSegmentID()),
		mlog.String("subJobType", st.GetSubJobType().String()), mlog.String("state", st.GetState().String()))
	return nil
}

// commitTextIndexStats publishes a completed standalone TextIndexJob. For a
// StorageV3 segment DataCoord runs the manifest transaction itself: it rebuilds
// the text-index StatEntries from the worker's raw result and commits them onto
// the segment's *current* manifest via CommitSegmentManifest (a structured
// ManifestMutationCommitUpdates), rebasing rather than adopting a manifest the
// worker pre-baked against a possibly stale base. TextStatsLogs already carries
// full object keys, so the same entries feed both the loon transaction and the
// SegmentInfo dual-write. For a V2 segment (no manifest) it falls back to the
// ordinary operator that persists the stats into SegmentInfo.
func (st *statsTask) commitTextIndexStats(ctx context.Context, result *workerpb.StatsResult) error {
	segment := st.meta.GetSegment(ctx, st.GetSegmentID())
	if !canCommitStatsManifestDelta(segment) {
		// V2 segment, or one retired by compaction while the task ran: the operator
		// persists stats when no manifest is present and discards the obsolete
		// result otherwise, so the task still reaches a terminal state.
		return st.meta.UpdateSegmentsInfo(ctx, updateStatsResultIfManifestMatches(ctx, st.GetSegmentID(), st.GetTaskID(), result))
	}
	textStats := result.GetTextStatsLogs()
	if len(textStats) == 0 {
		return nil
	}
	if statsAlreadyCommitted(segment.GetTextStatsLogs(), textStats, func(s *datapb.TextIndexStats) int64 { return s.GetBuildID() }) {
		mlog.Info(ctx, "text index stats already applied; skipping manifest commit",
			mlog.FieldTaskID(st.GetTaskID()), mlog.FieldSegmentID(st.GetSegmentID()))
		return nil
	}
	// No ExpectedManifest CAS: the per-segment commit lock serializes framework
	// writers, so the transaction is generated from whatever pointer is current
	// under that lock, and CommitSegmentManifest itself fails publication as stale
	// if the pointer moves during manifest I/O (an out-of-lock writer). Pinning the
	// pointer read a moment ago would only spuriously discard a result a concurrent
	// sibling sub-job (e.g. the JSON-key commit) merely committed past.
	return classifyStatsManifestCommitError(st.meta.CommitSegmentManifest(ctx, SegmentManifestCommit{
		SegmentID:     st.GetSegmentID(),
		StorageConfig: createStorageConfig(),
		Mutation: ManifestMutation{
			Type: ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{
				// Pin current_scalar_index_version to the value the worker actually
				// built the index with (echoed per entry), not a fresh resolve which
				// could drift from the shipped index.
				Stats: packed.TextIndexStatEntries(textStats),
			},
		},
		CatalogMutation: SegmentCatalogMutation{TextStats: textStats},
	}))
}

// commitJSONKeyStats is the JsonKeyIndexJob analog of commitTextIndexStats.
// The manifest requires absolute stat-file paths, while the result ships
// manifest-relative paths (kept relative for the SegmentInfo dual-write and read
// reconstruction), so it rebuilds the absolute form against the segment's stable
// base path — exactly the conversion the worker applied before it stopped baking.
func (st *statsTask) commitJSONKeyStats(ctx context.Context, result *workerpb.StatsResult) error {
	segment := st.meta.GetSegment(ctx, st.GetSegmentID())
	if !canCommitStatsManifestDelta(segment) {
		return st.meta.UpdateSegmentsInfo(ctx, updateStatsResultIfManifestMatches(ctx, st.GetSegmentID(), st.GetTaskID(), result))
	}
	jsonStats := result.GetJsonKeyStatsLogs()
	if len(jsonStats) == 0 {
		return nil
	}
	if statsAlreadyCommitted(segment.GetJsonKeyStats(), jsonStats, func(s *datapb.JsonKeyStats) int64 { return s.GetBuildID() }) {
		mlog.Info(ctx, "json key stats already applied; skipping manifest commit",
			mlog.FieldTaskID(st.GetTaskID()), mlog.FieldSegmentID(st.GetSegmentID()))
		return nil
	}
	entries, err := jsonKeyStatEntriesForManifest(segment.GetManifestPath(), jsonStats)
	if err != nil {
		return err
	}
	// No ExpectedManifest CAS: the per-segment commit lock serializes framework
	// writers, so the transaction is generated from whatever pointer is current
	// under that lock, and CommitSegmentManifest itself fails publication as stale
	// if the pointer moves during manifest I/O (an out-of-lock writer). Pinning the
	// pointer read a moment ago would only spuriously discard a result a concurrent
	// sibling sub-job (e.g. the text-index commit) merely committed past.
	return classifyStatsManifestCommitError(st.meta.CommitSegmentManifest(ctx, SegmentManifestCommit{
		SegmentID:     st.GetSegmentID(),
		StorageConfig: createStorageConfig(),
		Mutation: ManifestMutation{
			Type: ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{
				Stats: entries,
			},
		},
		CatalogMutation: SegmentCatalogMutation{JSONKeyStats: jsonStats},
	}))
}

// canCommitStatsManifestDelta reports whether the DataCoord-run manifest
// transaction applies: a live StorageV3 segment with a published manifest. A
// nil/unhealthy segment or a V2 segment routes to the non-manifest fallback.
func canCommitStatsManifestDelta(segment *SegmentInfo) bool {
	return segment != nil &&
		isSegmentHealthy(segment) &&
		segment.GetStorageVersion() == storage.StorageV3 &&
		segment.GetManifestPath() != ""
}

// statsAlreadyCommitted is a restart-safe idempotent-replay guard. Every field's
// BuildID equals its stats task's globally unique task ID, and CommitSegmentManifest
// dual-writes the stats into SegmentInfo atomically with the manifest pointer, so a
// persisted BuildID that matches this result is an exactly-once token: the commit
// already landed (even across a DataCoord restart, since TextStatsLogs/JsonKeyStats
// are persisted to etcd and reloaded, unlike a V3 segment's manifest-only binlogs).
// A different or absent BuildID means this is a fresh build to publish. Stats commits
// overwrite by key and are therefore idempotent regardless, so this guard only avoids
// minting a redundant manifest revision on a retry, never a correctness hazard.
func statsAlreadyCommitted[T any](existing, incoming map[int64]T, buildID func(T) int64) bool {
	for fieldID, in := range incoming {
		cur, ok := existing[fieldID]
		if !ok || buildID(cur) != buildID(in) {
			return false
		}
	}
	return true
}

// jsonKeyStatEntriesForManifest rebuilds JSON key StatEntries with absolute file
// paths for the manifest transaction. The segment base path is version-independent,
// so reconstructing against the current manifest yields the same physical location
// the worker uploaded to. It clones so the caller's manifest-relative result (reused
// for the SegmentInfo dual-write) is left untouched.
func jsonKeyStatEntriesForManifest(manifestPath string, jsonStats map[int64]*datapb.JsonKeyStats) ([]packed.StatEntry, error) {
	basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, merr.Wrap(err, "parse manifest path for json stats base path")
	}
	manifestStats := make(map[int64]*datapb.JsonKeyStats, len(jsonStats))
	for fieldID, stats := range jsonStats {
		cloned := proto.Clone(stats).(*datapb.JsonKeyStats)
		prefix := fmt.Sprintf("%s/_stats/json_stats.%d", basePath, fieldID)
		for i, f := range cloned.GetFiles() {
			cloned.Files[i] = prefix + "/" + f
		}
		manifestStats[fieldID] = cloned
	}
	return packed.JSONKeyStatEntries(manifestStats), nil
}

// classifyStatsManifestCommitError preserves the typed manifest conflict while
// marking it with the stats scheduler's stale-result identity. QueryTaskOnWorker
// consumes that identity and discards the obsolete worker result instead of
// retrying it as a generic service-unavailable failure.
func classifyStatsManifestCommitError(err error) error {
	if errors.Is(err, errSegmentManifestStale) {
		return staleStatsResultError{cause: err}
	}
	return err
}

// staleStatsResultError tags a segment-manifest conflict as a stale stats
// result while preserving the wrapped chain (ServiceUnavailable +
// errSegmentManifestStale via Unwrap). It implements Is so both stdlib
// errors.Is (used by testify's ErrorIs) and cockroachdb errors.Is detect
// errStatsResultStale; cockroachdb v1.9.1's errors.Mark yields a marker with no
// Is method, invisible to stdlib errors.Is.
type staleStatsResultError struct{ cause error }

func (e staleStatsResultError) Error() string        { return e.cause.Error() }
func (e staleStatsResultError) Unwrap() error        { return e.cause }
func (e staleStatsResultError) Is(target error) bool { return target == errStatsResultStale }

// shouldPublishPreparedManifest identifies the temporary compatibility path
// for workers which still return a prepared stats manifest, including the
// first manifest. The Noop adapter keeps pointer publication serialized while
// a follow-up changes workers to return structured deltas.
func (st *statsTask) shouldPublishPreparedManifest(ctx context.Context, segmentID int64, result *workerpb.StatsResult) bool {
	segment := st.meta.GetSegment(ctx, segmentID)
	return segment != nil &&
		// Skip a segment retired by compaction while the stats task was still
		// running: GetSegment returns dropped segments, and routing one into
		// CommitSegmentManifest would only fail its health check. The ordinary
		// fallback path (updateStatsResultIfManifestMatches) discards the
		// obsolete result instead, so the task reaches a terminal state.
		isSegmentHealthy(segment) &&
		segment.GetStorageVersion() == storage.StorageV3 &&
		result.GetManifest() != "" &&
		// Nothing to publish when the worker's manifest already matches the
		// current pointer; fall through to the ordinary no-op path instead of
		// re-publishing an identical revision.
		result.GetManifest() != segment.GetManifestPath()
}

func updateStatsResultIfManifestMatches(ctx context.Context, segmentID, taskID int64, result *workerpb.StatsResult) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		current := modPack.meta.segments.GetSegment(segmentID)
		if current == nil || !isSegmentHealthy(current) {
			mlog.Warn(ctx, "discard stats result for missing or unhealthy segment",
				mlog.FieldTaskID(taskID),
				mlog.FieldSegmentID(segmentID),
				mlog.Bool("segmentMissing", current == nil))
			return modPack.fail(errStatsResultDiscarded)
		}
		if result.GetBaseManifest() != "" && current.GetManifestPath() != result.GetBaseManifest() {
			mlog.Info(ctx, "discard stale stats result",
				mlog.FieldTaskID(taskID),
				mlog.FieldSegmentID(segmentID),
				mlog.String("baseManifest", result.GetBaseManifest()),
				mlog.String("currentManifest", current.GetManifestPath()),
				mlog.String("resultManifest", result.GetManifest()))
			return modPack.fail(errStatsResultStale)
		}

		hasTextStats := len(result.GetTextStatsLogs()) > 0
		hasJSONStats := len(result.GetJsonKeyStatsLogs()) > 0
		manifestChanged := result.GetManifest() != "" && current.GetManifestPath() != result.GetManifest()
		if !hasTextStats && !hasJSONStats && !manifestChanged {
			return false
		}
		if manifestChanged && current.GetStorageVersion() == storage.StorageV3 {
			return modPack.fail(merr.WrapErrServiceInternalMsg(
				"StorageV3 stats manifest publication must use CommitSegmentManifest, segmentID=%d", segmentID))
		}

		segment := modPack.Get(segmentID)
		if segment == nil {
			return modPack.fail(errStatsResultDiscarded)
		}

		if hasTextStats {
			if segment.TextStatsLogs == nil {
				segment.TextStatsLogs = make(map[int64]*datapb.TextIndexStats)
			}
			for fieldID, logs := range result.GetTextStatsLogs() {
				segment.TextStatsLogs[fieldID] = logs
			}
		}

		if hasJSONStats {
			if segment.JsonKeyStats == nil {
				segment.JsonKeyStats = make(map[int64]*datapb.JsonKeyStats)
			}
			for fieldID, logs := range result.GetJsonKeyStatsLogs() {
				segment.JsonKeyStats[fieldID] = logs
			}
		}
		if result.GetManifest() != "" && segment.GetManifestPath() != result.GetManifest() {
			segment.ManifestPath = result.GetManifest()
		}
		return true
	}
}
