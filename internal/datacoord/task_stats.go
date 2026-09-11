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
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/taskresource"
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

	pricing requirementCache
}

// TaskResources prices this stats sub-job before a node is picked for it, from
// the fields the sub-job actually reads rather than from the whole segment.
// calculateStatsTaskSlot charged whole-segment size for every sub-job, so a
// text index over one matched field of twenty cost twenty times what it reads.
func (st *statsTask) TaskResources() *datapb.TaskResources {
	return st.pricing.get(func() (taskresource.Requirement, bool) {
		segment := st.meta.GetHealthySegment(context.TODO(), st.GetSegmentID())
		if segment == nil {
			return taskresource.LegacySlotToRequirement(st.taskSlot), false
		}
		collection, err := st.handler.GetCollection(context.TODO(), st.GetCollectionID())
		if err != nil || collection == nil || collection.Schema == nil {
			return taskresource.LegacySlotToRequirement(st.taskSlot), false
		}
		return statsRequirement(segment, collection.Schema, st.GetSubJobType()), true
	}).ToProto()
}

var _ globalTask.Task = (*statsTask)(nil)

var (
	errStatsResultStale     = errors.New("stale stats result")
	errStatsResultDiscarded = errors.New("discarded stats result")
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

func (st *statsTask) cleanupRejectedStatsResultFiles(ctx context.Context, result *workerpb.StatsResult) {
	if st.meta == nil || st.meta.chunkManager == nil {
		return
	}

	files, err := collectRejectedStatsResultFiles(result)
	if err != nil {
		mlog.Warn(ctx, "failed to collect rejected stats result files",
			mlog.FieldTaskID(st.GetTaskID()),
			mlog.FieldSegmentID(st.GetSegmentID()),
			mlog.Err(err))
	}
	if len(files) == 0 {
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

func collectRejectedStatsResultFiles(result *workerpb.StatsResult) ([]string, error) {
	files := make([]string, 0)
	seen := make(map[string]struct{})
	addFile := func(file string) {
		if file == "" {
			return
		}
		if _, ok := seen[file]; ok {
			return
		}
		seen[file] = struct{}{}
		files = append(files, file)
	}

	for _, stats := range result.GetTextStatsLogs() {
		for _, file := range stats.GetFiles() {
			addFile(file)
		}
	}

	jsonStats := result.GetJsonKeyStatsLogs()
	if len(jsonStats) == 0 {
		return files, nil
	}

	manifest := result.GetBaseManifest()
	if manifest == "" {
		manifest = result.GetManifest()
	}
	if manifest == "" {
		return files, merr.WrapErrServiceInternalMsg("manifest is empty for rejected json stats result")
	}
	basePath, _, err := packed.UnmarshalManifestPath(manifest)
	if err != nil {
		return files, err
	}
	for fieldID, stats := range jsonStats {
		statsBasePath := fmt.Sprintf("%s/_stats/json_stats.%d", basePath, fieldID)
		for _, file := range metautil.BuildStatsFilePaths(statsBasePath, stats.GetFiles()) {
			addFile(file)
		}
	}
	return files, nil
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
		EnableJsonKeyStats:               Params.CommonCfg.EnabledJSONKeyStats.GetAsBool(),
		JsonKeyStatsDataFormat:           common.JSONStatsDataFormatVersion,
		TaskSlot:                         st.taskSlot,
		StorageVersion:                   segment.StorageVersion,
		CurrentScalarIndexVersion:        st.ievm.ResolveScalarIndexVersion(),
		JsonStatsMaxShreddingColumns:     Params.DataCoordCfg.JSONStatsMaxShreddingColumns.GetAsInt64(),
		JsonStatsShreddingRatioThreshold: Params.DataCoordCfg.JSONStatsShreddingRatioThreshold.GetAsFloat(),
		JsonStatsWriteBatchSize:          Params.DataCoordCfg.JSONStatsWriteBatchSize.GetAsInt64(),
		ManifestPath:                     segment.GetManifestPath(),
	}
	// The same figure the scheduler placed this task on. task_slot above keeps
	// its whole-segment meaning for a worker that predates the vector.
	req.TaskResources = st.pricing.fill(
		statsRequirement(segment, collInfo.Schema, st.GetSubJobType()),
	).ToProto()
	WrapPluginContext(segment.GetCollectionID(), collInfo.Schema.GetProperties(), req)

	return req, nil
}

func (st *statsTask) SetJobInfo(ctx context.Context, result *workerpb.StatsResult) error {
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
				Stats: packed.TextIndexStatEntries(textStats, pinnedScalarIndexVersion(textStats)),
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

// pinnedScalarIndexVersion returns the current_scalar_index_version the worker
// built the text index with, echoed identically on every entry.
func pinnedScalarIndexVersion(textStats map[int64]*datapb.TextIndexStats) int32 {
	for _, ts := range textStats {
		return ts.GetCurrentScalarIndexVersion()
	}
	return 0
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
