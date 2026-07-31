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
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
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

func (st *statsTask) UpdateTaskVersion(nodeID int64, dispatchedManifest string) error {
	if err := st.meta.statsTaskMeta.UpdateVersion(st.GetTaskID(), nodeID, dispatchedManifest); err != nil {
		return err
	}
	st.Version++
	st.NodeID = nodeID
	st.DispatchedManifest = dispatchedManifest
	st.SetState(indexpb.JobState_JobStateInProgress, "")
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
	workerMayExist := false
	defer func() {
		if err != nil {
			if workerMayExist {
				// CreateStats may have reached the worker even when its RPC returned an
				// error. Keep the task InProgress unless DropStats confirms cancellation;
				// otherwise the scheduler could dispatch a second live attempt.
				st.dropAndResetTaskOnWorker(ctx, cluster, err.Error())
			} else {
				st.resetTask(ctx, err.Error())
			}
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

	// The manifest this attempt is dispatched on. Read ONCE here and fed to both
	// consumers: the task record (the CAS expected value) and the request (what
	// the worker builds on). They must be the same string or the fence is
	// meaningless - a stamp staler than the request rejects every result, and a
	// stamp fresher than it silently adopts a stale one, which is precisely the
	// lost update the fence exists to prevent. Threading one local through both
	// makes that a compile-time fact rather than a convention: the two calls
	// cannot be reordered apart, and prepareJobRequest cannot drift by re-reading
	// the segment on its own.
	dispatchedManifest := segment.GetManifestPath()

	// Open the attempt before sending the RPC. The same persistent write records
	// the node/version/manifest and changes the task to InProgress, so an ambiguous
	// CreateStats failure cannot leave a dispatchable Init task behind.
	if err := st.UpdateTaskVersion(nodeID, dispatchedManifest); err != nil {
		log.Warn(context.TODO(), "failed to update stats task version", mlog.Err(err))
		return
	}

	// Prepare request
	req, err := st.prepareJobRequest(ctx, segment, dispatchedManifest)
	if err != nil {
		log.Warn(context.TODO(), "failed to prepare stats request", mlog.Err(err))
		return
	}

	// Execute task creation
	workerMayExist = true
	if err = cluster.CreateStats(nodeID, req); err != nil {
		log.Warn(context.TODO(), "failed to create stats task on worker", mlog.Err(err))
		return
	}
	log.Info(context.TODO(), "assign stats task to worker successfully", mlog.FieldTaskID(st.GetTaskID()))

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
	st.dropAndResetTaskOnWorker(ctx, cluster, "task not found in results")
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
		return
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

// writtenSegmentManifest returns a manifest of the segment this attempt writes,
// read from DataCoord's own meta. Only the base path encoded in it is used, and
// that is version-independent, so a segment whose manifest has moved on since
// the attempt was dispatched still yields the correct prefix.
//
// Returns empty when no such manifest is known, which the caller treats as
// "cannot safely resolve paths" and skips the deletion rather than guessing.
func (st *statsTask) writtenSegmentManifest(ctx context.Context) string {
	segID := st.GetSegmentID()
	if st.GetSubJobType() == indexpb.StatsSubJob_Sort {
		segID = st.GetTargetSegmentID()
	}
	if segment := st.meta.GetHealthySegment(ctx, segID); segment != nil && segment.GetManifestPath() != "" {
		return segment.GetManifestPath()
	}
	// The dispatched manifest is a valid fallback ONLY for an in-place attempt.
	// For a cross-segment one it describes the origin, and using the origin's base
	// path to delete files written under the target is exactly the mistake this
	// helper exists to prevent.
	if segID == st.GetSegmentID() {
		return st.GetDispatchedManifest()
	}
	return ""
}

func (st *statsTask) cleanupRejectedStatsResultFiles(ctx context.Context, result *workerpb.StatsResult) {
	if st.meta == nil || st.meta.chunkManager == nil {
		return
	}

	files, err := collectRejectedStatsResultFiles(result, st.writtenSegmentManifest(ctx))
	if err != nil {
		// Do NOT fall through to MultiRemove on a partial list. An error here means
		// the paths could not be resolved against a manifest DataCoord vouches for,
		// so every entry collected so far is unverified - deleting them would be
		// acting on exactly the result we just rejected. Orphaned files are the
		// cheaper failure; GC and this log are the recovery path.
		mlog.Warn(ctx, "failed to collect rejected stats result files; skipping cleanup",
			mlog.FieldTaskID(st.GetTaskID()),
			mlog.FieldSegmentID(st.GetSegmentID()),
			mlog.Err(err))
		return
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

// collectRejectedStatsResultFiles lists the files a rejected stats result wrote,
// so they can be removed instead of being orphaned.
//
// trustedManifest must be a manifest of the segment the attempt was WRITING,
// obtained from DataCoord's own state - never echoed back from the result. See
// the comment on the JSON branch for why that distinction is load-bearing.
func collectRejectedStatsResultFiles(result *workerpb.StatsResult, trustedManifest string) ([]string, error) {
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

	textStats := result.GetTextStatsLogs()
	jsonStats := result.GetJsonKeyStatsLogs()
	if len(textStats) == 0 && len(jsonStats) == 0 {
		return files, nil
	}

	// Everything below resolves against a manifest base path, and this list feeds
	// a MultiRemove - so the base path decides what gets DELETED. It must
	// therefore come from a manifest DataCoord itself vouches for and that belongs
	// to the segment this attempt was writing.
	//
	// Not result.BaseManifest: a Sort echoes the ORIGIN's manifest while writing
	// the TARGET, which would aim the deletion at the origin's live files. Not
	// result.Manifest either: it is worker-supplied and pointing at a foreign or
	// unparseable segment is one of the reasons a result gets rejected in the
	// first place, so trusting it here would let a bad result steer deletion into
	// another segment. A base path is version-independent, so the written
	// segment's current manifest yields the right prefix no matter how far its
	// version has advanced past the rejected result.
	if trustedManifest == "" {
		return files, merr.WrapErrServiceInternalMsg("no trusted manifest for rejected stats result")
	}
	basePath, _, err := packed.UnmarshalManifestPath(trustedManifest)
	if err != nil {
		return files, err
	}
	segmentBase := path.Clean(strings.TrimSuffix(basePath, "/"))
	validateWithin := func(file, trustedBase string) error {
		cleanBase := path.Clean(strings.TrimSuffix(trustedBase, "/"))
		cleanFile := path.Clean(file)
		// Object-store keys are POSIX paths. Requiring the worker-supplied key to
		// already be canonical is what closes the `trusted/prefix/../foreign`
		// bypass: a raw string-prefix check alone accepts it, while path.Clean
		// resolves it outside the segment before MultiRemove sees the key.
		if cleanFile != file || !strings.HasPrefix(cleanFile, cleanBase+"/") {
			return merr.WrapErrServiceInternalMsg(
				"rejected stats result lists non-canonical or out-of-prefix file %q outside trusted prefix %q",
				file, cleanBase+"/")
		}
		return nil
	}

	// Text stats differ from JSON in that the worker ships ABSOLUTE object keys
	// (BuildStatsFilePaths is applied worker-side, for mixed-version metadata
	// compatibility), so there is nothing to rebuild here - only to verify. Verify
	// we must: an unchecked key goes straight into MultiRemove, and this result was
	// rejected, so its contents have already failed to be trustworthy once.
	//
	// A key outside the written segment's base path is not a file this attempt was
	// entitled to create, so it is not ours to delete. Treat it as a hard error
	// rather than skipping the single key: a worker emitting foreign paths is
	// broken or tampered with, and the honest response is to delete nothing and
	// leave the orphans for a human, not to half-clean on the word of a result we
	// just rejected.
	//
	// The prefix is the right shape only because this cleanup is external-only
	// (shouldCleanupRejectedStatsResultFiles) and an external segment always
	// carries a manifest - validateExternalRefreshPatch rejects a patch with an
	// empty manifest path - so its text stats are addressed under the manifest base
	// path by computeStatsBasePath's StorageV3 branch. A StorageV2 segment would
	// instead put them under BuildTextIndexPrefix(rootPath, ...), which no manifest
	// prefix covers; that combination cannot reach here today, and if it ever does
	// it fails closed (nothing deleted) rather than deleting the wrong thing.
	for _, stats := range textStats {
		for _, file := range stats.GetFiles() {
			if file == "" {
				continue
			}
			if err := validateWithin(file, segmentBase); err != nil {
				// Return nil, not the partial list: the caller must not delete a
				// prefix of a list this function refused to vouch for.
				return nil, merr.Wrap(err, "rejected text stats result file is outside the written segment prefix")
			}
			addFile(file)
		}
	}

	for fieldID, stats := range jsonStats {
		statsBasePath := fmt.Sprintf("%s/_stats/json_stats.%d", basePath, fieldID)
		for _, rawFile := range stats.GetFiles() {
			if rawFile == "" {
				continue
			}
			resolved := metautil.BuildStatsFilePaths(statsBasePath, []string{rawFile})[0]
			if err := validateWithin(resolved, statsBasePath); err != nil {
				return nil, merr.Wrapf(err, "rejected JSON stats result field %d contains unsafe file", fieldID)
			}
			addFile(resolved)
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
// prepareJobRequest builds the worker request for one attempt.
//
// dispatchedManifest is passed in rather than read off `segment` so it is the
// same value the caller stamped on the task record. Do NOT re-derive it here:
// the CAS compares the stamp against the segment's manifest at adoption time,
// so a request built on a different manifest than the one stamped either
// rejects every result or silently adopts a stale one.
func (st *statsTask) prepareJobRequest(ctx context.Context, segment *SegmentInfo, dispatchedManifest string) (*workerpb.CreateStatsRequest, error) {
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
		ManifestPath:                     dispatchedManifest,
	}
	WrapPluginContext(segment.GetCollectionID(), collInfo.Schema.GetProperties(), req)

	return req, nil
}

func (st *statsTask) SetJobInfo(ctx context.Context, result *workerpb.StatsResult) error {
	var err error
	switch st.GetSubJobType() {
	case indexpb.StatsSubJob_TextIndexJob:
		err = st.meta.UpdateSegmentsInfo(ctx, updateStatsResultIfManifestMatches(ctx, statsAdoption{
			segmentID:       st.GetSegmentID(),
			originSegmentID: st.GetSegmentID(),
			base:            st.GetDispatchedManifest(),
			taskID:          st.GetTaskID(),
		}, result))
		if err != nil {
			mlog.Warn(ctx, "save text index stats result failed", mlog.FieldTaskID(st.GetTaskID()),
				mlog.FieldSegmentID(st.GetSegmentID()), mlog.Err(err))
			break
		}
	case indexpb.StatsSubJob_JsonKeyIndexJob:
		err = st.meta.UpdateSegmentsInfo(ctx, updateStatsResultIfManifestMatches(ctx, statsAdoption{
			segmentID:       st.GetSegmentID(),
			originSegmentID: st.GetSegmentID(),
			base:            st.GetDispatchedManifest(),
			taskID:          st.GetTaskID(),
		}, result))
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
	// Route through updateStatsResultIfManifestMatches (not a blind UpdateManifest) so the
	// adoption performs the same base==current CAS as the text/json paths: a result built on a
	// stale base (a concurrent commit advanced the segment's manifest in between) is rejected
	// with errStatsResultStale and the task re-triggers on the current manifest, instead of
	// overwriting the concurrent commit. A Sort writes to a freshly allocated TARGET segment
	// instead of the origin, and the operator skips the CAS for that cross-segment adoption -
	// see its comment for why the base is meaningless there.
	if manifest := result.GetManifest(); manifest != "" &&
		st.GetSubJobType() != indexpb.StatsSubJob_TextIndexJob &&
		st.GetSubJobType() != indexpb.StatsSubJob_JsonKeyIndexJob {
		segID := st.GetSegmentID()
		if st.GetSubJobType() == indexpb.StatsSubJob_Sort {
			segID = st.GetTargetSegmentID()
		}
		if updateErr := st.meta.UpdateSegmentsInfo(ctx, updateStatsResultIfManifestMatches(ctx, statsAdoption{
			segmentID:       segID,
			originSegmentID: st.GetSegmentID(),
			base:            st.GetDispatchedManifest(),
			taskID:          st.GetTaskID(),
		}, result)); updateErr != nil {
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

// statsAdoption describes where a stats result is being adopted and what it was
// built on. Grouped into a struct because segmentID and originSegmentID are
// adjacent int64s that mean opposite things - swapping them at a call site would
// silently invert the fence.
type statsAdoption struct {
	// segmentID is the segment being written.
	segmentID int64
	// originSegmentID is the segment the task READ. For every in-place sub-job
	// (text / JSON index) it equals segmentID. A Sort differs: it reads the origin
	// and writes a freshly allocated target, so the two diverge and the adoption is
	// a birth rather than a rewrite - see the cross-segment branch below.
	originSegmentID int64
	// base is the manifest the attempt was dispatched on, taken from the task
	// record (StatsTask.dispatched_manifest), NOT from the worker's response.
	base   string
	taskID int64
}

// updateStatsResultIfManifestMatches adopts a stats result onto a.segmentID.
func updateStatsResultIfManifestMatches(ctx context.Context, a statsAdoption, result *workerpb.StatsResult) UpdateOperator {
	segmentID, originSegmentID, taskID := a.segmentID, a.originSegmentID, a.taskID
	return func(modPack *updateSegmentPack) bool {
		current := modPack.meta.segments.GetSegment(segmentID)
		if current == nil || !isSegmentHealthy(current) {
			mlog.Warn(ctx, "discard stats result for missing or unhealthy segment",
				mlog.FieldTaskID(taskID),
				mlog.FieldSegmentID(segmentID),
				mlog.Bool("segmentMissing", current == nil))
			return modPack.fail(errStatsResultDiscarded)
		}
		base := a.base
		resultManifest := result.GetManifest()
		currentManifest := current.GetManifestPath()
		reject := func(cause error) bool {
			mlog.Info(ctx, "discard stale or illegitimate stats result",
				mlog.FieldTaskID(taskID),
				mlog.FieldSegmentID(segmentID),
				mlog.String("baseManifest", base),
				mlog.String("currentManifest", currentManifest),
				mlog.String("resultManifest", resultManifest),
				mlog.Err(cause))
			return modPack.fail(errStatsResultStale)
		}
		// Base fence. It answers "did the segment I am about to overwrite move
		// since the worker read it", so it only means anything for an IN-PLACE
		// rewrite. Two cases opt out:
		//
		//   - Cross-segment adoption (a Sort writing its freshly allocated target).
		//     The base describes the ORIGIN, the current manifest belongs to the
		//     TARGET; they are different entities and comparing them would reject
		//     every sort result. A newly allocated target also has no second writer
		//     to fence against - the successor fence below is what still protects it
		//     once it has been materialized by an earlier attempt.
		//   - An empty base, which means the attempt was dispatched by a DataCoord
		//     predating StatsTask.dispatched_manifest and was still in flight across
		//     the upgrade. Fail open for it. Note this no longer depends on the
		//     DataNode's version at all - the base comes from our own task record,
		//     so an old worker is fenced exactly like a new one, and the window here
		//     drains within one upgrade cycle instead of persisting for as long as
		//     any old worker survives.
		crossSegment := segmentID != originSegmentID
		if !crossSegment && base != "" && currentManifest != base {
			return reject(nil)
		}
		// Successor fence: whenever the result actually MOVES the pointer of an
		// already-materialized segment, it must be a legal, strictly-forward
		// successor on the same base path — not a cross-segment pointer or a
		// rollback. base==current alone is not enough, so this also guards a
		// correct-base worker that emits a bogus result. A true birth (empty current
		// manifest) or a stats-only result that does not move the pointer skips it.
		if resultManifest != "" && currentManifest != "" && resultManifest != currentManifest {
			cmp, err := packed.CompareManifestPath(resultManifest, currentManifest)
			if err != nil || cmp <= 0 {
				if err == nil {
					err = merr.WrapErrServiceInternalMsg("stats result manifest %q does not advance the current manifest %q",
						resultManifest, currentManifest)
				}
				return reject(err)
			}
		}

		hasTextStats := len(result.GetTextStatsLogs()) > 0
		hasJSONStats := len(result.GetJsonKeyStatsLogs()) > 0
		manifestChanged := result.GetManifest() != "" && current.GetManifestPath() != result.GetManifest()
		if !hasTextStats && !hasJSONStats && !manifestChanged {
			return false
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
