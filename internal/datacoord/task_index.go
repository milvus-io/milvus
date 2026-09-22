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
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type indexBuildTask struct {
	*model.SegmentIndex

	// stateGuard makes the fields below readable by the scheduler without the
	// per-task key lock; see statsTask.stateGuard.
	stateGuard sync.RWMutex

	taskSlot int64

	times *taskcommon.Times

	meta                      *meta
	handler                   Handler
	chunkManager              storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager
}

var _ globalTask.Task = (*indexBuildTask)(nil)

var errVectorArrayFieldBinlogNotFound = errors.New("vector array field binlog not found")

func fieldSchemaForIndexBuild(schema *schemapb.CollectionSchema, field *schemapb.FieldSchema) *schemapb.FieldSchema {
	if field == nil || field.GetNullable() {
		return field
	}
	for _, structField := range schema.GetStructArrayFields() {
		if !structField.GetNullable() {
			continue
		}
		for _, subField := range structField.GetFields() {
			if subField.GetFieldID() == field.GetFieldID() {
				buildField := proto.Clone(field).(*schemapb.FieldSchema)
				buildField.Nullable = true
				return buildField
			}
		}
	}
	return field
}

func newIndexBuildTask(segIndex *model.SegmentIndex,
	taskSlot int64,
	meta *meta,
	handler Handler,
	chunkManager storage.ChunkManager,
	indexEngineVersionManager IndexEngineVersionManager,
) *indexBuildTask {
	return &indexBuildTask{
		SegmentIndex:              segIndex,
		taskSlot:                  taskSlot,
		times:                     taskcommon.NewTimes(),
		meta:                      meta,
		handler:                   handler,
		chunkManager:              chunkManager,
		indexEngineVersionManager: indexEngineVersionManager,
	}
}

func (it *indexBuildTask) GetTaskID() int64 {
	return it.BuildID
}

func (it *indexBuildTask) GetTaskSlot() int64 {
	return it.taskSlot
}

func (it *indexBuildTask) GetTaskState() taskcommon.State {
	it.stateGuard.RLock()
	defer it.stateGuard.RUnlock()
	return taskcommon.State(it.IndexState)
}

func (it *indexBuildTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	it.times.SetTaskTime(timeType, time)
}

func (it *indexBuildTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(it.times)
}

func (it *indexBuildTask) GetTaskType() taskcommon.Type {
	return taskcommon.Index
}

func (it *indexBuildTask) GetTaskVersion() int64 {
	// Index retries use fresh BuildIDs. IndexVersion remains a persisted object
	// path marker and is intentionally not exposed as an attempt counter.
	return 0
}

func (it *indexBuildTask) SetState(state indexpb.JobState, failReason string) {
	it.stateGuard.Lock()
	defer it.stateGuard.Unlock()
	it.IndexState = commonpb.IndexState(state)
	it.FailReason = failReason
}

func (it *indexBuildTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	if err := it.meta.indexMeta.UpdateIndexState(it.BuildID, commonpb.IndexState(state), failReason); err != nil {
		return err
	}
	it.SetState(state, failReason)
	return nil
}

func (it *indexBuildTask) assignTask(nodeID int64) error {
	if err := it.meta.indexMeta.AssignTask(it.BuildID, nodeID); err != nil {
		return err
	}
	it.stateGuard.Lock()
	it.NodeID = nodeID
	it.IndexVersion = 1
	it.IndexState = commonpb.IndexState_InProgress
	it.FailReason = ""
	it.stateGuard.Unlock()
	return nil
}

func (it *indexBuildTask) setJobInfo(result *workerpb.IndexTaskInfo) error {
	published, err := it.publishIndexToManifest(result)
	if err != nil {
		return err
	}
	if !published {
		if err := it.meta.indexMeta.FinishTask(result); err != nil {
			return err
		}
	}
	it.SetState(indexpb.JobState(result.GetState()), result.GetFailReason())
	return nil
}

// publishIndexToManifest records a completed StorageV3 index build in the
// segment manifest and in the index task metadata as one commit, and reports
// whether it took ownership of the result.
//
// The worker only uploads index files; every manifest revision for a segment
// is created here, serialized against the segment's other manifest writers by
// CommitSegmentManifest. That is what lets the entry be built from the
// segment's current revision instead of the possibly-stale revision the build
// was issued against.
func (it *indexBuildTask) publishIndexToManifest(result *workerpb.IndexTaskInfo) (bool, error) {
	ctx := it.meta.ctx
	// Manifest publication is opt-in and exclusive with the etcd record: off
	// (the default) is the pure legacy path and must produce no manifest index
	// entry at all, so the decline happens before any other inspection.
	if !writeSegmentIndexToManifest() {
		return false, nil
	}
	if result.GetState() != commonpb.IndexState_Finished {
		return false, nil
	}
	// A fake-finished build (a segment too small to train an index) uploads no
	// files and has no artifact to register.
	if len(result.GetIndexFileKeys()) == 0 {
		return false, nil
	}
	segment := it.meta.GetSegment(ctx, it.SegmentID)
	if segment == nil || segment.GetStorageVersion() != storage.StorageV3 || segment.GetManifestPath() == "" {
		return false, nil
	}
	// A segment dropped or compacted away while the build ran publishes no
	// further manifest revision. Record the result the legacy way and let the
	// task retire with the segment, instead of retrying a commit that a
	// manifest-less segment can never accept.
	if !isSegmentHealthy(segment) {
		return false, nil
	}
	segIdx, ok := it.meta.indexMeta.GetIndexJob(it.BuildID)
	if !ok || segIdx == nil {
		return false, nil
	}
	// The index definition can be dropped while the build runs: GC's
	// recycleUnusedIndexes removes it without waiting for in-flight builds,
	// and only this build's SegmentIndex record survives. The manifest entry
	// is named from that definition (GetIndexNameByID), so publishing now
	// would mint an entry with an empty IndexName that every fail-closed
	// reader rejects and GC can therefore never retire. Record the result the
	// legacy way instead; the record goes terminal and dies with the ordinary
	// dropped-index GC path.
	if !it.meta.indexMeta.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
		return false, nil
	}
	// Project the worker result now so an invalid one is rejected before any
	// manifest I/O. CommitSegmentManifest repeats this under indexMeta's
	// per-buildID lock and persists that authoritative copy.
	finished, _, err := it.meta.indexMeta.buildFinishedSegmentIndex(segIdx, result)
	if err != nil {
		return false, err
	}
	// The manifest entry names the indexed column; read the schema from its
	// authoritative owner rather than a local snapshot. Focused tests build
	// the task without a handler and only exercise the task-owned fields.
	var schema *schemapb.CollectionSchema
	if it.handler != nil {
		collectionInfo, err := it.handler.GetCollection(ctx, segIdx.CollectionID)
		if err != nil {
			return false, merr.Wrap(err, "get collection schema for index manifest publication")
		}
		if collectionInfo == nil || collectionInfo.Schema == nil {
			return false, merr.WrapErrCollectionNotFound(segIdx.CollectionID)
		}
		schema = collectionInfo.Schema
	}
	manifestIndex, err := buildManifestIndexInfo(it.meta, schema, segment, finished)
	if err != nil {
		return false, err
	}
	// Backstop: never commit an entry the fail-closed readers would refuse.
	// GC's retraction resolve and the startup reload both gate on
	// manifestIndexFilePathInfo, so an entry it rejects could never be retired
	// and, with the record manifest-resident, would abort every restart. Fail
	// this publish attempt - do NOT fall back to legacy - so the retry
	// surfaces the metadata bug instead of papering over it.
	if err := validateManifestIndexPublishable(it.SegmentID, manifestIndex); err != nil {
		return false, err
	}

	if err := it.meta.CommitSegmentManifest(ctx, SegmentManifestCommit{
		SegmentID:     it.SegmentID,
		StorageConfig: createStorageConfig(),
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{manifestIndex}},
		},
		CatalogMutation: SegmentCatalogMutation{
			SegmentIndexes: []SegmentIndexMutation{{
				Type:         SegmentIndexUpsert,
				BuildID:      it.BuildID,
				FinishedTask: result,
			}},
		},
	}); err != nil {
		// The segment can be retired between the health check above and the
		// commit. CommitSegmentManifest reports that as ErrSegmentNotFound, the
		// same benign-terminal contract the stats and L0 callers honor: record
		// the result the legacy way instead of re-polling a commit that a
		// retired segment can never accept.
		if errors.Is(err, merr.ErrSegmentNotFound) {
			mlog.Info(ctx, "segment retired during index manifest publication, recording result without it",
				mlog.Int64("buildID", it.BuildID), mlog.Int64("segmentID", it.SegmentID))
			return false, nil
		}
		return false, merr.Wrap(err, "publish index artifact through segment manifest")
	}
	return true, nil
}

func (it *indexBuildTask) retryTask(reason string) {
	if err := it.UpdateStateWithMeta(indexpb.JobState_JobStateRetry, reason); err != nil {
		// The scheduler must still release this attempt when the catalog write
		// failed. The index inspector re-reads the authoritative Init/InProgress
		// record on its own interval and retries from there.
		it.SetState(indexpb.JobState_JobStateRetry, reason)
	}
}

func (it *indexBuildTask) dropAndRetryTaskOnWorker(cluster session.Cluster, reason string) {
	// Drop is best effort. The retry receives a fresh BuildID, so even if the
	// old worker ignores cancellation its late result has no metadata to adopt.
	_ = it.tryDropTaskOnWorker(cluster)
	it.retryTask(reason)
}

func (it *indexBuildTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := context.TODO()
	log := mlog.With(mlog.Int64("taskID", it.BuildID), mlog.Int64("segmentID", it.SegmentID))

	// The scheduler holds the BuildID lock, but this wrapper may come from an
	// inspector snapshot taken before the previous wrapper finished and left.
	// Only an authoritative Unissued record can start a worker attempt.
	segIndex, exist := it.meta.indexMeta.GetIndexJob(it.BuildID)
	if !exist || segIndex == nil || segIndex.IsDeleted || segIndex.IndexState != commonpb.IndexState_Unissued {
		const reason = "index task is no longer dispatchable in meta"
		log.Info(ctx, reason)
		it.SetState(indexpb.JobState_JobStateNone, reason)
		return
	}

	// Check segment health and index existence
	segment := it.meta.GetSegment(ctx, segIndex.SegmentID)
	if reason, dropped := it.droppedTargetReason(segment, segIndex); dropped {
		log.Info(ctx, "task is no need to build index, marking it failed", mlog.String("reason", reason))
		it.abortForDroppedTarget(ctx, reason)
		return
	}
	if it.handler == nil {
		log.Warn(ctx, "collection handler is not configured; defer index build")
		return
	}
	collectionInfo, err := it.handler.GetCollection(ctx, segIndex.CollectionID)
	if err != nil || collectionInfo == nil || collectionInfo.Schema == nil {
		// Keep Init unchanged. The inspector will enqueue the authoritative task
		// again, rather than making index-threshold decisions without a schema.
		log.Warn(ctx, "cannot resolve collection schema; defer index build", mlog.Err(err))
		return
	}

	// Handle special cases for certain index types or small segments
	indexParams := it.meta.indexMeta.GetIndexParams(segIndex.CollectionID, segIndex.IndexID)
	indexType := GetIndexType(indexParams)
	effectiveRows := segIndex.NumRows
	estimatedVectorArrayVectors := int64(0)
	isVectorArrayIndex := false
	isEmbeddingListIndex := false
	skipVectorArrayThreshold := false
	if fieldID := it.meta.indexMeta.GetFieldIDByIndexID(segIndex.CollectionID, segIndex.IndexID); fieldID > 0 {
		for _, f := range typeutil.GetAllFieldSchemas(collectionInfo.Schema) {
			if f.FieldID == fieldID {
				if f.GetNullable() && typeutil.IsVectorType(f.GetDataType()) {
					// Derive valid rows from the persisted Statistics NullCounts
					// instead of iterating field binlogs.
					nullCount, ok := segment.EnsureStats().GetNullCounts()[fieldID]
					if !ok {
						// NullCounts carries an entry for every field present
						// in the segment's data (zero included). A missing key
						// means the field was added to the schema after this
						// segment was flushed: every row reads as null, so
						// there is nothing to index.
						log.Info(ctx, "field has no NullCounts entry, treating all rows as null",
							mlog.FieldFieldID(fieldID))
						nullCount = segIndex.NumRows
					}
					effectiveRows = segIndex.NumRows - nullCount
				}
				isVectorArrayIndex = typeutil.IsVectorArrayType(f.GetDataType())
				isEmbeddingListIndex = isVectorArrayIndex && isEmbeddingListMetric(indexParams)
				if isVectorArrayIndex {
					estimate, err := estimateVectorArrayElementCountForIndexBuild(segment.SegmentInfo, collectionInfo.Schema, f)
					if err != nil {
						failReason := "failed to estimate vector array element count, count is unknown: " + err.Error()
						log.Warn(ctx, "failed to estimate vector array element count",
							mlog.FieldFieldID(f.GetFieldID()),
							mlog.String("fieldName", f.GetName()),
							mlog.String("failReason", failReason),
							mlog.Err(err))
						if updateErr := it.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, failReason); updateErr != nil {
							log.Warn(ctx, "failed to update vector array index task state to Failed",
								mlog.String("failReason", failReason),
								mlog.Err(updateErr))
						}
						return
					}
					estimatedVectorArrayVectors = estimate.vectorCount
					if estimate.emptyOnStaleSchema {
						effectiveRows = 0
						log.Info(ctx, "vector array field binlog is absent on stale schema segment, treating as empty field",
							mlog.FieldFieldID(f.GetFieldID()),
							mlog.String("fieldName", f.GetName()),
							mlog.Int32("segmentSchemaVersion", segment.GetSchemaVersion()),
							mlog.Int32("collectionSchemaVersion", collectionInfo.Schema.GetVersion()))
					}
					if estimate.manifestBacked {
						// Recovered StorageV3 segment: the element count can't be derived
						// from the empty in-memory binlog arrays. Skip the element-count
						// threshold so DataCoord doesn't fake-finish/block a build that the
						// manifest-aware worker can complete.
						skipVectorArrayThreshold = true
						log.Info(ctx, "vector array element count unknown for manifest-backed segment, skipping element-count threshold",
							mlog.FieldFieldID(f.GetFieldID()),
							mlog.String("fieldName", f.GetName()),
							mlog.String("manifestPath", segment.GetManifestPath()))
					}
				}
				break
			}
		}
	}
	minRowsToBuildIndex := Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64()
	rowCountBelowThreshold := effectiveRows < minRowsToBuildIndex
	vectorArrayVectorCountBelowThreshold := false
	indexDataBelowThreshold := rowCountBelowThreshold
	if isVectorArrayIndex {
		// Element-level ArrayOfVector indexes are built from flattened inner vectors,
		// so row count alone should not block index building. MaxSim metrics build
		// EmbList indexes and additionally require enough logical rows.
		vectorArrayVectorCountBelowThreshold = !skipVectorArrayThreshold && estimatedVectorArrayVectors < minRowsToBuildIndex
		indexDataBelowThreshold = vectorArrayVectorCountBelowThreshold ||
			(isEmbeddingListIndex && rowCountBelowThreshold)
	}
	if isNoTrainIndex(indexType) || indexDataBelowThreshold {
		log.Info(ctx, "segment does not need index really, marking as finished",
			mlog.Int64("numRows", segIndex.NumRows),
			mlog.Int64("effectiveRows", effectiveRows),
			mlog.Int64("estimatedVectorArrayVectors", estimatedVectorArrayVectors),
			mlog.Int64("minRowsToBuildIndex", minRowsToBuildIndex),
			mlog.String("indexType", indexType),
			mlog.Bool("vectorArrayIndex", isVectorArrayIndex),
			mlog.Bool("embeddingListIndex", isEmbeddingListIndex),
			mlog.Bool("rowCountBelowThreshold", rowCountBelowThreshold),
			mlog.Bool("vectorArrayVectorCountBelowThreshold", vectorArrayVectorCountBelowThreshold),
			mlog.Bool("indexDataBelowThreshold", indexDataBelowThreshold),
		)
		now := time.Now()
		it.SetTaskTime(taskcommon.TimeStart, now)
		it.SetTaskTime(taskcommon.TimeEnd, now)
		it.UpdateStateWithMeta(indexpb.JobState_JobStateFinished, "fake finished index success")
		return
	}

	// Create job request
	req, err := it.prepareJobRequest(ctx, segment, segIndex, collectionInfo, indexParams, indexType)
	if err != nil {
		log.Warn(ctx, "failed to prepare job request", mlog.Err(err))
		return
	}

	// Persist the assignment before the at-least-once Create boundary. An error
	// response is ambiguous, so fail-stop and let restart recover the
	// authoritative Init or InProgress record before any Create is retried.
	if err := it.assignTask(nodeID); err != nil {
		if it.meta.ctx == nil || it.meta.ctx.Err() == nil {
			mlog.Fatal(ctx, "failed to persist index task assignment; terminating process",
				mlog.FieldBuildID(it.BuildID), mlog.FieldNodeID(nodeID), mlog.Err(err))
		}
		log.Warn(ctx, "failed to persist index task assignment", mlog.Err(err))
		return
	}

	// Send request to worker
	if err = cluster.CreateIndex(nodeID, req); err != nil {
		log.Warn(ctx, "failed to send job to worker", mlog.Err(err))
		it.dropAndRetryTaskOnWorker(cluster, err.Error())
		return
	}

	log.Info(ctx, "index task assigned successfully")
}

func isEmbeddingListMetric(indexParams []*commonpb.KeyValuePair) bool {
	metricType, err := getIndexParam(indexParams, common.MetricTypeKey)
	if err != nil {
		return false
	}
	switch strings.ToUpper(metricType) {
	case metric.MaxSim,
		metric.MaxSimCosine,
		metric.MaxSimL2,
		metric.MaxSimIP,
		metric.MaxSimHamming,
		metric.MaxSimJaccard:
		return true
	default:
		return false
	}
}

type vectorArrayElementCountEstimate struct {
	vectorCount        int64
	emptyOnStaleSchema bool
	// manifestBacked is set for a recovered StorageV3 segment whose in-memory
	// binlog arrays are empty: the element count can't be derived here, but the
	// manifest-aware worker build can, so the pre-check must not fail or
	// fake-finish — it lets the build proceed.
	manifestBacked bool
}

func estimateVectorArrayElementCountForIndexBuild(segment *datapb.SegmentInfo, schema *schemapb.CollectionSchema, field *schemapb.FieldSchema) (vectorArrayElementCountEstimate, error) {
	count, err := estimateVectorArrayElementCount(segment, field)
	if err == nil {
		return vectorArrayElementCountEstimate{vectorCount: count}, nil
	}
	if isMissingVectorArrayFieldOnStaleSchema(err, segment, schema, field) {
		// A nullable field added after this segment was written has no binlog in the
		// stale segment. For index build purposes it contributes zero vectors and
		// should be fake-finished by the threshold check below.
		return vectorArrayElementCountEstimate{emptyOnStaleSchema: true}, nil
	}
	if errors.Is(err, errVectorArrayFieldBinlogNotFound) && segment.GetManifestPath() != "" {
		// A recovered StorageV3 segment reloads with empty in-memory binlog arrays
		// (per-field KVs are not persisted), so the element count is unknowable
		// here — but the manifest is authoritative and the worker build reads it.
		// Don't fail the pre-check; let the manifest-aware build proceed.
		return vectorArrayElementCountEstimate{manifestBacked: true}, nil
	}
	return vectorArrayElementCountEstimate{}, err
}

func estimateVectorArrayElementCount(segment *datapb.SegmentInfo, field *schemapb.FieldSchema) (int64, error) {
	if segment == nil {
		return 0, merr.WrapErrServiceInternalMsg("segment info is nil")
	}
	if field == nil {
		return 0, merr.WrapErrServiceInternalMsg("field schema is nil")
	}
	elementSize, err := vectorArrayElementSize(field)
	if err != nil {
		return 0, err
	}

	var totalPayloadBytes int64
	var seenFieldLog bool
	for _, fieldBinlog := range segment.GetBinlogs() {
		if !fieldBinlogContainsField(fieldBinlog, field.GetFieldID()) {
			continue
		}

		seenFieldLog = true
		for _, binlog := range fieldBinlog.GetBinlogs() {
			payloadBytes := binlog.GetMemorySize()
			if payloadBytes <= 0 {
				continue
			}

			// VectorArrayFieldData stores each logical row with a 4-byte inner-vector byte length.
			payloadBytes -= binlog.GetEntriesNum() * 4
			if field.GetNullable() {
				payloadBytes -= binlog.GetEntriesNum()
			}
			if payloadBytes > 0 {
				totalPayloadBytes += payloadBytes
			}
		}
	}

	if !seenFieldLog {
		return 0, merr.WrapErrServiceInternalErr(errVectorArrayFieldBinlogNotFound, "fieldID=%d", field.GetFieldID())
	}
	return totalPayloadBytes / elementSize, nil
}

func isMissingVectorArrayFieldOnStaleSchema(err error, segment *datapb.SegmentInfo, schema *schemapb.CollectionSchema, field *schemapb.FieldSchema) bool {
	return errors.Is(err, errVectorArrayFieldBinlogNotFound) &&
		segment != nil &&
		schema != nil &&
		field != nil &&
		field.GetNullable() &&
		segment.GetSchemaVersion() < schema.GetVersion()
}

func fieldBinlogContainsField(fieldBinlog *datapb.FieldBinlog, fieldID int64) bool {
	if fieldBinlog.GetFieldID() == fieldID {
		return true
	}
	for _, childFieldID := range fieldBinlog.GetChildFields() {
		if childFieldID == fieldID {
			return true
		}
	}
	return false
}

func vectorArrayElementSize(field *schemapb.FieldSchema) (int64, error) {
	if field == nil {
		return 0, merr.WrapErrServiceInternalMsg("field schema is nil")
	}
	dim, err := storage.GetDimFromParams(field.GetTypeParams())
	if err != nil {
		return 0, merr.WrapErrServiceInternalErr(err, "invalid vector array dim, fieldID=%d", field.GetFieldID())
	}
	if dim <= 0 {
		return 0, merr.WrapErrParameterInvalidMsg("invalid vector array dim %d, fieldID=%d", dim, field.GetFieldID())
	}

	switch field.GetElementType() {
	case schemapb.DataType_FloatVector:
		return int64(dim) * 4, nil
	case schemapb.DataType_BinaryVector:
		return int64(dim+7) / 8, nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return int64(dim) * 2, nil
	case schemapb.DataType_Int8Vector:
		return int64(dim), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unsupported vector array element type %s, fieldID=%d", field.GetElementType().String(), field.GetFieldID())
	}
}

// Helper method to prepare job request
func (it *indexBuildTask) prepareJobRequest(ctx context.Context, segment *SegmentInfo, segIndex *model.SegmentIndex,
	collectionInfo *collectionInfo, indexParams []*commonpb.KeyValuePair, indexType string,
) (*workerpb.CreateJobRequest, error) {
	log := mlog.With(mlog.Int64("taskID", it.BuildID), mlog.Int64("segmentID", segment.GetID()))

	typeParams := it.meta.indexMeta.GetTypeParams(segIndex.CollectionID, segIndex.IndexID)
	fieldID := it.meta.indexMeta.GetFieldIDByIndexID(segIndex.CollectionID, segIndex.IndexID)

	binlogIDs := getBinLogIDs(segment, fieldID)
	totalRows := getTotalBinlogRows(segment, fieldID)

	// Update index parameters as needed
	params := indexParams
	if vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType) && Params.KnowhereConfig.Enable.GetAsBool() {
		var err error
		params, err = Params.KnowhereConfig.UpdateIndexParams(GetIndexType(params), paramtable.BuildStage, params)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "failed to update index build params")
		}
	}

	if isDiskANNIndex(GetIndexType(params)) {
		var err error
		params, err = indexparams.UpdateDiskIndexBuildParams(Params, params)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "failed to append index build params")
		}
	}

	if collectionInfo == nil || collectionInfo.Schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("collection schema is unavailable")
	}

	// Get field from the operation-local collection snapshot.
	schema := collectionInfo.Schema
	var field *schemapb.FieldSchema

	allFields := typeutil.GetAllFieldSchemas(schema)
	for _, f := range allFields {
		if f.FieldID == fieldID {
			field = f
			break
		}
	}

	if field == nil {
		return nil, merr.WrapErrFieldNotFound(fieldID)
	}
	buildField := fieldSchemaForIndexBuild(schema, field)

	// Extract dim only for vector types to avoid unnecessary warnings
	dim := -1
	dataType := field.GetDataType()
	if typeutil.IsVectorArrayType(dataType) {
		dataType = field.GetElementType()
	}
	if typeutil.IsFixDimVectorType(dataType) {
		if dimVal, err := storage.GetDimFromParams(field.GetTypeParams()); err != nil {
			log.Warn(ctx, "failed to get dim from field type params",
				mlog.String("field type", field.GetDataType().String()), mlog.Err(err))
		} else {
			dim = dimVal
		}
	}

	// Prepare optional fields for vector index
	optionalFields, partitionKeyIsolation := it.prepareOptionalFields(ctx, collectionInfo, segment, schema, indexType, field)
	indexNonEncoding := "false"
	if it.indexEngineVersionManager.GetIndexNonEncoding() {
		indexNonEncoding = "true"
	}
	params = append(params, &commonpb.KeyValuePair{
		Key:   common.IndexNonEncoding,
		Value: indexNonEncoding,
	})

	currentVecIndexVersion := it.indexEngineVersionManager.ResolveVecIndexVersion()
	currentScalarIndexVersion := it.indexEngineVersionManager.ResolveScalarIndexVersion()

	// Create the job request. The path layout (v0/v1) is propagated via
	// IndexStorePathVersion; C++ indexbuilder assembles the remote prefix locally.
	// external_source is passed raw (AWS-form or Milvus-form). C++ indexbuilder
	// InjectExternalSpecProperties handles Tier-1/2 endpoint derivation + AWS-form swap.
	req := &workerpb.CreateJobRequest{
		ClusterID:             Params.CommonCfg.ClusterPrefix.GetValue(),
		IndexFilePrefix:       path.Join(it.chunkManager.RootPath(), common.SegmentIndexV0Path),
		BuildID:               it.BuildID,
		IndexStorePathVersion: segIndex.IndexStorePathVersion,
		// IndexVersion remains the legacy object-path/API marker. Fresh BuildID is
		// the attempt identity, so new builds keep the historical first value and
		// never increment or compare it for ownership.
		IndexVersion:              1,
		StorageConfig:             createStorageConfig(),
		IndexParams:               params,
		TypeParams:                typeParams,
		NumRows:                   segIndex.NumRows,
		CurrentIndexVersion:       currentVecIndexVersion,
		CurrentScalarIndexVersion: currentScalarIndexVersion,
		CollectionID:              segment.GetCollectionID(),
		PartitionID:               segment.GetPartitionID(),
		SegmentID:                 segment.GetID(),
		FieldID:                   fieldID,
		FieldName:                 field.GetName(),
		FieldType:                 field.GetDataType(),
		Dim:                       int64(dim),
		DataIds:                   binlogIDs,
		OptionalScalarFields:      optionalFields,
		Field:                     buildField,
		PartitionKeyIsolation:     partitionKeyIsolation,
		StorageVersion:            segment.GetStorageVersion(),
		TaskSlot:                  it.taskSlot,
		LackBinlogRows:            segIndex.NumRows - totalRows,
		InsertLogs:                segment.GetBinlogs(),
		Manifest:                  segment.GetManifestPath(),
		ExternalSource:            schema.GetExternalSource(),
		ExternalSpec:              schema.GetExternalSpec(),
	}

	WrapPluginContext(segment.GetCollectionID(), schema.GetProperties(), req)

	return req, nil
}

// Helper method to prepare optional fields
func (it *indexBuildTask) prepareOptionalFields(ctx context.Context, collectionInfo *collectionInfo,
	segment *SegmentInfo, schema *schemapb.CollectionSchema, indexType string, field *schemapb.FieldSchema,
) ([]*indexpb.OptionalFieldInfo, bool) {
	optionalFields := make([]*indexpb.OptionalFieldInfo, 0)
	partitionKeyIsolation := false

	isVectorTypeSupported := typeutil.IsDenseFloatVectorType(field.DataType) || typeutil.IsBinaryVectorType(field.DataType)
	if Params.CommonCfg.EnableMaterializedView.GetAsBool() && isVectorTypeSupported && isMvSupported(indexType) {
		partitionKeyField, _ := typeutil.GetPartitionKeyFieldSchema(schema)
		if partitionKeyField != nil && typeutil.IsFieldDataTypeSupportMaterializedView(partitionKeyField) {
			optionalFields = append(optionalFields, &indexpb.OptionalFieldInfo{
				FieldID:     partitionKeyField.FieldID,
				FieldName:   partitionKeyField.Name,
				FieldType:   int32(partitionKeyField.DataType),
				ElementType: int32(partitionKeyField.GetElementType()),
				DataIds:     getBinLogIDs(segment, partitionKeyField.FieldID),
			})

			iso, isoErr := common.IsPartitionKeyIsolationPropEnabled(collectionInfo.Properties)
			if isoErr != nil {
				mlog.Warn(ctx, "failed to parse partition key isolation", mlog.Err(isoErr))
			}
			if iso {
				partitionKeyIsolation = true
			}
		}
	}

	return optionalFields, partitionKeyIsolation
}

func (it *indexBuildTask) QueryTaskOnWorker(cluster session.Cluster) {
	ctx := context.TODO()
	log := mlog.With(mlog.Int64("taskID", it.BuildID), mlog.Int64("segmentID", it.SegmentID), mlog.Int64("nodeID", it.NodeID))

	// Check if task exists in meta
	segIndex, exist := it.meta.indexMeta.GetIndexJob(it.BuildID)
	if !exist || segIndex == nil {
		log.Info(ctx, "index task has not exist in meta table, removing task")
		if it.tryDropTaskOnWorker(cluster) != nil {
			return
		}
		it.SetState(indexpb.JobState_JobStateNone, "index task has not exist in meta table")
		return
	}
	// Do not turn a completed attempt back into Retry when its worker result
	// has already been cleaned up. Retire only this stale scheduler wrapper.
	if segIndex.IsDeleted || segIndex.IndexState != commonpb.IndexState_InProgress {
		it.SetState(indexpb.JobState_JobStateNone, "index task is no longer running in meta")
		return
	}

	// The index or the segment may have been dropped while the task is in
	// flight. Abort it now instead of letting the worker finish useless work.
	if reason, dropped := it.droppedTargetReason(it.meta.GetSegment(ctx, segIndex.SegmentID), segIndex); dropped {
		log.Info(ctx, "index task target dropped while in progress, aborting", mlog.String("reason", reason))
		it.abortInflightTask(ctx, cluster, reason)
		return
	}

	results, err := cluster.QueryIndex(it.NodeID, &workerpb.QueryJobsRequest{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []UniqueID{it.BuildID},
	})
	if err != nil {
		log.Warn(ctx, "query index task result from worker failed", mlog.Err(err))
		it.dropAndRetryTaskOnWorker(cluster, err.Error())
		return
	}

	// indexInfos length is always one.
	for _, info := range results.GetResults() {
		if info.GetBuildID() == it.BuildID {
			switch info.GetState() {
			case commonpb.IndexState_Finished, commonpb.IndexState_Failed:
				log.Info(ctx, "query task index info successfully",
					mlog.Int64("taskID", it.BuildID), mlog.String("result state", info.GetState().String()),
					mlog.String("failReason", info.GetFailReason()))
				if err := it.setJobInfo(info); err != nil {
					// Leave the task InProgress: the worker keeps the result
					// until it is dropped, so the next query retries publication.
					log.Warn(ctx, "failed to record index task result", mlog.Err(err))
					return
				}
			case commonpb.IndexState_Retry, commonpb.IndexState_IndexStateNone:
				log.Info(ctx, "query task index info successfully",
					mlog.Int64("taskID", it.BuildID), mlog.String("result state", info.GetState().String()),
					mlog.String("failReason", info.GetFailReason()))
				it.dropAndRetryTaskOnWorker(cluster, info.GetFailReason())
			}
			// inProgress or unissued, keep InProgress state
			return
		}
	}
	it.dropAndRetryTaskOnWorker(cluster, "index is not in info response")
	// Task not found in results will be return error
}

const (
	indexTaskAbortReasonSegmentDropped = "segment is dropped, index task aborted"
	indexTaskAbortReasonIndexDropped   = "index is dropped, index task aborted"
)

// droppedTargetReason reports whether the task's build target is gone: the
// segment is no longer healthy or the field index has been dropped. Both are
// irreversible, so the task can never be needed again.
func (it *indexBuildTask) droppedTargetReason(segment *SegmentInfo, segIndex *model.SegmentIndex) (string, bool) {
	if !isSegmentHealthy(segment) {
		return indexTaskAbortReasonSegmentDropped, true
	}
	if !it.meta.indexMeta.IsIndexExist(segIndex.CollectionID, segIndex.IndexID) {
		return indexTaskAbortReasonIndexDropped, true
	}
	return "", false
}

// abortInflightTask cancels a dispatched task whose target is gone. The job is
// dropped on the worker before the terminal state is persisted: a failed drop
// RPC returns without touching meta, so the task stays InProgress and the next
// check round retries, and a DataCoord restart in between reloads it as
// InProgress and repeats the same idempotent steps. A missing node counts as
// dropped. Files the worker already uploaded, or uploads still in flight that
// the cgo code cannot interrupt, are reclaimed by the orphan index file scans
// once GC removes the record.
func (it *indexBuildTask) abortInflightTask(ctx context.Context, cluster session.Cluster, reason string) {
	if err := it.tryDropTaskOnWorker(cluster); err != nil {
		mlog.Warn(ctx, "failed to cancel index task on worker, keeping it for the next round",
			mlog.Int64("taskID", it.BuildID), mlog.Int64("nodeID", it.NodeID), mlog.Err(err))
		return
	}
	it.abortForDroppedTarget(ctx, reason)
}

// abortForDroppedTarget persists a terminal state for a task whose target is
// gone. Writing only the in-memory state would leave the SegmentIndex
// non-terminal in meta: GC skips non-terminal tasks, and every restart would
// re-enqueue it. On a meta write failure the state is left untouched so the
// next round retries.
func (it *indexBuildTask) abortForDroppedTarget(ctx context.Context, reason string) {
	if err := it.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, reason); err != nil {
		mlog.Warn(ctx, "failed to persist aborted index task state, will retry",
			mlog.Int64("taskID", it.BuildID), mlog.String("reason", reason), mlog.Err(err))
	}
}

func (it *indexBuildTask) tryDropTaskOnWorker(cluster session.Cluster) error {
	ctx := context.TODO()
	log := mlog.With(mlog.Int64("taskID", it.BuildID), mlog.Int64("segmentID", it.SegmentID), mlog.Int64("nodeID", it.NodeID))
	if it.NodeID <= 0 {
		return nil
	}

	if err := cluster.DropIndex(it.NodeID, it.BuildID); err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		log.Warn(ctx, "notify worker drop the index task failed", mlog.Err(err))
		return err
	}

	log.Info(ctx, "index task dropped successfully")
	return nil
}

func (it *indexBuildTask) DropTaskOnWorker(cluster session.Cluster) {
	it.tryDropTaskOnWorker(cluster)
}
