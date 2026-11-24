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

package index

import (
	"context"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
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

	taskSlot int64

	times *taskcommon.Times

	meta                      *meta.Meta
	indexMeta                 *IndexMeta
	handler                   meta.Handler
	chunkManager              storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager

	resource taskresource.Cache
	// Each build targets one immutable field. The manifest path includes its revision.
	lobManifest string
	lobResource taskcommon.Resource
}

var (
	_ globalTask.Task            = (*indexBuildTask)(nil)
	_ globalTask.CoordinatorTask = (*indexBuildTask)(nil)
)

var errVectorArrayFieldBinlogNotFound = errors.New("vector array field binlog not found")

func fieldSchemaForIndexBuild(schema *schemapb.CollectionSchema, field *schemapb.FieldSchema) *schemapb.FieldSchema {
	if field == nil {
		return field
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, subField := range structField.GetFields() {
			if subField.GetFieldID() == field.GetFieldID() {
				if field.GetNullable() == structField.GetNullable() {
					return field
				}
				// A struct sub-field shares its parent's row validity, including
				// when the parent is non-nullable.
				buildField := proto.Clone(field).(*schemapb.FieldSchema)
				buildField.Nullable = structField.GetNullable()
				return buildField
			}
		}
	}
	return field
}

func newIndexBuildTask(segIndex *model.SegmentIndex,
	taskSlot int64,
	mt *meta.Meta,
	indexMeta *IndexMeta,
	handler meta.Handler,
	chunkManager storage.ChunkManager,
	indexEngineVersionManager IndexEngineVersionManager,
) *indexBuildTask {
	return &indexBuildTask{
		SegmentIndex:              segIndex,
		taskSlot:                  taskSlot,
		times:                     taskcommon.NewTimes(),
		meta:                      mt,
		indexMeta:                 indexMeta,
		handler:                   handler,
		chunkManager:              chunkManager,
		indexEngineVersionManager: indexEngineVersionManager,
	}
}

func (it *indexBuildTask) GetTaskID() int64 {
	return it.BuildID
}

// GetTaskResource returns the price used for placement and dispatch. NGRAM on
// LOB-backed TEXT is priced during coordinator preparation for a specific
// manifest revision. Other builds cache a field estimate once their metadata
// resolves. Until then they use the segment estimate and retry the lookup.
func (it *indexBuildTask) GetTaskResource() taskcommon.Resource {
	// Target liveness is not immutable: check before consulting the cached price.
	segment := it.meta.GetHealthySegment(context.TODO(), it.SegmentID)
	if segment == nil ||
		!it.indexMeta.IsIndexExist(it.CollectionID, it.IndexID) {
		return taskresource.Default()
	}
	if it.lobManifest != "" && it.lobManifest == segment.GetManifestPath() {
		return it.lobResource
	}
	return it.resource.Get(func() (taskcommon.Resource, bool) {
		// GetIndexType answers the invalidIndex sentinel, not "", when the params
		// carry no index_type -- which is exactly what an index whose params have
		// not been read back yet looks like.
		indexParams := it.indexMeta.GetIndexParams(it.CollectionID, it.IndexID)
		indexType := meta.GetIndexType(indexParams)
		coll := it.meta.GetCollection(it.CollectionID)
		if coll == nil || coll.Schema == nil {
			if indexType == indexparamcheck.IndexFMINDEX {
				// The params already identify the peak model. With no cached
				// schema, use the indexed field's recorded size or the whole
				// segment as its upper bound, and recompute once schema arrives.
				fieldID := it.indexMeta.GetFieldIDByIndexID(it.CollectionID, it.IndexID)
				bound := taskresource.EstimateFieldSize(segment, nil, fieldID)
				return taskresource.FMIndex(estimateFMIndexBuildPeakBytes(bound, segment.GetNumOfRows(), indexParams)), false
			}
			return taskresource.Index(taskresource.EstimateSegmentSize(segment, nil), false), false
		}
		if indexType == "" || indexType == meta.InvalidIndexType {
			return taskresource.Index(taskresource.EstimateSegmentSize(segment, coll.Schema), false), false
		}
		isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)
		fieldID := it.indexMeta.GetFieldIDByIndexID(it.CollectionID, it.IndexID)
		fieldSize := taskresource.EstimateFieldSize(segment, coll.Schema, fieldID)
		if fieldSize <= 0 {
			return taskresource.Default(), false
		}
		if indexType == indexparamcheck.IndexFMINDEX {
			return taskresource.FMIndex(estimateFMIndexBuildPeakBytes(fieldSize, segment.GetNumOfRows(), indexParams)), true
		}
		return taskresource.Index(fieldSize, isVectorIndex), true
	})
}

func (it *indexBuildTask) GetTaskSlot() int64 {
	return it.taskSlot
}

func (it *indexBuildTask) GetTaskState() taskcommon.State {
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
	return it.IndexVersion
}

func (it *indexBuildTask) SetState(state indexpb.JobState, failReason string) {
	it.IndexState = commonpb.IndexState(state)
	it.FailReason = failReason
}

func (it *indexBuildTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	return it.updateStateWithMeta(it.indexMeta.ctx, state, failReason)
}

func (it *indexBuildTask) updateStateWithMeta(ctx context.Context, state indexpb.JobState, failReason string) error {
	if err := it.indexMeta.UpdateIndexStateWithContext(ctx, it.BuildID, commonpb.IndexState(state), failReason); err != nil {
		return err
	}
	it.SetState(state, failReason)
	return nil
}

func (it *indexBuildTask) UpdateTaskVersion(nodeID int64) error {
	if err := it.indexMeta.UpdateVersion(it.BuildID, nodeID); err != nil {
		return err
	}
	it.IndexVersion++
	it.NodeID = nodeID
	return nil
}

func (it *indexBuildTask) setJobInfo(result *workerpb.IndexTaskInfo) error {
	if err := it.indexMeta.FinishTask(result); err != nil {
		return err
	}
	it.SetState(indexpb.JobState(result.GetState()), result.GetFailReason())
	return nil
}

func (it *indexBuildTask) resetTask(reason string) {
	it.UpdateStateWithMeta(indexpb.JobState_JobStateInit, reason)
}

func (it *indexBuildTask) dropAndResetTaskOnWorker(cluster session.Cluster, reason string) {
	if err := it.tryDropTaskOnWorker(cluster); err != nil {
		return
	}
	it.resetTask(reason)
}

// indexBuildPreparation contains only live inputs for a real worker build.
type indexBuildPreparation struct {
	segment     *meta.SegmentInfo
	segIndex    *model.SegmentIndex
	indexParams []*commonpb.KeyValuePair
	indexType   string
	field       *schemapb.FieldSchema
	schema      *schemapb.CollectionSchema
}

func isManifestNgram(segment *meta.SegmentInfo, indexType string) bool {
	return indexType == indexparamcheck.IndexNGRAM && segment.GetStorageVersion() == storage.StorageV3 && segment.GetManifestPath() != ""
}

// The native index reader resolves LOB references only for Milvus-written TEXT.
func isLobText(schema *schemapb.CollectionSchema, field *schemapb.FieldSchema) bool {
	return field.GetDataType() == schemapb.DataType_Text && !typeutil.NewStorageColumnResolver(schema).IsSourceDataField(field)
}

// CanCompleteOnCoordinator uses cached metadata only. Schema lookup, request
// preparation and persistence stay off the scheduling goroutine.
func (it *indexBuildTask) CanCompleteOnCoordinator() bool {
	segIndex, exists := it.indexMeta.GetIndexJob(it.BuildID)
	if !exists {
		return true
	}
	segment := it.meta.GetSegment(context.TODO(), segIndex.SegmentID)
	if _, dropped := it.droppedTargetReason(segment, segIndex); dropped {
		return true
	}
	indexType := meta.GetIndexType(it.indexMeta.GetIndexParams(segIndex.CollectionID, segIndex.IndexID))
	if isNoTrainIndex(indexType) {
		return true
	}
	needsManifest := isManifestNgram(segment, indexType) && it.lobManifest != segment.GetManifestPath()
	collection := it.meta.GetCollection(segIndex.CollectionID)
	if collection == nil {
		return needsManifest
	}
	fieldID := it.indexMeta.GetFieldIDByIndexID(segIndex.CollectionID, segIndex.IndexID)
	field := typeutil.GetFieldByID(collection.Schema, fieldID)
	if field == nil {
		// Preparation may already see a pending schema change.
		return needsManifest
	}
	if needsManifest && isLobText(collection.Schema, field) {
		return true
	}
	// Array indexes use element counts; leave their preparation to dispatch.
	if typeutil.IsVectorArrayType(field.GetDataType()) {
		return false
	}
	return effectiveIndexRows(segment, segIndex.NumRows, field) < Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64()
}

func (it *indexBuildTask) CompleteOnCoordinator(ctx context.Context) globalTask.CoordinatorResult {
	if prepared := it.prepareForWorker(ctx); prepared != nil {
		if isManifestNgram(prepared.segment, prepared.indexType) {
			if prepared.field == nil {
				return globalTask.LocalRetry
			}
			resource, err := prepared.ngramResource(ctx)
			if err != nil {
				mlog.Warn(ctx, "failed to price NGRAM input from manifest", mlog.FieldTaskID(it.BuildID),
					mlog.String("manifest", prepared.segment.GetManifestPath()), mlog.Err(err))
				return globalTask.LocalRetry
			}
			it.lobResource = resource
			it.lobManifest = prepared.segment.GetManifestPath()
		}
		return globalTask.LocalNeedsWorker
	}
	switch it.GetTaskState() {
	case taskcommon.None, taskcommon.Failed, taskcommon.Finished:
		return globalTask.LocalCompleted
	default:
		return globalTask.LocalRetry
	}
}

func (p *indexBuildPreparation) ngramResource(ctx context.Context) (taskcommon.Resource, error) {
	fieldID := p.field.GetFieldID()
	size := taskresource.EstimateFieldSize(p.segment, p.schema, fieldID)
	if isLobText(p.schema, p.field) {
		io := packed.NewManifestIOContext(1)
		defer io.Close()
		files, err := packed.GetManifestLobFilesAsync(ctx, io, p.segment.GetManifestPath(), meta.CreateStorageConfig())
		if err != nil {
			return taskcommon.Resource{}, err
		}
		// Column groups hold inline values and references. The worker also
		// materializes the referenced text during input preparation.
		for _, file := range files {
			if file.FieldID == fieldID {
				size += file.FileSizeBytes
			}
		}
	}
	return taskresource.Index(size, false), nil
}

// effectiveIndexRows applies the same nullable-vector rule during cached
// classification and worker preparation. A missing NullCounts entry means the
// field was added after this segment was flushed, so every row reads as null.
func effectiveIndexRows(segment *meta.SegmentInfo, rows int64, field *schemapb.FieldSchema) int64 {
	if !field.GetNullable() || !typeutil.IsVectorType(field.GetDataType()) {
		return rows
	}
	nullCount, exists := segment.EnsureStats().GetNullCounts()[field.GetFieldID()]
	if !exists {
		return 0
	}
	return rows - nullCount
}

// prepareForWorker is shared with dispatch so target liveness and nullable /
// vector-array thresholds are rechecked after placement, without two rules.
func (it *indexBuildTask) prepareForWorker(ctx context.Context) *indexBuildPreparation {
	log := mlog.With(mlog.FieldTaskID(it.BuildID), mlog.FieldSegmentID(it.SegmentID))

	// Check if task exists in meta
	segIndex, exist := it.indexMeta.GetIndexJob(it.BuildID)
	if !exist || segIndex == nil {
		log.Info(ctx, "index task has not exist in meta table, removing task")
		it.SetState(indexpb.JobState_JobStateNone, "index task has not exist in meta table")
		return nil
	}

	// Check segment health and index existence
	segment := it.meta.GetSegment(ctx, segIndex.SegmentID)
	if reason, dropped := it.droppedTargetReason(segment, segIndex); dropped {
		log.Info(ctx, "task is no need to build index, marking it failed", mlog.String("reason", reason))
		it.abortForDroppedTarget(ctx, reason)
		return nil
	}

	// Handle special cases for certain index types or small segments
	indexParams := it.indexMeta.GetIndexParams(segIndex.CollectionID, segIndex.IndexID)
	indexType := meta.GetIndexType(indexParams)
	effectiveRows := segIndex.NumRows
	estimatedVectorArrayVectors := int64(0)
	isVectorArrayIndex := false
	isEmbeddingListIndex := false
	prepared := &indexBuildPreparation{segment: segment, segIndex: segIndex, indexParams: indexParams, indexType: indexType}
	skipVectorArrayThreshold := false
	if fieldID := it.indexMeta.GetFieldIDByIndexID(segIndex.CollectionID, segIndex.IndexID); fieldID > 0 && !isNoTrainIndex(indexType) {
		collectionInfo, err := it.handler.GetCollection(ctx, segIndex.CollectionID)
		if err != nil {
			log.Warn(ctx, "failed to get collection for index preparation, will retry", mlog.Err(err))
			return nil
		}
		for _, f := range typeutil.GetAllFieldSchemas(collectionInfo.Schema) {
			if f.FieldID == fieldID {
				prepared.field = f
				prepared.schema = collectionInfo.Schema
				effectiveRows = effectiveIndexRows(segment, segIndex.NumRows, f)
				isVectorArrayIndex = typeutil.IsVectorArrayType(f.GetDataType())
				isEmbeddingListIndex = isVectorArrayIndex && isEmbeddingListMetric(indexParams)
				validElements, exact := exactVectorArrayValidElementCount(segment, f)
				if exact {
					estimatedVectorArrayVectors = validElements
					log.Info(ctx, "vector array valid element count from segment statistics",
						mlog.FieldFieldID(f.GetFieldID()),
						mlog.Int64("validElements", validElements))
				}
				if isVectorArrayIndex && !exact {
					estimate, err := estimateVectorArrayElementCountForIndexBuild(segment.SegmentInfo, collectionInfo.Schema, f)
					if err != nil {
						failReason := "failed to estimate vector array element count, count is unknown: " + err.Error()
						log.Warn(ctx, "failed to estimate vector array element count",
							mlog.FieldFieldID(f.GetFieldID()),
							mlog.String("fieldName", f.GetName()),
							mlog.String("failReason", failReason),
							mlog.Err(err))
						if updateErr := it.updateStateWithMeta(ctx, indexpb.JobState_JobStateFailed, failReason); updateErr != nil {
							log.Warn(ctx, "failed to update vector array index task state to Failed",
								mlog.String("failReason", failReason),
								mlog.Err(updateErr))
						}
						return nil
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
		it.updateStateWithMeta(ctx, indexpb.JobState_JobStateFinished, "fake finished index success")
		return nil
	}

	return prepared
}

func (it *indexBuildTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster, resource taskcommon.Resource) {
	ctx := context.TODO()
	prepared := it.prepareForWorker(ctx)
	if prepared == nil {
		return
	}
	segment, segIndex := prepared.segment, prepared.segIndex
	indexParams, indexType := prepared.indexParams, prepared.indexType
	log := mlog.With(mlog.FieldTaskID(it.BuildID), mlog.FieldSegmentID(it.SegmentID))

	// The segment can acquire a new revision between placement and dispatch.
	if isManifestNgram(segment, indexType) && segment.GetManifestPath() != it.lobManifest && isLobText(prepared.schema, prepared.field) {
		return
	}

	// Create job request
	req, err := it.prepareJobRequest(ctx, segment, segIndex, indexParams, indexType)
	if err != nil {
		log.Warn(ctx, "failed to prepare job request", mlog.Err(err))
		return
	}

	// Update task version
	if err := it.UpdateTaskVersion(nodeID); err != nil {
		log.Warn(ctx, "failed to update task version", mlog.Err(err))
		return
	}

	defer func() {
		if err != nil {
			it.tryDropTaskOnWorker(cluster)
		}
	}()

	// Send request to worker
	if err = cluster.CreateIndex(nodeID, req, resource); err != nil {
		log.Warn(ctx, "failed to send job to worker", mlog.Err(err))
		return
	}

	// Update state to in progress
	if err = it.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn(ctx, "failed to update task state", mlog.Err(err))
		return
	}

	log.Info(ctx, "index task assigned successfully")
}

func isEmbeddingListMetric(indexParams []*commonpb.KeyValuePair) bool {
	metricType, err := GetIndexParam(indexParams, common.MetricTypeKey)
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

// exactVectorArrayValidElementCount returns the valid element count of an
// element-nullable VectorArray field recorded in the segment Statistics. The
// byte-size estimate counts the offset/validity overhead of null elements as
// vectors, so the recorded count replaces it whenever it exists. Segments
// written before the count was recorded have no entry and keep the estimate.
func exactVectorArrayValidElementCount(segment *meta.SegmentInfo, field *schemapb.FieldSchema) (int64, bool) {
	if segment == nil || !typeutil.IsVectorArrayType(field.GetDataType()) || !field.GetElementNullable() {
		return 0, false
	}
	count, ok := segment.EnsureStats().GetValidElementCounts()[field.GetFieldID()]
	return count, ok
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
		// An added sub-field has no binlog in an older segment even when its
		// parent struct is non-nullable. It contributes zero indexed vectors.
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
	if !errors.Is(err, errVectorArrayFieldBinlogNotFound) || segment == nil || schema == nil || field == nil || segment.GetSchemaVersion() >= schema.GetVersion() {
		return false
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, subField := range structField.GetFields() {
			if subField.GetFieldID() == field.GetFieldID() {
				// Two schema changes leave an old segment without this child's
				// binlog: adding the child to an existing struct (the child is
				// element-nullable) and adding the whole nullable struct with
				// AddCollectionStructField (its old rows are null struct rows,
				// whatever the child's element_nullable).
				return structField.GetNullable() || subField.GetElementNullable()
			}
		}
	}
	return field.GetNullable()
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
func (it *indexBuildTask) prepareJobRequest(ctx context.Context, segment *meta.SegmentInfo, segIndex *model.SegmentIndex,
	indexParams []*commonpb.KeyValuePair, indexType string,
) (*workerpb.CreateJobRequest, error) {
	log := mlog.With(mlog.Int64("taskID", it.BuildID), mlog.Int64("segmentID", segment.GetID()))

	typeParams := it.indexMeta.GetTypeParams(segIndex.CollectionID, segIndex.IndexID)
	fieldID := it.indexMeta.GetFieldIDByIndexID(segIndex.CollectionID, segIndex.IndexID)

	binlogIDs := meta.GetBinLogIDs(segment, fieldID)
	totalRows := meta.GetTotalBinlogRows(segment, fieldID)

	// Update index parameters as needed
	params := indexParams
	if vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType) && Params.KnowhereConfig.Enable.GetAsBool() {
		var err error
		params, err = Params.KnowhereConfig.UpdateIndexParams(meta.GetIndexType(params), paramtable.BuildStage, params)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "failed to update index build params")
		}
	}

	if isDiskANNIndex(meta.GetIndexType(params)) {
		var err error
		params, err = indexparams.UpdateDiskIndexBuildParams(Params, params)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "failed to append index build params")
		}
	}

	// Get collection info and field
	collectionInfo, err := it.handler.GetCollection(ctx, segment.GetCollectionID())
	if err != nil {
		return nil, merr.Wrap(err, "failed to get collection info")
	}

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
		ClusterID:                 Params.CommonCfg.ClusterPrefix.GetValue(),
		IndexFilePrefix:           path.Join(it.chunkManager.RootPath(), common.SegmentIndexV0Path),
		BuildID:                   it.BuildID,
		IndexStorePathVersion:     segIndex.IndexStorePathVersion,
		IndexVersion:              segIndex.IndexVersion + 1,
		StorageConfig:             meta.CreateStorageConfig(),
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

	if err := meta.WrapPluginContext(segment.GetCollectionID(), schema.GetProperties(), req); err != nil {
		return nil, err
	}

	return req, nil
}

// Helper method to prepare optional fields
func (it *indexBuildTask) prepareOptionalFields(ctx context.Context, collectionInfo *meta.CollectionInfo,
	segment *meta.SegmentInfo, schema *schemapb.CollectionSchema, indexType string, field *schemapb.FieldSchema,
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
				DataIds:     meta.GetBinLogIDs(segment, partitionKeyField.FieldID),
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
	segIndex, exist := it.indexMeta.GetIndexJob(it.BuildID)
	if !exist || segIndex == nil {
		log.Info(ctx, "index task has not exist in meta table, removing task")
		if it.tryDropTaskOnWorker(cluster) != nil {
			return
		}
		it.SetState(indexpb.JobState_JobStateNone, "index task has not exist in meta table")
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
		it.dropAndResetTaskOnWorker(cluster, err.Error())
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
				it.setJobInfo(info)
			case commonpb.IndexState_Retry, commonpb.IndexState_IndexStateNone:
				log.Info(ctx, "query task index info successfully",
					mlog.Int64("taskID", it.BuildID), mlog.String("result state", info.GetState().String()),
					mlog.String("failReason", info.GetFailReason()))
				it.dropAndResetTaskOnWorker(cluster, info.GetFailReason())
			}
			// inProgress or unissued, keep InProgress state
			return
		}
	}
	it.UpdateStateWithMeta(indexpb.JobState_JobStateInit, "index is not in info response")
	// Task not found in results will be return error
}

const (
	IndexTaskAbortReasonSegmentDropped = "segment is dropped, index task aborted"
	indexTaskAbortReasonIndexDropped   = "index is dropped, index task aborted"
)

// droppedTargetReason reports whether the task's build target is gone: the
// segment is no longer healthy or the field index has been dropped. Both are
// irreversible, so the task can never be needed again.
func (it *indexBuildTask) droppedTargetReason(segment *meta.SegmentInfo, segIndex *model.SegmentIndex) (string, bool) {
	if !meta.IsSegmentHealthy(segment) {
		return IndexTaskAbortReasonSegmentDropped, true
	}
	if !it.indexMeta.IsIndexExist(segIndex.CollectionID, segIndex.IndexID) {
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
	if err := it.updateStateWithMeta(ctx, indexpb.JobState_JobStateFailed, reason); err != nil {
		mlog.Warn(ctx, "failed to persist aborted index task state, will retry",
			mlog.Int64("taskID", it.BuildID), mlog.String("reason", reason), mlog.Err(err))
	}
}

func (it *indexBuildTask) tryDropTaskOnWorker(cluster session.Cluster) error {
	ctx := context.TODO()
	log := mlog.With(mlog.Int64("taskID", it.BuildID), mlog.Int64("segmentID", it.SegmentID), mlog.Int64("nodeID", it.NodeID))

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
