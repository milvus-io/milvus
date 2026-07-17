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
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
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

	meta                      *meta
	handler                   Handler
	chunkManager              storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager
}

var _ globalTask.Task = (*indexBuildTask)(nil)

var errVectorArrayFieldBinlogNotFound = errors.New("vector array field binlog not found")

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
	if err := it.meta.indexMeta.UpdateIndexState(it.BuildID, commonpb.IndexState(state), failReason); err != nil {
		return err
	}
	it.SetState(state, failReason)
	return nil
}

func (it *indexBuildTask) UpdateTaskVersion(nodeID int64) error {
	if err := it.meta.indexMeta.UpdateVersion(it.BuildID, nodeID); err != nil {
		return err
	}
	it.IndexVersion++
	it.NodeID = nodeID
	return nil
}

func (it *indexBuildTask) setJobInfo(result *workerpb.IndexTaskInfo) error {
	if err := it.meta.indexMeta.FinishTask(result); err != nil {
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

func (it *indexBuildTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := context.TODO()
	log := mlog.With(mlog.Int64("taskID", it.BuildID), mlog.Int64("segmentID", it.SegmentID))

	// Check if task exists in meta
	segIndex, exist := it.meta.indexMeta.GetIndexJob(it.BuildID)
	if !exist || segIndex == nil {
		log.Info(ctx, "index task has not exist in meta table, removing task")
		it.SetState(indexpb.JobState_JobStateNone, "index task has not exist in meta table")
		return
	}

	// Check segment health and index existence
	segment := it.meta.GetSegment(ctx, segIndex.SegmentID)
	if !isSegmentHealthy(segment) || !it.meta.indexMeta.IsIndexExist(segIndex.CollectionID, segIndex.IndexID) {
		log.Info(ctx, "task is no need to build index, removing it")
		it.SetState(indexpb.JobState_JobStateNone, "task is no need to build index")
		return
	}

	// Handle special cases for certain index types or small segments
	indexParams := it.meta.indexMeta.GetIndexParams(segIndex.CollectionID, segIndex.IndexID)
	indexType := GetIndexType(indexParams)
	effectiveRows := segIndex.NumRows
	estimatedVectorArrayVectors := int64(0)
	isVectorArrayIndex := false
	isEmbeddingListIndex := false
	if fieldID := it.meta.indexMeta.GetFieldIDByIndexID(segIndex.CollectionID, segIndex.IndexID); fieldID > 0 {
		if collectionInfo, err := it.handler.GetCollection(ctx, segIndex.CollectionID); err == nil {
			for _, f := range typeutil.GetAllFieldSchemas(collectionInfo.Schema) {
				if f.FieldID == fieldID {
					if f.GetNullable() && typeutil.IsVectorType(f.GetDataType()) {
						effectiveRows = segmentutil.CalcValidRowCountFromFieldBinLog(segment.SegmentInfo, fieldID)
					}
					isVectorArrayIndex = typeutil.IsVectorArrayType(f.GetDataType())
					isEmbeddingListIndex = isVectorArrayIndex && isEmbeddingListMetric(indexParams)
					if isVectorArrayIndex {
						estimate, err := estimateVectorArrayElementCountForIndexBuild(segment.SegmentInfo, collectionInfo.Schema, f)
						if err != nil {
							failReason := "failed to estimate vector array element count, count is unknown: " + err.Error()
							log.Warn(ctx, "failed to estimate vector array element count",
								mlog.Int64("fieldID", f.GetFieldID()),
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
								mlog.Int64("fieldID", f.GetFieldID()),
								mlog.String("fieldName", f.GetName()),
								mlog.Int32("segmentSchemaVersion", segment.GetSchemaVersion()),
								mlog.Int32("collectionSchemaVersion", collectionInfo.Schema.GetVersion()))
						}
					}
					break
				}
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
		vectorArrayVectorCountBelowThreshold = estimatedVectorArrayVectors < minRowsToBuildIndex
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
	if err = cluster.CreateIndex(nodeID, req); err != nil {
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

// isNestedArrayIndex reports whether a scalar array index for `field`, built at
// scalarIndexVersion, is a nested (element-level) array index whose loading
// requires nested-array-capable (scalar index version >= 5) code.
//
// The rule itself lives in typeutil.IsNestedArrayIndex (see its doc for the
// marker semantics and the struct sub-field carve-out) because it must be
// applied identically on both sides of a version-skewed build: datacoord
// derives the marker here for the build request (prepareJobRequest) and the
// snapshot-restore copy path (syncVectorScalarIndexes), and the index worker
// independently re-derives it in PreExecute from the same rule — so neither an
// old datacoord (no nested field in its request proto) nor an old copy worker
// (echoing a dropped marker bit) can produce a scalar-version>=5 plain-ARRAY
// index that is not nested.
func isNestedArrayIndex(field *schemapb.FieldSchema, scalarIndexVersion int32) bool {
	return typeutil.IsNestedArrayIndex(field, scalarIndexVersion)
}

// Helper method to prepare job request
func (it *indexBuildTask) prepareJobRequest(ctx context.Context, segment *SegmentInfo, segIndex *model.SegmentIndex,
	indexParams []*commonpb.KeyValuePair, indexType string,
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

	// Decide here whether this build produces a nested (element-level) array
	// index. The scalar index engine version is the capability signal (minimum
	// across all querynodes, so during a rolling upgrade no pre-nested node can
	// be handed an element-level index); the decision is frozen into the request
	// and persisted with the segment index meta, and load routing uses the
	// persisted marker, never the version. See isNestedArrayIndex for the
	// struct-sub-field carve-out and why it is the single source of truth.
	isNestedIndex := isNestedArrayIndex(field, currentScalarIndexVersion)

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
		StorageConfig:             createStorageConfig(),
		IndexParams:               params,
		TypeParams:                typeParams,
		NumRows:                   segIndex.NumRows,
		CurrentIndexVersion:       currentVecIndexVersion,
		CurrentScalarIndexVersion: currentScalarIndexVersion,
		IsNestedIndex:             isNestedIndex,
		CollectionID:              segment.GetCollectionID(),
		PartitionID:               segment.GetPartitionID(),
		SegmentID:                 segment.GetID(),
		FieldID:                   fieldID,
		FieldName:                 field.GetName(),
		FieldType:                 field.GetDataType(),
		Dim:                       int64(dim),
		DataIds:                   binlogIDs,
		OptionalScalarFields:      optionalFields,
		Field:                     field,
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
