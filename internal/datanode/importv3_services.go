// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package datanode

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc64"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/datanode/importv3"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"google.golang.org/protobuf/proto"
)

func (node *DataNode) createImportV3WorkerTask(
	_ context.Context,
	taskID, runID, slot int64,
	execute importv3.Run,
) (*commonpb.Status, error) {
	if node.importV3TaskMgr == nil {
		return merr.Status(merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized")), nil
	}
	if err := node.importV3TaskMgr.Add(taskID, runID, slot, execute); err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

func (node *DataNode) queryImportV3WorkerTask(
	_ context.Context,
	taskID, runID int64,
	taskType taskcommon.Type,
) (*workerpb.QueryTaskResponse, error) {
	if node.importV3TaskMgr == nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized"))}, nil
	}
	snapshot, ok := node.importV3TaskMgr.Query(taskID, runID)
	if !ok {
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrNodeNotFound(node.GetNodeID(),
			"cannot find current import V3 task run"))}, nil
	}
	properties := taskcommon.NewProperties(nil)
	properties.AppendTaskState(importV3TaskCommonState(snapshot.State))
	properties.AppendReason(snapshot.Reason)

	var payload any
	switch taskType {
	case taskcommon.Reshard:
		payload = &datapb.QueryReshardTaskResponse{
			Status:       merr.Success(),
			State:        reshardTaskState(snapshot.State),
			Reason:       snapshot.Reason,
			ResultRef:    resultRef(snapshot.Result),
			ResultDigest: resultDigest(snapshot.Result),
			FailureCode:  snapshot.FailureCode,
		}
	case taskcommon.ImportV3:
		payload = &datapb.QueryImportTaskV3Response{
			Status:       merr.Success(),
			State:        importTaskV3State(snapshot.State),
			Reason:       snapshot.Reason,
			ResultRef:    resultRef(snapshot.Result),
			ResultDigest: resultDigest(snapshot.Result),
			FailureCode:  snapshot.FailureCode,
		}
	default:
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg(
			"invalid V3 task type %q", taskType))}, nil
	}
	// Keep the concrete proto types at the boundary so wrapQueryTaskResult can
	// enforce the existing GetStatus payload contract.
	switch result := payload.(type) {
	case *datapb.QueryReshardTaskResponse:
		return wrapQueryTaskResult(result, properties)
	case *datapb.QueryImportTaskV3Response:
		return wrapQueryTaskResult(result, properties)
	default:
		panic("unreachable import V3 query payload")
	}
}

func (node *DataNode) dropImportV3WorkerTask(taskID, runID int64) (*commonpb.Status, error) {
	if node.importV3TaskMgr == nil {
		return merr.Success(), nil
	}
	// Best effort and idempotent.  A stale run must not cancel a newer run;
	// TaskManager.Drop returns false for both stale and already-absent tasks.
	node.importV3TaskMgr.Drop(taskID, runID)
	return merr.Success(), nil
}

func (node *DataNode) executeReshardTask(ctx context.Context, req *datapb.ReshardTaskRequest, runID int64) (*importv3.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req == nil || req.GetRunId() != runID || req.GetTaskPlanRef() == "" || len(req.GetTaskPlanDigest()) == 0 || req.GetOutputPrefix() == "" || req.GetStorageConfig() == nil || req.GetTaskSlot() <= 0 {
		return nil, merr.WrapErrImportSysFailedMsg("invalid or incomplete ReshardTask request")
	}
	plan, err := node.readReshardTaskPlan(ctx, req.GetStorageConfig(), req.GetTaskPlanRef(), req.GetTaskPlanDigest())
	if err != nil {
		return nil, err
	}
	if plan.GetTaskId() != req.GetTaskId() || plan.GetJobId() != req.GetJobId() {
		return nil, merr.WrapErrDataIntegrityMsg("ReshardTask plan identity mismatch")
	}
	cm, err := node.storageFactory.NewChunkManager(ctx, req.GetStorageConfig())
	if err != nil {
		return nil, err
	}
	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), plan.GetCollectionId())
	if err != nil {
		return nil, err
	}
	return executeReshardPlan(ctx, cm, req, plan, pluginContext)
}

type reshardBucket struct {
	vchannelOrdinal  int
	partitionOrdinal int
	data             *storage.InsertData
}

func executeReshardPlan(ctx context.Context, cm storage.ChunkManager, req *datapb.ReshardTaskRequest, plan *datapb.ReshardTaskPlan, pluginContext *indexcgopb.StoragePluginContext) (*importv3.Result, error) {
	temporarySchema := plan.GetTemporarySchema()
	if plan.GetFormatVersion() == 0 || plan.GetSourceSchema() == nil || temporarySchema == nil ||
		len(plan.GetVchannels()) == 0 || len(plan.GetPartitionIds()) == 0 || len(plan.GetSources()) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid ReshardTask plan")
	}
	sortFields, err := importv3.SortFields(plan.GetSortSpec(), temporarySchema)
	if err != nil {
		return nil, err
	}
	bufferSize := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	maxFileSize := int64(paramtable.Get().DataNodeCfg.MaxImportFileSizeInGB.GetAsFloat() * 1024 * 1024 * 1024)
	manifest := &datapb.ReshardManifest{
		FormatVersion: 1, JobId: plan.GetJobId(), TaskId: plan.GetTaskId(), RunId: req.GetRunId(),
		TaskPlanDigest: append([]byte(nil), req.GetTaskPlanDigest()...), SortSpec: proto.Clone(plan.GetSortSpec()).(*datapb.SortSpec),
	}
	var fragmentSeq int64
	seenSources := make(map[int32]struct{}, len(plan.GetSources()))

	for _, source := range plan.GetSources() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if source == nil || source.GetFile() == nil {
			return nil, merr.WrapErrDataIntegrityMsg("nil ReshardTask source")
		}
		if _, ok := seenSources[source.GetSourceOrdinal()]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate source ordinal %d", source.GetSourceOrdinal())
		}
		seenSources[source.GetSourceOrdinal()] = struct{}{}
		reader, err := newReshardSourceReader(ctx, cm, plan.GetSourceSchema(), source, bufferSize, req.GetStorageConfig(), pluginContext)
		if err != nil {
			return nil, err
		}
		size, err := reader.Size()
		if err != nil {
			reader.Close()
			return nil, merr.Wrapf(err, "get import source %d size", source.GetSourceOrdinal())
		}
		if size > maxFileSize {
			reader.Close()
			return nil, merr.WrapErrParameterInvalidMsg("import file size (%d bytes) exceeds the maximum allowed size (%d bytes)", size, maxFileSize)
		}

		var actualRows int64
		var idOffset int64
		for {
			batch, readErr := reader.Read()
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				reader.Close()
				return nil, merr.Wrapf(readErr, "read import source %d", source.GetSourceOrdinal())
			}
			rowNum, _ := importv2.GetInsertDataRowCount(batch, plan.GetSourceSchema())
			if err := normalizeReshardBatch(source, plan.GetSourceSchema(), batch, rowNum, &idOffset); err != nil {
				reader.Close()
				return nil, err
			}
			if rowNum == 0 {
				continue
			}
			actualRows += int64(rowNum)
			hashed, err := importv2.HashDataBySchema(temporarySchema, plan.GetVchannels(), plan.GetPartitionIds(), batch)
			if err != nil {
				reader.Close()
				return nil, err
			}
			for channelOrdinal := range hashed {
				for partitionOrdinal, bucketData := range hashed[channelOrdinal] {
					if bucketData.GetRowNum() == 0 {
						continue
					}
					bucket := reshardBucket{vchannelOrdinal: channelOrdinal, partitionOrdinal: partitionOrdinal, data: bucketData}
					descriptor, err := writeReshardFragment(ctx, req, plan, temporarySchema, bucket, fragmentSeq, bufferSize, sortFields, pluginContext)
					if err != nil {
						reader.Close()
						return nil, err
					}
					fragmentSeq++
					manifest.Fragments = append(manifest.Fragments, descriptor)
					manifest.TotalRows += descriptor.GetRows()
					manifest.TotalLogicalBytes += descriptor.GetLogicalBytes()
					manifest.TotalPhysicalBytes += descriptor.GetPhysicalBytes()
				}
			}
		}
		reader.Close()
		manifest.Sources = append(manifest.Sources, &datapb.SourceCoverage{
			SourceOrdinal: source.GetSourceOrdinal(), FileId: source.GetFile().GetId(), ActualRows: actualRows, ReachedEof: true,
		})
	}
	return publishReshardManifest(ctx, cm, req, manifest)
}

func newReshardSourceReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, source *datapb.SourceFileSpec, bufferSize int64, storageConfig *indexpb.StorageConfig, pluginContext *indexcgopb.StoragePluginContext) (importutilv2.Reader, error) {
	if source == nil || source.GetFile() == nil {
		return nil, merr.WrapErrDataIntegrityMsg("nil ReshardTask source")
	}
	options := make(importutilv2.Options, 0, 6)
	appendOption := func(key, value string) {
		options = append(options, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	readerOptions := source.GetReaderOptions()
	if source.GetFormat() == datapb.ImportSourceFormat_IMPORT_SOURCE_FORMAT_CSV {
		if readerOptions.GetCsvSeparator() != "" {
			appendOption(importutilv2.CSVSep, readerOptions.GetCsvSeparator())
		}
		if readerOptions.GetCsvNullKey() != "" {
			appendOption(importutilv2.CSVNullKey, readerOptions.GetCsvNullKey())
		}
	}
	if source.GetIsBackup() {
		insertLogs := make(map[int64][]string, len(source.GetExpandedInsertFields()))
		for _, field := range source.GetExpandedInsertFields() {
			if field == nil || len(field.GetPaths()) == 0 {
				return nil, merr.WrapErrDataIntegrityMsg("backup source has an empty expanded insert field")
			}
			if _, ok := insertLogs[field.GetFieldOrGroupId()]; ok {
				return nil, merr.WrapErrDataIntegrityMsg("backup source has duplicate expanded field/group %d", field.GetFieldOrGroupId())
			}
			insertLogs[field.GetFieldOrGroupId()] = append([]string(nil), field.GetPaths()...)
		}
		return binlog.NewExplicitReader(ctx, cm, schema, storageConfig, readerOptions.GetSourceStorageVersion(), insertLogs,
			append([]string(nil), source.GetExpandedDeltaObjects()...), readerOptions.GetBackupStartTs(), readerOptions.GetBackupEndTs(), int(bufferSize), pluginContext)
	}
	return importutilv2.NewReader(ctx, cm, schema, source.GetFile(), options, int(bufferSize), storageConfig)
}

func normalizeReshardBatch(source *datapb.SourceFileSpec, schema *schemapb.CollectionSchema, data *storage.InsertData, rowNum int, idOffset *int64) error {
	if data == nil {
		return merr.WrapErrDataIntegrityMsg("source reader returned nil data")
	}
	if err := importv2.CheckRowsEqual(schema, data); err != nil {
		return err
	}
	if err := importv2.CheckStructArrayConsistency(schema, data); err != nil {
		return err
	}
	if err := importv2.AppendNullableDefaultFieldsData(schema, data, rowNum); err != nil {
		return err
	}
	if err := importv2.FillDynamicData(schema, data, rowNum); err != nil {
		return err
	}
	if source.GetIsBackup() {
		return nil
	}
	return importv2.AppendPreallocatedSystemFields(schema, data, rowNum, source.GetFile().GetPreAllocatedAutoIds(), idOffset)
}

func writeReshardFragment(ctx context.Context, req *datapb.ReshardTaskRequest, plan *datapb.ReshardTaskPlan, temporarySchema *schemapb.CollectionSchema, bucket reshardBucket, seq, bufferSize int64, sortFields []int64, pluginContext *indexcgopb.StoragePluginContext) (*datapb.FragmentDescriptor, error) {
	fragmentPath := path.Join(req.GetOutputPrefix(), "fragments", strconv.Itoa(bucket.vchannelOrdinal), strconv.FormatInt(plan.GetPartitionIds()[bucket.partitionOrdinal], 10), fmt.Sprintf("%d_%d.parquet", req.GetRunId(), seq))
	fields := typeutil.GetAllFieldSchemas(temporarySchema)
	columns := make([]int, len(fields))
	fieldIDs := make([]int64, len(fields))
	for index, field := range fields {
		columns[index], fieldIDs[index] = index, field.GetFieldID()
	}
	writer, err := storage.NewPackedRecordWriter(req.GetStorageConfig().GetBucketName(), []string{fragmentPath}, temporarySchema, bufferSize, packed.DefaultMultiPartUploadSize, []storagecommon.ColumnGroup{{GroupID: 0, Columns: columns, Fields: fieldIDs}}, req.GetStorageConfig(), pluginContext)
	if err != nil {
		return nil, err
	}
	reader, err := storage.NewInsertDataRecordReader(bucket.data, temporarySchema)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	rows, _, err := storage.Sort(uint64(bufferSize), temporarySchema, []storage.RecordReader{reader}, writer, func(storage.Record, int, int) bool { return true }, sortFields)
	if err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if int64(rows) != int64(bucket.data.GetRowNum()) || writer.GetWrittenRowNum() != int64(rows) {
		return nil, merr.WrapErrDataIntegrityMsg("fragment row count mismatch: input=%d sorted=%d written=%d", bucket.data.GetRowNum(), rows, writer.GetWrittenRowNum())
	}
	first, last, err := reshardSortKeyBounds(bucket.data, plan.GetSortSpec())
	if err != nil {
		return nil, err
	}
	logicalBytes := int64(writer.GetWrittenUncompressed())
	physicalBytes := int64(writer.GetColumnGroupWrittenCompressed(0))
	stats := make([]*datapb.ColumnSizeStat, 0, len(fields))
	for _, field := range fields {
		fd := bucket.data.Data[field.GetFieldID()]
		var size int64
		for row := 0; row < fd.RowNum(); row++ {
			size += int64(fd.GetRowSize(row))
		}
		stats = append(stats, &datapb.ColumnSizeStat{FieldId: field.GetFieldID(), Rows: int64(fd.RowNum()), LogicalBytes: size})
	}
	// The descriptor is published only after Close succeeds.  A future version
	// can add content-checksum validation here without changing manifest-last.
	return &datapb.FragmentDescriptor{
		VchannelOrdinal: int32(bucket.vchannelOrdinal), Vchannel: plan.GetVchannels()[bucket.vchannelOrdinal],
		PartitionId: plan.GetPartitionIds()[bucket.partitionOrdinal], PartitionOrdinal: int32(bucket.partitionOrdinal), FragmentSeq: seq,
		Path: writer.GetWrittenPaths(0), Rows: int64(rows), LogicalBytes: logicalBytes, EstimatedFinalBytes: logicalBytes,
		PhysicalBytes: physicalBytes, FirstSortKey: first, LastSortKey: last, Format: storage.ImportFragmentFormatParquet, ColumnSizeStats: stats,
	}, nil
}

func reshardSortKeyBounds(data *storage.InsertData, spec *datapb.SortSpec) (*datapb.SortKey, *datapb.SortKey, error) {
	if data.GetRowNum() == 0 {
		return nil, nil, merr.WrapErrDataIntegrityMsg("cannot compute sort key bounds for empty fragment")
	}
	keyAt := func(row int) (*datapb.SortKey, error) {
		key := &datapb.SortKey{Components: make([]*datapb.SortKeyComponent, 0, len(spec.GetFields()))}
		for _, field := range spec.GetFields() {
			fieldData := data.Data[field.GetFieldId()]
			if fieldData == nil {
				return nil, merr.WrapErrDataIntegrityMsg("sort key field %d is missing from fragment", field.GetFieldId())
			}
			value := fieldData.GetRow(row)
			switch field.GetKeyType() {
			case datapb.SortKeyType_SORT_KEY_TYPE_INT64:
				intValue, ok := value.(int64)
				if !ok {
					return nil, merr.WrapErrDataIntegrityMsg("sort key field %d does not contain int64", field.GetFieldId())
				}
				key.Components = append(key.Components, &datapb.SortKeyComponent{KeyType: field.GetKeyType(), Int64Value: intValue})
			case datapb.SortKeyType_SORT_KEY_TYPE_STRING:
				stringValue, ok := value.(string)
				if !ok {
					return nil, merr.WrapErrDataIntegrityMsg("sort key field %d does not contain string", field.GetFieldId())
				}
				key.Components = append(key.Components, &datapb.SortKeyComponent{KeyType: field.GetKeyType(), StringValue: []byte(stringValue)})
			default:
				return nil, merr.WrapErrDataIntegrityMsg("unsupported sort key type %s", field.GetKeyType())
			}
		}
		return key, nil
	}
	less := func(left, right *datapb.SortKey) bool {
		for index, l := range left.GetComponents() {
			r := right.GetComponents()[index]
			if l.GetKeyType() == datapb.SortKeyType_SORT_KEY_TYPE_INT64 {
				if l.GetInt64Value() != r.GetInt64Value() {
					return l.GetInt64Value() < r.GetInt64Value()
				}
			} else if cmp := bytes.Compare(l.GetStringValue(), r.GetStringValue()); cmp != 0 {
				return cmp < 0
			}
		}
		return false
	}
	first, err := keyAt(0)
	if err != nil {
		return nil, nil, err
	}
	last := proto.Clone(first).(*datapb.SortKey)
	for row := 1; row < data.GetRowNum(); row++ {
		key, err := keyAt(row)
		if err != nil {
			return nil, nil, err
		}
		if less(key, first) {
			first = key
		}
		if less(last, key) {
			last = key
		}
	}
	return first, last, nil
}

func publishReshardManifest(ctx context.Context, cm storage.ChunkManager, req *datapb.ReshardTaskRequest, manifest *datapb.ReshardManifest) (*importv3.Result, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(manifest)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "marshal ReshardManifest")
	}
	digestValue := crc64.Checksum(payload, crc64.MakeTable(crc64.ECMA))
	digest := []byte(fmt.Sprintf("crc64-ecma:%016x", digestValue))
	ref := path.Join(req.GetOutputPrefix(), "manifests", fmt.Sprintf("%d_%016x.pb", req.GetRunId(), digestValue))
	if err := cm.Write(ctx, ref, payload); err != nil {
		return nil, merr.Wrap(err, "write ReshardManifest")
	}
	return &importv3.Result{Ref: ref, Digest: digest, Rows: manifest.GetTotalRows(), Bytes: manifest.GetTotalPhysicalBytes()}, nil
}

func (node *DataNode) executeImportTaskV3(ctx context.Context, req *datapb.ImportTaskV3Request, runID int64) (*importv3.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req == nil || req.GetRunId() != runID || req.GetTaskPlanRef() == "" || len(req.GetTaskPlanDigest()) == 0 || req.GetOutputPrefix() == "" || req.GetStorageConfig() == nil || req.GetTaskSlot() <= 0 || req.GetMergeFanIn() < 2 || req.GetMergeFanIn() > 1024 {
		return nil, merr.WrapErrImportSysFailedMsg("invalid or incomplete ImportTaskV3 request")
	}
	plan, err := node.readImportTaskPlan(ctx, req.GetStorageConfig(), req.GetTaskPlanRef(), req.GetTaskPlanDigest())
	if err != nil {
		return nil, err
	}
	if plan.GetTaskId() != req.GetTaskId() || plan.GetJobId() != req.GetJobId() || plan.GetPlanningGeneration() != req.GetPlanningGeneration() || plan.GetMergeFanIn() != req.GetMergeFanIn() {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 plan/request mismatch")
	}
	if req.GetPlanningSnapshotRef() == "" || len(req.GetPlanningSnapshotDigest()) == 0 || plan.GetPlanningSnapshotRef() != req.GetPlanningSnapshotRef() || !bytes.Equal(plan.GetPlanningSnapshotDigest(), req.GetPlanningSnapshotDigest()) {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 planning snapshot reference mismatch")
	}
	snapshot, err := node.readPlanningSnapshot(ctx, req.GetStorageConfig(), req.GetPlanningSnapshotRef(), req.GetPlanningSnapshotDigest())
	if err != nil {
		return nil, err
	}
	if snapshot.GetJobId() != req.GetJobId() || snapshot.GetGeneration() != req.GetPlanningGeneration() || snapshot.GetTargetSchema() == nil || snapshot.GetTemporarySchema() == nil {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 planning snapshot identity/schema mismatch")
	}
	if snapshot.GetCollectionId() == 0 || len(req.GetOutputSegmentIds()) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 planning snapshot/output segments are incomplete")
	}
	seenSegments := make(map[int64]struct{}, len(req.GetOutputSegmentIds()))
	for _, segmentID := range req.GetOutputSegmentIds() {
		if segmentID <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 output segment ID is invalid: %d", segmentID)
		}
		if _, ok := seenSegments[segmentID]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 output segment ID is duplicated: %d", segmentID)
		}
		seenSegments[segmentID] = struct{}{}
	}
	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), snapshot.GetCollectionId())
	if err != nil {
		return nil, err
	}
	cm, err := node.storageFactory.NewChunkManager(ctx, req.GetStorageConfig())
	if err != nil {
		return nil, err
	}
	return executeImportPlan(ctx, cm, req, plan, snapshot, pluginContext)
}

func (node *DataNode) readPlanningSnapshot(ctx context.Context, cfg *indexpb.StorageConfig, ref string, digest []byte) (*datapb.PlanningSnapshot, error) {
	cm, err := node.storageFactory.NewChunkManager(ctx, cfg)
	if err != nil {
		return nil, err
	}
	payload, err := cm.Read(ctx, ref)
	if err != nil {
		return nil, merr.Wrap(err, "read ImportTaskV3 planning snapshot")
	}
	if err := validateImportV3Digest(ref, payload, digest); err != nil {
		return nil, err
	}
	snapshot := &datapb.PlanningSnapshot{}
	if err := proto.Unmarshal(payload, snapshot); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("decode planning snapshot %s: %s", ref, err.Error())
	}
	return snapshot, nil
}

func executeImportPlan(ctx context.Context, cm storage.ChunkManager, req *datapb.ImportTaskV3Request, plan *datapb.ImportTaskPlan, snapshot *datapb.PlanningSnapshot, pluginContext *indexcgopb.StoragePluginContext) (*importv3.Result, error) {
	temporarySchema := snapshot.GetTemporarySchema()
	targetSchema := snapshot.GetTargetSchema()
	if err := validateImportV3Schemas(temporarySchema, targetSchema); err != nil {
		return nil, err
	}
	sortFields, err := importv3.SortFields(snapshot.GetSortSpec(), temporarySchema)
	if err != nil {
		return nil, err
	}
	isSorted, isNamespaceSorted, err := importv3.ResultSortFlags(snapshot.GetSortSpec(), targetSchema)
	if err != nil {
		return nil, err
	}
	if len(plan.GetSegmentPlans()) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 plan has no segment plans")
	}
	if snapshot.GetCollectionId() == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 snapshot collection ID is empty")
	}
	if len(req.GetOutputSegmentIds()) != len(plan.GetSegmentPlans()) {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 output segment IDs/segment plans mismatch: ids=%d plans=%d", len(req.GetOutputSegmentIds()), len(plan.GetSegmentPlans()))
	}
	if req.GetLogIdRange() == nil || req.GetLogIdRange().GetEnd() <= req.GetLogIdRange().GetBegin() {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 log ID range is empty")
	}
	logAllocator := allocator.NewLocalAllocator(req.GetLogIdRange().GetBegin(), req.GetLogIdRange().GetEnd())
	manifest := &datapb.ImportResultManifest{FormatVersion: 1, JobId: req.GetJobId(), TaskId: req.GetTaskId(), RunId: req.GetRunId(), PlanningGeneration: req.GetPlanningGeneration(), TaskPlanDigest: append([]byte(nil), req.GetTaskPlanDigest()...)}
	if len(plan.GetWriterSpecsDigest()) > 0 {
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&datapb.PlanningSnapshot{WriterSpecs: snapshot.GetWriterSpecs()})
		if err != nil {
			return nil, merr.WrapErrSerializationFailed(err, "marshal ImportTaskV3 WriterSpecs")
		}
		if !bytes.Equal(importV3Digest(payload), plan.GetWriterSpecsDigest()) {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpecs digest mismatch")
		}
	}
	seenOrdinals := make(map[int64]struct{}, len(plan.GetSegmentPlans()))
	for segmentIndex, segment := range plan.GetSegmentPlans() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if segment == nil {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 segment plan is nil")
		}
		ordinal := segment.GetLogicalSegmentOrdinal()
		if ordinal < 0 {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 segment ordinal is out of range: %d", ordinal)
		}
		if _, ok := seenOrdinals[ordinal]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 segment ordinal is duplicated: %d", ordinal)
		}
		seenOrdinals[ordinal] = struct{}{}
		physicalSegmentID := req.GetOutputSegmentIds()[segmentIndex]
		if physicalSegmentID <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 physical segment ID is invalid: %d", physicalSegmentID)
		}
		writerSpecIndex := int(segment.GetWriterSpecIndex())
		if writerSpecIndex < 0 || writerSpecIndex >= len(snapshot.GetWriterSpecs()) {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 writer spec index is out of range: %d", writerSpecIndex)
		}
		writerSpec := snapshot.GetWriterSpecs()[writerSpecIndex]
		writerOptions, err := buildImportV3WriterOptions(req.GetStorageConfig(), snapshot.GetCollectionId(), segment, targetSchema, writerSpec, pluginContext)
		if err != nil {
			return nil, err
		}
		sources := make([]importv3.Source, 0, len(segment.GetFragments()))
		for _, ref := range segment.GetFragments() {
			source, err := importv3.SourceFromFragment(ref, temporarySchema, paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64(), req.GetStorageConfig(), pluginContext)
			if err != nil {
				return nil, err
			}
			sources = append(sources, source)
		}
		var writer storage.BinlogRecordWriter
		finalWriter := func(_ context.Context) (storage.RecordWriter, error) {
			if writer != nil {
				return writer, nil
			}
			writer, err = storage.NewBinlogRecordWriter(ctx, snapshot.GetCollectionId(), segment.GetPartitionId(), physicalSegmentID, targetSchema, logAllocator, uint64(paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsInt64()), segment.GetPlannedRows(), writerOptions...)
			return writer, err
		}
		predicate := importv3.NewTTLOnlyPredicate(temporarySchema, writerSpec.GetCollectionTtlNanos(), snapshot.GetDataTs())
		executor := &importv3.MergeExecutor{
			FanIn: int(req.GetMergeFanIn()), BatchSize: uint64(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()),
			Schema: temporarySchema, SortFields: sortFields, Predicate: predicate,
			FinalWriter: func(output storage.RecordWriter) storage.RecordWriter {
				return newImportV3FinalWriter(ctx, output, temporarySchema, targetSchema, snapshot.GetDataTs(), snapshot.GetClusterId())
			},
			Intermediate: newImportV3IntermediateFactory(req, segment, temporarySchema, pluginContext),
		}
		rows, err := executor.Execute(ctx, sources, finalWriter)
		if err != nil {
			return nil, err
		}
		segmentResult := &datapb.SegmentResult{LogicalSegmentOrdinal: ordinal, PhysicalSegmentId: physicalSegmentID, VchannelOrdinal: segment.GetVchannelOrdinal(), Vchannel: segment.GetVchannel(), PartitionOrdinal: segment.GetPartitionOrdinal(), PartitionId: segment.GetPartitionId(), Rows: rows, Materialized: rows > 0, IsSorted: isSorted, IsSortedByNamespace: isNamespaceSorted, StorageVersion: writerSpec.GetTargetStorageVersion(), SchemaVersion: int32(writerSpec.GetTargetSchemaVersion())}
		if writer != nil {
			fieldBinlogs, statsLog, bm25Logs, manifestPath, expiration := writer.GetLogs()
			segmentResult.InsertLogs = storage.SortFieldBinlogs(fieldBinlogs)
			segmentResult.PkStatsLog = statsLog
			segmentResult.Bm25Logs = storage.SortFieldBinlogs(bm25Logs)
			segmentResult.ManifestPath = manifestPath
			segmentResult.ExpirationQuantiles = expiration
			segmentResult.Statistics = storage.BuildStatsFromFieldBinlogs(segmentResult.InsertLogs, nil, segmentResult.Bm25Logs, nil)
			segmentResult.Statistics.StatsBinlogSize = writer.GetStatsBlobSize()
			segmentResult.MinTimestamp = segmentResult.Statistics.GetTimestampFrom()
			segmentResult.MaxTimestamp = segmentResult.Statistics.GetTimestampTo()
			segmentResult.PhysicalBytes, err = importV3PhysicalBytes(ctx, cm, req.GetStorageConfig(), writerSpec.GetTargetStorageVersion(), segmentResult)
			if err != nil {
				return nil, err
			}
			if writer.GetRowNum() != rows {
				return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 writer/result row mismatch: writer=%d result=%d", writer.GetRowNum(), rows)
			}
		} else if rows != 0 {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 kept rows without materializing a writer: %d", rows)
		}
		manifest.Segments = append(manifest.Segments, segmentResult)
		manifest.TotalRows += rows
		manifest.TotalPhysicalBytes += segmentResult.GetPhysicalBytes()
	}
	if len(manifest.GetSegments()) != len(plan.GetSegmentPlans()) {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 result segment count mismatch")
	}
	return publishImportResultManifest(ctx, cm, req, manifest)
}

func validateImportV3Schemas(temporary, target *schemapb.CollectionSchema) error {
	if temporary == nil || target == nil {
		return merr.WrapErrDataIntegrityMsg("ImportTaskV3 snapshot schema is nil")
	}
	temporaryFields := make(map[int64]*schemapb.FieldSchema)
	for _, field := range typeutil.GetAllFieldSchemas(temporary) {
		temporaryFields[field.GetFieldID()] = field
	}
	targetFields := make(map[int64]*schemapb.FieldSchema)
	for _, field := range typeutil.GetAllFieldSchemas(target) {
		targetFields[field.GetFieldID()] = field
	}
	for _, systemField := range []int64{common.RowIDField, common.TimeStampField} {
		field := targetFields[systemField]
		if field == nil || field.GetDataType() != schemapb.DataType_Int64 {
			return merr.WrapErrDataIntegrityMsg("ImportTaskV3 target schema is missing int64 system field %d", systemField)
		}
	}
	if field := temporaryFields[common.RowIDField]; field == nil || field.GetDataType() != schemapb.DataType_Int64 {
		return merr.WrapErrDataIntegrityMsg("ImportTaskV3 temporary schema is missing RowID")
	}
	for fieldID, sourceField := range temporaryFields {
		targetField := targetFields[fieldID]
		if targetField == nil || targetField.GetDataType() != sourceField.GetDataType() {
			return merr.WrapErrDataIntegrityMsg("ImportTaskV3 temporary field %d is incompatible with target schema", fieldID)
		}
	}
	return nil
}

func buildImportV3WriterOptions(storageConfig *indexpb.StorageConfig, collectionID int64, segment *datapb.SegmentPlan, targetSchema *schemapb.CollectionSchema, spec *datapb.WriterSpec, pluginContext *indexcgopb.StoragePluginContext) ([]storage.RwOption, error) {
	if spec == nil || spec.GetFormatVersion() == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec is nil or has no version")
	}
	if spec.GetTargetStorageVersion() != storage.StorageV2 && spec.GetTargetStorageVersion() != storage.StorageV3 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec storage version is unsupported: %d", spec.GetTargetStorageVersion())
	}
	if spec.GetTargetSchemaVersion() != targetSchema.GetVersion() {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec schema version mismatch: spec=%d snapshot=%d", spec.GetTargetSchemaVersion(), targetSchema.GetVersion())
	}
	if spec.GetPkStatsCapacity() <= 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec PK stats capacity must be positive")
	}
	if segment.GetPlannedRows() <= 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 planned rows must be positive")
	}
	if spec.GetPkStatsCapacity() < segment.GetPlannedRows() {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec PK stats capacity is smaller than planned rows: capacity=%d rows=%d", spec.GetPkStatsCapacity(), segment.GetPlannedRows())
	}
	bfType := bloomfilter.BFTypeFromString(spec.GetBloomFilterType())
	if (bfType != bloomfilter.BasicBF && bfType != bloomfilter.BlockedBF) || spec.GetMaxBloomFalsePositive() <= 0 || spec.GetMaxBloomFalsePositive() >= 1 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec Bloom filter config is invalid")
	}
	if spec.GetWriterFormat() == "" {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec writer format is empty")
	}
	if len(spec.GetTargetSchemaDigest()) > 0 {
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(targetSchema)
		if err != nil {
			return nil, merr.WrapErrSerializationFailed(err, "marshal ImportTaskV3 target schema")
		}
		if !bytes.Equal(importV3Digest(payload), spec.GetTargetSchemaDigest()) {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec target schema digest mismatch")
		}
	}
	groups, err := importV3ColumnGroups(targetSchema, spec.GetColumnGroups())
	if err != nil {
		return nil, err
	}
	bufferSize := int64(packed.DefaultWriteBufferSize)
	multipartSize := int64(packed.DefaultMultiPartUploadSize)
	if spec.GetV2Io() != nil {
		if spec.GetV2Io().GetBufferSize() <= 0 || spec.GetV2Io().GetMultipartUploadSize() <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec V2 IO sizes must be positive")
		}
		bufferSize = spec.GetV2Io().GetBufferSize()
		multipartSize = spec.GetV2Io().GetMultipartUploadSize()
	}
	options := []storage.RwOption{
		storage.WithVersion(spec.GetTargetStorageVersion()),
		storage.WithBufferSize(bufferSize),
		storage.WithMultiPartUploadSize(multipartSize),
		storage.WithColumnGroups(groups),
		storage.WithStorageConfig(storageConfig),
		storage.WithPluginContext(pluginContext),
		storage.WithWriterFormat(spec.GetWriterFormat()),
		storage.WithPkStatsConfig(storage.PkStatsConfig{
			Capacity: spec.GetPkStatsCapacity(), BloomFilterType: spec.GetBloomFilterType(), MaxBloomFalsePositive: spec.GetMaxBloomFalsePositive(),
		}),
	}
	if len(spec.GetTextColumns()) > 0 {
		if spec.GetTargetStorageVersion() != storage.StorageV3 {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 TEXT columns require StorageV3")
		}
		partitionBase := path.Join(storageConfig.GetRootPath(), common.SegmentInsertLogPath, strconv.FormatInt(collectionID, 10), strconv.FormatInt(segment.GetPartitionId(), 10))
		textConfigs := make([]packed.TextColumnConfig, 0, len(spec.GetTextColumns()))
		for _, text := range spec.GetTextColumns() {
			if text == nil || text.GetFieldId() < common.StartOfUserFieldID || text.GetInlineThreshold() < 0 || text.GetMaxLobFileBytes() <= 0 || text.GetFlushThresholdBytes() <= 0 {
				return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec TEXT config is invalid")
			}
			textConfigs = append(textConfigs, packed.TextColumnConfig{
				FieldID: text.GetFieldId(), LobBasePath: path.Join(partitionBase, "lobs", strconv.FormatInt(text.GetFieldId(), 10)),
				InlineThreshold: text.GetInlineThreshold(), MaxLobFileBytes: text.GetMaxLobFileBytes(), FlushThresholdBytes: text.GetFlushThresholdBytes(),
			})
		}
		options = append(options, storage.WithTextColumnConfigs(textConfigs))
	}
	if spec.GetTargetStorageVersion() == storage.StorageV3 && len(groups) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 StorageV3 WriterSpec has no column groups")
	}
	if spec.GetTextIndex() != nil && spec.GetTextIndex().GetBuildInline() {
		return nil, merr.WrapErrImportSysFailedMsg("ImportTaskV3 inline text index creation is not implemented")
	}
	return options, nil
}

func importV3ColumnGroups(schema *schemapb.CollectionSchema, specs []*datapb.ColumnGroupSpec) ([]storagecommon.ColumnGroup, error) {
	fields := typeutil.GetAllFieldSchemas(schema)
	fieldColumns := make(map[int64]int, len(fields))
	for column, field := range fields {
		fieldColumns[field.GetFieldID()] = column
	}
	if len(specs) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column groups are empty")
	}
	groups := make([]storagecommon.ColumnGroup, 0, len(specs))
	covered := make(map[int64]struct{}, len(fields))
	for _, spec := range specs {
		if spec == nil || len(spec.GetFieldIds()) == 0 || spec.GetFormat() == "" {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column group is incomplete")
		}
		group := storagecommon.ColumnGroup{GroupID: spec.GetGroupId(), Format: spec.GetFormat()}
		for _, fieldID := range spec.GetFieldIds() {
			column, ok := fieldColumns[fieldID]
			if !ok {
				return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column group references unknown field %d", fieldID)
			}
			if _, ok := covered[fieldID]; ok {
				return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column groups overlap at field %d", fieldID)
			}
			covered[fieldID] = struct{}{}
			group.Fields = append(group.Fields, fieldID)
			group.Columns = append(group.Columns, column)
		}
		groups = append(groups, group)
	}
	if len(covered) != len(fields) {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column groups do not cover target schema: covered=%d fields=%d", len(covered), len(fields))
	}
	return groups, nil
}

type importV3FinalWriter struct {
	ctx             context.Context
	output          storage.RecordWriter
	temporarySchema *schemapb.CollectionSchema
	targetSchema    *schemapb.CollectionSchema
	dataTS          uint64
	clusterID       string
	runFunctions    bool
}

func newImportV3FinalWriter(ctx context.Context, output storage.RecordWriter, temporarySchema, targetSchema *schemapb.CollectionSchema, dataTS uint64, clusterID string) storage.RecordWriter {
	return &importV3FinalWriter{ctx: ctx, output: output, temporarySchema: temporarySchema, targetSchema: targetSchema, dataTS: dataTS, clusterID: clusterID, runFunctions: !temporarySchemaContainsFunctionOutput(temporarySchema, targetSchema)}
}

func temporarySchemaContainsFunctionOutput(temporarySchema, targetSchema *schemapb.CollectionSchema) bool {
	temporaryFields := make(map[int64]struct{})
	for _, field := range typeutil.GetAllFieldSchemas(temporarySchema) {
		temporaryFields[field.GetFieldID()] = struct{}{}
	}
	for _, field := range typeutil.GetAllFieldSchemas(targetSchema) {
		if field.GetIsFunctionOutput() {
			if _, ok := temporaryFields[field.GetFieldID()]; ok {
				return true
			}
		}
	}
	return false
}

func (w *importV3FinalWriter) Write(record storage.Record) error {
	required := typeutil.NewSet[int64]()
	for _, field := range typeutil.GetAllFieldSchemas(w.temporarySchema) {
		required.Insert(field.GetFieldID())
	}
	data, err := storage.RecordToInsertData(record, w.targetSchema, required)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "convert ImportTaskV3 merged record")
	}
	rows := data.GetRowNum()
	if rows == 0 {
		return nil
	}
	if ts := data.Data[common.TimeStampField]; ts == nil || ts.RowNum() == 0 {
		timestamps := make([]int64, rows)
		for index := range timestamps {
			timestamps[index] = int64(w.dataTS)
		}
		data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: timestamps}
	} else if ts.RowNum() != rows {
		return merr.WrapErrDataIntegrityMsg("ImportTaskV3 source timestamp rows mismatch: timestamps=%d rows=%d", ts.RowNum(), rows)
	}
	if w.runFunctions && len(w.targetSchema.GetFunctions()) > 0 {
		if err := embedding.RunAll(w.ctx, w.targetSchema, data, embedding.RunOptions{
			ClusterID: w.clusterID, DBName: w.targetSchema.GetDbName(),
			AllowNonBM25Outputs: common.GetCollectionAllowInsertNonBM25FunctionOutputs(w.targetSchema.GetProperties()),
		}); err != nil {
			return merr.Wrap(err, "run ImportTaskV3 functions")
		}
	}
	reader, err := storage.NewInsertDataRecordReader(data, w.targetSchema)
	if err != nil {
		return err
	}
	defer reader.Close()
	finalRecord, err := reader.Next()
	if err != nil {
		return merr.Wrap(err, "build ImportTaskV3 final record")
	}
	return w.output.Write(finalRecord)
}

func (w *importV3FinalWriter) GetWrittenUncompressed() uint64 {
	return w.output.GetWrittenUncompressed()
}

func (w *importV3FinalWriter) Close() error {
	return w.output.Close()
}

func newImportV3IntermediateFactory(req *datapb.ImportTaskV3Request, segment *datapb.SegmentPlan, schema *schemapb.CollectionSchema, pluginContext *indexcgopb.StoragePluginContext) importv3.IntermediateWriterFactory {
	return func(_ context.Context, round, group int, _ []importv3.Source) (storage.RecordWriter, func(int64) (importv3.Source, error), error) {
		fields := typeutil.GetAllFieldSchemas(schema)
		columns := make([]int, len(fields))
		fieldIDs := make([]int64, len(fields))
		for index, field := range fields {
			columns[index], fieldIDs[index] = index, field.GetFieldID()
		}
		intermediatePath := path.Join(req.GetOutputPrefix(), "merge", fmt.Sprintf("%d_%d_%d_%d.parquet", req.GetRunId(), segment.GetLogicalSegmentOrdinal(), round, group))
		writer, err := storage.NewPackedRecordWriter(req.GetStorageConfig().GetBucketName(), []string{intermediatePath}, schema,
			paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64(), packed.DefaultMultiPartUploadSize,
			[]storagecommon.ColumnGroup{{GroupID: 0, Columns: columns, Fields: fieldIDs}}, req.GetStorageConfig(), pluginContext)
		if err != nil {
			return nil, nil, err
		}
		commit := func(rows int64) (importv3.Source, error) {
			if rows <= 0 || writer.GetWrittenRowNum() != rows {
				return importv3.Source{}, merr.WrapErrDataIntegrityMsg("ImportTaskV3 intermediate rows mismatch: writer=%d merge=%d", writer.GetWrittenRowNum(), rows)
			}
			return importv3.Source{
				ID: intermediatePath, Rows: rows,
				Open: func(ctx context.Context) (storage.RecordReader, error) {
					return storage.NewImportFragmentRecordReader(ctx, storage.ImportFragmentReaderSpec{Path: intermediatePath, Format: storage.ImportFragmentFormatParquet, StartRow: 0, EndRow: rows, Rows: rows}, schema,
						storage.WithBufferSize(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()), storage.WithStorageConfig(req.GetStorageConfig()), storage.WithPluginContext(pluginContext))
				},
			}, nil
		}
		return writer, commit, nil
	}
}

func importV3PhysicalBytes(ctx context.Context, cm storage.ChunkManager, storageConfig *indexpb.StorageConfig, storageVersion int64, result *datapb.SegmentResult) (int64, error) {
	if storageVersion == storage.StorageV2 {
		var total int64
		for _, fieldBinlog := range append(append([]*datapb.FieldBinlog{}, result.GetInsertLogs()...), append([]*datapb.FieldBinlog{result.GetPkStatsLog()}, result.GetBm25Logs()...)...) {
			if fieldBinlog == nil {
				continue
			}
			for _, binlog := range fieldBinlog.GetBinlogs() {
				total += binlog.GetLogSize()
			}
		}
		return total, nil
	}
	if storageVersion != storage.StorageV3 || result.GetManifestPath() == "" {
		return 0, merr.WrapErrDataIntegrityMsg("ImportTaskV3 result has invalid storage version/manifest")
	}
	fragments, err := packed.ReadFragmentsFromManifest(result.GetManifestPath(), storageConfig, nil)
	if err != nil {
		return 0, merr.Wrap(err, "read ImportTaskV3 output manifest fragments")
	}
	seen := make(map[string]struct{}, len(fragments))
	var total int64
	for _, fragment := range fragments {
		if _, ok := seen[fragment.FilePath]; ok {
			continue
		}
		seen[fragment.FilePath] = struct{}{}
		size, err := cm.Size(ctx, fragment.FilePath)
		if err != nil {
			return 0, merr.Wrap(err, "stat ImportTaskV3 output fragment")
		}
		total += size
	}
	stats, err := packed.GetManifestStats(result.GetManifestPath(), storageConfig)
	if err != nil {
		return 0, merr.Wrap(err, "read ImportTaskV3 output manifest stats")
	}
	for _, stat := range stats {
		for _, statPath := range stat.Paths {
			if _, ok := seen[statPath]; ok {
				continue
			}
			seen[statPath] = struct{}{}
			size, err := cm.Size(ctx, statPath)
			if err != nil {
				return 0, merr.Wrap(err, "stat ImportTaskV3 output stats")
			}
			total += size
		}
	}
	lobs, err := packed.GetManifestLobFiles(result.GetManifestPath(), storageConfig)
	if err != nil {
		return 0, merr.Wrap(err, "read ImportTaskV3 output LOB files")
	}
	for _, lob := range lobs {
		if _, ok := seen[lob.Path]; ok {
			continue
		}
		seen[lob.Path] = struct{}{}
		if lob.FileSizeBytes > 0 {
			total += lob.FileSizeBytes
			continue
		}
		size, err := cm.Size(ctx, lob.Path)
		if err != nil {
			return 0, merr.Wrap(err, "stat ImportTaskV3 output LOB")
		}
		total += size
	}
	return total, nil
}

func publishImportResultManifest(ctx context.Context, cm storage.ChunkManager, req *datapb.ImportTaskV3Request, manifest *datapb.ImportResultManifest) (*importv3.Result, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(manifest)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "marshal ImportResultManifest")
	}
	digestValue := crc64.Checksum(payload, crc64.MakeTable(crc64.ECMA))
	digest := []byte(fmt.Sprintf("crc64-ecma:%016x", digestValue))
	ref := path.Join(req.GetOutputPrefix(), "results", fmt.Sprintf("%d_%016x.pb", req.GetRunId(), digestValue))
	if err := cm.Write(ctx, ref, payload); err != nil {
		return nil, merr.Wrap(err, "write ImportResultManifest")
	}
	return &importv3.Result{Ref: ref, Digest: digest, Rows: manifest.GetTotalRows(), Bytes: manifest.GetTotalPhysicalBytes()}, nil
}

func importV3Digest(payload []byte) []byte {
	value := crc64.Checksum(payload, crc64.MakeTable(crc64.ECMA))
	return []byte(fmt.Sprintf("crc64-ecma:%016x", value))
}

func validateImportV3Digest(ref string, payload, expected []byte) error {
	if len(expected) == 0 {
		return merr.WrapErrDataIntegrityMsg("import V3 digest is empty for %s", ref)
	}
	actual := importV3Digest(payload)
	if !bytes.Equal(actual, expected) {
		return merr.WrapErrDataIntegrityMsg("import V3 digest mismatch for %s: expected=%s actual=%s", ref, string(expected), string(actual))
	}
	// Keep the digest token in the immutable object name as well. This is a
	// cheap stale-reference check; it is not a second checksum algorithm.
	digestToken := strings.TrimPrefix(string(expected), "crc64-ecma:")
	if digestToken == "" || !strings.Contains(path.Base(ref), digestToken) {
		return merr.WrapErrDataIntegrityMsg("import V3 object ref %s does not contain digest token %s", ref, digestToken)
	}
	return nil
}

func (node *DataNode) readReshardTaskPlan(ctx context.Context, cfg *indexpb.StorageConfig, ref string, digest []byte) (*datapb.ReshardTaskPlan, error) {
	cm, err := node.storageFactory.NewChunkManager(ctx, cfg)
	if err != nil {
		return nil, err
	}
	payload, err := cm.Read(ctx, ref)
	if err != nil {
		return nil, err
	}
	if err := validateImportV3Digest(ref, payload, digest); err != nil {
		return nil, err
	}
	plan := &datapb.ReshardTaskPlan{}
	if err := proto.Unmarshal(payload, plan); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("decode ReshardTask plan %s: %s", ref, err.Error())
	}
	return plan, nil
}

func (node *DataNode) readImportTaskPlan(ctx context.Context, cfg *indexpb.StorageConfig, ref string, digest []byte) (*datapb.ImportTaskPlan, error) {
	cm, err := node.storageFactory.NewChunkManager(ctx, cfg)
	if err != nil {
		return nil, err
	}
	payload, err := cm.Read(ctx, ref)
	if err != nil {
		return nil, err
	}
	if err := validateImportV3Digest(ref, payload, digest); err != nil {
		return nil, err
	}
	plan := &datapb.ImportTaskPlan{}
	if err := proto.Unmarshal(payload, plan); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("decode ImportTaskV3 plan %s: %s", ref, err.Error())
	}
	return plan, nil
}

func importV3TaskCommonState(state importv3.State) taskcommon.State {
	switch state {
	case importv3.StatePending:
		return taskcommon.Init
	case importv3.StateRunning:
		return taskcommon.InProgress
	case importv3.StateRetry:
		return taskcommon.Retry
	case importv3.StateCompleted:
		return taskcommon.Finished
	case importv3.StateFailed:
		return taskcommon.Failed
	default:
		return taskcommon.None
	}
}

func reshardTaskState(state importv3.State) datapb.ReshardTask_State {
	switch state {
	case importv3.StatePending:
		return datapb.ReshardTask_Pending
	case importv3.StateRunning:
		return datapb.ReshardTask_Running
	case importv3.StateRetry:
		return datapb.ReshardTask_Retry
	case importv3.StateCompleted:
		return datapb.ReshardTask_Completed
	case importv3.StateFailed:
		return datapb.ReshardTask_Failed
	default:
		return datapb.ReshardTask_None
	}
}

func importTaskV3State(state importv3.State) datapb.ImportTaskV3_State {
	switch state {
	case importv3.StatePending:
		return datapb.ImportTaskV3_Pending
	case importv3.StateRunning:
		return datapb.ImportTaskV3_Running
	case importv3.StateRetry:
		return datapb.ImportTaskV3_Retry
	case importv3.StateCompleted:
		return datapb.ImportTaskV3_Completed
	case importv3.StateFailed:
		return datapb.ImportTaskV3_Failed
	default:
		return datapb.ImportTaskV3_None
	}
}

func resultRef(result *importv3.Result) string {
	if result == nil {
		return ""
	}
	return result.Ref
}

func resultDigest(result *importv3.Result) []byte {
	if result == nil {
		return nil
	}
	return append([]byte(nil), result.Digest...)
}
