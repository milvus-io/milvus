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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/datanode/importv3"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
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
	taskID, runID int64,
	execute importv3.Run,
) (*commonpb.Status, error) {
	if node.importV3TaskMgr == nil {
		return merr.Status(merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized")), nil
	}
	if err := node.importV3TaskMgr.Add(taskID, runID, execute); err != nil {
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
	if plan.GetFormatVersion() == 0 || plan.GetSourceSchema() == nil || plan.GetTemporarySchema() == nil ||
		len(plan.GetVchannels()) == 0 || len(plan.GetPartitionIds()) == 0 || len(plan.GetSources()) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid ReshardTask plan")
	}
	sortFields, err := importv3.SortFields(plan.GetSortSpec(), plan.GetTemporarySchema())
	if err != nil {
		return nil, err
	}
	bufferSize := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	maxFileSize := int64(paramtable.Get().DataNodeCfg.MaxImportFileSizeInGB.GetAsFloat() * 1024 * 1024 * 1024)
	manifest := &datapb.ReshardManifest{
		FormatVersion: 1, JobId: plan.GetJobId(), TaskId: plan.GetTaskId(), RunId: req.GetRunId(),
		TaskPlanDigest: append([]byte(nil), req.GetTaskPlanDigest()...), SortSpec: proto.Clone(plan.GetSortSpec()).(*datapb.SortSpec),
	}
	fragmentSeq := make(map[[2]int]int64)
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
		reader, err := newReshardSourceReader(ctx, cm, plan.GetSourceSchema(), source, bufferSize, req.GetStorageConfig())
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
			hashed, err := importv2.HashDataBySchema(plan.GetTemporarySchema(), plan.GetVchannels(), plan.GetPartitionIds(), batch)
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
					key := [2]int{channelOrdinal, partitionOrdinal}
					descriptor, err := writeReshardFragment(ctx, req, plan, bucket, fragmentSeq[key], bufferSize, sortFields, pluginContext)
					if err != nil {
						reader.Close()
						return nil, err
					}
					fragmentSeq[key]++
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

func newReshardSourceReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, source *datapb.SourceFileSpec, bufferSize int64, storageConfig *indexpb.StorageConfig) (importutilv2.Reader, error) {
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
		appendOption(importutilv2.BackupFlag, "true")
		appendOption(importutilv2.StartTs, strconv.FormatUint(readerOptions.GetBackupStartTs(), 10))
		appendOption(importutilv2.EndTs, strconv.FormatUint(readerOptions.GetBackupEndTs(), 10))
		appendOption(importutilv2.StorageVersion, strconv.FormatInt(readerOptions.GetSourceStorageVersion(), 10))
	}
	// TODO(import-v3): backup currently goes through the legacy prefix reader.
	// The plan already contains explicit expanded objects; switch this call to
	// the explicit-object reader when that small binlog adapter lands.
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

func writeReshardFragment(ctx context.Context, req *datapb.ReshardTaskRequest, plan *datapb.ReshardTaskPlan, bucket reshardBucket, seq, bufferSize int64, sortFields []int64, pluginContext *indexcgopb.StoragePluginContext) (*datapb.FragmentDescriptor, error) {
	fragmentPath := path.Join(req.GetOutputPrefix(), "fragments", strconv.Itoa(bucket.vchannelOrdinal), strconv.FormatInt(plan.GetPartitionIds()[bucket.partitionOrdinal], 10), fmt.Sprintf("%d_%d.parquet", req.GetRunId(), seq))
	fields := typeutil.GetAllFieldSchemas(plan.GetTemporarySchema())
	columns := make([]int, len(fields))
	fieldIDs := make([]int64, len(fields))
	for index, field := range fields {
		columns[index], fieldIDs[index] = index, field.GetFieldID()
	}
	writer, err := storage.NewPackedRecordWriter(req.GetStorageConfig().GetBucketName(), []string{fragmentPath}, plan.GetTemporarySchema(), bufferSize, packed.DefaultMultiPartUploadSize, []storagecommon.ColumnGroup{{GroupID: 0, Columns: columns, Fields: fieldIDs}}, req.GetStorageConfig(), pluginContext)
	if err != nil {
		return nil, err
	}
	reader, err := storage.NewInsertDataRecordReader(bucket.data, plan.GetTemporarySchema())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	rows, _, err := storage.Sort(uint64(bufferSize), plan.GetTemporarySchema(), []storage.RecordReader{reader}, writer, func(storage.Record, int, int) bool { return true }, sortFields)
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
	return nil, merr.WrapErrImportSysFailedMsg("ImportTaskV3 merge executor is unavailable for plan %d", plan.GetTaskId())
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
	// TODO(import-v3): validate the persisted plan digest here when the shared
	// digest helper is finalized. The first implementation deliberately does not
	// calculate SHA-256.
	_ = digest
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
	// TODO(import-v3): validate the persisted plan digest here; do not add a
	// second ad-hoc SHA-256 implementation at this boundary.
	_ = digest
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
