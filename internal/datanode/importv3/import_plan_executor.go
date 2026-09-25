// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// This file owns the execution of one ImportTaskV3 run: merge the plan's
// fragments into the formal segment and build its result.

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	flushio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type importV3PackedRecordWriter interface {
	storage.RecordWriter
	GetWrittenRowNum() int64
	GetWrittenPaths(columnGroup typeutil.UniqueID) string
}

func newImportV3PackedRecordWriter(bucketName string, paths []string, schema *schemapb.CollectionSchema, bufferSize int64, storageConfig *indexpb.StorageConfig, pluginContext *indexcgopb.StoragePluginContext) (importV3PackedRecordWriter, error) {
	fields := typeutil.GetAllFieldSchemas(schema)
	columns := make([]int, len(fields))
	fieldIDs := make([]int64, len(fields))
	for index, field := range fields {
		columns[index], fieldIDs[index] = index, field.GetFieldID()
	}
	return storage.NewPackedRecordWriterWithFieldIDNames(bucketName, paths, schema, bufferSize, packed.DefaultMultiPartUploadSize,
		[]storagecommon.ColumnGroup{{GroupID: 0, Columns: columns, Fields: fieldIDs}}, storageConfig, pluginContext)
}

func executeImportPlan(ctx context.Context, cm storage.ChunkManager, req *datapb.ImportTaskV3Request, plan *datapb.ImportTaskPlan, pluginContext *indexcgopb.StoragePluginContext) ([]*datapb.SegmentResult, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "ImportV3-Import",
		trace.WithAttributes(
			attribute.Int64("job_id", req.GetJobId()),
			attribute.Int64("task_id", req.GetTaskId()),
			attribute.Int64("run_id", req.GetRunId()),
			attribute.Int64("segment_id", req.GetSegmentId()),
		))
	defer span.End()
	temporarySchema := plan.GetTempSchema()
	targetSchema := plan.GetSchema()
	sortFields, err := SortFields(plan.GetSort(), temporarySchema)
	if err != nil {
		return nil, err
	}
	logAllocator := allocator.NewLocalAllocator(req.GetLogRange().GetBegin(), req.GetLogRange().GetEnd())
	writerSpec := plan.GetWriter()
	segmentID := req.GetSegmentId()
	// The final segment writer must encrypt with the target collection's zone,
	// never the read/source context (which for a backup import is the source
	// cluster's zone). Passing no plugin context lets the writer derive the
	// target zone from the schema properties, matching the V2 import path. The
	// read paths below keep pluginContext so backup fragments decrypt correctly.
	writerOptions, err := buildImportV3WriterOptions(req.GetStorageConfig(), plan.GetCollectionId(), plan.GetPartitionId(), targetSchema, writerSpec)
	if err != nil {
		return nil, err
	}
	// The packed/manifest writer uploads its stats blobs (PK stats, BM25) through
	// the BlobsWriter callback, so it needs an uploader even for StorageV2/V3;
	// without it the writer dereferences a nil callback on the first stats write.
	writerOptions = append(writerOptions, storage.WithUploader(flushio.NewBinlogIO(cm).Upload))
	sources := make([]Source, 0, len(plan.GetFragments()))
	for _, ref := range plan.GetFragments() {
		source, err := SourceFromFragment(ref, temporarySchema, paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64(), req.GetStorageConfig(), pluginContext)
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
		writer, err = storage.NewBinlogRecordWriter(ctx, plan.GetCollectionId(), plan.GetPartitionId(), segmentID, targetSchema, logAllocator, uint64(paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsInt64()), plan.GetRows(), writerOptions...)
		return writer, err
	}
	predicate := NewTTLOnlyPredicate(temporarySchema, writerSpec.GetTtlNanos(), plan.GetDataTs())
	executor := &MergeExecutor{
		FanIn: int(plan.GetFanIn()), BatchSize: uint64(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()),
		Schema: temporarySchema, SortFields: sortFields, Predicate: predicate,
		FinalWriter: func(output storage.RecordWriter) storage.RecordWriter {
			return newImportV3FinalWriter(ctx, output, temporarySchema, targetSchema, plan.GetDataTs(), plan.GetBackup())
		},
		Intermediate: newImportV3IntermediateFactory(req, 0, temporarySchema),
	}
	// The merge intermediates live on local disk; drop them with the task.
	defer func() {
		_ = os.RemoveAll(importV3MergeLocalRunDir(req.GetJobId(), req.GetTaskId(), req.GetRunId()))
	}()
	rows, err := executor.Execute(ctx, sources, finalWriter)
	if err != nil {
		return nil, err
	}
	segmentResult := &datapb.SegmentResult{Rows: rows}
	if writer != nil {
		fieldBinlogs, statsLog, bm25Logs, manifestPath, expiration := writer.GetLogs()
		insertLogs := storage.SortFieldBinlogs(fieldBinlogs)
		sortedBM25Logs := storage.SortFieldBinlogs(bm25Logs)
		statistics := storage.BuildStatsFromFieldBinlogs(insertLogs, nil, sortedBM25Logs, nil)
		statistics.StatsBinlogSize = writer.GetStatsBlobSize()
		segmentResult.Statistics = statistics
		segmentResult.ExpirationQuantiles = expiration
		if manifestPath != "" {
			// Storage V3: only the manifest path crosses the RPC boundary. The
			// coordinator persists SegmentInfo from the manifest/stats, not from
			// FieldBinlog arrays.
			segmentResult.ManifestPath = manifestPath
		} else {
			segmentResult.InsertLogs = insertLogs
			segmentResult.PkLog = statsLog
			segmentResult.Bm25Logs = sortedBM25Logs
		}
		if writer.GetRowNum() != rows {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 writer/result row mismatch: writer=%d result=%d", writer.GetRowNum(), rows)
		}
	} else if rows != 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 kept rows without materializing a writer: %d", rows)
	}
	return []*datapb.SegmentResult{segmentResult}, nil
}

// buildImportV3WriterOptions builds the options for the final segment writer.
// It deliberately takes no plugin context: the writer must encrypt with the
// target collection's encryption zone, which the writer derives from the schema
// properties (see storage.NewBinlogRecordWriter). Feeding the read/source
// context here would encrypt target data with a foreign zone's key.
func buildImportV3WriterOptions(storageConfig *indexpb.StorageConfig, collectionID, partitionID int64, targetSchema *schemapb.CollectionSchema, spec *datapb.WriterSpec) ([]storage.RwOption, error) {
	bfType := bloomfilter.BFTypeFromString(spec.GetBloomType())
	if (bfType != bloomfilter.BasicBF && bfType != bloomfilter.BlockedBF) || spec.GetBloomFpp() <= 0 || spec.GetBloomFpp() >= 1 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec Bloom filter config is invalid")
	}
	if spec.GetFormat() == "" {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec writer format is empty")
	}
	groups, err := importV3Groups(targetSchema, spec.GetGroups())
	if err != nil {
		return nil, err
	}
	bufferSize := int64(packed.DefaultWriteBufferSize)
	multipartSize := int64(packed.DefaultMultiPartUploadSize)
	if spec.GetV2() != nil {
		if spec.GetV2().GetBufferSize() <= 0 || spec.GetV2().GetMultipartSize() <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec V2 IO sizes must be positive")
		}
		bufferSize = spec.GetV2().GetBufferSize()
		multipartSize = spec.GetV2().GetMultipartSize()
	}
	options := []storage.RwOption{
		storage.WithVersion(spec.GetStorageVersion()),
		storage.WithBufferSize(bufferSize),
		storage.WithMultiPartUploadSize(multipartSize),
		storage.WithColumnGroups(groups),
		storage.WithStorageConfig(storageConfig),
		storage.WithWriterFormat(spec.GetFormat()),
		storage.WithPkStatsConfig(storage.PkStatsConfig{
			Capacity: spec.GetPkCapacity(), BloomFilterType: spec.GetBloomType(), MaxBloomFalsePositive: spec.GetBloomFpp(),
		}),
	}
	if len(spec.GetText()) > 0 {
		partitionBase := path.Join(storageConfig.GetRootPath(), common.SegmentInsertLogPath, strconv.FormatInt(collectionID, 10), strconv.FormatInt(partitionID, 10))
		textConfigs := make([]packed.TextColumnConfig, 0, len(spec.GetText()))
		for _, text := range spec.GetText() {
			if text == nil || text.GetFieldId() < common.StartOfUserFieldID || text.GetInlineLimit() < 0 || text.GetLobLimit() <= 0 || text.GetFlushLimit() <= 0 {
				return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec TEXT config is invalid")
			}
			textConfigs = append(textConfigs, packed.TextColumnConfig{
				FieldID: text.GetFieldId(), LobBasePath: path.Join(partitionBase, "lobs", strconv.FormatInt(text.GetFieldId(), 10)),
				InlineThreshold: text.GetInlineLimit(), MaxLobFileBytes: text.GetLobLimit(), FlushThresholdBytes: text.GetFlushLimit(),
			})
		}
		options = append(options, storage.WithTextColumnConfigs(textConfigs))
	}
	return options, nil
}

func importV3Groups(schema *schemapb.CollectionSchema, specs []*datapb.ColumnGroupSpec) ([]storagecommon.ColumnGroup, error) {
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
		if spec == nil || len(spec.GetFields()) == 0 || spec.GetFormat() == "" {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column group is incomplete")
		}
		group := storagecommon.ColumnGroup{GroupID: spec.GetId(), Format: spec.GetFormat()}
		for _, fieldID := range spec.GetFields() {
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
	backup          bool
	// requiredFields is the immutable set of temporary-schema field IDs that
	// RecordToInsertData must find in every merged record. The temporary schema
	// never changes for the lifetime of the writer, so building the set per
	// batch only repeated an allocation and a map insert loop on the hottest
	// path of the import stage.
	requiredFields typeutil.Set[int64]
}

func newImportV3FinalWriter(ctx context.Context, output storage.RecordWriter, temporarySchema, targetSchema *schemapb.CollectionSchema, dataTS uint64, backup bool) storage.RecordWriter {
	required := typeutil.NewSet[int64]()
	for _, field := range typeutil.GetAllFieldSchemas(temporarySchema) {
		required.Insert(field.GetFieldID())
	}
	return &importV3FinalWriter{ctx: ctx, output: output, temporarySchema: temporarySchema, targetSchema: targetSchema, dataTS: dataTS, backup: backup, requiredFields: required}
}

// Write performs the final transform: every function output column is already
// physically present in the fragments (Reshard computed it for ordinary
// imports, the source binlog carries it for backups), so the only remaining
// work is materializing the timestamp column for ordinary imports and handing
// the record to the formal writer.
func (w *importV3FinalWriter) Write(record storage.Record) error {
	data, err := storage.RecordToInsertData(record, w.targetSchema, w.requiredFields)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "convert ImportTaskV3 merged record")
	}
	rows := data.GetRowNum()
	if rows == 0 {
		return nil
	}
	ts := data.Data[common.TimeStampField]
	if w.backup {
		// Backup fragments carry the source binlog timestamp; the final merge must
		// not overwrite it with the import data timestamp.
		if ts == nil {
			return merr.WrapErrDataIntegrityMsg("ImportTaskV3 backup timestamp is missing")
		}
		if ts.RowNum() != rows {
			return merr.WrapErrDataIntegrityMsg("ImportTaskV3 backup timestamp rows mismatch: timestamps=%d rows=%d", ts.RowNum(), rows)
		}
	} else if ts == nil || ts.RowNum() == 0 {
		timestamps := make([]int64, rows)
		for index := range timestamps {
			timestamps[index] = int64(w.dataTS)
		}
		data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: timestamps}
	} else if ts.RowNum() != rows {
		return merr.WrapErrDataIntegrityMsg("ImportTaskV3 source timestamp rows mismatch: timestamps=%d rows=%d", ts.RowNum(), rows)
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

// newImportV3IntermediateFactory writes each hierarchical merge round's
// intermediate product to the node-local merge directory. The next round reads
// it back from there, so the doubled IO of a bounded-fan-in merge never touches
// the object store; executeImportPlan removes the directory when the task ends.
func newImportV3IntermediateFactory(req *datapb.ImportTaskV3Request, segmentIndex int, schema *schemapb.CollectionSchema) IntermediateWriterFactory {
	mergeDir := importV3MergeLocalDir(req.GetJobId(), req.GetTaskId(), req.GetRunId(), segmentIndex)
	return func(_ context.Context, round, group int) (storage.RecordWriter, func(int64) (Source, error), error) {
		intermediatePath := filepath.Join(mergeDir, fmt.Sprintf("%d_%d.arrow", round, group))
		writer, err := newLocalFragmentWriter(intermediatePath, schema)
		if err != nil {
			return nil, nil, err
		}
		commit := func(rows int64) (Source, error) {
			if rows <= 0 || writer.GetWrittenRowNum() != rows {
				return Source{}, merr.WrapErrDataIntegrityMsg("ImportTaskV3 intermediate rows mismatch: writer=%d merge=%d", writer.GetWrittenRowNum(), rows)
			}
			return Source{
				ID: intermediatePath, Rows: rows,
				Open: func(_ context.Context) (storage.RecordReader, error) {
					return newLocalFragmentReader(intermediatePath, schema)
				},
			}, nil
		}
		return writer, commit, nil
	}
}
