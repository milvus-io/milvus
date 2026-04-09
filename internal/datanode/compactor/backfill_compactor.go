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

package compactor

import (
	"context"
	"fmt"
	sio "io"
	"path"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// backfillWriter abstracts the packed writer interface for V2/V3 compatibility.
// Both packed.PackedWriter (V2) and packed.FFIPackedWriter (V3) implement WriteRecordBatch,
// but their Close signatures differ, so we use wrappers for both.
// Manifest() returns the V3 manifest path after Close(); V2 always returns "".
type backfillWriter interface {
	WriteRecordBatch(arrow.Record) error
	Close() error
	Manifest() string
}

// ffiWriterWrapper wraps packed.FFIPackedWriter to implement backfillWriter,
// capturing the manifest string returned by Close.
type ffiWriterWrapper struct {
	writer   *packed.FFIPackedWriter
	manifest string
}

func (w *ffiWriterWrapper) WriteRecordBatch(r arrow.Record) error {
	return w.writer.WriteRecordBatch(r)
}

func (w *ffiWriterWrapper) Close() error {
	manifest, err := w.writer.Close()
	if err != nil {
		return err
	}
	w.manifest = manifest
	return nil
}

func (w *ffiWriterWrapper) Manifest() string {
	return w.manifest
}

// v2WriterWrapper wraps packed.PackedWriter to implement backfillWriter.
// Manifest() always returns "" for V2 segments (no manifest concept).
// fileSizes is populated by Close() via CloseAndTell, indexed by column group order.
type v2WriterWrapper struct {
	writer    *packed.PackedWriter
	numGroups int
	fileSizes []int64
}

func (w *v2WriterWrapper) WriteRecordBatch(r arrow.Record) error {
	return w.writer.WriteRecordBatch(r)
}

func (w *v2WriterWrapper) Close() error {
	sizes, err := w.writer.CloseAndTell(w.numGroups)
	if err != nil {
		return err
	}
	w.fileSizes = sizes
	return nil
}

func (w *v2WriterWrapper) Manifest() string {
	return ""
}

type backfillCompactionTask struct {
	ctx              context.Context
	cancel           context.CancelFunc
	plan             *datapb.CompactionPlan
	compactionParams compaction.Params
	done             chan struct{}
	logIDAlloc       allocator.Interface
	functionRunner   function.FunctionRunner
	chunkManager     storage.ChunkManager
}

func (t *backfillCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	if !funcutil.CheckCtxValid(t.ctx) {
		return nil, t.ctx.Err()
	}
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("BackfillCompact-%d", t.GetPlanID()))
	defer span.End()

	if err := t.preCompact(); err != nil {
		log.Ctx(ctx).Warn("failed to preCompact", zap.Error(err))
		return nil, err
	}
	defer t.functionRunner.Close()

	compactStart := time.Now()
	log := log.Ctx(ctx).With(
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.GetCollection()),
	)

	log.Info("backfill compact start")

	result, err := t.runBackfillFunction(ctx, t.functionRunner)
	if err != nil {
		log.Warn("backfill compact failed", zap.Error(err), zap.Duration("compact cost", time.Since(compactStart)))
		return nil, err
	}

	log.Info("backfill compact done", zap.Duration("compact cost", time.Since(compactStart)))
	return result, nil
}

// preCompact validates the compaction plan, checks context, and sets up the function runner.
func (t *backfillCompactionTask) preCompact() error {
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		return t.ctx.Err()
	}

	// Check segment binlogs: must have exactly one segment
	if len(t.plan.GetSegmentBinlogs()) != 1 {
		return errors.Newf("backfill compaction plan is illegal, must have exactly one segment, but got %d segments, planID = %d", len(t.plan.GetSegmentBinlogs()), t.GetPlanID())
	}

	segment := t.plan.GetSegmentBinlogs()[0]
	if segment.GetManifest() == "" && len(segment.GetFieldBinlogs()) == 0 {
		return errors.Newf("compaction plan is illegal, segment's field binlogs are empty, planID = %d, segmentID = %d", t.GetPlanID(), segment.GetSegmentID())
	}

	backfillFunctions := t.plan.GetFunctions()
	if len(backfillFunctions) != 1 {
		return errors.New("backfill functions should be exactly one")
	}
	backfillFunction := backfillFunctions[0]

	functionRunner, err := function.NewFunctionRunner(t.plan.GetSchema(), backfillFunction)
	if err != nil {
		return err
	}
	if functionRunner == nil {
		return errors.New("failed to set up backfill function runner")
	}

	// Validate function runner
	if err := t.checkFunctionRunner(functionRunner); err != nil {
		functionRunner.Close()
		return err
	}

	t.functionRunner = functionRunner
	return nil
}

func (t *backfillCompactionTask) checkFunctionRunner(functionRunner function.FunctionRunner) error {
	switch functionRunner.GetSchema().GetType() {
	case schemapb.FunctionType_BM25:
		functionSchema := functionRunner.GetSchema()

		// 1. Check inputFieldIDs: must have exactly one, and type must be varchar
		inputFieldIDs := functionSchema.GetInputFieldIds()
		if len(inputFieldIDs) != 1 {
			return errors.New("bm25 function should have exactly one input field")
		}
		inputFieldID := inputFieldIDs[0]
		inputField := typeutil.GetField(t.plan.GetSchema(), inputFieldID)
		if inputField == nil {
			return errors.New("input field not found in schema")
		}
		if inputField.GetDataType() != schemapb.DataType_VarChar && inputField.GetDataType() != schemapb.DataType_Text {
			return errors.New("input field data type must be varchar or text for bm25 function backfill")
		}

		// 2. Check outputFieldIDs: must have exactly one, and type must be SparseFloatVector
		outputFieldIDs := functionSchema.GetOutputFieldIds()
		if len(outputFieldIDs) != 1 {
			return errors.New("bm25 function should have exactly one output field")
		}
		outputFieldID := outputFieldIDs[0]
		outputField := typeutil.GetField(t.plan.GetSchema(), outputFieldID)
		if outputField == nil {
			return errors.New("output field not found in schema")
		}
		if outputField.GetDataType() != schemapb.DataType_SparseFloatVector {
			return errors.New("output field data type must be sparse float vector for bm25 function backfill")
		}

		return nil
	default:
		return errors.New("unsupported function type")
	}
}

func (t *backfillCompactionTask) runBackfillFunction(ctx context.Context, functionRunner function.FunctionRunner) (*datapb.CompactionPlanResult, error) {
	switch functionRunner.GetSchema().GetType() {
	case schemapb.FunctionType_BM25:
		return t.runBm25Function(ctx, functionRunner)
	default:
		return nil, errors.New("unsupported function type")
	}
}

func (t *backfillCompactionTask) openBinlogReader(functionRunner function.FunctionRunner) (storage.RecordReader, error) {
	functionSchema := functionRunner.GetSchema()
	inputFieldID := functionSchema.GetInputFieldIds()[0]
	inputField := typeutil.GetField(t.plan.GetSchema(), inputFieldID)

	segment := t.plan.GetSegmentBinlogs()[0]
	collectionID := segment.GetCollectionID()
	partitionID := segment.GetPartitionID()
	segmentID := segment.GetSegmentID()

	inputSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		inputField,
	}}

	if segment.GetManifest() != "" {
		return storage.NewManifestRecordReader(t.ctx, segment.GetManifest(), inputSchema,
			storage.WithVersion(segment.GetStorageVersion()),
			storage.WithDownloader(t.chunkManager.MultiRead),
			storage.WithStorageConfig(t.compactionParams.StorageConfig),
		)
	}

	if err := binlog.DecompressBinLogWithRootPath(t.compactionParams.StorageConfig.GetRootPath(),
		storage.InsertBinlog, collectionID, partitionID,
		segmentID, segment.GetFieldBinlogs()); err != nil {
		log.Ctx(t.ctx).Warn("Decompress insert binlog error", zap.Error(err))
		return nil, err
	}
	return storage.NewBinlogRecordReader(t.ctx, segment.GetFieldBinlogs(), inputSchema,
		storage.WithVersion(segment.GetStorageVersion()),
		storage.WithDownloader(t.chunkManager.MultiRead),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	)
}

func (t *backfillCompactionTask) processBatch(functionRunner function.FunctionRunner, inputStrs []string, outputFieldID int64) (*storage.InsertData, int, error) {
	// run function on this batch
	output, err := functionRunner.BatchRun(inputStrs)
	if err != nil {
		return nil, 0, err
	}
	if len(output) != 1 {
		return nil, 0, errors.New("bm25 function backfill should return exactly one output")
	}
	outputSparseArray, ok := output[0].(*schemapb.SparseFloatArray)
	if !ok {
		return nil, 0, errors.New("unexpected output type from BM25 function runner, expected SparseFloatArray")
	}

	// build output field data
	outputFieldData := &storage.SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Contents: outputSparseArray.GetContents(),
			Dim:      outputSparseArray.GetDim(),
		},
	}

	// build insert data
	insertData := &storage.InsertData{
		Data: make(map[int64]storage.FieldData),
	}
	insertData.Data[outputFieldID] = outputFieldData
	sparseFieldMemorySize := proto.Size(&outputFieldData.SparseFloatArray)

	return insertData, sparseFieldMemorySize, nil
}

// backfillWriterResult holds the writer and associated metadata needed after writing.
type backfillWriterResult struct {
	writer         backfillWriter
	arrowSchema    *arrow.Schema
	outputSchema   *schemapb.CollectionSchema
	columnGroups   []storagecommon.ColumnGroup
	paths          []string // V2 only: log paths for buildMergedLogs
	truePaths      []string // V2 only: true paths for log path resolution
	fileSizes      []int64  // V2 only: on-disk compressed sizes from CloseAndTell, indexed by column group order
	storageVersion int64
	basePath       string             // V3 only: parsed from existing manifest in setupWriter
	v3Stats        []packed.StatEntry // V3 only: stats to commit atomically with column groups
}

func (t *backfillCompactionTask) setupWriter(outputField *schemapb.FieldSchema, outputFieldID int64, segment *datapb.CompactionSegmentBinlogs, collectionID, partitionID, segmentID int64) (*backfillWriterResult, error) {
	outputSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		outputField,
	}}
	arrowSchema, err := storage.ConvertToArrowSchema(outputSchema, false)
	if err != nil {
		return nil, err
	}
	if segment.GetManifest() == "" && len(segment.GetFieldBinlogs()) == 0 {
		return nil, errors.New("segment field binlogs is empty, wrong state for compaction segments")
	}
	newColumnGroups := []storagecommon.ColumnGroup{
		{
			GroupID: outputFieldID,
			Columns: []int{0},
			Fields:  []int64{outputFieldID},
		},
	}

	var pluginContext *indexcgopb.StoragePluginContext
	if hookutil.IsClusterEncryptionEnabled() {
		ez := hookutil.GetEzByCollProperties(t.plan.GetSchema().GetProperties(), collectionID)
		if ez != nil {
			unsafe := hookutil.GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
			if len(unsafe) > 0 {
				pluginContext = &indexcgopb.StoragePluginContext{
					EncryptionZoneId: ez.EzID,
					CollectionId:     ez.CollectionID,
					EncryptionKey:    string(unsafe),
				}
			}
		}
	}

	// Determine effective storage version: V3 only if segment already has a manifest.
	// V2 segments on V3 clusters must stay V2 — a partial manifest (only new columns)
	// would cause segcore to ignore original binlog data, corrupting the segment.
	storageVersion := t.compactionParams.StorageVersion
	existingManifest := segment.GetManifest()
	if storageVersion == storage.StorageV3 && existingManifest == "" {
		storageVersion = storage.StorageV2 // force V2 path for V2 segment on V3 cluster
	}

	result := &backfillWriterResult{
		arrowSchema:    arrowSchema,
		outputSchema:   outputSchema,
		columnGroups:   newColumnGroups,
		storageVersion: storageVersion,
	}

	if storageVersion == storage.StorageV3 {
		// V3: extend existing manifest by appending new column groups.
		// Parse basePath and version from the segment's current manifest,
		// so the transaction reads the existing manifest and merges new columns in.
		basePath, existingVersion, err := packed.UnmarshalManifestPath(existingManifest)
		if err != nil {
			return nil, merr.WrapErrServiceInternal("failed to parse existing manifest for V3 backfill", err.Error())
		}
		ffiWriter, err := packed.NewFFIPackedWriter(basePath, existingVersion, arrowSchema, newColumnGroups, t.compactionParams.StorageConfig, pluginContext)
		if err != nil {
			return nil, err
		}
		// The output field is a new addition to the existing manifest (backfill).
		// Use AddColumnGroup semantics so Loon does not require the count to match
		// the existing groups in the manifest.
		ffiWriter.AsNewColumnGroups()
		result.writer = &ffiWriterWrapper{writer: ffiWriter}
		result.basePath = basePath
	} else {
		// V2: use PackedWriter with explicit file paths
		logIdStart, _, err := t.logIDAlloc.Alloc(uint32(len(newColumnGroups)))
		if err != nil {
			return nil, err
		}
		paths := []string{}
		for _, columnGroup := range newColumnGroups {
			p := metautil.BuildInsertLogPath(t.compactionParams.StorageConfig.GetRootPath(), collectionID, partitionID, segmentID, columnGroup.GroupID, logIdStart)
			paths = append(paths, p)
			logIdStart++
		}
		truePaths := lo.Map(paths, func(p string, _ int) string {
			if t.compactionParams.StorageConfig.GetStorageType() == "local" {
				return p
			}
			return path.Join(t.compactionParams.StorageConfig.GetBucketName(), p)
		})
		writer, err := packed.NewPackedWriter(truePaths, arrowSchema, packed.DefaultWriteBufferSize, packed.DefaultMultiPartUploadSize, newColumnGroups, t.compactionParams.StorageConfig, pluginContext)
		if err != nil {
			return nil, err
		}
		result.writer = &v2WriterWrapper{writer: writer, numGroups: len(newColumnGroups)}
		result.paths = paths
		result.truePaths = truePaths
	}

	return result, nil
}

func (t *backfillCompactionTask) writeBatch(writer backfillWriter, arrowSchema *arrow.Schema, insertData *storage.InsertData, outputSchema *schemapb.CollectionSchema) error {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	err := storage.BuildRecord(builder, insertData, outputSchema)
	if err != nil {
		return err
	}
	arrowRecord := builder.NewRecord()
	defer arrowRecord.Release()
	return writer.WriteRecordBatch(arrowRecord)
}

func (t *backfillCompactionTask) updateStats(stats *storage.BM25Stats, collectionID, partitionID, segmentID, outputFieldID int64, writerResult *backfillWriterResult) ([]byte, string, error) {
	cli := t.chunkManager
	statsID, _, err := t.logIDAlloc.Alloc(uint32(1))
	if err != nil {
		return nil, "", err
	}

	bytes, err := stats.Serialize()
	if err != nil {
		return nil, "", err
	}

	if writerResult.storageVersion == storage.StorageV3 {
		// V3: write stats file under manifest basePath/_stats/ directory.
		// Stats will be registered in manifest via AddStatsToManifest after Close().
		// basePath was already parsed from the manifest in setupWriter — reuse it here.
		basePath := writerResult.basePath
		statsRelPath := fmt.Sprintf("_stats/bm25.%d/%d", outputFieldID, statsID)
		absStatsPath := path.Join(basePath, statsRelPath)
		if err := cli.Write(t.ctx, absStatsPath, bytes); err != nil {
			return nil, "", merr.WrapErrServiceInternal("failed to write V3 BM25 stats", err.Error())
		}
		writerResult.v3Stats = append(writerResult.v3Stats, packed.StatEntry{
			Key:   fmt.Sprintf("bm25.%d", outputFieldID),
			Files: []string{absStatsPath}, // C++ converts absolute to relative at commit
			Metadata: map[string]string{
				"memory_size": strconv.FormatInt(int64(len(bytes)), 10),
			},
		})
		return bytes, "", nil // no separate bm25LogPathForResult for V3
	}

	// V2: write stats file to bm25_stats path, return path for result metadata
	statsPath := metautil.JoinIDPath(collectionID, partitionID, segmentID, outputFieldID, statsID)
	bm25LogPath := path.Join(cli.RootPath(), common.SegmentBm25LogPath, statsPath)
	if err := cli.Write(t.ctx, bm25LogPath, bytes); err != nil {
		return nil, "", err
	}

	bm25LogPathForResult := metautil.BuildBm25LogPath(
		t.compactionParams.StorageConfig.GetRootPath(),
		collectionID, partitionID, segmentID, outputFieldID, statsID)
	if t.compactionParams.StorageConfig.GetStorageType() != "local" {
		bm25LogPathForResult = path.Join(t.compactionParams.StorageConfig.GetBucketName(), bm25LogPathForResult)
	}

	return bytes, bm25LogPathForResult, nil
}

func (t *backfillCompactionTask) buildMergedLogsV2(segment *datapb.CompactionSegmentBinlogs, writerResult *backfillWriterResult, sparseFieldMemorySize int, totalRows int64, bytes []byte, bm25LogPathForResult string, outputFieldID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	newInsertLogs := make(map[int64]*datapb.FieldBinlog)
	for i, columnGroup := range writerResult.columnGroups {
		fileSize := writerResult.fileSizes[i]
		logPath := writerResult.paths[i]
		truePath := writerResult.truePaths[i]
		if t.compactionParams.StorageConfig.GetStorageType() != "local" {
			logPath = truePath
		}
		fieldBinlog := &datapb.FieldBinlog{
			FieldID:     columnGroup.GroupID,
			ChildFields: columnGroup.Fields,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:    fileSize,
					MemorySize: int64(sparseFieldMemorySize),
					LogPath:    logPath,
					EntriesNum: totalRows,
				},
			},
		}
		newInsertLogs[columnGroup.GroupID] = fieldBinlog
	}

	return t.finalizeMergedLogs(segment, newInsertLogs, totalRows, bytes, bm25LogPathForResult, outputFieldID)
}

func (t *backfillCompactionTask) buildMergedLogsV3(segment *datapb.CompactionSegmentBinlogs, writerResult *backfillWriterResult, sparseFieldMemorySize int, totalRows int64, bytes []byte, bm25LogPathForResult string, outputFieldID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	// V3: paths are managed by C++, use mock paths similar to PackedManifestRecordWriter.finalizeBinlogs
	newInsertLogs := make(map[int64]*datapb.FieldBinlog)
	for _, columnGroup := range writerResult.columnGroups {
		fieldBinlog := &datapb.FieldBinlog{
			FieldID:     columnGroup.GroupID,
			ChildFields: columnGroup.Fields,
			Binlogs: []*datapb.Binlog{
				{
					MemorySize: int64(sparseFieldMemorySize),
					EntriesNum: totalRows,
				},
			},
		}
		newInsertLogs[columnGroup.GroupID] = fieldBinlog
	}

	return t.finalizeMergedLogs(segment, newInsertLogs, totalRows, bytes, bm25LogPathForResult, outputFieldID)
}

func (t *backfillCompactionTask) finalizeMergedLogs(segment *datapb.CompactionSegmentBinlogs, newInsertLogs map[int64]*datapb.FieldBinlog, totalRows int64, bytes []byte, bm25LogPathForResult string, outputFieldID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	// build new Bm25Logs
	bm25FileSize := int64(len(bytes))
	newBm25Logs := []*datapb.FieldBinlog{
		{
			FieldID: outputFieldID,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:    bm25FileSize,
					MemorySize: bm25FileSize,
					LogPath:    bm25LogPathForResult,
					EntriesNum: totalRows,
				},
			},
		},
	}

	// merge with original segment's binlogs
	originalInsertLogs := segment.GetFieldBinlogs()
	mergedInsertLogs := make([]*datapb.FieldBinlog, 0, len(originalInsertLogs)+len(newInsertLogs))
	mergedInsertLogs = append(mergedInsertLogs, originalInsertLogs...)
	newInsertLogsList := storage.SortFieldBinlogs(newInsertLogs)
	mergedInsertLogs = append(mergedInsertLogs, newInsertLogsList...)

	return mergedInsertLogs, newBm25Logs, nil
}

func (t *backfillCompactionTask) runBm25Function(ctx context.Context, functionRunner function.FunctionRunner) (*datapb.CompactionPlanResult, error) {
	// 1. set up function schema (validation already done in checkFunctionRunner)
	functionSchema := functionRunner.GetSchema()
	outputFieldID := functionSchema.GetOutputFieldIds()[0]
	inputFieldID := functionSchema.GetInputFieldIds()[0]
	outputField := typeutil.GetField(t.plan.GetSchema(), outputFieldID)

	// 2. get segment info
	segment := t.plan.GetSegmentBinlogs()[0]
	collectionID := segment.GetCollectionID()
	partitionID := segment.GetPartitionID()
	segmentID := segment.GetSegmentID()

	// Track durations for each heavy operation
	var readDuration, computeDuration, writeDuration, updateStatsDuration time.Duration

	// 3. open binlog reader
	reader, err := t.openBinlogReader(functionRunner)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 4. set up writer
	writerResult, err := t.setupWriter(outputField, outputFieldID, segment, collectionID, partitionID, segmentID)
	if err != nil {
		return nil, err
	}
	writerClosed := false
	defer func() {
		if !writerClosed {
			writerResult.writer.Close()
		}
	}()

	// 5. batch loop: read → compute → write → accumulate stats
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BackfillCompact.batchProcess")
	stats := storage.NewBM25Stats()
	var totalRows int64
	var totalSparseMemorySize int

	for {
		// read one batch
		readStart := time.Now()
		record, err := reader.Next()
		if err != nil {
			if err == sio.EOF {
				readDuration += time.Since(readStart)
				break
			}
			span.End()
			return nil, err
		}
		readDuration += time.Since(readStart)

		// extract input strings from this batch
		col := record.Column(inputFieldID)
		if col == nil {
			record.Release()
			span.End()
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("input field %d not found in record", inputFieldID))
		}
		recordStr, ok := col.(*array.String)
		if !ok {
			record.Release()
			span.End()
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("input field %d data type must be varchar or text for bm25 function backfill, got %T", inputFieldID, col))
		}
		batchStrs := make([]string, record.Len())
		for i := 0; i < record.Len(); i++ {
			batchStrs[i] = recordStr.Value(i)
		}
		record.Release()

		// compute BM25 for this batch
		computeStart := time.Now()
		insertData, batchMemSize, err := t.processBatch(functionRunner, batchStrs, outputFieldID)
		computeDuration += time.Since(computeStart)
		if err != nil {
			span.End()
			return nil, err
		}

		// accumulate stats from this batch
		outputFieldData := insertData.Data[outputFieldID].(*storage.SparseFloatVectorFieldData)
		stats.AppendBytes(outputFieldData.SparseFloatArray.GetContents()...)

		// write this batch
		writeStart := time.Now()
		if err := t.writeBatch(writerResult.writer, writerResult.arrowSchema, insertData, writerResult.outputSchema); err != nil {
			span.End()
			return nil, err
		}
		writeDuration += time.Since(writeStart)

		totalRows += int64(len(batchStrs))
		totalSparseMemorySize += batchMemSize
	}
	span.End()

	// 6. write stats file (must happen before Close for V3 — stats are committed atomically)
	_, span2 := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BackfillCompact.updateStats")
	startTime := time.Now()
	bytes, bm25LogPathForResult, err := t.updateStats(stats, collectionID, partitionID, segmentID, outputFieldID, writerResult)
	updateStatsDuration = time.Since(startTime)
	span2.End()
	if err != nil {
		return nil, err
	}

	// Close writer — for V3, this commits new column groups to the existing manifest
	// (version N → N+1). BM25 stats are added separately via AddStatsToManifest.
	if err := writerResult.writer.Close(); err != nil {
		return nil, err
	}
	writerClosed = true
	// For V2: capture on-disk compressed file sizes written by CloseAndTell.
	if v2w, ok := writerResult.writer.(*v2WriterWrapper); ok {
		writerResult.fileSizes = v2w.fileSizes
	}
	// Read the manifest produced by Close(); "" for V2, real path for V3.
	manifestPath := writerResult.writer.Manifest()

	// For V3: register BM25 stats in manifest (version N+1 → N+2).
	// Channel-level scheduler exclusion guarantees no concurrent manifest modification.
	if writerResult.storageVersion == storage.StorageV3 && len(writerResult.v3Stats) > 0 {
		newManifest, err := packed.AddStatsToManifest(
			manifestPath, t.compactionParams.StorageConfig, writerResult.v3Stats)
		if err != nil {
			return nil, merr.WrapErrServiceInternal("failed to add BM25 stats to V3 manifest", err.Error())
		}
		manifestPath = newManifest
	}

	// 7. build merged logs
	var mergedInsertLogs, mergedBm25Logs []*datapb.FieldBinlog
	if writerResult.storageVersion == storage.StorageV3 {
		mergedInsertLogs, mergedBm25Logs, err = t.buildMergedLogsV3(segment, writerResult, totalSparseMemorySize, totalRows, bytes, bm25LogPathForResult, outputFieldID)
	} else {
		mergedInsertLogs, mergedBm25Logs, err = t.buildMergedLogsV2(segment, writerResult, totalSparseMemorySize, totalRows, bytes, bm25LogPathForResult, outputFieldID)
	}
	if err != nil {
		return nil, err
	}

	// 8. manifest path is already set from Close() and optionally updated by AddStatsToManifest above.

	// 9. return compaction result
	// For V3: BM25 stats are embedded in the manifest (committed atomically with
	// column groups), so Bm25Logs should be nil — PackSegmentLoadInfo skips
	// Bm25Logs when ManifestPath is set, relying on StatsResolver to read from manifest.
	resultBm25Logs := mergedBm25Logs
	if writerResult.storageVersion == storage.StorageV3 {
		resultBm25Logs = nil
	}
	ret := &datapb.CompactionPlanResult{
		PlanID: t.plan.GetPlanID(),
		State:  datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID:           segmentID,
				NumOfRows:           totalRows,
				InsertLogs:          mergedInsertLogs,
				Bm25Logs:            resultBm25Logs,
				Field2StatslogPaths: segment.GetField2StatslogPaths(),
				Deltalogs:           segment.GetDeltalogs(),
				Channel:             segment.GetInsertChannel(),
				StorageVersion:      writerResult.storageVersion,
				Manifest:            manifestPath,
			},
		},
		Type: t.plan.GetType(),
	}
	log.Ctx(ctx).Info("backfill compaction result",
		zap.Int64("segmentID", segmentID),
		zap.Int64("numOfRows", totalRows),
		zap.Int("insertLogsCount", len(mergedInsertLogs)),
		zap.Int("bm25LogsCount", len(mergedBm25Logs)),
		zap.Duration("readDuration", readDuration),
		zap.Duration("computeDuration", computeDuration),
		zap.Duration("writeDuration", writeDuration),
		zap.Duration("updateStatsDuration", updateStatsDuration))
	return ret, nil
}

func (t *backfillCompactionTask) Complete() {
	if t.done != nil {
		select {
		case t.done <- struct{}{}:
		default:
		}
	}
}

func (t *backfillCompactionTask) Stop() {
	if t.cancel != nil {
		t.cancel()
	}
	if t.done != nil {
		<-t.done
	}
}

func (t *backfillCompactionTask) GetPlanID() typeutil.UniqueID {
	return t.plan.GetPlanID()
}

func (t *backfillCompactionTask) GetCollection() typeutil.UniqueID {
	// Get collection ID from the first segment binlog
	if len(t.plan.GetSegmentBinlogs()) > 0 {
		return t.plan.GetSegmentBinlogs()[0].GetCollectionID()
	}
	return 0
}

func (t *backfillCompactionTask) GetChannelName() string {
	return t.plan.GetChannel()
}

func (t *backfillCompactionTask) GetCompactionType() datapb.CompactionType {
	return t.plan.GetType()
}

func (t *backfillCompactionTask) GetSlotUsage() int64 {
	return t.plan.GetSlotUsage()
}

func (t *backfillCompactionTask) GetStorageConfig() *indexpb.StorageConfig {
	return t.compactionParams.StorageConfig
}

var _ Compactor = (*backfillCompactionTask)(nil)

func NewBackfillCompactionTask(ctx context.Context, cm storage.ChunkManager, plan *datapb.CompactionPlan, compactionParams compaction.Params) *backfillCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &backfillCompactionTask{
		ctx:              ctx,
		cancel:           cancel,
		plan:             plan,
		compactionParams: compactionParams,
		done:             make(chan struct{}, 1),
		logIDAlloc:       allocator.NewLocalAllocator(plan.GetPreAllocatedLogIDs().GetBegin(), plan.GetPreAllocatedLogIDs().GetEnd()),
		chunkManager:     cm,
	}
}
