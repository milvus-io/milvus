package compactor

import (
	"context"
	"fmt"
	sio "io"
	"path"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
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
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/samber/lo"
)

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

// preCompact validates the compaction plan, checks context, and sets up the function runner
func (t *backfillCompactionTask) preCompact() error {
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		return t.ctx.Err()
	}

	// Check segment binlogs: must have exactly one segment
	if len(t.plan.GetSegmentBinlogs()) != 1 {
		return errors.Newf("backfill compaction plan is illegal, must have exactly one segment, but got %d segments, planID = %d", len(t.plan.GetSegmentBinlogs()), t.GetPlanID())
	}

	segment := t.plan.GetSegmentBinlogs()[0]
	if len(segment.GetFieldBinlogs()) == 0 {
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

	// Initialize chunk manager for stats writing
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(paramtable.Get())
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(t.ctx)
	if err != nil {
		functionRunner.Close()
		return err
	}

	t.functionRunner = functionRunner
	t.chunkManager = cli
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
		if inputField.GetDataType() != schemapb.DataType_VarChar {
			return errors.New("input field data type must be varchar for bm25 function backfill")
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
	if err := binlog.DecompressBinLogWithRootPath(t.compactionParams.StorageConfig.GetRootPath(),
		storage.InsertBinlog, collectionID, partitionID,
		segmentID, segment.GetFieldBinlogs()); err != nil {
		log.Ctx(t.ctx).Warn("Decompress insert binlog error", zap.Error(err))
		return nil, err
	}
	inputSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		inputField,
	}}
	rOption := []storage.RwOption{
		storage.WithVersion(segment.GetStorageVersion()),
		storage.WithDownloader(t.chunkManager.MultiRead),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	}
	r, err := storage.NewBinlogRecordReader(t.ctx, segment.GetFieldBinlogs(), inputSchema, rOption...)
	if err != nil {
		return nil, err
	}
	return r, nil
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

func (t *backfillCompactionTask) setupWriter(outputField *schemapb.FieldSchema, outputFieldID int64, segment *datapb.CompactionSegmentBinlogs, collectionID, partitionID, segmentID int64) (*packed.PackedWriter, *arrow.Schema, *schemapb.CollectionSchema, []storagecommon.ColumnGroup, []string, []string, error) {
	outputSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		outputField,
	}}
	arrowSchema, err := storage.ConvertToArrowSchema(outputSchema, false)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	if len(segment.GetFieldBinlogs()) == 0 {
		return nil, nil, nil, nil, nil, nil, errors.New("segment field binlogs is empty, wrong state for compaction segments")
	}
	newColumnGroups := []storagecommon.ColumnGroup{
		{
			GroupID: outputFieldID,
			Columns: []int{0},
			Fields:  []int64{outputFieldID},
		},
	}
	logIdStart, _, err := t.logIDAlloc.Alloc(uint32(len(newColumnGroups)))
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	paths := []string{}
	for _, columnGroup := range newColumnGroups {
		path := metautil.BuildInsertLogPath(t.compactionParams.StorageConfig.GetRootPath(), collectionID, partitionID, segmentID, columnGroup.GroupID, logIdStart)
		paths = append(paths, path)
		logIdStart++
	}
	truePaths := lo.Map(paths, func(p string, _ int) string {
		if t.compactionParams.StorageConfig.GetStorageType() == "local" {
			return p
		}
		return path.Join(t.compactionParams.StorageConfig.GetBucketName(), p)
	})

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
	writer, err := packed.NewPackedWriter(truePaths, arrowSchema, packed.DefaultWriteBufferSize, packed.DefaultMultiPartUploadSize, newColumnGroups, t.compactionParams.StorageConfig, pluginContext)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	return writer, arrowSchema, outputSchema, newColumnGroups, paths, truePaths, nil
}

func (t *backfillCompactionTask) writeBatch(writer *packed.PackedWriter, arrowSchema *arrow.Schema, insertData *storage.InsertData, outputSchema *schemapb.CollectionSchema) error {
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

func (t *backfillCompactionTask) updateStats(stats *storage.BM25Stats, collectionID, partitionID, segmentID, outputFieldID int64) ([]byte, string, error) {
	cli := t.chunkManager
	statsID, _, err := t.logIDAlloc.Alloc(uint32(1))
	if err != nil {
		return nil, "", err
	}
	statsPath := metautil.JoinIDPath(collectionID, partitionID, segmentID, outputFieldID, statsID)
	bm25LogPath := path.Join(cli.RootPath(), common.SegmentBm25LogPath, statsPath)

	bytes, err := stats.Serialize()
	if err != nil {
		return nil, "", err
	}
	err = cli.Write(t.ctx, bm25LogPath, bytes)
	if err != nil {
		return nil, "", err
	}

	// Build bm25LogPathForResult
	bm25LogPathForResult := metautil.BuildBm25LogPath(
		t.compactionParams.StorageConfig.GetRootPath(),
		collectionID, partitionID, segmentID, outputFieldID, statsID)
	// For non-local storage, prepend bucket name to match the truePaths pattern
	if t.compactionParams.StorageConfig.GetStorageType() != "local" {
		bm25LogPathForResult = path.Join(t.compactionParams.StorageConfig.GetBucketName(), bm25LogPathForResult)
	}

	return bytes, bm25LogPathForResult, nil
}

func (t *backfillCompactionTask) buildMergedLogs(segment *datapb.CompactionSegmentBinlogs, newColumnGroups []storagecommon.ColumnGroup, paths, truePaths []string, sparseFieldMemorySize int, totalRows int64, bytes []byte, bm25LogPathForResult string, outputFieldID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	// build new InsertLogs from writer
	newInsertLogs := make(map[int64]*datapb.FieldBinlog)
	for i, columnGroup := range newColumnGroups {
		truePath := truePaths[i]
		fileSize, err := packed.GetFileSize(truePath, t.compactionParams.StorageConfig)
		if err != nil {
			return nil, nil, err
		}
		// Use the path without bucket prefix for LogPath
		logPath := paths[i]
		if t.compactionParams.StorageConfig.GetStorageType() != "local" {
			logPath = truePath
		}
		fieldBinlog := &datapb.FieldBinlog{
			FieldID:     columnGroup.GroupID,
			ChildFields: columnGroup.Fields,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:    fileSize,
					MemorySize: int64(sparseFieldMemorySize), // For packed format, compressed size equals memory size
					LogPath:    logPath,
					EntriesNum: totalRows,
				},
			},
		}
		newInsertLogs[columnGroup.GroupID] = fieldBinlog
	}

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
	mergedBm25Logs := newBm25Logs

	return mergedInsertLogs, mergedBm25Logs, nil
}

func (t *backfillCompactionTask) runBm25Function(ctx context.Context, functionRunner function.FunctionRunner) (*datapb.CompactionPlanResult, error) {
	//1. set up function schema (validation already done in checkFunctionRunner)
	functionSchema := functionRunner.GetSchema()
	outputFieldID := functionSchema.GetOutputFieldIds()[0]
	inputFieldID := functionSchema.GetInputFieldIds()[0]
	outputField := typeutil.GetField(t.plan.GetSchema(), outputFieldID)

	//2. get segment info
	segment := t.plan.GetSegmentBinlogs()[0]
	collectionID := segment.GetCollectionID()
	partitionID := segment.GetPartitionID()
	segmentID := segment.GetSegmentID()

	// Track durations for each heavy operation
	var readDuration, computeDuration, writeDuration, updateStatsDuration time.Duration

	//3. open binlog reader
	reader, err := t.openBinlogReader(functionRunner)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	//4. set up writer
	writer, arrowSchema, outputSchema, newColumnGroups, paths, truePaths, err := t.setupWriter(outputField, outputFieldID, segment, collectionID, partitionID, segmentID)
	if err != nil {
		return nil, err
	}
	writerClosed := false
	defer func() {
		if !writerClosed {
			writer.Close()
		}
	}()

	//5. batch loop: read → compute → write → accumulate stats
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
		recordStr, ok := record.Column(inputFieldID).(*array.String)
		if !ok {
			span.End()
			return nil, errors.New("input field data type must be varchar for bm25 function backfill")
		}
		batchStrs := make([]string, record.Len())
		for i := 0; i < record.Len(); i++ {
			batchStrs[i] = recordStr.Value(i)
		}

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
		if err := t.writeBatch(writer, arrowSchema, insertData, outputSchema); err != nil {
			span.End()
			return nil, err
		}
		writeDuration += time.Since(writeStart)

		totalRows += int64(len(batchStrs))
		totalSparseMemorySize += batchMemSize
	}
	span.End()

	// close writer after all batches
	writerClosed = true // prevent double-close in defer regardless of Close() result
	if err := writer.Close(); err != nil {
		return nil, err
	}

	//6. update stats (IO operation: write stats file)
	_, span2 := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BackfillCompact.updateStats")
	startTime := time.Now()
	bytes, bm25LogPathForResult, err := t.updateStats(stats, collectionID, partitionID, segmentID, outputFieldID)
	updateStatsDuration = time.Since(startTime)
	span2.End()
	if err != nil {
		return nil, err
	}

	//7. build merged logs
	mergedInsertLogs, mergedBm25Logs, err := t.buildMergedLogs(segment, newColumnGroups, paths, truePaths, totalSparseMemorySize, totalRows, bytes, bm25LogPathForResult, outputFieldID)
	if err != nil {
		return nil, err
	}

	//8. return compaction result
	ret := &datapb.CompactionPlanResult{
		PlanID: t.plan.GetPlanID(),
		State:  datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID:           segmentID,
				NumOfRows:           totalRows,
				InsertLogs:          mergedInsertLogs,
				Bm25Logs:            mergedBm25Logs,
				Field2StatslogPaths: segment.GetField2StatslogPaths(),
				Deltalogs:           segment.GetDeltalogs(),
				Channel:             segment.GetInsertChannel(),
				StorageVersion:      segment.GetStorageVersion(),
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

func NewBackfillCompactionTask(ctx context.Context, plan *datapb.CompactionPlan, compactionParams compaction.Params) *backfillCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &backfillCompactionTask{
		ctx:              ctx,
		cancel:           cancel,
		plan:             plan,
		compactionParams: compactionParams,
		done:             make(chan struct{}, 1),
		logIDAlloc:       allocator.NewLocalAllocator(plan.GetPreAllocatedLogIDs().GetBegin(), plan.GetPreAllocatedLogIDs().GetEnd()),
	}
}
