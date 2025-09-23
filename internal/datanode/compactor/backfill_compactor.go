package compactor

import (
	"context"
	"fmt"
	"path"
	"time"

	sio "io"

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

func (t *backfillCompactionTask) getInputData(functionRunner function.FunctionRunner) ([]string, error) {
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
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	}
	r, err := storage.NewBinlogRecordReader(t.ctx, segment.GetFieldBinlogs(), inputSchema, rOption...)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	inputStrs := make([]string, 0, t.plan.GetTotalRows())
	for {
		record, err := r.Next()
		if err != nil {
			if err == sio.EOF {
				break
			}
			return nil, err
		}
		recordStr, ok := record.Column(inputFieldID).(*array.String)
		if !ok {
			return nil, errors.New("input field data type must be varchar for bm25 function backfill")
		}
		for i := 0; i < record.Len(); i++ {
			inputStrs = append(inputStrs, recordStr.Value(i))
		}
	}
	return inputStrs, nil
}

func (t *backfillCompactionTask) executeBM25Function(functionRunner function.FunctionRunner, inputStrs []string) (*storage.InsertData, int, error) {
	functionSchema := functionRunner.GetSchema()
	outputFieldID := functionSchema.GetOutputFieldIds()[0]

	// run function
	output, err := functionRunner.BatchRun(inputStrs)
	if err != nil {
		return nil, 0, err
	}
	if len(output) != 1 {
		return nil, 0, errors.New("bm25 function backfill should return exactly one output")
	}
	outputSparseArray := output[0].(*schemapb.SparseFloatArray)

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
	oldColumnGroups := storage.RecoverColumnGroup(segment.GetFieldBinlogs())
	if len(oldColumnGroups) == 0 {
		return nil, nil, nil, nil, nil, nil, errors.New("old column groups is empty, Wrong state for compaction segments")
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
		ez := hookutil.GetEzByCollProperties(outputSchema.GetProperties(), collectionID)
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

func (t *backfillCompactionTask) writeRecord(writer *packed.PackedWriter, arrowSchema *arrow.Schema, insertData *storage.InsertData, outputSchema *schemapb.CollectionSchema) error {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	err := storage.BuildRecord(builder, insertData, outputSchema)
	if err != nil {
		return err
	}
	arrowRecord := builder.NewRecord()
	defer arrowRecord.Release()
	err = writer.WriteRecordBatch(arrowRecord)
	if err != nil {
		return err
	}
	return writer.Close()
}

func (t *backfillCompactionTask) updateStats(stats *storage.BM25Stats, collectionID, partitionID, segmentID, outputFieldID int64) ([]byte, string, error) {
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(paramtable.Get())
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(t.ctx)
	if err != nil {
		return nil, "", err
	}
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

func (t *backfillCompactionTask) buildMergedLogs(segment *datapb.CompactionSegmentBinlogs, newColumnGroups []storagecommon.ColumnGroup, paths, truePaths []string, sparseFieldMemorySize int, inputStrs []string, bytes []byte, bm25LogPathForResult string, outputFieldID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
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
					EntriesNum: int64(len(inputStrs)),
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
					EntriesNum: int64(len(inputStrs)),
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
	outputField := typeutil.GetField(t.plan.GetSchema(), outputFieldID)

	//2. get segment info
	segment := t.plan.GetSegmentBinlogs()[0]
	collectionID := segment.GetCollectionID()
	partitionID := segment.GetPartitionID()
	segmentID := segment.GetSegmentID()

	// Track durations for each heavy operation
	var getInputDataDuration, executeBM25Duration, writeRecordDuration, updateStatsDuration time.Duration

	//3. get input data (IO operation: read binlog)
	_, span1 := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BackfillCompact.getInputData")
	startTime := time.Now()
	inputStrs, err := t.getInputData(functionRunner)
	getInputDataDuration = time.Since(startTime)
	span1.End()
	if err != nil {
		return nil, err
	}

	//4. execute BM25 function (heavy computation operation)
	_, span2 := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BackfillCompact.executeBM25Function")
	startTime = time.Now()
	insertData, sparseFieldMemorySize, err := t.executeBM25Function(functionRunner, inputStrs)
	executeBM25Duration = time.Since(startTime)
	span2.End()
	if err != nil {
		return nil, err
	}
	outputFieldData := insertData.Data[outputFieldID].(*storage.SparseFloatVectorFieldData)
	stats := storage.NewBM25Stats()
	stats.AppendBytes(outputFieldData.SparseFloatArray.GetContents()...)

	//5. set up writer
	writer, arrowSchema, outputSchema, newColumnGroups, paths, truePaths, err := t.setupWriter(outputField, outputFieldID, segment, collectionID, partitionID, segmentID)
	if err != nil {
		return nil, err
	}

	//6. build record and write back (IO operation: write file)
	_, span3 := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BackfillCompact.writeRecord")
	startTime = time.Now()
	err = t.writeRecord(writer, arrowSchema, insertData, outputSchema)
	writeRecordDuration = time.Since(startTime)
	span3.End()
	if err != nil {
		return nil, err
	}

	//７. update stats (IO operation: write stats file)
	_, span4 := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BackfillCompact.updateStats")
	startTime = time.Now()
	bytes, bm25LogPathForResult, err := t.updateStats(stats, collectionID, partitionID, segmentID, outputFieldID)
	updateStatsDuration = time.Since(startTime)
	span4.End()
	if err != nil {
		return nil, err
	}

	//8. build merged logs
	mergedInsertLogs, mergedBm25Logs, err := t.buildMergedLogs(segment, newColumnGroups, paths, truePaths, sparseFieldMemorySize, inputStrs, bytes, bm25LogPathForResult, outputFieldID)
	if err != nil {
		return nil, err
	}

	//9. return compaction result
	ret := &datapb.CompactionPlanResult{
		PlanID: t.plan.GetPlanID(),
		State:  datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID:           segmentID,
				NumOfRows:           int64(len(inputStrs)),
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
	// Log the final compaction result with duration for each heavy operation
	log.Ctx(ctx).Info("backfill compaction result",
		zap.Int64("segmentID", segmentID),
		zap.Int64("numOfRows", int64(len(inputStrs))),
		zap.Int("insertLogsCount", len(mergedInsertLogs)),
		zap.Any("insertLogs", mergedInsertLogs),
		zap.Int("bm25LogsCount", len(mergedBm25Logs)),
		zap.Any("bm25Logs", mergedBm25Logs),
		zap.Duration("getInputDataDuration", getInputDataDuration),
		zap.Duration("executeBM25Duration", executeBM25Duration),
		zap.Duration("writeRecordDuration", writeRecordDuration),
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
