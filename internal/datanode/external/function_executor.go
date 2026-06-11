package external

import (
	"context"
	"fmt"
	"io"
	"path"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// defaultReadBufferSize matches the buffer size used elsewhere for FFIPackedReader.
const defaultReadBufferSize = 64 * 1024 * 1024

// FunctionExecutionResult carries manifest and stats metadata produced by
// function execution for one external segment.
type FunctionExecutionResult struct {
	ManifestPath  string
	Bm25Statslogs []*datapb.FieldBinlog
}

// ExecuteFunctionsForSegment computes function-output columns for an external
// segment and returns a manifest that references both the external original
// files (for input columns) and a newly written packed file (for function
// output columns), plus coordinator-facing stats metadata for BM25 outputs.
//
// Streaming pipeline (no full-segment InsertData materialization):
//  1. Build an input manifest referencing the segment's external fragments.
//  2. Open storage.RecordReader on input + FFIPackedWriter on outputs.
//  3. For each Record batch read: convert to InsertData, run functions, append
//     BM25 stats, build output Record, write to packed file.
//  4. Close writer (commits manifest).
//  5. Serialize accumulated BM25 stats and add to manifest.
//
// Memory: peak ~ one Arrow batch (default 64 MiB) regardless of segment size.
// Input columns are never copied; segments reference the external original
// files via the same column-group layout Segcore already uses.
func ExecuteFunctionsForSegment(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	fragments []packed.Fragment,
	format string,
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
	segmentID int64,
	basePath string,
	clusterID string,
	bm25StatsLogIDs map[int64]int64,
) (*FunctionExecutionResult, error) {
	log := log.Ctx(ctx)
	log.Info("executing functions for external table segment",
		zap.Int64("segmentID", segmentID),
		zap.String("basePath", basePath),
		zap.Int("numFragments", len(fragments)),
		zap.Int("numFunctions", len(schema.GetFunctions())))

	sourceColumns := packed.GetColumnNamesFromSchema(schema)

	inputManifestPath, err := packed.CreateSegmentManifestWithBasePath(
		ctx, basePath, format, sourceColumns, fragments, storageConfig)
	if err != nil {
		return nil, merr.Wrap(err, "create input manifest")
	}
	_, inputVersion, err := packed.UnmarshalManifestPath(inputManifestPath)
	if err != nil {
		return nil, merr.Wrap(err, "parse input manifest path")
	}

	outputFields, outputSchema, err := buildOutputSchema(schema)
	if err != nil {
		return nil, err
	}
	outputArrow, err := storage.ConvertToArrowSchema(outputSchema, true)
	if err != nil {
		return nil, err
	}
	inputSchema, executionSchema, requiredInputFields, err := buildFunctionExecutionSchema(schema)
	if err != nil {
		return nil, err
	}

	reader, err := openInputReader(ctx, schema, inputManifestPath, inputSchema, storageConfig, collectionID)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	colGroups := []storagecommon.ColumnGroup{{Columns: lo.Range(len(outputFields))}}
	writer, err := packed.NewFFIPackedWriter(basePath, outputArrow, colGroups, storageConfig, nil)
	if err != nil {
		return nil, merr.Wrap(err, "open output writer")
	}
	writer.AsNewColumnGroups()

	bm25Acc := newBM25Accumulators(schema)

	totalRows, err := streamBatches(ctx, schema, executionSchema, outputSchema, outputArrow,
		requiredInputFields, reader, writer, bm25Acc, clusterID)
	if err != nil {
		return nil, err
	}

	output, err := writer.Close()
	if err != nil {
		return nil, merr.Wrap(err, "close output writer")
	}
	if output != nil {
		defer output.Destroy()
	}

	updates := &packed.ManifestUpdates{NewFiles: output}
	bm25Statslogs, err := appendBM25Stats(ctx, bm25Acc, storageConfig, basePath, updates, bm25StatsLogIDs)
	if err != nil {
		return nil, err
	}
	manifestPath, err := packed.CommitManifestUpdates(basePath, inputVersion, storageConfig, updates)
	if err != nil {
		return nil, merr.Wrap(err, "commit function output manifest")
	}

	log.Info("function execution completed",
		zap.Int64("segmentID", segmentID),
		zap.Int64("rows", totalRows),
		zap.String("manifestPath", manifestPath))
	return &FunctionExecutionResult{
		ManifestPath:  manifestPath,
		Bm25Statslogs: bm25Statslogs,
	}, nil
}

// buildOutputSchema returns the output FieldSchema list and a wrapping
// CollectionSchema used for arrow conversion. Errors if the schema declares no
// function outputs (the executor should not have been invoked at all).
func buildOutputSchema(schema *schemapb.CollectionSchema) ([]*schemapb.FieldSchema, *schemapb.CollectionSchema, error) {
	outputFields, err := functionOutputFields(schema)
	if err != nil {
		return nil, nil, err
	}
	if len(outputFields) == 0 {
		return nil, nil, merr.WrapErrServiceInternalMsg("no function output fields; executor should not have been invoked")
	}
	return outputFields, &schemapb.CollectionSchema{
		Name:   schema.GetName(),
		Fields: outputFields,
	}, nil
}

func functionOutputFields(schema *schemapb.CollectionSchema) ([]*schemapb.FieldSchema, error) {
	if schema == nil {
		return nil, nil
	}

	fieldsByID := make(map[int64]*schemapb.FieldSchema, len(schema.GetFields()))
	fieldsByName := make(map[string]*schemapb.FieldSchema, len(schema.GetFields()))
	outputIDs := make(map[int64]struct{})

	addOutputID := func(fieldID int64) {
		if fieldID == 0 {
			return
		}
		outputIDs[fieldID] = struct{}{}
	}

	for _, field := range schema.GetFields() {
		fieldsByID[field.GetFieldID()] = field
		fieldsByName[field.GetName()] = field
		if field.GetIsFunctionOutput() {
			addOutputID(field.GetFieldID())
		}
	}

	for _, fn := range schema.GetFunctions() {
		for _, fieldID := range fn.GetOutputFieldIds() {
			if fieldID == 0 {
				continue
			}
			if _, ok := fieldsByID[fieldID]; !ok {
				return nil, merr.WrapErrParameterInvalidMsg("function output field id %d not found in schema", fieldID)
			}
			addOutputID(fieldID)
		}
		for _, fieldName := range fn.GetOutputFieldNames() {
			if fieldName == "" {
				continue
			}
			field, ok := fieldsByName[fieldName]
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("function output field %s not found in schema", fieldName)
			}
			addOutputID(field.GetFieldID())
		}
	}

	outputFields := make([]*schemapb.FieldSchema, 0, len(outputIDs))
	for _, field := range schema.GetFields() {
		if _, ok := outputIDs[field.GetFieldID()]; ok {
			outputFields = append(outputFields, field)
		}
	}
	return outputFields, nil
}

func openInputReader(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	manifestPath string,
	inputSchema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
) (storage.RecordReader, error) {
	reader, err := storage.NewManifestRecordReader(ctx, manifestPath, inputSchema,
		storage.WithCollectionID(collectionID),
		storage.WithVersion(storage.StorageV3),
		storage.WithBufferSize(defaultReadBufferSize),
		storage.WithStorageConfig(storageConfig),
		storage.WithExternalReaderContext(packed.ExternalReaderContext{
			CollectionID: collectionID,
			Source:       schema.GetExternalSource(),
			Spec:         schema.GetExternalSpec(),
		}),
	)
	if err != nil {
		return nil, merr.Wrap(err, "open input manifest")
	}
	return reader, nil
}

// buildFunctionExecutionSchema returns the source schema needed for reading
// function inputs, a wider schema for InsertData conversion, and the input
// fields that must be present in each read batch. The
// wider schema includes function outputs so RunAll can fill them in-place, but
// unrelated external fields are not deserialized.
func buildFunctionExecutionSchema(
	schema *schemapb.CollectionSchema,
) (*schemapb.CollectionSchema, *schemapb.CollectionSchema, typeutil.Set[int64], error) {
	if schema == nil {
		return nil, nil, nil, merr.WrapErrParameterInvalidMsg("collection schema is nil")
	}
	inputIDs := make(map[int64]struct{})
	for _, fn := range schema.GetFunctions() {
		for _, id := range fn.GetInputFieldIds() {
			inputIDs[id] = struct{}{}
		}
	}

	outputFields, err := functionOutputFields(schema)
	if err != nil {
		return nil, nil, nil, err
	}
	outputIDs := make(map[int64]struct{}, len(outputFields))
	for _, field := range outputFields {
		outputIDs[field.GetFieldID()] = struct{}{}
	}

	inputSchema := &schemapb.CollectionSchema{
		Name:       schema.GetName(),
		DbName:     schema.GetDbName(),
		Properties: schema.GetProperties(),
	}
	executionSchema := &schemapb.CollectionSchema{
		Name:       schema.GetName(),
		DbName:     schema.GetDbName(),
		Properties: schema.GetProperties(),
	}
	seenExecutionFields := make(map[int64]struct{})
	seenInputFields := make(map[int64]struct{})
	requiredInputFields := typeutil.NewSet[int64]()

	addExecutionField := func(field *schemapb.FieldSchema) {
		if _, ok := seenExecutionFields[field.GetFieldID()]; ok {
			return
		}
		executionSchema.Fields = append(executionSchema.Fields, field)
		seenExecutionFields[field.GetFieldID()] = struct{}{}
	}

	for _, f := range schema.GetFields() {
		fieldID := f.GetFieldID()
		_, isInput := inputIDs[fieldID]
		_, isOutput := outputIDs[fieldID]
		if isInput || isOutput {
			addExecutionField(f)
		}
		if !isInput || isOutput || typeutil.IsExternalSystemOrVirtualField(f.GetName()) {
			continue
		}
		seenInputFields[fieldID] = struct{}{}
		requiredInputFields.Insert(fieldID)
		inputSchema.Fields = append(inputSchema.Fields, f)

		if schema.GetExternalSource() != "" && f.GetExternalField() == "" {
			return nil, nil, nil, merr.WrapErrParameterInvalidMsg("function input field %s has no external_field", f.GetName())
		}
	}

	for inputID := range inputIDs {
		if _, ok := seenInputFields[inputID]; !ok {
			if _, ok := seenExecutionFields[inputID]; !ok {
				return nil, nil, nil, merr.WrapErrParameterInvalidMsg("function input field id %d not found in schema", inputID)
			}
		}
	}
	for outputID := range outputIDs {
		if _, ok := seenExecutionFields[outputID]; !ok {
			return nil, nil, nil, merr.WrapErrParameterInvalidMsg("function output field id %d not found in schema", outputID)
		}
	}
	if len(inputSchema.GetFields()) == 0 {
		return nil, nil, nil, merr.WrapErrParameterInvalidMsg("no source input columns for function execution")
	}
	return inputSchema, executionSchema, requiredInputFields, nil
}

func streamBatches(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	executionSchema *schemapb.CollectionSchema,
	outputSchema *schemapb.CollectionSchema,
	outputArrow *arrow.Schema,
	requiredInputFields typeutil.Set[int64],
	reader storage.RecordReader,
	writer *packed.FFIPackedWriter,
	bm25Acc map[int64]*storage.BM25Stats,
	clusterID string,
) (int64, error) {
	var totalRows int64
	for {
		rec, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalRows, merr.Wrap(err, "read input batch")
		}
		if rec == nil {
			break
		}

		batch, err := storage.RecordToInsertData(rec, executionSchema, requiredInputFields)
		rec.Release()
		if err != nil {
			return totalRows, merr.Wrap(err, "record to InsertData")
		}
		if batch.GetRowNum() == 0 {
			continue
		}

		if err := embedding.RunAll(ctx, schema, batch, embedding.RunOptions{
			ClusterID: clusterID,
			DBName:    schema.GetDbName(),
		}); err != nil {
			return totalRows, merr.Wrap(err, "execute functions")
		}

		if err := accumulateBM25Stats(batch, bm25Acc); err != nil {
			return totalRows, err
		}

		if err := writeOutputBatch(batch, outputSchema, outputArrow, writer); err != nil {
			return totalRows, merr.Wrap(err, "write output batch")
		}
		totalRows += int64(batch.GetRowNum())
	}
	return totalRows, nil
}

func writeOutputBatch(
	batch *storage.InsertData,
	outputSchema *schemapb.CollectionSchema,
	outputArrow *arrow.Schema,
	writer *packed.FFIPackedWriter,
) error {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, outputArrow)
	defer builder.Release()
	if err := storage.BuildRecord(builder, batch, outputSchema); err != nil {
		return err
	}
	rec := builder.NewRecord()
	defer rec.Release()
	return writer.WriteRecordBatch(rec)
}

// newBM25Accumulators creates a stats accumulator per BM25 output field id.
// Returns an empty map if the schema declares no BM25 functions.
func newBM25Accumulators(schema *schemapb.CollectionSchema) map[int64]*storage.BM25Stats {
	acc := make(map[int64]*storage.BM25Stats)
	for _, outID := range bm25OutputFieldIDs(schema) {
		acc[outID] = storage.NewBM25Stats()
	}
	return acc
}

func bm25OutputFieldIDs(schema *schemapb.CollectionSchema) []int64 {
	if schema == nil {
		return nil
	}
	var outputFieldIDs []int64
	seen := make(map[int64]struct{})
	for _, fn := range schema.GetFunctions() {
		if fn.GetType() != schemapb.FunctionType_BM25 {
			continue
		}
		for _, outID := range fn.GetOutputFieldIds() {
			if _, ok := seen[outID]; ok {
				continue
			}
			seen[outID] = struct{}{}
			outputFieldIDs = append(outputFieldIDs, outID)
		}
	}
	return outputFieldIDs
}

// accumulateBM25Stats appends per-batch sparse vectors into the running
// per-field stats accumulator. avgdl/IDF need full-segment counts, so the
// accumulator persists across batches and is serialized once at the end.
func accumulateBM25Stats(batch *storage.InsertData, acc map[int64]*storage.BM25Stats) error {
	for outID, stats := range acc {
		raw, present := batch.Data[outID]
		if !present || raw == nil {
			return merr.WrapErrFunctionFailedMsg(
				"BM25 output field %d missing from batch; executeFunctions did not populate it",
				outID)
		}
		fd, ok := raw.(*storage.SparseFloatVectorFieldData)
		if !ok {
			return merr.WrapErrFunctionFailedMsg(
				"BM25 output field %d has wrong type %T (want *SparseFloatVectorFieldData)",
				outID, raw)
		}
		stats.AppendFieldData(fd)
	}
	return nil
}

func appendBM25Stats(
	ctx context.Context,
	acc map[int64]*storage.BM25Stats,
	storageConfig *indexpb.StorageConfig,
	basePath string,
	updates *packed.ManifestUpdates,
	bm25StatsLogIDs map[int64]int64,
) ([]*datapb.FieldBinlog, error) {
	if len(acc) == 0 {
		return nil, nil
	}
	log := log.Ctx(ctx)
	if updates == nil {
		return nil, merr.WrapErrServiceInternalMsg("manifest updates is nil")
	}

	entries := make([]packed.StatEntry, 0, len(acc))
	bm25Statslogs := make([]*datapb.FieldBinlog, 0, len(acc))
	for outID, stats := range acc {
		logID := bm25StatsLogIDs[outID]
		if logID == 0 {
			return nil, fmt.Errorf("bm25 stats log id not allocated for field %d", outID)
		}
		blob, err := stats.Serialize()
		if err != nil {
			return nil, merr.Wrapf(err, "serialize bm25 stats for field %d", outID)
		}
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/bm25.%d/%d", outID, logID))
		if err := packed.WriteFile(storageConfig, fullPath, blob); err != nil {
			return nil, merr.Wrapf(err, "write bm25 stats file %s", fullPath)
		}
		size := int64(len(blob))
		entries = append(entries, packed.StatEntry{
			Key:   fmt.Sprintf("bm25.%d", outID),
			Files: []string{fullPath},
			Metadata: map[string]string{
				"memory_size": strconv.FormatInt(size, 10),
			},
		})
		bm25Statslogs = append(bm25Statslogs, &datapb.FieldBinlog{
			FieldID: outID,
			Binlogs: []*datapb.Binlog{
				{
					LogID:      logID,
					LogSize:    size,
					MemorySize: size,
					EntriesNum: stats.NumRow(),
				},
			},
		})
		log.Info("registered bm25 stats",
			zap.Int64("fieldID", outID),
			zap.Int("bytes", len(blob)),
			zap.Int64("numRow", stats.NumRow()))
	}
	updates.Stats = append(updates.Stats, entries...)
	return bm25Statslogs, nil
}

// finalizeBM25Stats serializes each per-field accumulator, writes the blob
// under the packed segment stats path, and registers entries on the manifest.
// Required by QueryNode's idf_oracle for BM25 search.
//
// The stat blob write intentionally happens before manifest registration.
// A failure between WriteFile and CommitManifestUpdates can leave an
// unreferenced object under the segment base path, but it is not visible to
// readers because the manifest is the commit point. A retry rewrites the same
// deterministic path for the same field/version pair.
func finalizeBM25Stats(
	ctx context.Context,
	acc map[int64]*storage.BM25Stats,
	storageConfig *indexpb.StorageConfig,
	manifestPath string,
) (string, error) {
	if len(acc) == 0 {
		return manifestPath, nil
	}

	basePath, version, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", merr.Wrap(err, "parse manifest path")
	}

	updates := &packed.ManifestUpdates{}
	if _, err := appendBM25Stats(ctx, acc, storageConfig, basePath, updates, localBM25StatsLogIDs(acc)); err != nil {
		return "", err
	}
	return packed.CommitManifestUpdates(basePath, version, storageConfig, updates)
}

func localBM25StatsLogIDs(acc map[int64]*storage.BM25Stats) map[int64]int64 {
	logIDs := make(map[int64]int64, len(acc))
	nextLogID := int64(storage.CompoundStatsType)
	for fieldID := range acc {
		logIDs[fieldID] = nextLogID
		nextLogID++
	}
	return logIDs
}
